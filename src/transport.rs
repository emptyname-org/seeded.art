//! Connection admission and lifetime bounds, including stalled body transfers.
use axum::{
    extract::connect_info::Connected,
    serve::{IncomingStream, Listener},
};
use std::{
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::{OwnedSemaphorePermit, Semaphore},
    time::Sleep,
};

// Keep aligned with Caddy's edge write timeout. The upstream connection starts
// later, so accept + this limit is a conservative upper bound for the edge.
pub(crate) const PROXY_WRITE_TIMEOUT_SECONDS: u64 = 180;

#[derive(Clone)]
pub struct ConnectionInfo {
    pub address: SocketAddr,
    pub(crate) control: crate::delivery_control::Connection,
}
impl Connected<IncomingStream<'_, BoundedListener>> for ConnectionInfo {
    fn connect_info(stream: IncomingStream<'_, BoundedListener>) -> Self {
        Self {
            address: *stream.remote_addr(),
            control: stream.io().control.clone(),
        }
    }
}

pub struct BoundedListener {
    inner: TcpListener,
    slots: Arc<Semaphore>,
    lifetime: Duration,
    delivery_progress: bool,
}
impl BoundedListener {
    pub fn new(inner: TcpListener) -> Self {
        Self {
            inner,
            slots: Arc::new(Semaphore::new(1024)),
            lifetime: Duration::from_secs(240),
            delivery_progress: false,
        }
    }
    /// Single-request proxy connections may enforce a response progress deadline.
    pub fn with_delivery_progress(mut self) -> Self {
        self.delivery_progress = true;
        self
    }
}
impl Listener for BoundedListener {
    type Io = DeadlineIo<TcpStream>;
    type Addr = SocketAddr;
    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        // The semaphore is private and never closed.
        let permit = self
            .slots
            .clone()
            .acquire_owned()
            .await
            .expect("listener slots open");
        let (socket, address) = Listener::accept(&mut self.inner).await;
        let mut socket = DeadlineIo::new(socket, self.lifetime, permit);
        if self.delivery_progress {
            socket.progress_window = Some(Duration::from_secs(15));
            socket.control.enable_reclamation();
        }
        (socket, address)
    }
    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.inner.local_addr()
    }
}

pub struct DeadlineIo<T> {
    inner: T,
    deadline: Pin<Box<Sleep>>,
    _permit: OwnedSemaphorePermit,
    progress_window: Option<Duration>,
    progress: Option<Pin<Box<Sleep>>>,
    written: usize,
    control: crate::delivery_control::Connection,
}
impl<T> DeadlineIo<T> {
    fn new(inner: T, lifetime: Duration, permit: OwnedSemaphorePermit) -> Self {
        let deadline = tokio::time::Instant::now() + lifetime;
        let control = crate::delivery_control::Connection::new();
        control.set_deadline(deadline);
        Self {
            inner,
            deadline: Box::pin(tokio::time::sleep_until(deadline)),
            _permit: permit,
            progress_window: None,
            progress: None,
            written: 0,
            control,
        }
    }
    fn check(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.control.poll_aborted(cx) {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "stalled delivery reclaimed",
            ));
        }
        if self
            .progress
            .as_mut()
            .is_some_and(|timer| timer.as_mut().poll(cx).is_ready())
        {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "response delivery made insufficient progress",
            ));
        }
        if self.deadline.as_mut().poll(cx).is_ready() {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection lifetime exceeded",
            ))
        } else {
            Ok(())
        }
    }
}
impl<T: AsyncRead + Unpin> AsyncRead for DeadlineIo<T> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.check(cx)?;
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}
impl<T: AsyncWrite + Unpin> AsyncWrite for DeadlineIo<T> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.progress.is_none() {
            if let Some(window) = self.progress_window {
                self.progress = Some(Box::pin(tokio::time::sleep(window)));
            }
        }
        self.check(cx)?;
        let control = self.control.clone();
        let result = control.write(|| Pin::new(&mut self.inner).poll_write(cx, bytes));
        if let Poll::Ready(Ok(count)) = result {
            self.written = self.written.saturating_add(count);
            if self.written >= 64 * 1024 {
                self.written = 0;
                let window = self.progress_window;
                if let (Some(timer), Some(window)) = (&mut self.progress, window) {
                    timer.as_mut().reset(tokio::time::Instant::now() + window);
                }
            }
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check(cx)?;
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check(cx)?;
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct OneConnection(Option<DeadlineIo<tokio::io::DuplexStream>>);
    impl Listener for OneConnection {
        type Io = DeadlineIo<tokio::io::DuplexStream>;
        type Addr = SocketAddr;
        async fn accept(&mut self) -> (Self::Io, Self::Addr) {
            match self.0.take() {
                Some(io) => (io, "127.0.0.1:1".parse().unwrap()),
                None => std::future::pending().await,
            }
        }
        fn local_addr(&self) -> io::Result<Self::Addr> {
            Ok("127.0.0.1:1".parse().unwrap())
        }
    }
    /// Real Hyper server on a bounded transport; a released slot proves I/O exited.
    pub(crate) struct Transfer {
        pub control: crate::delivery_control::Connection,
        pub slots: Arc<Semaphore>,
        socket: Option<DeadlineIo<tokio::io::DuplexStream>>,
        peer: tokio::io::DuplexStream,
        server: Option<tokio::task::JoinHandle<()>>,
    }
    impl Transfer {
        pub async fn new() -> Self {
            let slots = Arc::new(Semaphore::new(1));
            let (socket, peer) = tokio::io::duplex(256);
            let socket = DeadlineIo::new(
                socket,
                Duration::from_secs(240),
                slots.clone().acquire_owned().await.unwrap(),
            );
            socket.control.enable_reclamation();
            Self {
                control: socket.control.clone(),
                slots,
                socket: Some(socket),
                peer,
                server: None,
            }
        }
        pub async fn start(&mut self, body: axum::body::Body) {
            let body = Arc::new(std::sync::Mutex::new(Some(body)));
            let app = axum::Router::new().route(
                "/",
                axum::routing::get(move || {
                    let body = body.lock().unwrap().take().unwrap();
                    async move { axum::response::Response::new(body) }
                }),
            );
            let listener = OneConnection(self.socket.take());
            self.server = Some(tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap()
            }));
            self.peer
                .write_all(b"GET / HTTP/1.1\r\nHost: fixture\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            // Reading only a header byte establishes that Hyper has started writing.
            self.peer.read_exact(&mut [0; 1]).await.unwrap();
        }
        pub async fn exited(&self) {
            for _ in 0..100 {
                if self.slots.available_permits() == 1 {
                    return;
                }
                tokio::task::yield_now().await;
            }
            panic!("cancelled transport retained its connection slot");
        }
        pub async fn drain(&mut self) -> usize {
            let mut bytes = Vec::new();
            self.peer.read_to_end(&mut bytes).await.unwrap();
            self.exited().await;
            bytes.len()
        }
    }
    impl Drop for Transfer {
        fn drop(&mut self) {
            if let Some(server) = &self.server {
                server.abort();
            }
        }
    }
    #[tokio::test]
    async fn stalled_reads_and_writes_expire_and_release_connection_slots() {
        for read in [true, false] {
            let slots = Arc::new(Semaphore::new(1));
            let (socket, _peer) = tokio::io::duplex(1);
            let mut socket = DeadlineIo::new(
                socket,
                Duration::from_millis(20),
                slots.clone().acquire_owned().await.unwrap(),
            );
            let operation = async {
                if read {
                    socket.read_exact(&mut [0u8; 1]).await.map(|_| ())
                } else {
                    socket.write_all(&[0u8; 2]).await
                }
            };
            let error = tokio::time::timeout(Duration::from_secs(1), operation)
                .await
                .unwrap()
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::TimedOut);
            assert_eq!(slots.available_permits(), 0);
            drop(socket);
            assert_eq!(slots.available_permits(), 1);
        }
    }
}

#[cfg(test)]
mod progress_tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    #[tokio::test]
    async fn trickle_writes_expire_but_meaningful_progress_renews_delivery() {
        for progressing in [false, true] {
            let slots = Arc::new(Semaphore::new(1));
            let mut socket = DeadlineIo::new(
                tokio::io::sink(),
                Duration::from_secs(2),
                slots.clone().acquire_owned().await.unwrap(),
            );
            socket.progress_window = Some(Duration::from_millis(40));
            let bytes = vec![0; if progressing { 64 * 1024 } else { 1 }];
            let mut expired = false;
            for _ in 0..10 {
                match socket.write_all(&bytes).await {
                    Ok(()) => {}
                    Err(error) => {
                        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
                        expired = true;
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            assert_eq!(expired, !progressing);
            drop(socket);
            assert_eq!(slots.available_permits(), 1);
        }
    }
    #[tokio::test]
    async fn blocked_proxy_write_has_progress_deadline_before_connection_lifetime() {
        let slots = Arc::new(Semaphore::new(1));
        let (socket, _peer) = tokio::io::duplex(1);
        let mut socket = DeadlineIo::new(
            socket,
            Duration::from_secs(2),
            slots.clone().acquire_owned().await.unwrap(),
        );
        socket.progress_window = Some(Duration::from_millis(20));
        let error = tokio::time::timeout(Duration::from_secs(1), socket.write_all(&[0; 2]))
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        drop(socket);
        assert_eq!(slots.available_permits(), 1);
    }
}
