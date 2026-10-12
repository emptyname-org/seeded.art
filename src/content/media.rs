/// Recognize media formats from their content signature.
pub(crate) fn sniff_media(body: &[u8]) -> Option<&'static str> {
    if body.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if body.starts_with(b"\xff\xd8\xff") {
        Some("image/jpeg")
    } else if body.starts_with(b"GIF87a") || body.starts_with(b"GIF89a") {
        Some("image/gif")
    } else if body.starts_with(b"RIFF") && body.get(8..12) == Some(b"WEBP") {
        Some("image/webp")
    } else if body.get(4..8) == Some(b"ftyp") {
        Some("video/mp4")
    } else if body.starts_with(b"OggS") {
        Some("audio/ogg")
    } else if body.starts_with(b"ID3") {
        Some("audio/mpeg")
    } else if std::str::from_utf8(&body[..body.len().min(512)])
        .ok()
        .is_some_and(|s| {
            let s = s.trim_start();
            s.starts_with("<svg") || (s.starts_with("<?xml") && s.contains("<svg"))
        })
    {
        Some("image/svg+xml")
    } else {
        None
    }
}
