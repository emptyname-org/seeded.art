/* Artwork playback is optional; metadata and navigation remain server rendered. */
(function () {
  'use strict';
  const players = new Map();
  const LOAD_TIMEOUT_MS = 45000;
  const VERIFICATION_TIMEOUT_MS = 5000;
  // An old artwork document must unload before the next one starts.
  const TEARDOWN_TIMEOUT_MS = 1000;
  // A Replay this soon after another Replay/Stop waits this long, so rapid clicks start nothing.
  const SETTLE_MS = 700;
  const NOTICE_CHARS = 300;

  function set_status(player, state, text) {
    player.state = state;
    player.root.dataset.state = state;
    player.status.textContent = text;
    player.play.textContent = 'Replay';
    player.play.hidden = false;
    player.reset.disabled = !player.wanted;
    player.stop.disabled = !player.wanted;
  }

  function show_notice(player, kind, detail) {
    if (typeof detail !== 'string') return;
    // Alerts usually carry the fatal message; otherwise the first error is the cause.
    if (player.notice_kind && !(player.notice_kind === 'error' && kind === 'alert')) return;
    const text = detail.length > NOTICE_CHARS ? detail.slice(0, NOTICE_CHARS) + '…' : detail;
    player.notice_kind = kind;
    // Errors from the artwork's own script files reach the page muted as "Script error.".
    if (kind === 'alert') player.notice.textContent = 'NFT alert: ' + text;
    else if (!text || /^script error\.?$/i.test(text)) player.notice.textContent = 'The NFT reported an error.';
    else player.notice.textContent = 'The NFT reported an error: ' + text;
    player.notice.hidden = false;
  }

  function clear_notice(player) {
    player.notice_kind = null;
    player.notice.textContent = '';
    player.notice.hidden = true;
  }

  function finish_teardown(player, entry) {
    const index = player.retiring.indexOf(entry);
    if (index < 0) return;
    player.retiring.splice(index, 1);
    clearTimeout(entry.timer);
    entry.frame.remove();
    schedule(player);
  }

  // Navigate the old document away in place so it unloads before the element goes.
  function retire(player, frame, started) {
    if (!started) { frame.remove(); return; }
    const entry = { frame, instance: frame.dataset.instance, timer: null };
    delete frame.dataset.loading;
    frame.dataset.retiring = 'true';
    frame.setAttribute('inert', '');
    frame.tabIndex = -1;
    player.retiring.push(entry);
    entry.timer = setTimeout(() => finish_teardown(player, entry), TEARDOWN_TIMEOUT_MS);
    frame.src = 'about:blank';
  }

  function remove_frame(player) {
    clearTimeout(player.timer);
    player.timer = null;
    player.instance = null;
    player.verification_controller?.abort();
    player.verification_controller = null;
    clearTimeout(player.verification_timer);
    clearTimeout(player.verification_retry);
    player.verification.hidden = true;
    if (player.frame) {
      const frame = player.frame;
      player.frame = null;
      retire(player, frame, player.initialized);
    }
    player.initialized = false;
    player.poster.hidden = false;
  }

  function verify_content(player) {
    if (!player.frame || player.verification_attempts >= 2) return;
    const frame = player.frame;
    const instance = player.instance;
    const current = () => player.frame === frame && player.instance === instance;
    const controller = new AbortController();
    player.verification_controller = controller;
    player.verification_attempts += 1;
    player.verification_timer = setTimeout(() => controller.abort(), VERIFICATION_TIMEOUT_MS);
    const url = new URL(player.url);
    url.pathname = url.pathname.replace(/^\/render\//, '/playback-status/');
    url.search = '?instance=' + instance;
    url.hash = '';
    (async () => {
      try {
        const response = await fetch(url, {signal: controller.signal, credentials: 'omit', cache: 'no-store'});
        if (!current()) return;
        if (response.status === 202 && player.verification_attempts < 2) {
          player.verification_retry = setTimeout(() => { if (current()) verify_content(player); }, 250);
          return;
        }
        if (response.status !== 200) throw new Error('Verification unavailable');
        const result = await response.json();
        if (!current()) return;
        if (result.verification === 'verified') player.verification.textContent = 'Main file verified';
        else if (result.verification === 'unverified') player.verification.textContent = 'Main file integrity not verified';
        else throw new Error('Invalid verification response');
      } catch (_) {
        if (current()) player.verification.textContent = 'Main file verification unavailable';
      } finally {
        if (player.verification_controller === controller) {
          clearTimeout(player.verification_timer);
          player.verification_controller = null;
        }
      }
    })();
  }

  function cancel_start(player) {
    player.wanted = false;
    clearTimeout(player.start_timer);
    player.start_timer = null;
  }

  function fail(player, text) {
    const focused = document.activeElement;
    const move_focus = focused === player.frame || focused === player.stop || focused === player.reset;
    cancel_start(player);
    remove_frame(player);
    set_status(player, 'unavailable', text);
    if (move_focus) player.play.focus({ preventScroll: true });
  }

  function create_frame(player) {
    const bytes = new Uint8Array(16);
    crypto.getRandomValues(bytes);
    const instance = Array.from(bytes, value => value.toString(16).padStart(2, '0')).join('');
    // Append without URLSearchParams serialization: fx(params) query/hash bytes matter.
    const hash_at = player.url.indexOf('#');
    const before_hash = hash_at < 0 ? player.url : player.url.slice(0, hash_at);
    const fragment = hash_at < 0 ? '' : player.url.slice(hash_at);
    const separator = before_hash.includes('?') ? '&' : '?';
    const frame = document.createElement('iframe');
    frame.className = 'player-frame';
    frame.title = player.root.dataset.title + ' — interactive NFT';
    frame.setAttribute('sandbox', 'allow-scripts');
    frame.setAttribute('referrerpolicy', 'no-referrer');
    frame.setAttribute('allow', "camera 'none'; microphone 'none'; geolocation 'none'; clipboard-read 'none'; clipboard-write 'none'");
    frame.dataset.loading = 'true';
    frame.dataset.instance = instance;
    player.instance = instance;
    player.frame = frame;
    player.resource_error = false;
    player.initialized = false;
    player.verification_attempts = 0;
    player.verification.textContent = 'Checking main file integrity…';
    player.verification.hidden = false;
    player.host.append(frame);
    player.timer = setTimeout(() => {
      if (player.instance === instance && player.frame === frame) {
        fail(player, 'NFT loading timed out. You can retry.');
      }
    }, LOAD_TIMEOUT_MS);
    frame.src = before_hash + separator + '__seeded_instance=' + instance + fragment;
  }

  // One artwork document per player: wait for teardown, then for clicking to settle.
  function schedule(player) {
    clearTimeout(player.start_timer);
    player.start_timer = null;
    if (!player.wanted || player.frame || player.retiring.length) return;
    const wait = player.settle_until - performance.now();
    if (wait > 0) {
      player.start_timer = setTimeout(() => schedule(player), wait);
      return;
    }
    create_frame(player);
  }

  function start(player) {
    const move_focus = document.activeElement === player.play;
    const now = performance.now();
    player.settle_until = now - player.last_action < SETTLE_MS ? now + SETTLE_MS : 0;
    player.last_action = now;
    remove_frame(player);
    clear_notice(player);
    player.wanted = true;
    set_status(player, 'loading', 'Loading NFT…');
    if (move_focus) player.stop.focus({ preventScroll: true });
    schedule(player);
  }

  function stop(player) {
    cancel_start(player);
    player.last_action = performance.now();
    remove_frame(player);
    set_status(player, 'stopped', 'Playback stopped. Static preview shown.');
    player.play.focus({ preventScroll: true });
  }

  document.querySelectorAll('[data-player]').forEach(root => {
    const url = root.dataset.renderUrl;
    try {
      const parsed = new URL(url);
      if (!['http:', 'https:'].includes(parsed.protocol) || parsed.origin === location.origin || parsed.username || parsed.password) return;
    } catch (_) { return; }
    const verification = document.createElement('p');
    verification.className = 'player-verification preview-notice';
    verification.dataset.playerVerification = '';
    verification.setAttribute('role', 'status');
    verification.hidden = true;
    const notice = document.createElement('p');
    notice.className = 'player-notice';
    notice.dataset.playerNotice = '';
    notice.setAttribute('role', 'status');
    notice.hidden = true;
    root.querySelector('.player-status').insertAdjacentElement('afterend', verification);
    verification.insertAdjacentElement('afterend', notice);
    const player = {
      root, url, host: root.querySelector('.player-frame-host'), poster: root.querySelector('.player-poster'),
      status: root.querySelector('.player-status'), play: root.querySelector('[data-action="play"]'),
      reset: root.querySelector('[data-action="reset"]'), stop: root.querySelector('[data-action="stop"]'),
      frame: null, instance: null, timer: null, state: 'preview', resource_error: false, initialized: false,
      wanted: false, start_timer: null, last_action: -Infinity, settle_until: 0, retiring: [], notice, notice_kind: null,
      verification, verification_attempts: 0, verification_controller: null, verification_timer: null, verification_retry: null,
    };
    players.set(root, player);
    root.querySelector('.player-controls').hidden = false;
    set_status(player, 'preview', 'Static preview.');
    player.play.addEventListener('click', () => start(player));
    player.reset.addEventListener('click', () => start(player));
    player.stop.addEventListener('click', () => stop(player));
    if (root.dataset.autoplay === 'true') start(player);
  });

  window.addEventListener('message', event => {
    const message = event.data;
    if (!message || typeof message !== 'object' || message.type !== 'seeded-player' || typeof message.instance !== 'string') return;
    for (const player of players.values()) {
      if (message.state === 'unloaded') {
        // Chromium delivers unload-time messages with a null source; the run identity still binds them.
        const entry = player.retiring.find(item => item.instance === message.instance);
        if (entry) { finish_teardown(player, entry); break; }
        continue;
      }
      // Sandboxed frames have opaque origins: WindowProxy and per-run identity are the boundary.
      if (!player.frame || event.source !== player.frame.contentWindow || message.instance !== player.instance) continue;
      if (message.state === 'initializing') {
        player.initialized = true;
        if (player.verification_attempts === 0) verify_content(player);
      } else if (message.state === 'document-loaded') {
        if (!player.initialized || player.state !== 'loading') continue;
        clearTimeout(player.timer);
        player.timer = null;
        player.frame.dataset.loading = 'false';
        player.poster.hidden = true;
        set_status(player, 'loaded', player.resource_error
          ? 'NFT loaded; a resource failed. Rendering may be incomplete.'
          : 'NFT loaded. Rendering may continue.');
      } else if (message.state === 'resource-error') {
        player.resource_error = true;
        if (player.state === 'loaded') set_status(player, 'loaded', 'A resource failed. Rendering may be incomplete; replay to retry.');
      } else if (message.state === 'runtime-error') {
        // Works often keep drawing after an uncaught error: report it, keep the frame.
        show_notice(player, 'error', message.detail);
      } else if (message.state === 'alert') {
        show_notice(player, 'alert', message.detail);
      } else if (message.state === 'unavailable') {
        fail(player, 'NFT is unavailable right now. You can retry.');
      }
      break;
    }
  });
})();
