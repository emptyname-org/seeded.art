/* Derived runner lifecycle bridge. Signals document state, never drawing completion. */
(function () {
  'use strict';
  var instance = document.currentScript.getAttribute('data-seeded-instance');
  var target = window.parent;
  var NOTICE_CHARS = 300;
  var notices = 0;
  var resource_failed = false;
  function send(state, detail) {
    var message = { type: 'seeded-player', instance: instance, state: state };
    if (detail !== undefined) message.detail = detail;
    try { target.postMessage(message, '*'); } catch (e) { /* parent gone */ }
  }
  function text(value) {
    try {
      if (value && typeof value === 'object' && typeof value.message === 'string') value = value.message;
      value = String(value);
    } catch (e) { value = ''; }
    return value.slice(0, NOTICE_CHARS);
  }
  // Bounded: a work that fails every frame must not flood the page.
  function notice(state, value) {
    if (notices >= 8) return;
    notices += 1;
    send(state, text(value));
  }
  window.addEventListener('error', function (event) {
    if (event.target === window) { notice('runtime-error', event.message || event.error); }
    else if (!resource_failed) { resource_failed = true; send('resource-error'); }
  }, true);
  window.addEventListener('unhandledrejection', function (event) { notice('runtime-error', event.reason); });
  // Sandboxed documents cannot open dialogs; report the text instead of dropping it.
  window.alert = function alert(message) { notice('alert', arguments.length ? message : ''); };

  // Teardown releases GPU memory now instead of at a later garbage collection.
  var contexts = [];
  var known = new WeakSet();
  function remember(context) {
    if (known.has(context)) return;
    known.add(context);
    if (contexts.length >= 64) {
      contexts = contexts.filter(function (ref) { var c = ref.deref(); return c && !c.isContextLost(); });
      if (contexts.length >= 64) return;
    }
    contexts.push(new WeakRef(context));
  }
  [typeof HTMLCanvasElement === 'function' && HTMLCanvasElement,
    typeof OffscreenCanvas === 'function' && OffscreenCanvas].forEach(function (type) {
    if (!type || typeof WeakRef !== 'function') return;
    var get_context = type.prototype.getContext;
    type.prototype.getContext = function getContext(kind) {
      var context = get_context.apply(this, arguments);
      if (context && typeof kind === 'string' && /webgl/i.test(kind)) remember(context);
      return context;
    };
  });
  window.addEventListener('pagehide', function (event) {
    if (event.persisted) return;
    contexts.forEach(function (ref) {
      try { var c = ref.deref(); var lose = c && c.getExtension('WEBGL_lose_context'); if (lose) lose.loseContext(); } catch (e) { /* already lost */ }
    });
    contexts = [];
    send('unloaded');
  });

  window.addEventListener('load', function () { send('document-loaded'); }, { once: true });
  send('initializing');
})();
