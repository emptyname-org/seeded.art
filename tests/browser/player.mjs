// Deterministic browser coverage of the real player and compatibility scripts.
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const require = createRequire(import.meta.url);
const tool_root = process.env.SEEDED_BROWSER_TOOLS || resolve(root, 'tmp/browser_tools/node_modules');
const { chromium } = require(resolve(tool_root, 'playwright-core'));
const shim = await readFile(resolve(root, 'assets/player/sandbox.js'), 'utf8');
const bridge = await readFile(resolve(root, 'assets/player/bridge.js'), 'utf8');
const player_js = await readFile(resolve(root, 'assets/static/player.js'));
const css = await readFile(resolve(root, 'assets/static/player.css'));
const site_css = await readFile(resolve(root, 'assets/static/site.css'));
// Exercise the production policy instead of maintaining a looser fixture copy.
const artwork_source = await readFile(resolve(root, 'src/web/artwork.rs'), 'utf8');
const csp_template = artwork_source.match(/"(default-src 'none'; script-src 'unsafe-inline'[^"\n]+)"/)?.[1];
assert(csp_template?.includes('sandbox allow-scripts'));
assert(!csp_template.includes('frame-ancestors'));
const attempts = new Map();
const requests = [];
const forbidden_requests = [];
const status_requests = [];
const status_attempts = new Map();
const cancelled_status = new Set();
const chain_tokens = JSON.parse(await readFile(resolve(root, 'tests/fixtures/ingest/tzkt/tokens.json'), 'utf8'));
const chain_mints = JSON.parse(await readFile(resolve(root, 'tests/fixtures/ingest/tzkt/mints.json'), 'utf8'));
const params_token = chain_tokens.find(token => token.tokenId === '150042');
const params_mint = chain_mints.find(mint => mint.parameter.value.token_id === '150042').parameter.value;
const params_suffix = params_token['metadata.artifactUri'].slice(params_token['metadata.artifactUri'].indexOf('?'));
const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jrU8AAAAASUVORK5CYII=', 'base64');

function listen(server) {
  return new Promise(resolve => server.listen(0, '127.0.0.1', () => resolve(`http://127.0.0.1:${server.address().port}`)));
}
function close(server) {
  return new Promise(resolve => { server.close(resolve); server.closeAllConnections(); });
}

let content_origin;
let app_origin;
const content = createServer((request, response) => {
  const url = new URL(request.url, content_origin);
  requests.push(url.href);
  response.setHeader('Access-Control-Allow-Origin', '*');
  response.setHeader('Cache-Control', 'no-store');
  response.setHeader('Content-Security-Policy', csp_template.replaceAll('{content}', content_origin));
  response.setHeader('X-Content-Type-Options', 'nosniff');
  response.setHeader('Referrer-Policy', 'no-referrer');
  response.setHeader('Cross-Origin-Resource-Policy', 'cross-origin');
  if (url.pathname === '/image.png') { response.setHeader('Content-Type', 'image/png'); response.end(png); return; }
  if (url.pathname === '/worker.js') { response.setHeader('Content-Type', 'text/javascript'); response.end("postMessage('worker-ready')"); return; }
  if (url.pathname === '/missing.png') { response.writeHead(404).end(); return; }
  if (url.pathname === '/nested.html') {
    response.setHeader('Content-Type', 'text/html; charset=utf-8');
    response.end(`<script>
      const nested={type:'fixture-nested'};
      try { parent.document.title; nested.parentAccessible=true } catch (_) { nested.parentAccessible=false }
      try { top.document.title; nested.topAccessible=true } catch (_) { nested.topAccessible=false }
      try { localStorage.setItem('nested','value'); nested.storageBlocked=false } catch (error) { nested.storageBlocked=error.name==='SecurityError' }
      parent.postMessage(nested,'*');
    </script>`);
    return;
  }
  const mode = url.pathname.split('/').pop();
  if (url.pathname.startsWith('/playback-status/')) {
    const instance = url.searchParams.get('instance');
    assert.match(instance, /^[0-9a-f]{32}$/);
    assert.equal(request.headers.cookie, undefined);
    assert.equal(request.headers.authorization, undefined);
    status_requests.push({mode, instance});
    const count = (status_attempts.get(mode) || 0) + 1;
    status_attempts.set(mode, count);
    response.setHeader('Content-Type', 'application/json');
    if (mode === 'verify-error') { response.writeHead(503).end('{}'); return; }
    if (mode === 'verify-pending' || mode === 'verify-cancel-retry' || (mode === 'verify-retry' && count === 1)) {
      response.writeHead(202).end(JSON.stringify({verification:'pending'})); return;
    }
    const value = mode === 'verify-unverified' || (['verify-stale', 'verify-late'].includes(mode) && count === 1) ? 'unverified' : 'verified';
    const finish = () => response.end(JSON.stringify({verification:value}));
    if ((['verify-stale', 'verify-late'].includes(mode) && count === 1) || mode === 'verify-stop' || mode === 'verify-timeout') {
      const timer = setTimeout(finish, mode === 'verify-timeout' ? 10000 : 1000);
      response.on('close', () => { clearTimeout(timer); if (!response.writableEnded) cancelled_status.add(instance); });
    } else finish();
    return;
  }
  const count = (attempts.get(mode) || 0) + 1;
  attempts.set(mode, count);
  const instance = url.searchParams.get('__seeded_instance');
  assert.match(instance, /^[0-9a-f]{32}$/);
  response.setHeader('Content-Type', 'text/html; charset=utf-8');
  if ((mode === 'retry' || mode === 'params-retry') && count === 1) {
    response.statusCode = 503;
    response.end(`<script>parent.postMessage({type:'seeded-player',instance:'${instance}',state:'unavailable'},'*')</script>`);
    return;
  }
  if (mode === 'timeout') { response.end('<!doctype html><p>No bridge</p>'); return; }
  const pre_shim = mode === 'richter' ? '<script>try { navigator.serviceWorker; window.preShimServiceWorkerGetterThrows = false } catch (error) { window.preShimServiceWorkerGetterThrows = error.name === \'SecurityError\' }</script>' : '';
  const fixture = `<!doctype html><html><head><base href="${content_origin}/">${pre_shim}<script data-seeded-instance="${instance}">${bridge}\n${shim}</script></head><body><canvas width="32" height="32"></canvas><script>
    window.fixture = {query:location.search, hash:location.hash, params:location.hash.slice(3), preShimServiceWorkerGetterThrows:window.preShimServiceWorkerGetterThrows};
    try { parent.document.title; fixture.parentAccessible=true } catch (_) { fixture.parentAccessible=false }
    localStorage.setItem('seed','one'); sessionStorage.setItem('seed','two'); document.cookie='seed=three';
    fixture.storage=[localStorage.getItem('seed'),sessionStorage.getItem('seed'),document.cookie];
    var image = new Image(); image.onload=function(){
      var canvas=document.querySelector('canvas');canvas.getContext('2d').drawImage(image,0,0);
      try{fixture.canvas=canvas.toDataURL().startsWith('data:image/png')}catch(_){fixture.canvas=false}
    }; image.src='./image.png';
    var worker = new Worker('./worker.js'); worker.onmessage=function(event){fixture.worker=event.data;worker.terminate()};
    ${mode === 'csp' ? `
      fixture.violations=[];
      addEventListener('securitypolicyviolation',event=>fixture.violations.push({directive:event.effectiveDirective,uri:event.blockedURI}));
      addEventListener('message',event=>{
        if(event.data?.type==='fixture-nested' && event.source===document.querySelector('iframe').contentWindow)
          fixture.nested={...event.data,origin:event.origin};
      });
      fetch('data:text/plain,inline-image').then(response=>response.text()).then(value=>fixture.data=value);
      fetch('data:application/wasm;base64,AGFzbQEAAAA=').then(response=>response.arrayBuffer()).then(bytes=>WebAssembly.instantiate(bytes)).then(()=>fixture.wasm=true);
      fetch('${app_origin}/csp-external-fetch').then(()=>fixture.externalFetch=true,()=>fixture.externalFetch=false);
      for(const src of ['./nested.html','${app_origin}/csp-external-frame','data:text/html,<p>blocked</p>']){
        const child=document.createElement('iframe');child.src=src;document.body.append(child);
      }
    ` : ''}
    ${mode === 'runtime' ? "throw new Error('Fixture failure')" : ''}
    ${mode === 'richter' ? 'fixture.serviceWorkerBranch = ("serviceWorker" in navigator); if ("serviceWorker" in navigator) navigator.serviceWorker.register("./sw.js");' : ''}
    ${mode === 'resource' ? "var missing = new Image(); document.body.append(missing); missing.src='./missing.png'" : ''}
    ${mode === 'alert' ? "alert('Fixture alert ' + 'x'.repeat(400)); for (var i = 0; i < 20; i++) alert('flood ' + i)" : ''}
    ${mode === 'webgl' ? "fixture.gl = document.createElement('canvas').getContext('webgl'); addEventListener('pagehide', function () { parent.postMessage({type:'fixture-webgl', lost: fixture.gl.isContextLost()}, '*') })" : ''}
  </script></body></html>`;
  if (mode === 'delay' || mode === 'params-delay') { const timer = setTimeout(() => response.end(fixture), 1000); response.on('close', () => clearTimeout(timer)); }
  else response.end(fixture);
});

content_origin = await listen(content);
const app = createServer((request, response) => {
  if (request.url.startsWith('/csp-external-')) forbidden_requests.push(request.url);
  response.setHeader('Content-Security-Policy', `default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self'; frame-src ${content_origin}; connect-src ${content_origin}; frame-ancestors 'none'; object-src 'none'; base-uri 'none'; form-action 'self'`);
  if (request.url === '/player.js') { response.setHeader('Content-Type', 'text/javascript; charset=utf-8'); response.end(player_js); return; }
  if (request.url === '/player.css') { response.setHeader('Content-Type', 'text/css'); response.end(Buffer.concat([site_css, css])); return; }
  const mode = request.url.slice(1) || 'normal';
  const modes = mode === 'pair' ? ['left', 'right'] : [mode];
  response.setHeader('Content-Type', 'text/html; charset=utf-8');
  response.end(`<!doctype html><html lang="en"><head><title>Playback fixture</title><link rel="stylesheet" href="/player.css"><script src="/player.js" defer></script></head><body>${modes.map(mode => {
    const suffix = mode.startsWith('params-') ? params_suffix : '?fxhash=ooSeed&fxiteration=9&fxminter=tz1Minter&custom=a+b#0x42%ab';
    return `<section data-player data-autoplay="${mode === 'autoplay'}" data-title="Fixture ${mode}" data-render-url="${content_origin}/render/test/${mode}${suffix.replaceAll('&', '&amp;')}"><div class="player-stage"><div class="player-poster">Static preview</div><div class="player-frame-host"></div></div><div class="player-controls" hidden><button data-action="play">Replay</button><button data-action="reset" hidden disabled>Replay</button><button data-action="stop" disabled>Stop</button></div><p class="player-status" role="status"></p></section>`;
  }).join('')}</body></html>`);
});
app_origin = await listen(app);
const embed = createServer((request, response) => {
  response.setHeader('Content-Type', 'text/html; charset=utf-8');
  response.setHeader('Content-Security-Policy', `default-src 'none'; frame-src ${content_origin}`);
  response.end(`<!doctype html><title>External embed</title><iframe src="${content_origin}/render/test/csp?__seeded_instance=${'c'.repeat(32)}"></iframe>`);
});
const embed_origin = await listen(embed);
let browser;
try {
  browser = await chromium.launch({ executablePath: process.env.SEEDED_CHROMIUM || '/usr/bin/chromium', headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();
  const errors = []; page.on('pageerror', error => errors.push(error.message));
  async function open(mode) { await page.goto(`${app_origin}/${mode}`); return page.locator('[data-player]'); }
  async function loaded(player) { await player.locator('[data-action=play]').click(); await player.locator('iframe[data-loading=false]').waitFor(); }
  // A stopped frame is navigated to about:blank and removed once its document reports unload.
  const live = player => player.locator('iframe:not([data-retiring])');
  async function frames_settle(target, count) { await target.waitForFunction(count => document.querySelectorAll('iframe').length === count, count, { timeout: 900 }); }
  let player = await open('autoplay');
  await player.locator('iframe[data-loading=false]').waitFor();
  assert.equal(await player.locator('.player-poster').isVisible(), false);
  assert.equal(await player.locator('iframe').isVisible(), true);
  assert.equal(await player.locator('[data-action=play]').innerText(), 'Replay');
  const autoplay_src = await player.locator('iframe').getAttribute('src');
  await player.locator('[data-action=play]').scrollIntoViewIfNeeded();
  await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  await player.locator('[data-action=play]').click();
  await player.locator('iframe[data-loading=false]').waitFor();
  assert.notEqual(await live(player).getAttribute('src'), autoplay_src);
  const teardown = await page.evaluate(() => {
    document.querySelector('[data-action=stop]').click();
    const frame = document.querySelector('iframe');
    return {retiring: frame.dataset.retiring, visibility: getComputedStyle(frame).visibility, inert: frame.inert, src: frame.getAttribute('src'), poster: !document.querySelector('.player-poster').hidden};
  });
  assert.deepEqual(teardown, {retiring: 'true', visibility: 'hidden', inert: true, src: 'about:blank', poster: true});
  await frames_settle(page, 0);
  assert.equal(await player.getAttribute('data-state'), 'stopped');
  player = await open('normal');
  assert.equal(await page.locator('iframe').count(), 0);
  const response_wait = page.waitForResponse(response => response.url().startsWith(content_origin + '/render/'));
  await loaded(player);
  const production_csp = (await response_wait).headers()['content-security-policy'];
  assert(production_csp.includes('sandbox allow-scripts'));
  assert(!production_csp.includes('frame-ancestors'));
  assert(!production_csp.includes('allow-same-origin'));
  const frame = page.frames().find(frame => frame.url().startsWith(content_origin));
  await frame.waitForFunction(() => fixture.worker && fixture.canvas);
  const fixture = await frame.evaluate(() => fixture);
  assert.equal(fixture.parentAccessible, false);
  assert.deepEqual(fixture.storage, ['one', 'two', 'seed=three']);
  assert.equal(fixture.worker, 'worker-ready'); assert.equal(fixture.canvas, true);
  await player.locator('[data-player-verification]').filter({hasText:/^Main file verified$/}).waitFor();
  assert.equal(fixture.hash, '#0x42%ab');
  assert(fixture.query.startsWith('?fxhash=ooSeed&fxiteration=9&fxminter=tz1Minter&custom=a+b&__seeded_instance='));
  assert.equal(await player.locator('iframe').getAttribute('sandbox'), 'allow-scripts');
  assert.match(await player.locator('.player-status').innerText(), /Rendering may continue/);
  player = await open('richter');
  const parent_service_worker = await page.evaluate(() => {
    window.parentServiceWorkerGetter = Object.getOwnPropertyDescriptor(Navigator.prototype, 'serviceWorker')?.get ?? null;
    return { has: 'serviceWorker' in navigator, getter: window.parentServiceWorkerGetter !== null };
  });
  await loaded(player);
  const richter_frame = page.frames().find(frame => frame.url().startsWith(content_origin + '/render/test/richter'));
  const richter_result = await richter_frame.evaluate(() => fixture);
  if (parent_service_worker.has) assert.equal(richter_result.preShimServiceWorkerGetterThrows, true);
  assert.equal(richter_result.serviceWorkerBranch, false);
  assert.equal(await player.locator('[data-player-notice]').isVisible(), false);
  assert.deepEqual(await page.evaluate(() => ({
    has: 'serviceWorker' in navigator,
    getter: Object.getOwnPropertyDescriptor(Navigator.prototype, 'serviceWorker')?.get === window.parentServiceWorkerGetter,
  })), parent_service_worker);
  assert.equal(requests.some(request => new URL(request).pathname.endsWith('/sw.js')), false);
  const regular_context = await browser.newContext({ bypassCSP: true });
  const regular_page = await regular_context.newPage();
  await regular_page.goto(`${app_origin}/normal`);
  const regular_before = await regular_page.evaluate(() => {
    window.originalServiceWorkerGetter = Object.getOwnPropertyDescriptor(Navigator.prototype, 'serviceWorker')?.get ?? null;
    let readable = true;
    try { navigator.serviceWorker } catch (_) { readable = false }
    return { has: 'serviceWorker' in navigator, getter: window.originalServiceWorkerGetter !== null, readable };
  });
  await regular_page.addScriptTag({ content: shim });
  assert.deepEqual(await regular_page.evaluate(() => {
    let readable = true;
    try { navigator.serviceWorker } catch (_) { readable = false }
    return {
      has: 'serviceWorker' in navigator,
      getter: Object.getOwnPropertyDescriptor(Navigator.prototype, 'serviceWorker')?.get === window.originalServiceWorkerGetter,
      readable,
    };
  }), regular_before);
  await regular_context.close();
  await page.evaluate(() => { window.oldSource = document.querySelector('iframe').contentWindow; window.oldInstance = new URL(document.querySelector('iframe').src).searchParams.get('__seeded_instance'); });
  await player.locator('[data-action=play]').click(); await player.locator('iframe[data-loading=false]').waitFor();
  await frames_settle(page, 1);
  await page.evaluate(() => {
    const current = document.querySelector('iframe'); const instance = new URL(current.src).searchParams.get('__seeded_instance');
    window.dispatchEvent(new MessageEvent('message', {source:oldSource,data:{type:'seeded-player',instance,state:'unavailable'}}));
    window.dispatchEvent(new MessageEvent('message', {source:current.contentWindow,data:{type:'seeded-player',instance:oldInstance,state:'unavailable'}}));
    window.postMessage({type:'seeded-player',instance,state:'unavailable'}, '*');
  });
  await page.waitForTimeout(50); assert.equal(await player.getAttribute('data-state'), 'loaded');
  await player.locator('[data-action=stop]').click(); await frames_settle(page, 0); assert.equal(await player.getAttribute('data-state'), 'stopped');
  player = await open('csp');
  await loaded(player);
  const csp_frame = page.frames().find(frame=>frame.url().startsWith(content_origin+'/render/'));
  await csp_frame.waitForFunction(()=>fixture.data && fixture.wasm && fixture.nested && fixture.externalFetch === false && fixture.violations.filter(v=>v.directive==='frame-src').length===2,undefined,{timeout:5000});
  const csp_result = await csp_frame.evaluate(()=>({data:fixture.data,wasm:fixture.wasm,parentAccessible:fixture.parentAccessible,nested:fixture.nested,violations:fixture.violations}));
  assert.equal(csp_result.data,'inline-image');
  assert.equal(csp_result.wasm,true);
  assert.equal(csp_result.parentAccessible,false);
  assert(csp_result.violations.some(v=>v.directive==='connect-src' && v.uri.startsWith(app_origin)));
  assert(csp_result.violations.some(v=>v.directive==='frame-src' && v.uri.startsWith(app_origin)));
  // Chromium redacts the blocked data: navigation's URI.
  assert(csp_result.violations.some(v=>v.directive==='frame-src' && v.uri===''));
  assert.deepEqual(forbidden_requests,[],'forbidden fetches and frames never reach the app');
  assert(requests.includes(content_origin+'/nested.html'),'nested content is fetched and executes');
  const nested_isolated = {type:'fixture-nested',parentAccessible:false,topAccessible:false,storageBlocked:true,origin:'null'};
  assert.deepEqual(csp_result.nested,nested_isolated);
  assert.equal(await csp_frame.evaluate(()=>{
    try { document.querySelector('iframe').contentWindow.document; return true } catch (_) { return false }
  }),false,'the opaque artwork parent cannot read its nested content');
  // External animation_url consumers need no app origin or iframe sandbox attribute.
  const embedded = await context.newPage();
  await embedded.goto(embed_origin);
  assert.equal(await embedded.locator('iframe').getAttribute('sandbox'),null);
  const embedded_frame = embedded.frames().find(frame=>frame.url().startsWith(content_origin+'/render/'));
  await embedded_frame.waitForFunction(()=>fixture.nested && fixture.externalFetch === false && fixture.violations.filter(v=>v.directive==='frame-src').length===2,undefined,{timeout:5000});
  assert.deepEqual(await embedded_frame.evaluate(()=>({origin:self.origin,parentAccessible:fixture.parentAccessible,nested:fixture.nested})),{
    origin:'null',parentAccessible:false,nested:nested_isolated,
  });
  assert.deepEqual(forbidden_requests,[],'external embedding does not permit external fetch or frame targets');
  await embedded.close();
  player = await open('retry'); await player.locator('[data-action=play]').click(); await page.waitForFunction(() => document.querySelector('[data-player]').dataset.state === 'unavailable'); assert.equal(await player.locator('iframe').count(),0); await loaded(player); assert.equal(attempts.get('retry'),2);
  player = await open('delay'); await player.locator('[data-action=play]').click(); await player.locator('[data-action=stop]').click(); await page.waitForTimeout(1100); assert.equal(await player.getAttribute('data-state'),'stopped'); assert.equal(await player.locator('iframe').count(),0);
  async function recorded_params() {
    const frame = page.frames().find(frame => frame.url().startsWith(content_origin));
    const actual = await frame.evaluate(() => fixture);
    assert.equal(actual.hash, '#' + params_suffix.split('#')[1]);
    assert.equal(actual.params, params_mint.input_bytes);
    assert(actual.query.startsWith(params_suffix.split('#')[0] + '&__seeded_instance='));
    const query = new URLSearchParams(actual.query);
    assert.equal(query.get('fxiteration'), params_mint.iteration);
    assert.equal(query.get('fxminter'), params_mint.address);
    assert.equal(query.get('fxhash'), new URLSearchParams(params_suffix.split('#')[0]).get('fxhash'));
    return query.get('__seeded_instance');
  }
  player = await open('params-retry');
  await player.locator('[data-action=play]').click();
  await page.waitForFunction(() => document.querySelector('[data-player]').dataset.state === 'unavailable');
  await loaded(player);
  const params_instance = await recorded_params();
  await player.locator('[data-action=play]').click();
  await player.locator('iframe[data-loading=false]').waitFor();
  assert.notEqual(await recorded_params(), params_instance);
  player = await open('params-delay');
  await player.locator('[data-action=play]').click();
  assert.equal(new URL(await player.locator('iframe').getAttribute('src')).hash, '#0x' + params_mint.input_bytes);
  await player.locator('[data-action=stop]').click();
  await page.waitForTimeout(1100);
  assert.equal(await player.getAttribute('data-state'), 'stopped');
  assert.equal(await player.locator('iframe').count(), 0);
  await loaded(player);
  await recorded_params();
  for (const kind of ['runtime', 'rejection']) {
    player = await open('normal');
    await loaded(player);
    const original_src = await live(player).getAttribute('src');
    const running_frame = page.frames().find(frame => frame.url().startsWith(content_origin));
    await running_frame.evaluate(kind => {
      if (kind === 'runtime') setTimeout(() => { throw new Error('Fixture late runtime'); }, 0);
      else void Promise.reject(new Error('Fixture late rejection'));
    }, kind);
    await player.locator('[data-player-notice]').filter({hasText: kind === 'runtime' ? /^The NFT reported an error: .*Fixture late runtime$/ : /^The NFT reported an error: Fixture late rejection$/}).waitFor();
    assert.equal(await player.getAttribute('data-state'), 'loaded');
    assert.equal(await live(player).getAttribute('src'), original_src);
    assert.equal(await player.locator('[data-action=play]').isEnabled(), true);
    await player.locator('[data-action=play]').click();
    await player.locator('iframe[data-loading=false]').waitFor();
    assert.notEqual(await live(player).getAttribute('src'), original_src);
    assert.equal(await player.locator('[data-player-notice]').isVisible(), false);
  }
  // An uncaught error before load is reported; the work keeps running and is shown.
  player = await open('runtime'); await loaded(player);
  assert.equal(await player.getAttribute('data-state'), 'loaded');
  assert.equal(await player.locator('.player-poster').isVisible(), false);
  assert.match(await player.locator('[data-player-notice]').innerText(), /^The NFT reported an error: .*Fixture failure$/);
  player = await open('alert');
  await page.evaluate(() => { window.alerts = 0; addEventListener('message', event => { if (event.data?.state === 'alert') window.alerts += 1; }); });
  await loaded(player);
  assert.equal(await player.locator('[data-player-notice]').innerText(), 'NFT alert: ' + ('Fixture alert ' + 'x'.repeat(400)).slice(0, 300));
  assert.equal(await page.evaluate(() => window.alerts), 8);
  // Full screen hides idle controls, never the NFT's own message.
  const full = await context.newPage();
  await full.goto(`${app_origin}/alert`);
  await full.evaluate(() => { document.body.classList.add('full-artwork'); document.querySelector('[data-player]').classList.add('artwork-player'); document.querySelector('[data-action=play]').click(); });
  await full.locator('iframe[data-loading=false]').waitFor();
  await full.locator('[data-player-notice]:not([hidden])').waitFor();
  assert.equal(await full.locator('[data-player-notice]').evaluate(notice => getComputedStyle(notice).opacity), '1');
  await full.close();
  assert.equal(await player.getAttribute('data-state'), 'loaded');
  player = await open('webgl'); await loaded(player);
  const webgl_lost = page.evaluate(() => new Promise(resolve => addEventListener('message', event => { if (event.data?.type === 'fixture-webgl') resolve(event.data.lost); })));
  await player.locator('[data-action=stop]').click();
  assert.equal(await webgl_lost, true);
  await frames_settle(page, 0);
  for (const [mode, notice, count] of [
    ['verify-unverified', 'Main file integrity not verified', 1],
    ['verify-retry', 'Main file verified', 2],
    ['verify-pending', 'Main file verification unavailable', 2],
    ['verify-error', 'Main file verification unavailable', 1],
    ['verify-timeout', 'Main file verification unavailable', 1],
  ]) {
    player = await open(mode); await loaded(player);
    await player.locator('[data-player-verification]').filter({hasText:notice}).waitFor();
    assert.equal(status_attempts.get(mode), count);
    assert.equal(await player.getAttribute('data-state'), 'loaded');
  }
  async function status_requested(mode) {
    for (let attempt = 0; attempt < 100; attempt += 1) {
      const request = status_requests.find(request => request.mode === mode);
      if (request) return request.instance;
      await new Promise(resolve => setTimeout(resolve, 10));
    }
    throw new Error('Expected verification request for ' + mode);
  }
  player = await open('verify-stale'); await loaded(player);
  const stale_instance = await status_requested('verify-stale');
  const verification_frame = page.frames().find(frame => frame.url().startsWith(content_origin));
  await verification_frame.evaluate(() => parent.postMessage({type:'seeded-player',instance:new URL(location.href).searchParams.get('__seeded_instance'),state:'verified',verification:'verified'}, '*'));
  assert.equal(await player.locator('[data-player-verification]').textContent(), 'Checking main file integrity…');
  await player.locator('[data-action=play]').click(); await player.locator('iframe[data-loading=false]').waitFor();
  await player.locator('[data-player-verification]').filter({hasText:/^Main file verified$/}).waitFor();
  await page.waitForTimeout(1100);
  assert.equal(await player.locator('[data-player-verification]').textContent(), 'Main file verified');
  assert(cancelled_status.has(stale_instance));
  player = await open('verify-stop'); await loaded(player);
  const stopped_instance = await status_requested('verify-stop');
  await player.locator('[data-action=stop]').click(); await page.waitForTimeout(100);
  assert(cancelled_status.has(stopped_instance));
  assert.equal(await player.locator('[data-player-verification]').isVisible(), false);
  player = await open('verify-late');
  // Simulate a transport whose result survives cancellation; ownership must still win.
  await page.evaluate(() => {
    const original_fetch = window.fetch;
    window.fetch = (input, options) => original_fetch(input, {...options, signal:undefined});
  });
  await loaded(player); await status_requested('verify-late');
  await player.locator('[data-action=play]').click(); await player.locator('iframe[data-loading=false]').waitFor();
  await player.locator('[data-player-verification]').filter({hasText:/^Main file verified$/}).waitFor();
  await page.waitForTimeout(1100);
  assert.equal(await player.locator('[data-player-verification]').textContent(), 'Main file verified');
  player = await open('verify-cancel-retry');
  const pending_status = page.waitForResponse(response => response.url().includes('/playback-status/test/verify-cancel-retry'));
  await loaded(player); await pending_status; await page.waitForTimeout(20);
  await player.locator('[data-action=stop]').click(); await page.waitForTimeout(350);
  assert.equal(status_attempts.get('verify-cancel-retry'), 1);
  player = await open('resource'); await loaded(player); await page.waitForFunction(() => document.querySelector('.player-status').textContent.toLowerCase().includes('resource failed')); assert.match(await player.locator('.player-status').innerText(),/resource failed/i);
  const timeout_page = await context.newPage(); await timeout_page.goto(`${app_origin}/timeout`); await timeout_page.clock.install(); const timeout_player = timeout_page.locator('[data-player]'); await timeout_player.locator('[data-action=play]').click(); await timeout_page.clock.fastForward(45001); assert.equal(await timeout_player.getAttribute('data-state'),'unavailable'); assert.match(await timeout_player.locator('.player-status').innerText(),/timed out/); await timeout_page.close();
  // Rapid Replay/Stop: one artwork document at most, nothing started while clicking, no timers left.
  const rapid_context = await browser.newContext();
  await rapid_context.addInitScript(() => {
    if (window.top !== window) return;
    const pending = new Set(); const set = window.setTimeout; const clear = window.clearTimeout;
    window.pending_timers = pending;
    window.setTimeout = function (callback, delay, ...rest) {
      const id = set.call(window, (...values) => { pending.delete(id); callback(...values); }, delay, ...rest);
      pending.add(id); return id;
    };
    window.clearTimeout = function (id) { pending.delete(id); return clear.call(window, id); };
  });
  const rapid = await rapid_context.newPage();
  rapid.on('pageerror', error => errors.push(error.message));
  await rapid.goto(`${app_origin}/autoplay`);
  const hammered = rapid.locator('[data-player]');
  await hammered.locator('iframe[data-loading=false]').waitFor();
  const quiet = () => rapid.waitForFunction(() => window.pending_timers.size === 0);
  let renders = attempts.get('autoplay');
  await rapid.evaluate(() => { for (let i = 0; i < 20; i += 1) document.querySelector(i % 2 ? '[data-action=play]' : '[data-action=stop]').click(); });
  await hammered.locator('iframe[data-loading=false]').waitFor();
  await quiet(); await frames_settle(rapid, 1);
  assert.equal(attempts.get('autoplay') - renders, 1);
  assert.equal(await hammered.getAttribute('data-state'), 'loaded');
  for (const last of ['play', 'stop']) {
    renders = attempts.get('autoplay');
    for (let i = 0; i < 20; i += 1) await hammered.locator(`[data-action=${(i % 2 === 0) === (last === 'stop') ? 'play' : 'stop'}]`).click();
    if (last === 'play') await hammered.locator('iframe[data-loading=false]').waitFor();
    await quiet(); await frames_settle(rapid, last === 'play' ? 1 : 0);
    assert(attempts.get('autoplay') - renders <= 2, 'rapid clicks started at most the final run');
    assert.equal(await hammered.getAttribute('data-state'), last === 'play' ? 'loaded' : 'stopped');
    assert.equal(await rapid.evaluate(() => document.activeElement.dataset.action), last === 'play' ? 'stop' : 'play');
  }
  await rapid_context.close();
  await open('pair');
  const left = page.locator('[data-player]').nth(0);
  const right = page.locator('[data-player]').nth(1);
  await loaded(left); await loaded(right);
  // Settle scrolling across two full-height cross-origin frames before the pointer action.
  await left.locator('[data-action=stop]').scrollIntoViewIfNeeded();
  await page.evaluate(() => new Promise(resolve => requestAnimationFrame(() => requestAnimationFrame(resolve))));
  await left.locator('[data-action=stop]').click();
  assert.equal(await left.getAttribute('data-state'), 'stopped');
  assert.equal(await right.getAttribute('data-state'), 'loaded');
  await frames_settle(page, 1);
  const disabled=await browser.newContext({javaScriptEnabled:false}); const plain=await disabled.newPage();await plain.goto(app_origin);assert.equal(await plain.locator('iframe').count(),0);assert.equal(await plain.locator('.player-controls').isVisible(),false);await disabled.close();
  assert.deepEqual(errors.filter(error => !['Fixture failure', 'Fixture late runtime', 'Fixture late rejection'].includes(error)), []);
  console.log('Playback browser: token autoplay, Replay/Stop, teardown via about:blank with WebGL release, rapid Replay/Stop without stacked frames or timers, kept frame with visible error and alert notices, production CSP, isolated nested frames and external embedding, blocked external targets, trusted integrity with bounded retry/abort/stale guards, non-destructive late errors, seed/hash, recorded params, opaque sandbox, CORS canvas, storage, worker, timeout, resource failure, independent players, and no-JS preview passed.');
} finally {
  await browser?.close(); await close(embed); await close(app); await close(content);
}
