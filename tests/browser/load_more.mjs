// Deterministic HTML fixtures exercise cursor merging, retries and appended controls.
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {readFile,mkdir,mkdtemp,rm} from 'node:fs/promises';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
import {dirname,resolve} from 'node:path';

const root=resolve(dirname(fileURLToPath(import.meta.url)),'../..');
const require=createRequire(import.meta.url);
const {chromium}=require(resolve(process.env.SEEDED_BROWSER_TOOLS||resolve(root,'tmp/browser_tools/node_modules'),'playwright-core'));
const files=new Map(await Promise.all(['load_more.js','market.js','auth.js','site.css','market.css','read_more.js'].map(async name=>[name,await readFile(resolve(root,'assets/static',name))])));
const actor='tz1P719ymGCtNwavZKh2LmhMs53dGsEMzjnm';
let requests=[],retryCount=0,marketPrepare=[];

function next(href,label='Next page') { return `<nav class="pagination" data-load-more="${label === 'Next records' ? 'market' : 'search-projects'}"><a rel="next" href="${href}">${label}</a></nav>`; }
function articlesSection(url,p,a){
  const r=url.searchParams.get('articles_after');
  const article=r==='r1'?'r1':r==='r2'?'r2':'r0';
  const rnext=r==='r1'?'r2':r==='r2'?null:'r1';
  const rHref=new URL('/search?q=x&scenario=articles',origin);if(p)rHref.searchParams.set('projects_after',p);if(a)rHref.searchParams.set('artists_after',a);if(rnext)rHref.searchParams.set('articles_after',rnext);rHref.hash='search-articles';
  return `<section id="search-articles"><ul data-load-more-region="search-articles"><li data-article="${article}"><a href="/article/${article}">${article}</a></li></ul>${rnext?`<nav class="pagination" data-load-more="search-articles"><a rel="next" href="${rHref.href}">Next page</a></nav>`:''}</section>`;
}
function searchHtml(url){
  const p=url.searchParams.get('projects_after'),a=url.searchParams.get('artists_after');
  const project=p==='p1'?'p1':p==='p2'?'p2':'p0';
  const artist=a==='a1'?'a1':a==='a2'?'a2':'a0';
  const pnext=p==='p1'?'p2':p==='p2'?null:'p1';
  const anext=a==='a1'?'a2':a==='a2'?null:'a1';
  const pHref=new URL('/search?q=x',origin);if(url.searchParams.has('scenario'))pHref.searchParams.set('scenario',url.searchParams.get('scenario'));if(pnext)pHref.searchParams.set('projects_after',pnext);if(a)pHref.searchParams.set('artists_after',a);pHref.hash='search-projects';
  const aHref=new URL('/search?q=x',origin);if(url.searchParams.has('scenario'))aHref.searchParams.set('scenario',url.searchParams.get('scenario'));if(p)aHref.searchParams.set('projects_after',p);if(anext)aHref.searchParams.set('artists_after',anext);aHref.hash='search-artists';
  return `<!doctype html><meta charset="utf-8"><link rel="stylesheet" href="/static/site.css"><main>
    <section id="search-projects"><div class="project-grid" data-load-more-region="search-projects"><article data-project-id="${project}"><a href="/project/${project}">${project}</a></article>${p?'<article data-project-id="duplicate"><script>window.__injected=true</script><a href="/project/duplicate">duplicate</a></article>':''}</div>${pnext?`<nav class="pagination" data-load-more="search-projects"><a rel="next" href="${pHref.href}">Next page</a></nav>`:''}</section>
    <section id="search-artists"><ul data-load-more-region="search-artists"><li><a href="/artist/${artist}">${artist}</a></li></ul>${anext?`<nav class="pagination" data-load-more="search-artists"><a rel="next" href="${aHref.href}">Next page</a></nav>`:''}</section>
    ${url.searchParams.get('scenario')==='articles'?articlesSection(url,p,a):''}
  </main><script src="/static/load_more.js" defer></script>`;
}
function marketHtml(url){
 const isNext=url.searchParams.has('after');
 const rows=isNext?[{id:'market-2',action:{action:'cancel',marketplace:'KT1Market',kind:'token',id:2}}]:[{id:'market-1',action:{action:'cancel',marketplace:'KT1Market',kind:'token',id:1}}];
 const records=rows.map(row=>`<article class="market-record" data-market-key="${row.id}" data-action='${JSON.stringify(row.action)}'><h2>Record ${row.id}</h2><button class="cancel-one" type="button">Review cancellation</button></article>`).join('');
 return `<!doctype html><meta charset="utf-8"><link rel="stylesheet" href="/static/site.css"><link rel="stylesheet" href="/static/market.css"><section id="market" data-address="${actor}"><p id="market-status" role="status"></p><button id="cancel-page">Cancel all</button><div class="market-records" data-load-more-region="market">${records}</div>${isNext?'':`<nav class="pagination" aria-label="Market records pages" data-load-more="market"><a class="button-link secondary-button" rel="next" href="/profile/${actor}/market?after=m1">Next page</a></nav>`}</section><dialog id="market-review"><div id="review-content"></div><button id="sign-market">Sign</button><button id="close-market">Close</button></dialog><script src="/static/load_more.js" defer></script><script type="module" src="/static/market.js"></script>`;
}
function editionHtml(url){
 const next=url.searchParams.has('after');
 const contract=next?'KT1SecondContract':'KT1FirstContract';
 const card=`<article class="artwork-card" data-token-id="7" data-edition-key="/token/${contract}/7"><a href="/token/${contract}/7">${contract}</a></article>`;
 return `<!doctype html><meta charset="utf-8"><div class="artwork-grid" data-load-more-region="editions">${card}</div>${next?'':`<nav class="pagination" data-load-more="editions"><a rel="next" href="/project/1?after=e1">Next page</a></nav>`}<script src="/static/load_more.js" defer></script>`;
}
function firstHtml(url){
 const after=url.searchParams.get('after');
 const row=after==='f2'?'f3':after==='f1'?'f2':'f1';
 const next=after==='f2'?'':`<a class="button-link" rel="next" href="/first?after=${after==='f1'?'f2':'f1'}">Next page</a>`;
 const first=after?'<a class="button-link" rel="first" href="/first">First page</a>':'';
 return `<!doctype html><meta charset="utf-8"><link rel="stylesheet" href="/static/site.css"><div class="project-grid" data-load-more-region="projects"><article data-project-id="${row}">${row}</article></div><nav class="pagination" data-load-more="projects">${first}${next}</nav><script src="/static/load_more.js" defer></script>`;
}
function limitedHtml(url){
 const rows=url.searchParams.has('after')?'<article data-project-id="last">last page</article>':Array.from({length:2400},(_,i)=>`<article data-project-id="row-${i}">row ${i}</article>`).join('');
 return `<!doctype html><meta charset="utf-8"><div class="project-grid" data-load-more-region="projects">${rows}</div>${url.searchParams.has('after')?'':`<nav class="pagination" data-load-more="projects"><a rel="next" href="/limited?after=last">Next page</a></nav>`}<script src="/static/load_more.js" defer></script>`;
}

const server=createServer(async(req,res)=>{
 const url=new URL(req.url,'http://127.0.0.1');
 if(url.pathname.startsWith('/static/')){const name=url.pathname.slice(8);res.setHeader('content-type',name.endsWith('.css')?'text/css':'text/javascript');res.end(files.get(name));return;}
 if(url.pathname==='/description'){res.setHeader('content-type','text/html');res.end(`<!doctype html><link rel="stylesheet" href="/static/site.css"><div data-read-more>${'<p>Recorded description.</p>'.repeat(12)}</div><script src="/static/read_more.js" defer></script>`);return;}
 if(url.pathname==='/api/auth/session'){res.setHeader('content-type','application/json');res.end(JSON.stringify({session:{address:actor,csrf:'csrf',expires_at:2147483647}}));return;}
 if(url.pathname==='/api/market/prepare'){let body='';for await(const part of req)body+=part;marketPrepare.push(JSON.parse(body));res.writeHead(409,{'content-type':'application/json'}).end(JSON.stringify({error:'Fixture refusal.'}));return;}
 if(url.pathname==='/search'){
  requests.push({path:url.pathname,projects_after:url.searchParams.get('projects_after'),artists_after:url.searchParams.get('artists_after')});
  if(url.searchParams.get('scenario')==='failure'&&url.searchParams.has('projects_after')&&retryCount++===0){res.writeHead(503,{'content-type':'text/html'}).end('<h1>Try again</h1>');return;}
  if(['unauthorized','nonhtml','missing-region'].includes(url.searchParams.get('scenario'))&&url.searchParams.has('projects_after')){const scenario=url.searchParams.get('scenario');res.writeHead(scenario==='unauthorized'?401:200,{'content-type':scenario==='nonhtml'?'application/json':'text/html'}).end('<h1>Native fallback page</h1>');return;}
  if(url.searchParams.get('scenario')==='timeout'&&url.searchParams.has('projects_after'))return;
  if(url.searchParams.get('scenario')==='redirect'&&url.searchParams.has('projects_after')){res.writeHead(302,{location:'/auth'}).end();return;}
  res.setHeader('content-type','text/html; charset=utf-8');res.end(searchHtml(url));return;
 }
 if(url.pathname==='/auth'){res.setHeader('content-type','text/html');res.end('<h1>Connect wallet</h1>');return;}
 if(url.pathname.startsWith('/profile/')){res.setHeader('content-type','text/html');res.end(marketHtml(url));return;}
 if(url.pathname==='/project/1'){res.setHeader('content-type','text/html');res.end(editionHtml(url));return;}
 if(url.pathname==='/first'){res.setHeader('content-type','text/html');res.end(firstHtml(url));return;}
 if(url.pathname==='/limited'){res.setHeader('content-type','text/html');res.end(limitedHtml(url));return;}
 res.writeHead(404).end();
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const origin=`http://127.0.0.1:${server.address().port}`;
const browser=await chromium.launch({executablePath:process.env.SEEDED_CHROMIUM||'/usr/bin/chromium',headless:true});
let checks=0;
try{
 let context=await browser.newContext({javaScriptEnabled:false});
 let page=await context.newPage();await page.goto(`${origin}/search?q=x`);
 assert.equal(await page.locator('a[rel="next"]').first().textContent(),'Next page');
 assert.match(await page.locator('a[rel="next"]').first().getAttribute('href'),/projects_after=p1/);
 await page.goto(`${origin}/first?after=f1`);
 assert.equal(await page.locator('a[rel="first"]').isVisible(),true,'without JavaScript First page stays');
 assert.equal(await page.locator('a[rel="next"]').textContent(),'Next page');
 await context.close();checks++;

 // With Load more, earlier results stay on screen: First page is hidden, also on a page opened at a cursor.
 context=await browser.newContext();page=await context.newPage();await page.goto(`${origin}/first?after=f1`);
 await page.waitForFunction(()=>document.querySelector('a[rel="first"]').hidden);
 await page.getByRole('link',{name:'Load more'}).click();
 await page.locator('[data-project-id="f3"]').waitFor();
 assert.equal(await page.locator('a[rel="first"]').isVisible(),false);
 await page.goto(`${origin}/first?after=f2`);
 assert.equal(await page.locator('a[rel="first"]').isVisible(),true,'the last page, with no Load more, keeps First page');
 await context.close();checks++;

 // Search articles page with their own cursor; other sections' cursors stay put.
 context=await browser.newContext();page=await context.newPage();await page.goto(`${origin}/search?q=x&scenario=articles&projects_after=p1`);
 for(const id of ['r1','r2']){await page.locator('#search-articles').getByRole('link',{name:'Load more'}).click();await page.locator(`#search-articles [data-article="${id}"]`).waitFor();}
 assert.deepEqual(await page.locator('#search-articles [data-article]').evaluateAll(items=>items.map(item=>item.dataset.article)),['r0','r1','r2']);
 assert.equal(new URL(page.url()).searchParams.get('projects_after'),'p1');
 assert.equal(new URL(page.url()).searchParams.get('articles_after'),'r2');
 await context.close();checks++;

 context=await browser.newContext();page=await context.newPage();await page.goto(`${origin}/description`);
 const description=page.locator('[data-read-more]');
 await page.getByRole('button',{name:'Read more',exact:true}).click();
 assert.equal(await page.getByRole('button',{name:'Read less',exact:true}).getAttribute('aria-expanded'),'true');
 assert.equal(await description.evaluate(node=>node.style.maxHeight),'');
 await page.getByRole('button',{name:'Read less',exact:true}).click();
 assert.equal(await page.getByRole('button',{name:'Read more',exact:true}).getAttribute('aria-expanded'),'false');
 assert.equal(await description.locator('p').count(),12);
 await context.close();checks++;

 context=await browser.newContext();page=await context.newPage();await page.goto(`${origin}/search?q=x`);
 const baselineRequests=requests.length;
 page.on('pageerror',error=>console.error('browser page error:',error));
 page.on('console',message=>{if(message.type()==='error')console.error('browser console:',message.text());});
 const modified=await page.locator('#search-projects a[rel="next"]').evaluate(link=>{const href=link.href,target=link.target;link.href='about:blank';link.target='_blank';const event=new MouseEvent('click',{bubbles:true,cancelable:true,ctrlKey:true});Object.defineProperty(event,'button',{value:0});const allowed=link.dispatchEvent(event);link.href=href;link.target=target;return allowed;});
 assert.equal(modified,true,'modified clicks keep normal browser navigation');
 await page.getByRole('link',{name:'Load more'}).first().dblclick();
 await page.locator('#search-projects [data-project-id="p1"]').waitFor();
 assert.equal(await page.locator('#search-projects [data-project-id="duplicate"]').count(),1);
 assert.equal(await page.evaluate(()=>window.__injected||false),false);
 assert.equal(await page.locator('#search-projects script').count(),0);
 assert.equal(requests.length-baselineRequests,1,'double-click must make one request');
 assert.match(new URL(page.url()).searchParams.get('projects_after'),/p1/);
 await page.getByRole('link',{name:'Load more'}).last().click();
 await page.locator('#search-artists li a[href="/artist/a1"]').waitFor();
 const merged=new URL(page.url());assert.equal(merged.searchParams.get('projects_after'),'p1');assert.equal(merged.searchParams.get('artists_after'),'a1');
 assert.equal(requests.at(-1).projects_after,'p1','loading a second section preserves the first section cursor');
 await page.evaluate(()=>history.replaceState({},'',`${location.pathname}?q=x`));
 await page.getByRole('link',{name:'Load more'}).first().click();
 await page.locator('#search-projects [data-project-id="p2"]').waitFor();
 const isolated=requests.at(-1);assert.equal(isolated.projects_after,'p2');assert.equal(isolated.artists_after,null,'stale cursor is removed after a First reset');
 assert.equal(new URL(page.url()).searchParams.get('artists_after'),null);
 assert.equal(await page.locator('#search-projects a[rel="next"]').count(),0,'last page hides its control');
 assert.match(await page.locator('#search-projects nav [data-load-more-status]').textContent(),/End of results/);
 await context.close();checks++;

 context=await browser.newContext();page=await context.newPage();await page.goto(`${origin}/project/1`);
 await page.locator('nav[data-load-more] a[rel="next"]').click();
 await page.locator('[data-edition-key="/token/KT1SecondContract/7"]').waitFor();
 assert.equal(await page.locator('.artwork-card').count(),2,'edition keys distinguish equal token IDs on different contracts');
 await context.close();checks++;

 context=await browser.newContext();page=await context.newPage();await page.goto(`${origin}/limited`);
 assert.equal(await page.locator('.project-grid > article').count(),2400);
 await page.locator('nav[data-load-more] a[rel="next"]').click();
 await page.locator('nav:not([data-load-more]) a[rel="next"]').waitFor();
 assert.equal(await page.locator('.project-grid > article').count(),2400,'overflow page is not appended beyond the bound');
 assert.match(await page.locator('nav [data-load-more-status]').textContent(),/display limit/);
 assert.equal(await page.locator('nav a[rel="next"]').textContent(),'Next page');
 await page.locator('nav a[rel="next"]').click();await page.locator('.project-grid [data-project-id="last"]').waitFor();
 assert.equal(new URL(page.url()).searchParams.get('after'),'last','native Next fallback opens the cursor page');
 await context.close();checks++;

 context=await browser.newContext();page=await context.newPage();await page.goto(`${origin}/search?q=x&scenario=failure`);
 const retryLink=page.locator('#search-projects a[rel="next"]');const original=await retryLink.getAttribute('href');
 await retryLink.click();await page.locator('[data-load-more-status]').filter({hasText:'Could not load more'}).waitFor();
 assert.equal(await retryLink.getAttribute('href'),original);assert.equal(await retryLink.isVisible(),true);
 await retryLink.click();await page.locator('#search-projects [data-project-id="p1"]').waitFor();await context.close();checks++;

 context=await browser.newContext();page=await context.newPage();await page.goto(`${origin}/search?q=x&scenario=redirect`);
 await page.locator('#search-projects a[rel="next"]').click();
 await page.locator('[data-load-more-status]').filter({hasText:'redirected'}).waitFor();
 assert.equal(await page.locator('#search-projects nav[data-load-more]').count(),0);await page.locator('#search-projects a[rel="next"]').click();await page.waitForURL(url=>url.pathname==='/auth');await context.close();checks++;

 context=await browser.newContext();page=await context.newPage();await page.goto(`${origin}/search?q=x&scenario=timeout`);
 await page.locator('#search-projects a[rel="next"]').click();
 await page.locator('[data-load-more-status]').filter({hasText:'timed out'}).waitFor({timeout:15000});
 assert.equal(await page.locator('#search-projects nav[data-load-more]').count(),1);assert.equal(await page.locator('#search-projects a[rel="next"]').count(),1);await context.close();checks++;

 context=await browser.newContext();page=await context.newPage();await page.goto(`${origin}/search?q=x&scenario=timeout`);
 await page.locator('#search-projects a[rel="next"]').click();await page.waitForFunction(()=>document.querySelector('nav[aria-busy="true"]'));
 await page.evaluate(()=>window.dispatchEvent(new Event('pagehide')));
 await page.evaluate(()=>window.dispatchEvent(new Event('pageshow')));
 assert.equal(await page.locator('#search-projects a[rel="next"]').getAttribute('aria-disabled'),null);
 assert.equal(await page.locator('nav[aria-busy="true"]').count(),0);await context.close();checks++;

 context=await browser.newContext();page=await context.newPage();await page.goto(`${origin}/search?q=x&scenario=timeout`);
 await page.locator('#search-projects a[rel="next"]').click();await page.waitForFunction(()=>document.querySelector('nav[aria-busy="true"]'));
 await page.evaluate(()=>{history.pushState({},'','/search?q=x&temporary=1');history.back();});
 await page.locator('#search-projects nav').filter({hasText:'page position changed'}).waitFor();
 assert.equal(await page.locator('#search-projects [data-project-id="p1"]').count(),0);
 assert.equal(await page.locator('#search-projects a[rel="next"]').getAttribute('aria-disabled'),null);await context.close();checks++;

 requests=[];marketPrepare=[];context=await browser.newContext();page=await context.newPage();await page.goto(`${origin}/profile/${actor}/market`);
 await page.locator('nav[data-load-more="market"] a[rel="next"]').click();await page.locator('.market-record').filter({hasText:'Record market-2'}).waitFor();
 assert.equal(await page.locator('nav[data-load-more="market"] a[rel="next"]').count(),0);
 await page.locator('#cancel-page').click();
 await page.waitForFunction(()=>document.querySelector('#market-status').textContent.includes('Fixture refusal'));
 assert.equal(marketPrepare.length,1);assert.equal(marketPrepare[0].actions.length,2,'cancel-all sees rows appended by Load more');
 await context.close();checks++;
 for(const scenario of ['unauthorized','nonhtml','missing-region']) {
  context=await browser.newContext();page=await context.newPage();await page.goto(`${origin}/search?q=x&scenario=${scenario}`);await page.locator('#search-projects a[rel="next"]').click();await page.waitForFunction(()=>!document.querySelector('#search-projects nav[data-load-more]'));await page.locator('#search-projects a[rel="next"]').click();await page.waitForURL('**projects_after=p1*');assert.equal(new URL(page.url()).searchParams.get('projects_after'),'p1');await context.close();checks++;
 }
 await mkdir(resolve(root,'tmp'),{recursive:true});
 const fixtureDir=await mkdtemp(resolve(root,'tmp/catalogue-browser-'));
 const fixture=spawn(resolve(process.env.CARGO_TARGET_DIR||resolve(root,'target'),'debug/examples/proxy_fixture'),[fixtureDir],{cwd:root,stdio:['ignore','pipe','pipe']});
 let fixtureError='';fixture.stderr.on('data',chunk=>{fixtureError=(fixtureError+chunk.toString()).slice(-2000);});
 const exited=once(fixture,'exit');
 try {
  const ports=await new Promise((resolve,reject)=>{let line='';const timer=setTimeout(()=>reject(new Error('Catalogue fixture startup timed out')),30000);fixture.once('error',error=>{clearTimeout(timer);reject(error);});fixture.once('exit',()=>{clearTimeout(timer);reject(new Error('Catalogue fixture exited: '+fixtureError));});fixture.stdout.on('data',chunk=>{line+=chunk;if(line.includes('\n')){clearTimeout(timer);try{resolve(JSON.parse(line.split('\n')[0]));}catch(error){reject(error);}}});});
  context=await browser.newContext({extraHTTPHeaders:{'x-forwarded-for':'192.0.2.23'}});page=await context.newPage();
  await page.goto(`http://127.0.0.1:${ports.app_port}/projects?sort=oldest`);
  assert.equal(await page.locator('[data-project-id]').count(),24);
  await page.getByRole('link',{name:'Load more',exact:true}).click();
  await page.waitForFunction(()=>document.querySelectorAll('[data-project-id]').length===25);
  assert(new URL(page.url()).searchParams.has('after'));
  assert.equal(await page.locator('nav[data-load-more] a[rel="next"]').count(),0);
  await page.reload();assert.equal(await page.locator('[data-project-id]').count(),1);
  await context.close();checks++;
  const seed='ooHBH4Q9m5BTSExqbvNCtpWDbdEqZqTXK4Ewh1HMU66V5PhMUE7';
  const tokenPath='/token/KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr/133459';
  context=await browser.newContext({viewport:{width:1440,height:900},extraHTTPHeaders:{'x-forwarded-for':'192.0.2.24'}});
  page=await context.newPage();
  await page.goto(`http://127.0.0.1:${ports.app_port}${tokenPath}`);
  assert.equal(await page.locator('.nft-seed code').textContent(),seed);
  const artBox=await page.locator('.player-stage').boundingBox();
  assert(artBox.width>800,'token artwork retains the original wide desktop stage');
  const popupEvent=page.waitForEvent('popup');
  await page.getByRole('link',{name:'Full screen'}).click();
  const popup=await popupEvent;await popup.waitForLoadState('domcontentloaded');
  assert.equal(new URL(popup.url()).searchParams.get('seed'),seed);
  assert.equal(await popup.evaluate(()=>window.opener),null);
  await popup.locator('iframe[data-loading=false]').waitFor({timeout:45000});
  assert.equal(await popup.locator('header,footer').count(),0);
  const frame=popup.locator('iframe');
  assert.equal(await frame.getAttribute('sandbox'),'allow-scripts');
  const src=new URL(await frame.getAttribute('src'));
  assert.equal(src.searchParams.get('fxhash'),seed);
  assert.match(src.searchParams.get('__seeded_instance'),/^[0-9a-f]{32}$/);
  assert.notEqual(src.origin,new URL(popup.url()).origin);
  const box=await frame.boundingBox();assert.equal(box.width,1440);assert.equal(box.height,900);
  const artFrame=popup.frames().find(f=>f.url()===src.href);
  assert.equal(await artFrame.evaluate(()=>{try{return Boolean(parent.document)}catch{return false}}),false);
  await popup.locator('[data-action=play]').focus();
  await popup.locator('[data-action=play]').click();
  await popup.locator('iframe[data-loading=false]').waitFor({timeout:45000});
  assert.notEqual(new URL(await frame.getAttribute('src')).searchParams.get('__seeded_instance'),src.searchParams.get('__seeded_instance'));
  await popup.setViewportSize({width:390,height:844});
  const phone=await frame.boundingBox();assert.equal(phone.width,390);assert.equal(phone.height,844);
  await popup.locator('[data-action=stop]').focus();await popup.locator('[data-action=stop]').click();
  // Stop navigates the old frame to about:blank and removes it once it has unloaded (at most one second).
  await popup.waitForFunction(()=>document.querySelectorAll('iframe').length===0,null,{timeout:2000});
  await popup.close();await context.close();checks++;
  // One header search on every page; the Artists tab narrows it to artists and collectors.
  const app=`http://127.0.0.1:${ports.app_port}`;
  context=await browser.newContext({extraHTTPHeaders:{'x-forwarded-for':'192.0.2.25'}});page=await context.newPage();
  const forms=[];
  for(const path of ['/projects','/artists','/collectors',tokenPath]){
   await page.goto(app+path);
   forms.push(await page.locator('#global-search-form').evaluate(form=>({action:form.getAttribute('action'),method:form.method,label:form.querySelector('label').textContent,placeholder:form.elements.q.placeholder,scope:form.elements.scope?.value??null})));
  }
  assert.deepEqual(forms.map(({scope,...form})=>form),Array(4).fill({action:'/search',method:'get',label:'Search the archive',placeholder:'Search'}));
  assert.deepEqual(forms.map(form=>form.scope),[null,'people','people',null]);
  // Each search below uses its own client address so the page loads above don't spend its rate-limit burst.
  await context.close();context=await browser.newContext({extraHTTPHeaders:{'x-forwarded-for':'192.0.2.26'}});page=await context.newPage();
  await page.goto(`${app}/artists`);await page.locator('#global-search').fill('zancan');await page.locator('#global-search').press('Enter');
  await page.waitForURL(url=>url.pathname==='/search');
  assert.equal(new URL(page.url()).search,'?q=zancan&scope=people');
  assert.equal(await page.locator('#global-search').inputValue(),'zancan');
  assert.equal(await page.locator('main > :not(link):not(h1)').first().getAttribute('class'),'search-result-nav','found types sit under the header search');
  const people=await page.locator('.search-result-nav a').evaluateAll(links=>links.map(link=>link.getAttribute('href')));
  assert(people.includes('#search-artists')&&people.every(href=>['#search-artists','#search-collectors'].includes(href)),people.join());
  assert.equal(await page.locator('#search-projects').count(),0);
  await page.goto(`${app}/artists?q=zancan`);
  assert.equal(new URL(page.url()).pathname+new URL(page.url()).search,'/search?q=zancan&scope=people','old in-place links open the same search');
  await context.close();context=await browser.newContext({extraHTTPHeaders:{'x-forwarded-for':'192.0.2.27'}});page=await context.newPage();
  await page.goto(`${app}/projects`);await page.locator('#global-search').fill('zancan');await page.locator('#global-search').press('Enter');
  await page.waitForURL(url=>url.pathname==='/search');
  assert.equal(new URL(page.url()).search,'?q=zancan');
  assert.equal(await page.locator('.search-result-nav a').first().getAttribute('href'),'#search-projects');
  assert.equal(await page.getByText('Clear search').count(),0);
  await context.close();checks++;
 } finally {
  if(fixture.exitCode===null)fixture.kill('SIGTERM');
  const kill=setTimeout(()=>fixture.kill('SIGKILL'),10000);await exited;clearTimeout(kill);
  await rm(fixtureDir,{recursive:true,force:true});
 }
 console.log(`${checks} Paging, descriptions and full-window browser scenarios passed`);
}finally{await browser.close();await new Promise(resolve=>server.close(resolve));}
