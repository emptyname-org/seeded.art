// Theme resolution, the icon toggle and its persistence, on the real base template and stylesheet.
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {readFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
import {dirname,resolve} from 'node:path';

const root=resolve(dirname(fileURLToPath(import.meta.url)),'../..');
const require=createRequire(import.meta.url);
const {chromium}=require(resolve(process.env.SEEDED_BROWSER_TOOLS||resolve(root,'tmp/browser_tools/node_modules'),'playwright-core'));
const files={'theme.js':await readFile(resolve(root,'assets/static/theme.js')),'site.css':await readFile(resolve(root,'assets/static/site.css'))};
// The header search is a partial of the base template; inline it so the header is complete.
const include='{% include "search_form.html" %}';
const base=await readFile(resolve(root,'templates/base.html'),'utf8');
assert(base.includes(include));
const template=base.replace(include,await readFile(resolve(root,'templates/search_form.html'),'utf8'));
const script='<script src="/static/theme.js"></script>';
assert(template.includes(script));
// Strip Askama tags; a probe right after theme.js records the theme it resolved before the stylesheet.
const page_html=template.replace(/\{%.*?%\}|\{\{.*?\}\}/gs,'')
  .replace(script,`${script}<script>window.__theme_at_head=document.documentElement.dataset.theme||null</script>`);
const LIGHT='rgb(247, 247, 242)',DARK='rgb(36, 38, 34)',KEY='seeded-theme';

const server=createServer((req,res)=>{
  const url=new URL(req.url,'http://127.0.0.1');
  if(url.pathname.startsWith('/static/')){
    const name=url.pathname.slice(8);
    res.setHeader('content-type',name.endsWith('.css')?'text/css':'text/javascript');
    res.end(files[name]??'');return;
  }
  // The page loads the usage-stats script; production serves it from GoatCounter.
  if(url.pathname==='/stats/count.js'){res.setHeader('content-type','text/javascript');res.end('');return;}
  res.setHeader('content-type','text/html; charset=utf-8');res.end(page_html);
});
const origin=await new Promise(done=>server.listen(0,'127.0.0.1',()=>done(`http://127.0.0.1:${server.address().port}`)));
const browser=await chromium.launch({executablePath:process.env.SEEDED_CHROMIUM||'/usr/bin/chromium',headless:true});
const state=page=>page.evaluate(()=>{
  const button=document.querySelector('.theme-toggle');
  return {theme:document.documentElement.dataset.theme||null,head:window.__theme_at_head??null,
    bg:getComputedStyle(document.body).backgroundColor,scheme:getComputedStyle(document.documentElement).colorScheme,
    pressed:button.getAttribute('aria-pressed'),label:button.getAttribute('aria-label'),tip:button.title,text:button.textContent.trim(),
    moon:getComputedStyle(button.querySelector('.icon-moon')).display!=='none',sun:getComputedStyle(button.querySelector('.icon-sun')).display!=='none',shown:getComputedStyle(button).display!=='none'};
});
const stored=page=>page.evaluate(key=>localStorage.getItem(key),KEY);
let passed=0;
async function scenario(name,options,run){
  const context=await browser.newContext(options);
  const errors=[];
  context.on('weberror',error=>errors.push(error.error().message));
  try{await run(context);assert.deepEqual(errors,[],name);passed++;console.log(`PASS ${name}`);}
  finally{await context.close();}
}
async function open(context){const page=await context.newPage();await page.goto(origin);return page;}

try{
  await scenario('light OS default without a saved choice',{colorScheme:'light'},async context=>{
    const page=await open(context);
    assert.deepEqual(await state(page),{theme:null,head:null,bg:LIGHT,scheme:'light dark',pressed:'false',shown:true,label:'Switch to dark theme',tip:'Switch to dark theme',text:'',moon:true,sun:false});
  });
  await scenario('dark OS default without a saved choice',{colorScheme:'dark'},async context=>{
    const page=await open(context);
    assert.deepEqual(await state(page),{theme:null,head:null,bg:DARK,scheme:'light dark',pressed:'true',shown:true,label:'Switch to light theme',tip:'Switch to light theme',text:'',moon:false,sun:true});
  });
  await scenario('toggle inverts, persists and applies before the stylesheet',{colorScheme:'light'},async context=>{
    const page=await open(context);
    await page.locator('.theme-toggle').click();
    assert.equal(await stored(page),'dark');
    assert.deepEqual(await state(page),{theme:'dark',head:null,bg:DARK,scheme:'dark',pressed:'true',shown:true,label:'Switch to light theme',tip:'Switch to light theme',text:'',moon:false,sun:true});
    await page.reload();
    assert.deepEqual(await state(page),{theme:'dark',head:'dark',bg:DARK,scheme:'dark',pressed:'true',shown:true,label:'Switch to light theme',tip:'Switch to light theme',text:'',moon:false,sun:true});
    await page.locator('.theme-toggle').focus();
    await page.keyboard.press('Enter');
    assert.equal(await stored(page),'light');
    assert.deepEqual(await state(page),{theme:'light',head:'dark',bg:LIGHT,scheme:'light',pressed:'false',shown:true,label:'Switch to dark theme',tip:'Switch to dark theme',text:'',moon:true,sun:false});
  });
  await scenario('saved light overrides a dark OS',{colorScheme:'dark'},async context=>{
    await context.addInitScript(key=>{if(location.protocol==='http:')localStorage.setItem(key,'light');},KEY);
    const page=await open(context);
    assert.deepEqual(await state(page),{theme:'light',head:'light',bg:LIGHT,scheme:'light',pressed:'false',shown:true,label:'Switch to dark theme',tip:'Switch to dark theme',text:'',moon:true,sun:false});
  });
  await scenario('invalid saved value falls back to the OS',{colorScheme:'dark'},async context=>{
    await context.addInitScript(key=>{if(location.protocol==='http:')localStorage.setItem(key,'sepia');},KEY);
    const page=await open(context);
    assert.deepEqual(await state(page),{theme:null,head:null,bg:DARK,scheme:'light dark',pressed:'true',shown:true,label:'Switch to light theme',tip:'Switch to light theme',text:'',moon:false,sun:true});
  });
  await scenario('other tabs follow storage changes',{colorScheme:'light'},async context=>{
    const first=await open(context),second=await open(context);
    await first.locator('.theme-toggle').click();
    await second.waitForFunction(()=>document.documentElement.dataset.theme==='dark');
    assert.equal((await state(second)).pressed,'true');
    await first.evaluate(()=>localStorage.clear());
    await second.waitForFunction(()=>!document.documentElement.dataset.theme);
    assert.deepEqual(await state(second),{theme:null,head:null,bg:LIGHT,scheme:'light dark',pressed:'false',shown:true,label:'Switch to dark theme',tip:'Switch to dark theme',text:'',moon:true,sun:false});
  });
  await scenario('OS changes update an unset theme',{colorScheme:'light'},async context=>{
    const page=await open(context);
    await page.emulateMedia({colorScheme:'dark'});
    await page.waitForFunction(()=>document.querySelector('.theme-toggle').getAttribute('aria-pressed')==='true');
    assert.equal((await state(page)).bg,DARK);
    await page.emulateMedia({colorScheme:'light'});
    await page.waitForFunction(()=>document.querySelector('.theme-toggle').getAttribute('aria-pressed')==='false');
    assert.equal((await state(page)).bg,LIGHT);
  });
  await scenario('blocked storage still toggles for the page',{colorScheme:'dark'},async context=>{
    await context.addInitScript(()=>Object.defineProperty(window,'localStorage',{get(){throw new DOMException('blocked','SecurityError');}}));
    const page=await open(context);
    assert.equal((await state(page)).pressed,'true');
    await page.locator('.theme-toggle').click();
    assert.deepEqual(await state(page),{theme:'light',head:null,bg:LIGHT,scheme:'light',pressed:'false',shown:true,label:'Switch to dark theme',tip:'Switch to dark theme',text:'',moon:true,sun:false});
  });
  await scenario('toggle is hidden without JavaScript and the OS theme applies',{colorScheme:'dark',javaScriptEnabled:false},async context=>{
    const page=await open(context);
    assert.equal(await page.locator('.theme-toggle').isVisible(),false);
    assert.equal(await page.evaluate(()=>getComputedStyle(document.body).backgroundColor),DARK);
  });
  for(const width of [320,360,390]){
    await scenario(`phone header fits the toggle at ${width}px`,{viewport:{width,height:700},isMobile:true,hasTouch:true,deviceScaleFactor:2,colorScheme:'light'},async context=>{
      const page=await open(context);
      const boxes=await Promise.all(['.site-header > .wordmark','[data-auth-link]','.primary-nav a:last-child','.theme-toggle','#global-search-form'].map(selector=>page.locator(selector).boundingBox()));
      const box=boxes[3];
      assert(boxes[4].x>=0&&boxes[4].x+boxes[4].width<=width,`header search within ${width}px`);
      assert(box&&box.x>=0&&box.x+box.width<=width,`toggle within ${width}px: ${JSON.stringify(box)}`);
      assert(box.height>=32&&Math.abs(box.height-box.width)<1,'square icon button');
      assert(box.x+box.width<=boxes[1].x,'toggle left of the account control');
      assert(box.y<boxes[1].y+boxes[1].height&&boxes[1].y<box.y+box.height,'toggle level with the account control');
      const overlaps=(a,b)=>a.x<b.x+b.width&&b.x<a.x+a.width&&a.y<b.y+b.height&&b.y<a.y+a.height;
      for(let i=0;i<boxes.length;i++)for(let j=i+1;j<boxes.length;j++)assert(!overlaps(boxes[i],boxes[j]),`header controls ${i} and ${j} overlap at ${width}px`);
      assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth),width);
      await page.locator('.theme-toggle').tap();
      assert.equal((await state(page)).theme,'dark');
    });
  }
  console.log(`theme browser checks: ${passed} passed`);
}finally{
  await browser.close();
  await new Promise(done=>{server.close(done);server.closeAllConnections();});
}
