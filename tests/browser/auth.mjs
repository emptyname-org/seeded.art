// Offline SDK/HTTP fixtures test the real browser module. auth_routes.rs verifies real signatures.
import assert from 'node:assert/strict';
import {createServer} from 'node:http';
import {readFile,mkdir,writeFile} from 'node:fs/promises';
import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
import {resolve,dirname} from 'node:path';
const root=resolve(dirname(fileURLToPath(import.meta.url)),'../..');
const require=createRequire(import.meta.url);
const {chromium}=require(resolve(process.env.SEEDED_BROWSER_TOOLS||resolve(root,'tmp/browser_tools/node_modules'),'playwright-core'));
const source=await readFile(resolve(root,'assets/static/auth.js'));
const bundle=await readFile(resolve(root,'assets/static/beacon/client.js'));
assert(!/\beval\s*\(/.test(bundle.toString()),'built SDK does not need unsafe-eval');
const address='tz1YJoT4wT65hm9yrrCQU7Vr2CtzTMhBmfQR';
const other='tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL';
const calls=[];
let mode='success',session=null,sdkLoads=0;
let dedicatedSignInPage=false,dedicatedNext='';
let holdNextSession=false,releaseSession=null;
const stub=`
export const BeaconEvent={ACTIVE_ACCOUNT_SET:'active'};
export const NetworkType={MAINNET:'mainnet'};
export class DAppClient {
 constructor(config){if(config.network?.type!=='mainnet'||'preferredNetwork' in config)throw new Error('Use constructor network');window.__sdkConfig=config;window.__instances=(window.__instances||0)+1;}
 async subscribeToEvent(event,callback){window.__accountChange=callback;}
 async getActiveAccount(){return window.__account||null;}
 async requestPermissions(input){
  if(input&&'network' in input)throw new Error('network is not a permission input');
  window.__permissions=(window.__permissions||0)+1;
  if(window.__mode==='pair-late')await new Promise(resolve=>{window.__releasePermission=resolve;});
  window.__account={address:'${address}',publicKey:'fixture-public-key',network:{type:'mainnet'}};
 }
 async requestSignPayload(request){
  window.__signed=request;
  if(window.__mode==='reject')throw new Error('[ABORTED_ERROR]:The action was aborted by the user.');
  if(window.__mode==='wallet-error')throw new Error('[UNKNOWN_ERROR]:Wallet fixture failure.');
  if(window.__mode==='late')await new Promise(resolve=>{window.__releaseSignature=resolve;});
  return {signature:'fixture-signature'};
 }
 async requestOperation(request){window.__operation=request;return {transactionHash:'fixture-operation'};}
 async clearActiveAccount(){window.__account=null;}
 async destroy(){window.__destroyed=(window.__destroyed||0)+1;}
}
`;
const html=()=>`<!doctype html><html><head><meta charset="utf-8"><script type="module" src="/static/auth.js"></script></head><body>
<a data-auth-link href="/auth">Connect wallet</a><section${dedicatedSignInPage?' data-auth-page':''}${dedicatedNext?` data-auth-next="${dedicatedNext.replaceAll('&','&amp;').replaceAll('"','&quot;')}"`:''}><p data-auth-status role="status"></p>
<button data-auth-connect>Connect wallet</button><button data-auth-cancel hidden>Cancel</button>
<div data-auth-connected hidden><a data-auth-profile href="/auth">Profile</a><button data-auth-logout>Sign out</button><button data-auth-logout-all>Sign out everywhere</button></div>
${dedicatedSignInPage?'':`<form data-auth-profile-form><input name="name"><textarea name="bio"></textarea><textarea name="links"></textarea><input type="checkbox" name="hide_bio"><button type="submit">Save profile</button></form>`}</section></body></html>`;
const server=createServer(async(request,response)=>{
 const path=new URL(request.url,'http://localhost').pathname;
 const relays=Array.from({length:8},(_,i)=>`https://beacon-node-${i+1}.octez.io`).join(' ');
 response.setHeader('Content-Security-Policy',`default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self' ${relays}; img-src 'self' data:; object-src 'none'`);
 response.setHeader('Cache-Control','no-store');
 if(path==='/static/auth.js'){response.setHeader('Content-Type','text/javascript');response.end(source);return;}
 if(path==='/static/beacon/client.js'){sdkLoads++;response.setHeader('Content-Type','text/javascript');response.end(mode==='real-sdk'?bundle:stub);return;}
 if(path==='/oauth/authorize'){response.setHeader('Content-Type','text/html');response.end('<!doctype html><html><body><h1>Authorize page</h1></body></html>');return;}
 if(path==='/auth'){response.setHeader('Content-Type','text/html');response.end(html());return;}
 if(path===`/profile/${address}`){response.setHeader('Content-Type','text/html');response.end('<!doctype html><html><body><h1>Profile page</h1></body></html>');return;}
 let body='';for await(const chunk of request){body+=chunk;if(body.length>20000){response.writeHead(413).end();return;}}
 const value=body?JSON.parse(body):null;
 calls.push({path,method:request.method,csrf:request.headers['x-csrf-token'],origin:request.headers.origin,body:value});
 response.setHeader('Content-Type','application/json');
 if(path==='/api/auth/session'){
  const captured=JSON.stringify({session});
  if(holdNextSession){holdNextSession=false;releaseSession=()=>response.end(captured);return;}
  response.end(captured);return;
 }
 if(path==='/api/auth/challenge'){response.end(JSON.stringify({message:'Test sign-in only',payload:'05010000000474657374',csrf:'challenge-csrf'}));return;}
 if(path==='/api/auth/verify'){
  assert.equal(request.headers['x-csrf-token'],'challenge-csrf');
  session={address,csrf:'session-csrf',expires_at:2147483647};
  response.setHeader('Set-Cookie','test_session=opaque; HttpOnly; SameSite=Strict; Path=/');
  response.end(JSON.stringify({session}));return;
 }
 if(path==='/api/auth/profile'){
  assert.equal(request.headers['x-csrf-token'],'session-csrf');
  if(mode==='timeout-profile')return;
  if(mode==='malformed-profile'){response.end('{incomplete');return;}
  if(mode==='invalid-profile'){response.writeHead(400).end(JSON.stringify({error:'Invalid name <img src=x onerror=alert(1)>'}));return;}
  response.end(JSON.stringify({saved:true}));return;
 }
 if(path==='/api/auth/logout'){
  if(mode==='switch-logout-fails'){response.writeHead(503).end(JSON.stringify({error:'Temporarily unavailable'}));return;}
  assert.equal(request.headers['x-csrf-token'],'session-csrf');session=null;response.writeHead(204).end();return;
 }
 if(path.startsWith('/api/market/')){
  if(mode==='market-timeout')return;
  response.writeHead(503).end(JSON.stringify({error:'Temporarily unavailable',code:'Busy'}));return;
 }
 response.writeHead(404).end('{}');
});
const origin=await new Promise(resolve=>server.listen(0,'127.0.0.1',()=>resolve(`http://127.0.0.1:${server.address().port}`)));
const browser=await chromium.launch({executablePath:process.env.SEEDED_CHROMIUM||'/usr/bin/chromium',headless:true});
let passed=0;
async function scenario(name,scenarioMode,run,waitInitial=true,signInPage=false,next='',signedIn=false){
 mode=scenarioMode;session=signedIn?{address,csrf:'session-csrf',expires_at:2147483647}:null;calls.length=0;sdkLoads=0;holdNextSession=!waitInitial;releaseSession=null;
 dedicatedSignInPage=signInPage;dedicatedNext=next;
 const context=await browser.newContext();
 const heldRoutes=[];
 await context.route('**/*',route=>{
  if(route.request().url().startsWith(origin))return route.continue();
  if(scenarioMode==='real-sdk'){heldRoutes.push(route);return;}
  return route.abort();
 });
 const page=await context.newPage();
 const errors=[];page.on('pageerror',error=>errors.push(error.message));
 page.on('console',message=>{if(message.type()==='error'&&/ReferenceError|TypeError|Uncaught|no longer accepted/.test(message.text()))errors.push(message.text());});
 await page.addInitScript(mode=>{window.__mode=mode;},scenarioMode);
 try{
  await page.goto(origin+'/auth');
  if(waitInitial)await page.waitForFunction(()=>document.querySelector('[data-auth-status]').textContent==='Not connected.');
  // Only the dedicated sign-in page fetches the SDK early, for a signed-out visitor; other pages wait for an action.
  if(dedicatedSignInPage&&!signedIn&&waitInitial){for(let i=0;i<100&&sdkLoads<1;i++)await page.waitForTimeout(20);assert.equal(sdkLoads,1,'sign-in page fetches the SDK once, early');}
  else assert.equal(sdkLoads,0,'no SDK fetched before an explicit action');
  await run(page);
  assert.deepEqual(errors,[],name);
  passed++;console.log(`PASS ${name}`);
 }catch(error){
  if(scenarioMode==='real-sdk'){
   const state=await page.evaluate(()=>({status:document.querySelector('[data-auth-status]').textContent,dialog:document.querySelector('beacon-alert')?.shadowRoot?.querySelector('.alert-body')?.textContent}));
   await writeFile(resolve(root,'tmp/auth/chooser-failure.json'),JSON.stringify({state,errors,requests:heldRoutes.map(route=>route.request().url())},null,2));
   await page.screenshot({path:resolve(root,'tmp/auth/chooser-failure.png')});
  }
  throw error;
 }finally{releaseSession?.();await context.close();}
}
async function signin(page){
 await page.locator('[data-auth-connect]').click();
 await page.waitForFunction(()=>document.querySelector('[data-auth-status]').textContent.startsWith('Signed in as'));
}
try{
 await scenario('dedicated sign-in page redirects after successful connection','success',async page=>{
  await page.locator('[data-auth-connect]').click();
  await page.waitForURL(`${origin}/profile/${address}`);
  assert.equal(session.address,address);
  assert(calls.some(call=>call.path==='/api/auth/verify'));
  assert.equal(await page.locator('h1').textContent(),'Profile page');
 },true,true);
 const authorize='/oauth/authorize?response_type=code&client_id=forum&state=a%2Fb';
 await scenario('sign-in page from the forum connects at once and returns to the authorize request','success',async page=>{
  for(let n=0;n<100&&!releaseSession;n++)await new Promise(resolve=>setTimeout(resolve,10));
  assert(releaseSession);releaseSession();releaseSession=null;
  await page.waitForURL(origin+authorize);
  assert(calls.some(call=>call.path==='/api/auth/verify'));
  assert.equal(await page.locator('h1').textContent(),'Authorize page');
 },false,true,authorize);
 await scenario('signed-in visitor on the sign-in page resumes the OAuth authorize request','success',async page=>{
  for(let n=0;n<100&&!releaseSession;n++)await new Promise(resolve=>setTimeout(resolve,10));
  assert(releaseSession);releaseSession();releaseSession=null;
  await page.waitForURL(origin+authorize);
  assert.equal(sdkLoads,0);
 },false,true,authorize,true);
 for(const next of ['//evil.example/oauth/authorize?x=1','https://evil.example/oauth/authorize?x=1','/oauth/authorizeX?x=1']){
  await scenario(`dedicated sign-in page ignores next ${next}`,'success',async page=>{
   await page.locator('[data-auth-connect]').click();
   await page.waitForURL(`${origin}/profile/${address}`);
  },true,true,next);
 }
 await scenario('explicit connection binds signed payload and saves local profile','success',async page=>{
  await signin(page);
  assert.equal(sdkLoads,1);
  assert.equal(await page.locator('[data-auth-link]').getAttribute('href'),`/profile/${address}`);
  assert.equal(await page.locator('[data-auth-link]').getAttribute('aria-label'),`Profile for ${address}`);
  assert.equal(await page.locator('[data-auth-link]').textContent(),`${address.slice(0,7)}…${address.slice(-5)}`);
  assert.deepEqual(await page.evaluate(()=>window.__signed),{signingType:'micheline',payload:'05010000000474657374',sourceAddress:address});
  assert.equal((await page.context().cookies()).find(cookie=>cookie.name==='test_session').httpOnly,true);
  await page.locator('[name=name]').fill('ExampleName');await page.locator('[name=bio]').fill('Local bio');
  await page.locator('[name=links]').fill('https://example.org\nhttps://example.net');
  await page.locator('button[type=submit]').click();
  await page.waitForFunction(()=>document.querySelector('[data-auth-status]').textContent==='Profile saved.');
  const save=calls.find(call=>call.path==='/api/auth/profile');
  assert.deepEqual(save.body,{name:'ExampleName',bio:'Local bio',links:['https://example.org','https://example.net']});
  assert.equal(save.origin,origin);
  await page.locator('[data-auth-logout-all]').click();
  await page.waitForFunction(()=>document.querySelector('[data-auth-status]').textContent==='Signed out.');
  assert.equal(calls.find(call=>call.path==='/api/auth/logout').body.all,true);
 });
 await scenario('blank profile fields preserve imports and bio hiding is explicit','success',async page=>{
  await signin(page);
  await page.locator('button[type=submit]').click();
  await page.waitForFunction(()=>document.querySelector('[data-auth-status]').textContent==='Profile saved.');
  assert.deepEqual(calls.filter(call=>call.path==='/api/auth/profile').at(-1).body,{name:null,bio:null,links:[]});
  await page.locator('[name=hide_bio]').check();
  await page.locator('button[type=submit]').click();
  await page.waitForFunction(()=>document.querySelector('[data-auth-status]').textContent==='Profile saved.');
  assert.deepEqual(calls.filter(call=>call.path==='/api/auth/profile').at(-1).body,{name:null,bio:'',links:[]});
  await page.locator('[name=hide_bio]').uncheck();
  await page.locator('button[type=submit]').click();
  await page.waitForFunction(()=>document.querySelector('[data-auth-status]').textContent==='Profile saved.');
  assert.equal(calls.filter(call=>call.path==='/api/auth/profile').at(-1).body.bio,null);
 });
 await scenario('prefilled imported values stay imported until edited','success',async page=>{
  await signin(page);
  await page.evaluate(()=>{
   const set=(name,value)=>{const field=document.querySelector(`[name=${name}]`);field.value=value;field.dataset.imported=value;};
   set('name','Imported Name');set('bio','Imported\nbio');set('links','https://imported.org\nhttps://imported.net');
  });
  const saved=async()=>{
   await page.locator('button[type=submit]').click();
   await page.waitForFunction(()=>document.querySelector('[data-auth-status]').textContent==='Profile saved.');
   return calls.filter(call=>call.path==='/api/auth/profile').at(-1).body;
  };
  assert.deepEqual(await saved(),{name:null,bio:null,links:[]});
  await page.locator('[name=hide_bio]').check();
  assert.deepEqual(await saved(),{name:null,bio:'',links:[]});
  await page.locator('[name=hide_bio]').uncheck();
  await page.locator('[name=name]').fill('Own Name');
  await page.locator('[name=links]').fill('https://imported.org');
  assert.deepEqual(await saved(),{name:'Own Name',bio:null,links:['https://imported.org']});
  await page.locator('[name=bio]').fill('');
  assert.equal((await saved()).bio,null);
 });
 await scenario('declining signature leaves session absent and connect enabled','reject',async page=>{
  await page.locator('[data-auth-connect]').click();
  await page.waitForFunction(()=>document.querySelector('[data-auth-status]').textContent==='Connection cancelled.');
  assert.equal(await page.locator('[data-auth-connect]').isDisabled(),false);
  assert(!calls.some(call=>call.path==='/api/auth/verify'));
  assert.equal(session,null);
 });
 await scenario('a wallet failure shows its number, not the wallet text','wallet-error',async page=>{
  await page.locator('[data-auth-connect]').click();
  await page.waitForFunction(()=>document.querySelector('[data-auth-status]').textContent==='Sign-in failed in your wallet; try again (error 110).');
  assert.equal(await page.locator('[data-auth-connect]').isDisabled(),false);
  assert.equal(session,null);
 });
 await scenario('late signature after cancellation never verifies and retry works','late',async page=>{
  await page.locator('[data-auth-connect]').click();
  await page.waitForFunction(()=>typeof window.__releaseSignature==='function');
  await page.locator('[data-auth-cancel]').click();
  await page.evaluate(()=>{window.__releaseSignature();window.__mode='success';});
  await page.waitForTimeout(100);
  assert(!calls.some(call=>call.path==='/api/auth/verify'));
  assert.equal(await page.evaluate(()=>window.__destroyed),1);
  await signin(page);
  assert.equal(await page.evaluate(()=>window.__instances),2);
 });
 await scenario('late pairing after cancellation never issues a challenge','pair-late',async page=>{
  await page.locator('[data-auth-connect]').click();
  await page.waitForFunction(()=>typeof window.__releasePermission==='function');
  await page.locator('[data-auth-cancel]').click();
  await page.evaluate(()=>window.__releasePermission());await page.waitForTimeout(100);
  assert(!calls.some(call=>call.path==='/api/auth/challenge'));
 });
 await scenario('address switch while signing discards the late signature','late',async page=>{
  await page.locator('[data-auth-connect]').click();
  await page.waitForFunction(()=>typeof window.__releaseSignature==='function');
  await page.evaluate(async other=>{
   window.__account={address:other,publicKey:'fixture-public-key',network:{type:'mainnet'}};
   await window.__accountChange(window.__account);window.__releaseSignature();
  },other);
  await page.waitForTimeout(100);
  assert(!calls.some(call=>call.path==='/api/auth/verify'));
  assert.equal(await page.locator('[data-auth-connected]').isVisible(),false);
 });
 await scenario('signer is rechecked before verification even if its event was missed','late',async page=>{
  await page.locator('[data-auth-connect]').click();
  await page.waitForFunction(()=>typeof window.__releaseSignature==='function');
  await page.evaluate(other=>{window.__account={address:other,network:{type:'mainnet'}};window.__releaseSignature();},other);
  await page.waitForFunction(()=>document.querySelector('[data-auth-status]').textContent.startsWith("Your wallet's address changed"));
  assert(!calls.some(call=>call.path==='/api/auth/verify'));
 });
 await scenario('late initial session response cannot overwrite a verified login','success',async page=>{
  await signin(page);assert(releaseSession);releaseSession();
  await page.waitForTimeout(100);
  assert.equal(await page.locator('[data-auth-connected]').isVisible(),true);
  assert((await page.locator('[data-auth-status]').textContent()).startsWith('Signed in as'));
 },false);
 await scenario('late session response cannot restore the UI after logout','success',async page=>{
  await signin(page);holdNextSession=true;
  await page.evaluate(()=>{window.__lateRead=import('/static/auth.js').then(auth=>auth.getSession());});
  for(let n=0;n<100&&!releaseSession;n++)await new Promise(resolve=>setTimeout(resolve,10));
  assert(releaseSession);
  await page.locator('[data-auth-logout]').click();
  await page.waitForFunction(()=>document.querySelector('[data-auth-status]').textContent==='Signed out.');
  releaseSession();await page.evaluate(()=>window.__lateRead);
  assert.equal(await page.locator('[data-auth-connected]').isVisible(),false);
  assert.equal(await page.locator('[data-auth-link]').textContent(),'Connect wallet');
  assert.equal(await page.locator('[data-auth-link]').getAttribute('href'),'/auth');
  assert.equal(await page.locator('[data-auth-link]').getAttribute('aria-label'),'Connect wallet');
  assert.equal(await page.locator('[data-auth-link]').getAttribute('title'),'');
 });
 await scenario('expired session prevents a profile save','success',async page=>{
  await signin(page);session=null;
  await page.locator('button[type=submit]').click();
  await page.waitForFunction(()=>document.querySelector('[data-auth-status]').textContent==='Sign in again (error 109).');
  assert(!calls.some(call=>call.path==='/api/auth/profile'));
 });
 await scenario('address switch revokes the authenticated session','success',async page=>{
  await signin(page);
  await page.evaluate(other=>window.__accountChange({address:other}),other);
  await page.waitForFunction(()=>document.querySelector('[data-auth-status]').textContent==="Your wallet's address changed; sign in again.");
  assert(calls.some(call=>call.path==='/api/auth/logout'));
  assert.equal(session,null);
 });
 await scenario('failed switch logout keeps actions blocked across session refresh and reload','switch-logout-fails',async page=>{
  await signin(page);
  await page.evaluate(other=>window.__accountChange({address:other,network:{type:'mainnet'}}),other);
  assert(session,'server logout intentionally failed');
  assert.equal(await page.evaluate(async()=>{const auth=await import('/static/auth.js');return auth.getSession();}),null);
  await page.reload();
  await page.waitForFunction(()=>document.querySelector('[data-auth-status]').textContent.startsWith("Your wallet's address changed"));
  await page.locator('button[type=submit]').click();
  await page.waitForFunction(()=>document.querySelector('[data-auth-status]').textContent==='Sign in again (error 109).');
  assert(!calls.some(call=>call.path==='/api/auth/profile'));
 });
 await scenario('server validation is displayed as text and form can retry','invalid-profile',async page=>{
  await signin(page);await page.locator('button[type=submit]').click();
  await page.waitForFunction(()=>document.querySelector('[data-auth-status]').textContent.startsWith('Invalid name'));
  assert.equal(await page.locator('[data-auth-status] img').count(),0);
  assert.equal(await page.locator('button[type=submit]').isDisabled(),false);
 });
 await scenario('incomplete response leaves the form available for retry','malformed-profile',async page=>{
  await signin(page);await page.locator('button[type=submit]').click();
  await page.waitForFunction(()=>document.querySelector('[data-auth-status]').textContent==='Request failed; try again (error 502).');
  assert.equal(await page.locator('button[type=submit]').isDisabled(),false);
 });
 await scenario('request timeout aborts the fetch and releases the form','timeout-profile',async page=>{
  await signin(page);await page.locator('button[type=submit]').click();
  await page.waitForFunction(()=>document.querySelector('[data-auth-status]').textContent==='Request timed out; try again (error 503).',{},{timeout:12000});
  assert.equal(await page.locator('button[type=submit]').isDisabled(),false);
 });
 await scenario('operations require the reviewed session and explicit source','success',async page=>{
  await signin(page);
  const result=await page.evaluate(async({address,other})=>{
    const auth=await import('/static/auth.js');const errors=[];
    for(const operations of [[{kind:'transaction',source:other}],[{kind:'transaction'}]]){
      try{await auth.requestOperations(operations,address);}catch(error){errors.push(error.message);}
    }
    const operations=[{kind:'transaction',source:address,destination:'KT1fixture',amount:'0'}];
    const result=await auth.requestOperations(operations,address);
    return {errors,result,request:window.__operation};
  },{address,other});
  assert.deepEqual(result.errors,["Your wallet's address changed; review again.","Your wallet's address changed; review again."]);assert.equal(result.result.transactionHash,'fixture-operation');
  assert.equal(result.request.operationDetails[0].source,address);
 });
 await scenario('market requests retain server status and typed retry code','success',async page=>{
  await signin(page);
  const error=await page.evaluate(async()=>{
    try{await(await import('/static/auth.js')).authFetch('/api/market/check',{});}
    catch(error){return {status:error.status,code:error.code};}
  });
  assert.deepEqual(error,{status:503,code:'Busy'});
 });
 await scenario('market deadlines allow bounded server preparation and confirmation','market-timeout',async page=>{
  await signin(page);
  const result=await page.evaluate(async()=>{
    const original=window.setTimeout,deadlines=[],errors=[];
    window.setTimeout=(callback,delay,...args)=>{
      if(delay===60000||delay===25000){deadlines.push(delay);return original(callback,20,...args);}
      return original(callback,delay,...args);
    };
    try{
      const auth=await import('/static/auth.js');
      for(const name of ['prepare','check','confirm']){
        try{await auth.authFetch('/api/market/'+name,{});}catch(error){errors.push(error.message);}
      }
    }finally{window.setTimeout=original;}
    return {deadlines,errors};
  });
  assert.deepEqual(result.deadlines,[60000,60000,25000]);
  assert.equal(result.errors.length,3);
  assert(result.errors.every(error=>error==='Request timed out; try again (error 503).'));
 });
 await scenario('actual bundled SDK initializes under CSP without unsafe-eval','real-sdk',async page=>{
  const result=await page.evaluate(async()=>{
    const {getBeaconClient}=await import('/static/auth.js');
    const client=await getBeaconClient();
    await client.keyPair;
    const account=await client.getActiveAccount();
    await client.destroy();
    return {account:account??null,name:client.name,globalBuffer:typeof window.Buffer};
  });
  assert.equal(result.account,null);assert.equal(result.name,'seeded.art');assert.equal(sdkLoads,1);
  assert.equal(result.globalBuffer,'undefined','polyfill stays inside the bundle');
 });
 await scenario('actual bundled Connect opens provider chooser and cancellation releases sign-in','real-sdk',async page=>{
  await page.locator('[data-auth-connect]').click();
  await page.locator('beacon-alert').getByText('Connect Wallet',{exact:true}).waitFor({timeout:10000});
  assert.equal(await page.locator('beacon-alert').getByText('Temple',{exact:true}).count(),1);
  await page.screenshot({path:resolve(root,'tmp/auth/beacon-chooser.png')});
  await page.locator('beacon-alert .alert-header .button-icon').last().click();
  await page.waitForFunction(()=>!document.querySelector('[data-auth-connect]').disabled);
  await page.locator('beacon-alert').getByText('Connect Wallet',{exact:true}).waitFor({state:'hidden'});
  assert.equal(session,null);
  assert(!calls.some(call=>call.path==='/api/auth/challenge'||call.path==='/api/auth/verify'));
  await page.locator('[data-auth-connect]').click();
  await page.locator('beacon-alert').getByText('Connect Wallet',{exact:true}).waitFor();
  await page.locator('beacon-alert .alert-header .button-icon').last().click();
  await page.waitForFunction(()=>!document.querySelector('[data-auth-connect]').disabled);
  await page.locator('beacon-alert').getByText('Connect Wallet',{exact:true}).waitFor({state:'hidden'});
  await page.screenshot({path:resolve(root,'tmp/auth/beacon-cancelled.png')});
 });
 console.log(`Auth browser: ${passed} scenarios passed`);
}finally{
 await browser.close();
 await new Promise(resolve=>{server.close(resolve);server.closeAllConnections();});
 await mkdir(resolve(root,'tmp/auth'),{recursive:true});
}
