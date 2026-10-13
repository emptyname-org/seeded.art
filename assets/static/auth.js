let clientPromise;
let currentSession=null;
let attempt=0;
let connecting=false;
let canCancel=false;
let pendingAddress=null;
let verifyPending=false;
let sessionRevision=0;
let sessionRead=0;
let signerMismatch=false;
try{signerMismatch=sessionStorage.getItem('seeded:signer-mismatch')==='true';}catch{}

function holdSigner(value){
  signerMismatch=value;
  try{if(value)sessionStorage.setItem('seeded:signer-mismatch','true');else sessionStorage.removeItem('seeded:signer-mismatch');}catch{}
}

// User-visible errors end with their number (docs/errors.md); statuses have none.
export function failure(text,number=0){
  const error=new Error(number?`${text} (error ${number}).`:text);
  error.number=number;
  return error;
}
// A wallet error the user caused by cancelling shows as a status; any other gets a number.
function walletFailure(error,cancelled,text,number){
  if(typeof error?.number==='number')return error;
  return /^\[ABORTED_ERROR\]/.test(String(error?.message))?failure(cancelled):failure(text,number);
}
function status(text){
  document.querySelectorAll('[data-auth-status]').forEach(node=>{node.textContent=text;});
}
function publish(session){
  sessionRevision++;
  if(signerMismatch)session=null;
  currentSession=session;
  document.querySelectorAll('[data-auth-link]').forEach(link=>{
    link.textContent=session?`${session.address.slice(0,7)}…${session.address.slice(-5)}`:'Connect wallet';
    link.href=session?`/profile/${session.address}`:'/auth';
    link.setAttribute('aria-label',session?`Profile for ${session.address}`:'Connect wallet');
    link.title=session?session.address:'';
    link.hidden=!!session&&!!document.querySelector('[data-auth-menu]');
  });
  document.querySelectorAll('[data-auth-address]').forEach(node=>{
    node.textContent=session?`${session.address.slice(0,7)}…${session.address.slice(-5)}`:'';
    node.title=session?session.address:'';
    if(!session)node.parentElement.open=false;
  });
  document.querySelectorAll('[data-auth-connected]').forEach(node=>{node.hidden=!session;});
  document.querySelectorAll('[data-auth-profile]').forEach(link=>{link.href=session?`/profile/${session.address}`:'/auth';});
  document.dispatchEvent(new CustomEvent('seeded:session',{detail:session?{address:session.address}:null}));
}
async function jsonRequest(path,options={}){
  const controller=new AbortController();
  const deadline=path==='/api/market/prepare'||path==='/api/market/check'?60000:path==='/api/market/confirm'?25000:10000;
  const timeout=setTimeout(()=>controller.abort(),deadline);
  let response=null,value=null;
  try{
    response=await fetch(path,{credentials:'same-origin',cache:'no-store',...options,signal:controller.signal});
    if(response.status!==204)value=await response.json().catch(()=>null);
  }catch{/* Reported below. */}
  finally{clearTimeout(timeout);}
  const timedOut=()=>failure('Request timed out; try again',503);
  if(!response)throw controller.signal.aborted?timedOut():failure('Request failed; try again',505);
  if(response.status===204)return null;
  if(!response.ok){
    // The server's sentence already carries its number.
    const error=value?.error?new Error(String(value.error)):controller.signal.aborted?timedOut():failure('Request failed; try again',501);
    error.status=response.status;
    error.code=value?.code;
    error.number=Number.isInteger(value?.number)?value.number:error.number??0;
    throw error;
  }
  if(value===null)throw controller.signal.aborted?timedOut():failure('Request failed; try again',502);
  return value;
}
export async function getSession(){
  const revision=sessionRevision,read=++sessionRead;
  const value=await jsonRequest('/api/auth/session');
  if(revision===sessionRevision&&read===sessionRead)publish(value.session||null);
  return currentSession;
}
export async function authFetch(path,body){
  if(!/^\/api\/(auth|market)\/[a-z-]+$/.test(path))throw failure('Request failed',504);
  const session=await getSession();
  if(!session)throw failure('Sign in again',109);
  return jsonRequest(path,{method:'POST',headers:{'content-type':'application/json','x-csrf-token':session.csrf},body:JSON.stringify(body)});
}
export async function getBeaconClient(){
  if(!clientPromise){
    clientPromise=(async()=>{
      const {DAppClient,BeaconEvent,NetworkType}=await import('./beacon/client.js');
      const client=new DAppClient({name:'seeded.art',appUrl:location.origin,network:{type:NetworkType.MAINNET},enableMetrics:false,requestTimeoutMs:120000});
      await client.subscribeToEvent(BeaconEvent.ACTIVE_ACCOUNT_SET,async account=>{
        const pendingChanged=pendingAddress&&(!account||account.address!==pendingAddress||account.network?.type!=='mainnet');
        const previous=currentSession;
        const sessionChanged=previous&&(!account||account.address!==previous.address||account.network?.type!=='mainnet');
        if(pendingChanged||sessionChanged){
          if(pendingChanged){attempt++;pendingAddress=null;canCancel=false;busy(verifyPending);}
          holdSigner(true);
          publish(null);
          status('Your wallet\'s address changed; sign in again.');
          if(previous){
            try{await jsonRequest('/api/auth/logout',{method:'POST',headers:{'content-type':'application/json','x-csrf-token':previous.csrf},body:JSON.stringify({all:false})});}
            catch{/* Keep actions blocked until an explicitly verified sign-in. */}
          }
        }
      });
      return client;
    })().catch(error=>{clientPromise=undefined;throw error;});
  }
  return clientPromise;
}
export async function requestOperations(operationDetails,expectedAddress){
  const session=await getSession();
  if(!session||session.address!==expectedAddress)throw failure('You signed in with another address; reload',308);
  if(!Array.isArray(operationDetails)||!operationDetails.length||operationDetails.some(operation=>operation.source!==expectedAddress))throw failure('Your wallet\'s address changed; review again.');
  try{
    const client=await getBeaconClient();
    const account=await client.getActiveAccount();
    if(!account||account.address!==expectedAddress||account.network?.type!=='mainnet')throw failure('Switch your wallet to the signed-in address',108);
    return await client.requestOperation({operationDetails});
  }catch(error){throw walletFailure(error,'Signing cancelled.','Signing failed in your wallet',309);}
}
function busy(value){
  connecting=value;
  document.querySelectorAll('[data-auth-connect]').forEach(button=>{button.disabled=value;});
  document.querySelectorAll('[data-auth-cancel]').forEach(button=>{button.hidden=!value||!canCancel;});
}
async function connect(){
  if(connecting)return;
  const ownAttempt=++attempt;
  let submitted=false;
  const active=()=>{if(attempt!==ownAttempt)throw failure('Connection cancelled.');};
  canCancel=true;busy(true);status('Approve the sign-in in your wallet.');
  try{
    const client=await getBeaconClient();active();
    let account=await client.getActiveAccount();active();
    if(!account){
      await client.requestPermissions();active();
      account=await client.getActiveAccount();active();
    }
    if(!account?.publicKey||!/^tz[123]/.test(account.address)||account.network?.type!=='mainnet')throw failure('Choose a Tezos mainnet tz1, tz2 or tz3 address',101);
    pendingAddress=account.address;
    const challenge=await jsonRequest('/api/auth/challenge',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({address:account.address,public_key:account.publicKey})});active();
    const signed=await client.requestSignPayload({signingType:'micheline',payload:challenge.payload,sourceAddress:account.address});active();
    const signingAccount=await client.getActiveAccount();active();
    if(!signingAccount||signingAccount.address!==account.address||signingAccount.network?.type!=='mainnet'){
      holdSigner(true);publish(null);
      throw failure('Your wallet\'s address changed; sign in again.');
    }
    // Cancellation cannot leave an unseen session after the verification response sets its cookie.
    canCancel=false;submitted=true;verifyPending=true;busy(true);
    const value=await jsonRequest('/api/auth/verify',{method:'POST',headers:{'content-type':'application/json','x-csrf-token':challenge.csrf},body:JSON.stringify({signature:signed.signature})});
    if(ownAttempt!==attempt){
      try{await jsonRequest('/api/auth/logout',{method:'POST',headers:{'content-type':'application/json','x-csrf-token':value.session.csrf},body:JSON.stringify({all:false})});}catch{}
      publish(null);
      return;
    }
    holdSigner(false);
    publish(value.session);
    // A signed-in address has no use for the sign-in page.
    if(document.querySelector('[data-auth-page]')){location.assign(authNext()||`/profile/${value.session.address}`);return;}
    status(`Signed in as ${value.session.address}.`);
  }catch(error){if(attempt===ownAttempt)status(walletFailure(error,'Connection cancelled.','Sign-in failed in your wallet; try again',110).message);}
  finally{
    if(submitted)verifyPending=false;
    if(submitted||attempt===ownAttempt){pendingAddress=null;canCancel=false;busy(false);}
  }
}
// Only a return to this site's OAuth authorize endpoint; the server checks it too.
function authNext(){
  const next=document.querySelector('[data-auth-page]')?.dataset.authNext||'';
  return next.length<=2048&&next.startsWith('/oauth/authorize?')&&!next.includes('//')&&/^[\x21-\x5b\x5d-\x7e]+$/.test(next)?next:'';
}
async function logout(all){
  try{
    await authFetch('/api/auth/logout',{all});
    holdSigner(false);
    publish(null);status('Signed out.');
    if(clientPromise){const client=await clientPromise;await client.clearActiveAccount();}
  }catch(error){status(error.message);}
}
document.querySelectorAll('[data-auth-connect]').forEach(button=>button.addEventListener('click',connect));
document.querySelectorAll('[data-auth-cancel]').forEach(button=>button.addEventListener('click',()=>{
  if(!canCancel)return;
  attempt++;pendingAddress=null;canCancel=false;busy(false);status('Connection cancelled.');
  const cancelledClient=clientPromise;
  clientPromise=undefined;
  if(cancelledClient)cancelledClient.then(client=>client.destroy()).catch(()=>{});
}));
document.querySelectorAll('[data-auth-logout]').forEach(button=>button.addEventListener('click',()=>logout(false)));
document.querySelectorAll('[data-auth-logout-all]').forEach(button=>button.addEventListener('click',()=>logout(true)));
document.querySelectorAll('[data-auth-profile-form]').forEach(form=>form.addEventListener('submit',async event=>{
  event.preventDefault();
  const button=form.querySelector('button[type=submit]');
  if(button.disabled)return;
  button.disabled=true;status('Saving…');
  const fields=new FormData(form);
  try{
    // Fields show imported values; sending one back unchanged keeps it imported.
    const imported=field=>String(form.elements[field]?.dataset.imported||'').replace(/\r\n/g,'\n').trim();
    const lines=text=>text.split('\n').map(link=>link.trim()).filter(Boolean);
    const name=String(fields.get('name')||'').trim();
    const links=lines(String(fields.get('links')||'').replace(/\r\n/g,'\n'));
    const bio=String(fields.get('bio')||'').replace(/\r\n/g,'\n').trim();
    const own=(value,field)=>value&&value!==imported(field)?value:'';
    await authFetch('/api/auth/profile',{name:own(name,'name')||null,bio:own(bio,'bio')||(fields.has('hide_bio')?'':null),links:links.join('\n')===lines(imported('links')).join('\n')?[]:links});
    status('Profile saved.');
  }catch(error){status(error.message);}
  finally{button.disabled=false;}
}));
getSession().then(session=>{
  // Signed in already (the cookie was withheld from a cross-site navigation): resume the authorize request.
  if(session&&attempt===0&&!connecting&&authNext()){location.assign(authNext());return;}
  // Arriving from the forum: open the wallet connection at once instead of the sign-in page.
  if(!session&&attempt===0&&!connecting&&authNext()){connect();return;}
  // A signed-out visitor on the sign-in page needs the wallet code next: fetch it now so Connect answers at once.
  if(!session&&document.querySelector('[data-auth-page]'))import('./beacon/client.js').catch(()=>{});
  if(attempt===0&&!connecting&&document.querySelector('[data-auth-connect]'))status(signerMismatch?'Your wallet\'s address changed; sign in again.':session?`Signed in as ${session.address}.`:'Not connected.');
}).catch(()=>{/* Pages remain readable if sign-in is unavailable. */});
function closeMenus(except){document.querySelectorAll('details.account-menu[open]').forEach(menu=>{if(menu!==except)menu.open=false;});}
document.addEventListener('click',event=>closeMenus(event.target.closest?.('details.account-menu')));
document.addEventListener('keydown',event=>{
  if(event.key!=='Escape')return;
  const menu=document.querySelector('details.account-menu[open]');
  if(!menu)return;
  menu.open=false;menu.querySelector('summary').focus();
});
window.addEventListener('focus',()=>getSession().catch(()=>{}));
