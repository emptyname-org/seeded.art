import {authFetch,failure,getSession,requestOperations} from './auth.js';

const root=document.querySelector('#market');
if(root){
  const status=document.querySelector('#market-status');
  const dialog=document.querySelector('#market-review');
  const content=document.querySelector('#review-content');
  const sign=document.querySelector('#sign-market');
  const close=document.querySelector('#close-market');
  const actor=root.dataset.address;
  let review=null,working=false,walletWait=false,signAttempt=0,returnFocus=null,queue=[],bulk=false,nextUrl=null,pageCount=0,groupSize=10;
  // Messages open as a pop-up above the page and the review dialog.
  const notice=status.closest('[popover]');
  const say=text=>{status.textContent=text;if(!notice?.showPopover)return;if(notice.matches(':popover-open'))notice.hidePopover();if(text)notice.showPopover();};
  notice?.querySelector('.market-notice-close')?.addEventListener('click',()=>notice.hidePopover());
  const tez=value=>{const n=BigInt(value);return `${n/1000000n}.${String(n%1000000n).padStart(6,'0')}`;};
  const mutez=value=>typeof value==='string'&&/^\d+$/.test(value);
  const controls=disabled=>{root.querySelectorAll('button:not(.market-notice-close)').forEach(b=>{b.disabled=disabled||b.hasAttribute('data-accept-disabled');});};
  const item=(parent,label,value)=>{const p=document.createElement('p');const strong=document.createElement('strong');strong.textContent=`${label}: `;p.append(strong,document.createTextNode(String(value)));parent.append(p);};
  const safeAction=action=>{if(!Number.isSafeInteger(action.id)||action.id<0)throw failure('This record can\'t be handled here',201);return action;};
  const isPlainTransfer=operation=>operation&&(operation.parameters==null||(operation.parameters.entrypoint==='default'&&operation.parameters.value?.prim==='Unit'));
  function transferMatches(operations,fees){
    if(!Array.isArray(operations))return false;
    const transfers=operations.map((operation,index)=>({operation,index})).filter(({operation})=>isPlainTransfer(operation));
    const amount=BigInt(fees?.platform_fee_mutez||'0');
    if(amount===0n)return transfers.length===0;
    if(transfers.length!==1)return false;
    const {operation,index}=transfers[0];
    return index===operations.length-1&&operation.source===actor&&operation.destination===fees.platform_fee_address&&mutez(operation.amount)&&BigInt(operation.amount)===amount;
  }
  function feeReviewValid(candidate){
    const accepts=candidate.items.filter(record=>record.action.action==='accept');
    const fees=candidate.simulation.accept_fees;
    if(accepts.length===0)return fees==null&&transferMatches(candidate.simulation.operations,null);
    if(accepts.length!==1||!fees)return false;
    if(!mutez(fees.price_mutez)||!mutez(fees.royalties_mutez)||!mutez(fees.fxhash_fee_mutez)||!mutez(fees.platform_fee_mutez)||!mutez(fees.platform_fee_returned_mutez)||!mutez(fees.seller_net_mutez)||!Number.isInteger(fees.platform_fee_bps)||fees.platform_fee_bps<0||fees.platform_fee_bps>1000||typeof fees.platform_fee_address!=='string'||!fees.platform_fee_address)return false;
    const price=BigInt(fees.price_mutez),royalties=BigInt(fees.royalties_mutez),fxhash=BigInt(fees.fxhash_fee_mutez),platform=BigInt(fees.platform_fee_mutez),returned=BigInt(fees.platform_fee_returned_mutez),net=BigInt(fees.seller_net_mutez);
    if(!mutez(accepts[0].price_mutez)||fees.price_mutez!==accepts[0].price_mutez)return false;
    if(fxhash!==price*25n/1000n||platform!==price*BigInt(fees.platform_fee_bps)/10000n)return false;
    const expectedNet=price-royalties-fxhash-platform+returned;
    if(expectedNet<0n||net!==expectedNet)return false;
    return transferMatches(candidate.simulation.operations,fees);
  }
  // An applied acceptance whose fee was not paid is done for the user; the server logs it.
  function feeEvasion(error){
    if(error.code!=='FeeEvasion')return false;
    clearPending(true);review=null;bulk=false;queue=[];say('');void redraw();
    return true;
  }
  function actionsFrom(doc){return [...doc.querySelectorAll('.market-record[data-action]')].filter(row=>row.querySelector('.cancel-one')).map(row=>safeAction(JSON.parse(row.dataset.action)));}
  function render(result){
    review=result.review;content.replaceChildren();
    for(const record of review.items){
      const box=document.createElement('section');const heading=document.createElement('h3');
      heading.textContent=`${record.action.action==='accept'?'Accept':'Cancel'} ${record.action.kind} #${record.action.id}`;box.append(heading);
      item(box,record.action.kind==='listing'?'Seller':'Buyer',`${result.names[record.counterparty]||record.counterparty} (${record.counterparty})`);
      item(box,record.action.kind==='listing'?'Listing price':'Offer price',`${tez(record.price_mutez)} tez per NFT`);
      item(box,'Created',record.created_at);
      item(box,'Remaining',record.remaining);
      if(record.token)item(box,'NFT',`${record.token.contract} / ${record.token.token_id}`);
      if(record.action.token&&!record.token)item(box,'NFT',`${record.action.token.contract} / ${record.action.token.token_id}`);
      if(record.project_id!==null)item(box,'Project',record.project_id);
      if(record.action.action==='cancel'&&record.action.kind!=='listing')item(box,'Refund',`${tez(BigInt(record.price_mutez)*BigInt(record.remaining))} tez`);
      content.append(box);
    }
    const hasAccept=review.items.some(record=>record.action.action==='accept');
    if(!hasAccept)item(content,'You receive',`${tez(review.simulation.proceeds_mutez)} tez`);
    let feeBreakdownValid=feeReviewValid(review);
    if(hasAccept){
      const fees=review.simulation.accept_fees;
      if(feeBreakdownValid){
        const rate=fees.platform_fee_bps;
        item(content,'Artist royalties',`${tez(fees.royalties_mutez)} tez`);
        item(content,'fxhash fee',`${tez(fees.fxhash_fee_mutez)} tez`);
        const rateText=(rate/100).toFixed(2).replace(/0$/,'').replace(/\.0$/,'');
        item(content,'seeded.art fee',`${tez(fees.platform_fee_mutez)} tez (${rateText}%)`);
        item(content,'You receive',`${tez(fees.seller_net_mutez)} tez`);
      }
    }
    item(content,'Network fee',`${tez(review.simulation.fee_mutez)} tez`);
    item(content,'Storage fee, up to',`${tez(review.simulation.storage_burn_cap_mutez)} tez`);
    sign.disabled=!feeBreakdownValid;close.disabled=false;dialog.showModal();sign.focus();
    if(!feeBreakdownValid)say(failure('Something changed; review again',202).message);
    return feeBreakdownValid;
  }
  async function prepare(actions){
    if(working)return;if(pending&&pending.expires>Date.now()){say('Waiting for your previous operation…');await watchPending(pending.ticket,pending.hash);if(pending&&pending.expires>Date.now())return;say('');}if(pending)clearPending();working=true;controls(true);returnFocus=document.activeElement;say('Preparing…');
    try{
      actions.forEach(safeAction);
      const session=await getSession();if(!session)throw failure('Sign in again',109);if(session.address!==actor)throw failure('You signed in with another address; reload',238);
      const result=await authFetch('/api/market/prepare',{actions});if(render(result))say('');
    }catch(error){
      if(feeEvasion(error)){dialog.close();controls(false);returnFocus?.focus();return;}
      if(bulk&&actions.length>1&&error.code==='TooLarge'){groupSize=Math.max(1,Math.floor(actions.length/2));working=false;await prepare(queue.slice(0,groupSize));return;}
      say(error.message);bulk=false;queue=[];controls(false);
    }finally{working=false;}
  }
  async function dismiss(){
    // While the signing app holds the request, closing hides the review; the request stays open there.
    if(working&&walletWait){signAttempt++;walletWait=false;working=false;review=null;bulk=false;queue=[];dialog.close();controls(false);say('Review closed; the request may still be open in your wallet.');return;}
    if(working)return;
    const old=review;review=null;bulk=false;queue=[];dialog.close();controls(false);returnFocus?.focus();
    if(old){try{await authFetch('/api/market/discard',{ticket:old.ticket});}catch{/* A consumed check remains protected on the server. */}}
  }
  close.addEventListener('click',dismiss);
  dialog.addEventListener('cancel',event=>{event.preventDefault();void dismiss();});
  async function nextBatch(){
    if(queue.length){await prepare(queue.slice(0,groupSize));return;}
    if(nextUrl&&pageCount<100){
      const nextFailure=number=>failure('Couldn\'t load the next page; reload',number);
      const url=new URL(nextUrl,location.origin);if(url.origin!==location.origin||url.pathname!==location.pathname)throw nextFailure(521);
      const controller=new AbortController();const timer=setTimeout(()=>controller.abort(),15000);let text;
      try{const response=await fetch(url,{credentials:'same-origin',cache:'no-store',signal:controller.signal});if(!response.ok)throw nextFailure(522);
        if(Number(response.headers.get('content-length'))>1024*1024)throw nextFailure(523);
        const reader=response.body.getReader();const decoder=new TextDecoder();text='';let bytes=0;
        try{while(true){const {value,done}=await reader.read();if(done)break;bytes+=value.byteLength;if(bytes>1024*1024)throw nextFailure(523);text+=decoder.decode(value,{stream:true});}text+=decoder.decode();}finally{await reader.cancel();}
      }catch(error){throw error.number?error:nextFailure(522);}
      finally{clearTimeout(timer);}
      const doc=new DOMParser().parseFromString(text,'text/html');if(doc.querySelector('#market')?.dataset.address!==actor)throw failure('You signed in with another address; reload',524);
      queue=actionsFrom(doc);nextUrl=doc.querySelector('nav[data-load-more="market"] a[rel="next"]')?.getAttribute('href')||null;pageCount++;
      if(queue.length){await prepare(queue.slice(0,groupSize));return;}
      // Empty/unsupported pages stop rather than creating an unbounded automatic scan.
    }
    bulk=false;controls(false);say(nextUrl?'Done; use Next page for more.':'All done.');void redraw();
  }
  const pendingKey=`seeded:market-confirm:${actor}`;
  function swap(doc){
    for(const selector of ['.profile-view-nav','.market-views','.market-tools']){const target=document.querySelector(selector),source=doc.querySelector(selector);if(target&&source)target.replaceWith(document.importNode(source,true));}
    const records=document.querySelector('.market-records'),fresh=doc.querySelector('.market-records');if(records&&fresh)records.replaceChildren(...[...fresh.childNodes].map(node=>document.importNode(node,true)));
    for(const selector of ['.empty-state','nav.pagination[data-load-more="market"]']){const target=document.querySelector(`#market ${selector}`),source=doc.querySelector(`#market ${selector}`);if(target&&source)target.replaceWith(document.importNode(source,true));else if(target)target.remove();else if(source)records?.after(document.importNode(source,true));}
  }
  // The server shows a submitted operation as done to its sender: redraw from it now.
  async function redraw(){
    try{const response=await fetch(location.href,{credentials:'same-origin',cache:'no-store'});if(!response.ok)return;const doc=new DOMParser().parseFromString(await response.text(),'text/html');if(doc.querySelector('#market')?.dataset.address===actor)swap(doc);}catch{/* The next load shows the server's view. */}
  }
  // Confirmation runs in the background; only a failure is shown, and its records come back.
  // One watcher per operation; a newer one retires the older, which then touches nothing.
  let watcher=null;
  const released=(ticket,hash,confirmed)=>{if(pending?.ticket===ticket&&pending?.hash===hash)clearPending(confirmed);};
  function watchPending(ticket,hash){
    if(watcher?.ticket===ticket&&watcher?.hash===hash)return watcher.promise;
    const token={ticket,hash};watcher=token;const current=()=>watcher===token;
    token.promise=(async()=>{
      const deadline=Date.now()+30*60*1000;let pause=Number(root.dataset.pollMs)||5000;
      while(Date.now()<deadline&&current()){
        await new Promise(resolve=>setTimeout(resolve,pause));pause=Math.min(pause*1.5,30000);if(!current())return;
        try{const result=await authFetch('/api/market/confirm',{ticket,hash});if(!current())return;if(result?.confirmed){released(ticket,hash,true);return;}}
        catch(error){
          if(!current())return;
          if(['Failed','Unverified'].includes(error.code)){released(ticket,hash,true);say(error.message);await redraw();return;}
          if(error.code==='FeeEvasion'){released(ticket,hash,true);await redraw();return;}
          if(['Expired','Unauthorized'].includes(error.code)){released(ticket,hash,false);return;}
        }
      }
    })().finally(()=>{if(watcher===token)watcher=null;});
    return token.promise;
  }
  let pending=null;
  function showPending(value){
    pending=value;
    let link=document.querySelector('#market-operation');if(!link){link=document.createElement('a');link.id='market-operation';status.insertAdjacentElement('afterend',link);}link.href=`https://tzkt.io/${value.hash}`;link.textContent='View operation';
  }
  function savePending(ticket,hash,expires){const value={ticket,hash,expires};showPending(value);try{sessionStorage.setItem(pendingKey,JSON.stringify(value));}catch{}}
  function clearPending(confirmed=false){try{if(confirmed)sessionStorage.removeItem(pendingKey);else if(pending)sessionStorage.setItem(pendingKey,JSON.stringify({...pending,released:true}));}catch{}pending=null;}
  const unconfirmed='The operation couldn\'t be confirmed; check it in your wallet';
  function expiredReceipt(){const error=failure(unconfirmed,303);error.code='Expired';error.receiptLost=true;return error;}
  function releaseUnavailableReceipt(error){if(pending&&['Expired','Unauthorized'].includes(error.code)){clearPending();if(error.number!==303){error.receiptLost=true;error.message=failure(unconfirmed,307).message;error.number=307;}}}
  try{const value=JSON.parse(sessionStorage.getItem(pendingKey)||'null');if(value&&/^[a-f0-9]{48}$/.test(value.ticket)&&/^o[1-9A-HJ-NP-Za-km-z]{50}$/.test(value.hash)&&Number.isSafeInteger(value.expires)){showPending(value);if(value.released||value.expires<=Date.now())clearPending();else void watchPending(value.ticket,value.hash);}else{sessionStorage.removeItem(pendingKey);}}catch{}
  function stillPending(){const error=failure('Still waiting for confirmation.');error.code='StillPending';return error;}
  async function confirm(ticket,hash){
    const deadline=Date.now()+120000;
    for(let attempt=0;attempt<18;attempt++){
      if(Date.now()>=deadline)throw stillPending();
      if(attempt)await new Promise(resolve=>setTimeout(resolve,5000));
      try{const result=await authFetch('/api/market/confirm',{ticket,hash});if(result.confirmed)return;if(!result.pending)throw failure('Request failed',304);}
      catch(error){if(error.code==='RateLimited')throw error;if(!['Unavailable','Busy'].includes(error.code)&&![429,503].includes(error.status))throw error;}
      if(attempt===17)throw stillPending();
    }
  }
  sign.addEventListener('click',async()=>{
    if(working||!review)return;working=true;sign.disabled=true;close.disabled=true;
    const attempt=++signAttempt;const selected=review;let lockSeconds=120;
    try{
      say('Checking…');const checkStarted=Date.now();const ready=await authFetch('/api/market/check',{ticket:selected.ticket});if(!feeReviewValid(selected)||!transferMatches(ready.operations,selected.simulation.accept_fees))throw failure('Something changed; review again',203);lockSeconds=ready.signing_lock_seconds||120;
      say('Approve it in your wallet.');walletWait=true;close.disabled=false;let result;try{result=await requestOperations(ready.operations,actor);}finally{if(attempt===signAttempt){walletWait=false;close.disabled=true;}}
      const hash=result.transactionHash;if(typeof hash!=='string'||!/^o[1-9A-HJ-NP-Za-km-z]{50}$/.test(hash))throw failure('Your wallet didn\'t return the operation; check your wallet before trying again',305);
      // Kept for the background confirmation.
      savePending(selected.ticket,hash,checkStarted+(ready.receipt_seconds||1800)*1000);
      try{await authFetch('/api/market/submitted',{ticket:selected.ticket,hash});}catch{/* The background check still confirms it. */}
      // Approved after the review was closed: show it as done without touching the page's current state.
      if(attempt!==signAttempt){say('');await redraw();void watchPending(selected.ticket,hash);return;}
      dialog.close();review=null;if(pending.expires<=Date.now())throw expiredReceipt();
      if(!bulk){say('');await redraw();working=false;controls(false);returnFocus?.focus();void watchPending(selected.ticket,hash);return;}
      say('Waiting for confirmation…');try{await confirm(selected.ticket,hash);}catch(error){error.submitted=hash;throw error;}
      clearPending(true);say('Confirmed.');queue.splice(0,selected.items.length);working=false;
      await nextBatch();
    }catch(error){if(attempt!==signAttempt)return;releaseUnavailableReceipt(error);const delayed=error.submitted&&!error.receiptLost&&!['StillPending','Failed','Unverified'].includes(error.code);if(!feeEvasion(error))say(delayed?failure('Submitted; confirmation is delayed',306).message:error.message);review=null;bulk=false;queue=[];dialog.close();controls(false);returnFocus?.focus();}
    finally{if(attempt===signAttempt){working=false;close.disabled=false;}}
  });
  root.addEventListener('click',event=>{const button=event.target.closest('.cancel-one');if(!button||!root.contains(button))return;bulk=false;void prepare([JSON.parse(button.closest('[data-action]').dataset.action)]);});
  root.addEventListener('submit',event=>{
    const form=event.target.closest('.accept-offer');if(!form||!root.contains(form))return;event.preventDefault();if(form.querySelector('[data-accept-disabled]'))return;const fields=new FormData(form);const choice=fields.has('token')?String(fields.get('token')).split('/'):null;const contract=choice?choice[0]:String(fields.get('contract'));const tokenId=Number(choice?choice[1]:fields.get('token_id'));
    if(!Number.isSafeInteger(tokenId)||tokenId<0){say(failure('Enter a valid token ID',239).message);return;}
    const action=JSON.parse(form.closest('[data-action]').dataset.action);action.token={contract,token_id:tokenId};bulk=false;void prepare([action]);
  });
  // A received collection offer shows the held NFT chosen to accept it with.
  root.addEventListener('change',event=>{const select=event.target.closest('.accept-offer select[name="token"]');if(!select||!root.contains(select))return;const option=select.selectedOptions[0];const link=select.closest('.market-record')?.querySelector('.market-thumb');if(!option||!link)return;if(/^\/token\/KT1[1-9A-HJ-NP-Za-km-z]{33}\/\d+$/.test(option.dataset.url||''))link.href=option.dataset.url;let img=link.querySelector('img');if(!option.dataset.thumb){img?.remove();return;}if(!img){img=document.createElement('img');img.alt='';img.width=88;img.height=88;img.decoding='async';link.append(img);}img.src=option.dataset.thumb;});
  document.querySelector('#cancel-page')?.addEventListener('click',()=>{try{bulk=true;queue=actionsFrom(document);nextUrl=document.querySelector('nav[data-load-more="market"] a[rel="next"]')?.getAttribute('href')||null;pageCount=1;groupSize=10;void nextBatch().catch(error=>{say(error.message);bulk=false;controls(false);});}catch(error){say(error.message);bulk=false;controls(false);}});
}
