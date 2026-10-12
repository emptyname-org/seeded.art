// Five independent HTTP/2 zero-window clients must leave full-response capacity.
import assert from 'node:assert/strict';
import http2 from 'node:http2';
import tls from 'node:tls';
import {once} from 'node:events';
const [portText, artPath] = process.argv.slice(2);
const port=Number(portText),results=[],sessions=[];
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function connect(host,address,stalled=false){
 const session=http2.connect(`https://${host}:${port}`,{settings:stalled?{initialWindowSize:0}:{},createConnection:()=>tls.connect({host:'127.0.0.1',port,servername:host,localAddress:address,rejectUnauthorized:false,ALPNProtocols:['h2']})});
 sessions.push(session);session.on('error',()=>{});await once(session,'remoteSettings');return session;
}
async function read(session,path,expected){
 return await new Promise((resolve,reject)=>{
  const stream=session.request({':path':path});let status,bytes=0;
  const timer=setTimeout(()=>{stream.destroy();reject(new Error('Healthy client timed out'));},15000);
  stream.on('response',headers=>{status=headers[':status'];});stream.on('data',chunk=>{bytes+=chunk.length;});stream.on('error',reject);
  stream.on('end',()=>{try{assert.equal(status,200);assert.equal(bytes,expected,'complete body required');resolve({status,bytes});}catch(error){reject(error);}});
  stream.on('close',()=>clearTimeout(timer));stream.end();
 });
}
try {
 for(const [index,host,path,expectedHeld,size,attempts] of [[1,'app.invalid','/static/large.bin',16,32<<20,16],[2,'artwork.invalid',artPath,2,31<<20,4]]){
  const attacks=[],held=[],statuses=[];
  for(let n=2;n<7;n++){
   const session=await connect(host,`127.0.${index}.${n}`,true);attacks.push(session);
   const perClient=[];statuses.push(perClient);
   for(let i=0;i<attempts;i++){
    const stream=session.request({':path':path});stream.on('response',headers=>perClient.push(headers[':status']));stream.on('error',()=>{});stream.end();held.push(stream);
   }
  }
  const deadline=Date.now()+15000;
  while(statuses.some(rows=>rows.filter(code=>code===200).length<expectedHeld)){
   assert(Date.now()<deadline,`Real delivery did not start: ${JSON.stringify(statuses)}`);await delay(20);
  }
  const healthy=await connect(host,`127.0.${index}.99`);
  const response=await read(healthy,path,size);
  assert(statuses.every(rows=>!rows.includes(503)),JSON.stringify(statuses));
  assert(statuses.every(rows=>rows.filter(code=>code===200).length===expectedHeld));
  for(const stream of held)stream.close(http2.constants.NGHTTP2_CANCEL);
  for(const session of attacks)session.destroy();await delay(500);
  const recovered=await connect(host,`127.0.${index}.2`);const recovery=await read(recovered,path,size);
  results.push({host,attackers:5,held:5*expectedHeld,healthy:response,recovery});healthy.destroy();recovered.destroy();
 }
 const api=await connect('app.invalid','127.0.9.9');
 for(const [path,body] of [['/api/auth/profile',JSON.stringify({bio:'x'.repeat(17*1024)})],['/api/market/prepare',JSON.stringify({actions:[]})+' '.repeat(25*1024)]]){
  const status=await new Promise((resolve,reject)=>{const stream=api.request({':method':'POST',':path':path,'content-type':'application/json'});stream.on('response',headers=>resolve(headers[':status']));stream.on('error',reject);stream.resume();stream.end(body);});
  assert.equal(status,413,'Real Rust extractor must reject a body below the edge cap');
 }
 api.destroy();
 const idle=[];
 for(let batch=0;batch<20;batch++){
  const opened=await Promise.all(Array.from({length:200},()=>connect('app.invalid','127.0.8.8')));idle.push(...opened);
 }
 const probe=await connect('app.invalid','127.0.8.9');
 await read(probe,'/static/large.bin',32<<20);
 const alive=idle.filter(session=>!session.closed&&!session.destroyed).length;
 assert(alive>=3000,`Idle-connection fixture did not hold enough connections: ${alive}`);
 results.push({idleConnections:alive,healthyBytes:32<<20});
 for(const session of idle)session.destroy();probe.destroy();
 console.log(JSON.stringify(results));
} finally {for(const session of sessions)session.destroy();}
