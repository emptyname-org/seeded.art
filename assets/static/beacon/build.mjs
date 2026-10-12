import {createRequire} from 'node:module';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import fs from 'node:fs/promises';
const root=fileURLToPath(new URL('../../../',import.meta.url));
const require=createRequire(path.join(root,'tmp/browser_tools/package.json'));
const {build}=require('esbuild');
await fs.mkdir(path.join(root,'tmp/auth'),{recursive:true});
await build({entryPoints:[path.join(root,'assets/static/beacon/entry.js')],outfile:path.join(root,'assets/static/beacon/client.js'),nodePaths:[path.join(root,'tmp/browser_tools/node_modules')],inject:[path.join(root,'assets/static/beacon/browser_buffer.js')],bundle:true,format:'esm',platform:'browser',target:['es2022'],minify:true,legalComments:'linked',metafile:true}).then(async result=>{
  await fs.writeFile(path.join(root,'tmp/auth/beacon-metafile.json'),JSON.stringify(result.metafile));
  const code=await fs.readFile(path.join(root,'assets/static/beacon/client.js'),'utf8');
  if(/\beval\s*\(/.test(code))throw new Error('Beacon bundle requires unsafe-eval');
  const packages=new Set();
  for(const input of Object.keys(result.metafile.inputs)){
    if(!input.includes('node_modules/'))continue;
    let directory=path.dirname(path.resolve(root,input));
    while(directory.startsWith(path.join(root,'tmp/browser_tools/node_modules'))){
      try{await fs.access(path.join(directory,'package.json'));packages.add(directory);break;}catch{}
      directory=path.dirname(directory);
    }
  }
  const notices=[];
  for(const directory of [...packages].sort()){
    const info=JSON.parse(await fs.readFile(path.join(directory,'package.json'),'utf8'));
    const files=(await fs.readdir(directory)).filter(name=>/^(licen[cs]e|notice|copying)(\.|$)/i.test(name));
    notices.push(`${info.name} ${info.version}\nLicense: ${info.license||'See package notices'}\n`);
    for(const file of files){
      const location=path.join(directory,file),stat=await fs.stat(location);
      if(stat.isFile()&&stat.size<=131072)notices.push(await fs.readFile(location,'utf8'));
    }
  }
  await fs.writeFile(path.join(root,'assets/static/beacon/LICENSES.txt'),notices.join('\n\n'));
});
