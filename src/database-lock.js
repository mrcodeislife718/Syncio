import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';

export class DatabaseLease {
  constructor(file,lockFile,owner,handle){this.file=file;this.lockFile=lockFile;this.owner=owner;this.handle=handle;this.closed=false;}
  static async acquire(file,{staleAfterMs=30_000}={}){
    const target=path.resolve(file),lockFile=`${target}.lock`;if(!Number.isFinite(staleAfterMs)||staleAfterMs<1_000)throw new TypeError('staleAfterMs must be at least 1000');
    await fs.mkdir(path.dirname(lockFile),{recursive:true});
    const owner={version:1,instanceId:crypto.randomUUID(),pid:process.pid,hostname:process.env.HOSTNAME??null,startedAt:Date.now(),heartbeatAt:Date.now()};
    for(let attempt=0;attempt<3;attempt++){
      try{const handle=await fs.open(lockFile,'wx',0o600);await writeOwner(handle,owner);return new DatabaseLease(target,lockFile,owner,handle);}catch(error){if(error.code!=='EEXIST')throw error;const existing=await readOwner(lockFile);if(!existing)throw locked(lockFile,null);if(await definitelyDead(existing,staleAfterMs)){await fs.rm(lockFile,{force:true});continue;}throw locked(lockFile,existing);}
    }
    throw locked(lockFile,await readOwner(lockFile));
  }
  async heartbeat(){if(this.closed)throw new Error('database lease closed');this.owner.heartbeatAt=Date.now();await writeOwner(this.handle,this.owner);return{...this.owner};}
  async release(){if(this.closed)return;this.closed=true;try{const current=await readOwner(this.lockFile);if(current?.instanceId===this.owner.instanceId)await fs.rm(this.lockFile,{force:true});}finally{await this.handle.close().catch(()=>undefined);}}
}
async function writeOwner(handle,owner){const payload=`${JSON.stringify(owner)}\n`;await handle.truncate(0);await handle.write(payload,0,'utf8');await handle.sync();}
async function readOwner(file){try{const value=JSON.parse(await fs.readFile(file,'utf8'));return value&&typeof value.instanceId==='string'&&Number.isInteger(value.pid)?value:null;}catch{return null;}}
async function definitelyDead(owner,staleAfterMs){if(owner.hostname&&process.env.HOSTNAME&&owner.hostname!==process.env.HOSTNAME)return Date.now()-(owner.heartbeatAt??owner.startedAt??0)>staleAfterMs*4;if(owner.pid===process.pid)return false;try{process.kill(owner.pid,0);return false;}catch(error){if(error.code==='ESRCH')return true;if(error.code==='EPERM')return false;return Date.now()-(owner.heartbeatAt??owner.startedAt??0)>staleAfterMs;}}
function locked(file,owner){const error=new Error('Syncio database is already owned by another live process');error.code='SYNCIO_DATABASE_LOCKED';error.statusCode=423;error.lockFile=file;error.owner=owner;return error;}
