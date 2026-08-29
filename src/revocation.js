import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';

export class DurableRevocationLedger {
  constructor(file,authority){if(!authority||typeof authority.revokeToken!=='function')throw new TypeError('revocation ledger requires rotating token authority');this.file=path.resolve(file);this.authority=authority;this.queue=Promise.resolve();this.events=0;}
  static async open(file,authority){const ledger=new DurableRevocationLedger(file,authority);try{const text=await fs.readFile(ledger.file,'utf8');for(const line of text.split('\n').filter(Boolean)){const envelope=JSON.parse(line);verify(envelope);ledger.#apply(envelope.event);ledger.events++;}}catch(error){if(error.code!=='ENOENT')throw error;}return ledger;}
  async revokeToken(jti,{expiresAt=Infinity}={}){return this.#record({type:'token',jti,expiresAt:Number.isFinite(expiresAt)?expiresAt:null});}
  async revokeSubject(subject,{before=Math.floor(Date.now()/1000)}={}){return this.#record({type:'subject',subject,before});}
  async revokeAll({before=Math.floor(Date.now()/1000)}={}){return this.#record({type:'all',before});}
  status(){return{events:this.events,file:this.file};}
  async close(){await this.queue;}
  async #record(event){const operation=this.queue.then(async()=>{this.#apply(event);const base={version:1,id:crypto.randomUUID(),at:new Date().toISOString(),event};const envelope={...base,digest:digest(base)};await fs.mkdir(path.dirname(this.file),{recursive:true});const handle=await fs.open(this.file,'a',0o600);try{await handle.writeFile(`${JSON.stringify(envelope)}\n`,'utf8');await handle.sync();}finally{await handle.close();}this.events++;return true;});this.queue=operation.catch(()=>undefined);return operation;}
  #apply(event){if(event.type==='token')return this.authority.revokeToken(event.jti,{expiresAt:event.expiresAt===null?Infinity:event.expiresAt});if(event.type==='subject')return this.authority.revokeSubject(event.subject,{before:event.before});if(event.type==='all')return this.authority.revokeAll({before:event.before});throw corrupt('unsupported revocation event');}
}
function verify(envelope){if(!envelope||envelope.version!==1||!envelope.event||envelope.digest!==digest({version:envelope.version,id:envelope.id,at:envelope.at,event:envelope.event}))throw corrupt('revocation ledger integrity failure');}
function digest(value){return crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');}
function corrupt(message){const error=new Error(message);error.code='SYNCIO_REVOCATION_CORRUPT';return error;}
