import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';

export class SyncView {
  constructor({collection,predicate=()=>true,maxDocuments=100000}){this.collection=collection;this.predicate=predicate;this.maxDocuments=maxDocuments;}
  select(records,context={}){const selected=[];for(const record of records){if(this.predicate(record,context)){selected.push(structuredClone(record));if(selected.length>this.maxDocuments)throw limit('sync view expansion limit exceeded');}}return selected;}
}
export class DurableIntentLog {
  constructor(file,state){this.file=file;this.state=state;this.queue=Promise.resolve();}
  static async open(file){let state={version:1,intents:{}};try{state=JSON.parse(await fs.readFile(file,'utf8'));}catch(e){if(e.code!=='ENOENT')throw e;}return new DurableIntentLog(file,state);}
  list(){return Object.values(this.state.intents).map(structuredClone);}
  async enqueue(intent){validateIntent(intent);const normalized={version:1,intentId:intent.intentId??crypto.randomUUID(),createdAt:Date.now(),status:'pending',preconditions:intent.preconditions??[],readAssumptions:intent.readAssumptions??[],mutation:structuredClone(intent.mutation),conflictPolicy:intent.conflictPolicy??'reject',expiresAt:intent.expiresAt??null,retryPolicy:intent.retryPolicy??{maxAttempts:8},origin:intent.origin??'client',attempts:0};this.state.intents[normalized.intentId]=normalized;await this.#persist();return structuredClone(normalized);}
  async reconcile(executor,{now=Date.now()}={}){const results=[];for(const intent of this.list().filter(x=>x.status==='pending')){if(intent.expiresAt&&intent.expiresAt<=now){this.state.intents[intent.intentId].status='expired';results.push({intentId:intent.intentId,status:'expired'});continue;}try{const result=await executor(intent);this.state.intents[intent.intentId].status=result?.status??'committed';this.state.intents[intent.intentId].attempts++;results.push({intentId:intent.intentId,...result});}catch(error){const live=this.state.intents[intent.intentId];live.attempts++;if(live.attempts>=live.retryPolicy.maxAttempts)live.status='failed';results.push({intentId:intent.intentId,status:live.status,error:error.code??'execution_failed'});}}await this.#persist();return results;}
  async #persist(){this.queue=this.queue.then(async()=>{await fs.mkdir(path.dirname(this.file),{recursive:true});const tmp=`${this.file}.${crypto.randomUUID()}.tmp`;await fs.writeFile(tmp,JSON.stringify(this.state),{mode:0o600});const h=await fs.open(tmp,'r');await h.sync();await h.close();await fs.rename(tmp,this.file);});return this.queue;}
}
function validateIntent(i){if(!i||typeof i!=='object'||!i.mutation)throw new TypeError('intent mutation required');}
function limit(message){const e=new Error(message);e.code='SYNCIO_RESOURCE_LIMIT';return e;}
