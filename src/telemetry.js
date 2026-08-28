import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';

const clone=(value)=>structuredClone(value);

export class DurableTelemetryExporter {
  constructor(file,{endpoint,fetchImpl=globalThis.fetch,batchSize=100,maxItems=10000,headers={}}={}){
    if(!endpoint)throw new TypeError('telemetry endpoint required');if(typeof fetchImpl!=='function')throw new TypeError('telemetry fetch required');if(!Number.isSafeInteger(batchSize)||batchSize<1||!Number.isSafeInteger(maxItems)||maxItems<batchSize)throw new TypeError('invalid telemetry queue sizing');
    this.file=path.resolve(file);this.endpoint=endpoint;this.fetch=fetchImpl;this.batchSize=batchSize;this.maxItems=maxItems;this.headers=headers;this.state={version:1,items:[],dropped:0};this.queue=Promise.resolve();
  }
  static async open(file,options){const exporter=new DurableTelemetryExporter(file,options);try{exporter.state=JSON.parse(await fs.readFile(exporter.file,'utf8'));}catch(error){if(error.code!=='ENOENT')throw error;}if(!exporter.state||exporter.state.version!==1||!Array.isArray(exporter.state.items))throw new Error('invalid telemetry spool');return exporter;}
  get size(){return this.state.items.length;}
  status(){return clone({pending:this.size,dropped:this.state.dropped,endpoint:this.endpoint});}
  observe(event){void this.enqueue({type:'event',event}).catch(()=>undefined);}
  async enqueue(payload){if(payload===undefined)throw new TypeError('telemetry payload required');return this.#mutate((draft)=>{const item={id:crypto.randomUUID(),at:new Date().toISOString(),payload:clone(payload),attempts:0};draft.items.push(item);while(draft.items.length>this.maxItems){draft.items.shift();draft.dropped++;}return clone(item);});}
  async flush({maxBatches=100}={}){if(!Number.isSafeInteger(maxBatches)||maxBatches<1)throw new TypeError('maxBatches must be positive');let delivered=0,batches=0;while(this.state.items.length&&batches<maxBatches){const batch=this.state.items.slice(0,this.batchSize);await this.#mutate((draft)=>{for(let i=0;i<Math.min(batch.length,draft.items.length);i++)draft.items[i].attempts++;});let response;try{response=await this.fetch(this.endpoint,{method:'POST',headers:{'content-type':'application/json',...this.headers},body:JSON.stringify({format:'syncio-telemetry/1',items:batch})});}catch(error){return{delivered,pending:this.size,batches,error};}if(!response.ok){return{delivered,pending:this.size,batches,error:Object.assign(new Error(`telemetry endpoint returned ${response.status}`),{status:response.status})};}await this.#mutate((draft)=>{draft.items.splice(0,batch.length);});delivered+=batch.length;batches++;}return{delivered,pending:this.size,batches};}
  async close(){await this.queue;}
  async #mutate(work){const operation=this.queue.then(async()=>{const draft=clone(this.state);const result=work(draft);await atomicWriteJson(this.file,draft);this.state=draft;return result;});this.queue=operation.catch(()=>undefined);return operation;}
}

export class SloMonitor{
  constructor({availability=0.999,p95LatencyMs=250,errorBudgetWindow=1000}={}){if(!(availability>0&&availability<=1)||!Number.isFinite(p95LatencyMs)||p95LatencyMs<0||!Number.isSafeInteger(errorBudgetWindow)||errorBudgetWindow<1)throw new TypeError('invalid SLO configuration');this.targets={availability,p95LatencyMs,errorBudgetWindow};this.samples=[];}
  record({ok,status,durationMs}={}){const success=ok??(Number.isFinite(status)&&status<500);if(!Number.isFinite(durationMs)||durationMs<0)throw new TypeError('durationMs required');this.samples.push({success:Boolean(success),durationMs});if(this.samples.length>this.targets.errorBudgetWindow)this.samples.shift();return this.evaluate();}
  evaluate(){if(!this.samples.length)return{status:'insufficient_data',samples:0};const successes=this.samples.filter((s)=>s.success).length;const availability=successes/this.samples.length;const sorted=this.samples.map((s)=>s.durationMs).sort((a,b)=>a-b);const p95=sorted[Math.min(sorted.length-1,Math.ceil(sorted.length*.95)-1)];const allowedFailures=(1-this.targets.availability)*this.samples.length;const failures=this.samples.length-successes;return{status:availability>=this.targets.availability&&p95<=this.targets.p95LatencyMs?'healthy':'breached',samples:this.samples.length,availability,p95LatencyMs:p95,errorBudget:{allowedFailures,failures,remaining:allowedFailures-failures},targets:clone(this.targets)};}
}

export function createTelemetryObserver(exporter,slo){if(!exporter||typeof exporter.observe!=='function')throw new TypeError('telemetry exporter required');return(event)=>{exporter.observe(event);if(slo&&Number.isFinite(event?.durationMs))slo.record({status:event.status,durationMs:event.durationMs});};}
async function atomicWriteJson(target,value){await fs.mkdir(path.dirname(target),{recursive:true});const temp=`${target}.${process.pid}.${crypto.randomUUID()}.tmp`;try{await fs.writeFile(temp,`${JSON.stringify(value)}\n`,{encoding:'utf8',mode:0o600});const handle=await fs.open(temp,'r');try{await handle.sync();}finally{await handle.close();}await fs.rename(temp,target);const dir=await fs.open(path.dirname(target),'r');try{await dir.sync();}finally{await dir.close();}}finally{await fs.rm(temp,{force:true}).catch(()=>undefined);}}
