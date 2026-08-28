import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';

export class RuntimeUsageSampler {
  constructor({meter,projectId,dataPaths=[],backupPaths=[],pitrPaths=[],intervalMs=60*60*1000,now=()=>Date.now()}={}){
    if(!meter||typeof meter.record!=='function')throw new TypeError('runtime usage sampler requires meter');
    if(typeof projectId!=='string'||!projectId)throw new TypeError('runtime usage sampler requires projectId');
    if(!Number.isFinite(intervalMs)||intervalMs<1000)throw new TypeError('runtime usage sampler intervalMs must be at least 1000');
    if(typeof now!=='function')throw new TypeError('runtime usage sampler now must be a function');
    this.meter=meter;this.projectId=projectId;this.paths={data:normalizePaths(dataPaths),backup:normalizePaths(backupPaths),pitr:normalizePaths(pitrPaths)};this.intervalMs=intervalMs;this.now=now;this.lastAt=null;this.timer=null;this.queue=Promise.resolve();
  }
  async sample(){
    const operation=this.queue.then(async()=>{
      const current=this.now();if(!Number.isFinite(current)||current<0)throw new TypeError('runtime usage sampler clock returned invalid time');
      const sizes={data:await totalBytes(this.paths.data),backup:await totalBytes(this.paths.backup),pitr:await totalBytes(this.paths.pitr)};
      if(this.lastAt===null){this.lastAt=current;return{projectId:this.projectId,elapsedMs:0,sizes,recorded:false};}
      const elapsedMs=Math.max(0,current-this.lastAt);this.lastAt=current;if(elapsedMs===0)return{projectId:this.projectId,elapsedMs,sizes,recorded:false};
      const hours=elapsedMs/3_600_000;const windowId=`${Math.floor(current)}:${crypto.createHash('sha256').update(JSON.stringify(sizes)).digest('hex').slice(0,12)}`;const at=new Date(current).toISOString();
      const records=[];
      if(sizes.data>0)records.push(this.meter.record({projectId:this.projectId,metric:'storage_byte_hours',quantity:sizes.data*hours,at,idempotencyKey:`runtime:${windowId}:storage`}));
      if(sizes.backup>0)records.push(this.meter.record({projectId:this.projectId,metric:'backup_byte_hours',quantity:sizes.backup*hours,at,idempotencyKey:`runtime:${windowId}:backup`}));
      if(sizes.pitr>0)records.push(this.meter.record({projectId:this.projectId,metric:'pitr_byte_hours',quantity:sizes.pitr*hours,at,idempotencyKey:`runtime:${windowId}:pitr`}));
      records.push(this.meter.record({projectId:this.projectId,metric:'compute_milliseconds',quantity:elapsedMs,at,idempotencyKey:`runtime:${windowId}:compute`}));
      await Promise.all(records);return{projectId:this.projectId,elapsedMs,sizes,recorded:true};
    });
    this.queue=operation.catch(()=>undefined);return operation;
  }
  async start(){if(this.timer)return this;await this.sample();this.timer=setInterval(()=>void this.sample().catch(()=>undefined),this.intervalMs);this.timer.unref?.();return this;}
  async close(){if(this.timer){clearInterval(this.timer);this.timer=null;}if(this.lastAt!==null)await this.sample();await this.queue;}
}

export async function pathBytes(target){
  if(typeof target!=='string'||!target)return 0;
  let stat;try{stat=await fs.stat(target);}catch(error){if(error.code==='ENOENT')return 0;throw error;}
  if(stat.isFile())return stat.size;if(!stat.isDirectory())return 0;
  let total=0;for(const entry of await fs.readdir(target,{withFileTypes:true})){const child=path.join(target,entry.name);if(entry.isSymbolicLink())continue;total+=await pathBytes(child);}return total;
}

async function totalBytes(paths){let total=0;for(const target of paths)total+=await pathBytes(target);return total;}
function normalizePaths(values){if(!Array.isArray(values)||values.some((value)=>typeof value!=='string'||!value))throw new TypeError('usage sampler paths must be strings');return[...new Set(values.map((value)=>path.resolve(value)))];}
