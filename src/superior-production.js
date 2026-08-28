import fs from 'node:fs/promises';
import path from 'node:path';
import { ProductionSyncioDatabase } from './production-db.js';
import { SuperiorIndexedSyncioDatabase } from './superior-indexed.js';
import { DatabaseLease } from './database-lock.js';

export async function openSuperiorProduction(file,options={}){
  const lease=await DatabaseLease.acquire(file,options.lease);
  const heartbeatMs=options.lease?.heartbeatMs??10_000;
  let base;
  try{
    base=await SuperiorIndexedSyncioDatabase.open(file,options);
    const metadataFile=path.resolve(`${base.file}.capabilities.json`);
    let metadata={version:1,schemas:{},ttl:{},text:{},geo:{}};
    try{metadata=JSON.parse(await fs.readFile(metadataFile,'utf8'));}catch(error){if(error.code!=='ENOENT')throw error;}
    const db=new ProductionSyncioDatabase(base,metadataFile,metadata);
    const closeBase=db.close.bind(db);let closed=false;
    const heartbeat=setInterval(()=>lease.heartbeat().catch(()=>undefined),heartbeatMs);heartbeat.unref?.();
    db.lease=Object.freeze({instanceId:lease.owner.instanceId,lockFile:lease.lockFile});
    db.close=async()=>{if(closed)return;closed=true;clearInterval(heartbeat);try{await closeBase();}finally{await lease.release();}};
    return db;
  }catch(error){if(base)await base.close().catch(()=>undefined);await lease.release();throw error;}
}
