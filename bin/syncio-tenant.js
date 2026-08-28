#!/usr/bin/env node
import { startSelfHostedSyncio } from '../src/self-host.js';

const config = readConfig();
let runtime;
try {
  runtime = await startSelfHostedSyncio({
    file: config.file,
    secret: config.secret,
    projectId: config.projectId,
    host: config.host ?? '127.0.0.1',
    port: config.port ?? 0,
    ttlSweepIntervalMs: config.ttlSweepIntervalMs ?? 60_000,
    pitrSnapshotIntervalMs: config.pitrSnapshotIntervalMs ?? 3_600_000,
    databaseOptions: config.databaseOptions ?? {},
    usageObserver: (event) => process.send?.({ type:'usage', projectId:config.projectId, event })
  });
  process.send?.({ type:'ready', projectId:config.projectId, region:config.region, address:runtime.address, pid:process.pid });
} catch (error) {
  process.send?.({ type:'failed', code:error.code??'TENANT_START_FAILED', message:error.message });
  process.exit(1);
}

process.on('message',(message)=>{
  if(message?.type==='status')process.send?.({type:'status',projectId:config.projectId,region:config.region,address:runtime.address,pid:process.pid,sequence:runtime.db.sequence,storage:runtime.db.storageStatus(),slo:runtime.slo.evaluate()});
  if(message?.type==='shutdown')void shutdown('ipc');
});
async function shutdown(signal){try{await runtime?.close();process.send?.({type:'stopped',signal});process.exit(0);}catch(error){process.send?.({type:'failed',code:error.code??'TENANT_STOP_FAILED',message:error.message});process.exit(1);}}
process.once('SIGTERM',()=>void shutdown('SIGTERM'));process.once('SIGINT',()=>void shutdown('SIGINT'));
function readConfig(){const raw=process.env.SYNCIO_TENANT_CONFIG;if(!raw)throw new Error('SYNCIO_TENANT_CONFIG required');const value=JSON.parse(Buffer.from(raw,'base64url').toString('utf8'));if(!value.projectId||!value.file||!value.secret)throw new Error('tenant config requires projectId file secret');return value;}
