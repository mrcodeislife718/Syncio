import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { SyncioDatabase, createSyncioServer, ReplicationClient } from '../src/index.js';
import { DurableOfflineQueue } from '../src/operations.js';

const allowAll=[{effect:'allow'}];

test('replication client queued mutation survives process-style reopen before delivery',async(t)=>{
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-durable-client-'));t.after(()=>fs.rm(root,{recursive:true,force:true}));
  const serverDb=await SyncioDatabase.open(path.join(root,'server.json'));t.after(()=>serverDb.close());
  const service=createSyncioServer({db:serverDb,policies:allowAll});t.after(()=>service.close());
  const address=await service.listen();
  const queueFile=path.join(root,'client-queue.json');
  let queue=await DurableOfflineQueue.open(queueFile);
  let client=new ReplicationClient({baseUrl:address.url,nodeId:'client-a',offlineQueue:queue});
  await client.queue({collection:'items',type:'upsert',record:{id:'offline',value:9,updatedAt:'2026-08-28T15:00:00Z'} });
  assert.equal(queue.size,1);
  queue=await DurableOfflineQueue.open(queueFile);
  client=new ReplicationClient({baseUrl:address.url,nodeId:'client-a',offlineQueue:queue});
  const result=await client.flush();
  assert.equal(result.pending,0);
  assert.equal(result.delivered,1);
  assert.equal(serverDb.collection('items').get('offline').value,9);
  const reopened=await DurableOfflineQueue.open(queueFile);
  assert.equal(reopened.size,0);
});
