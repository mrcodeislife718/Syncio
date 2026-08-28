import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { SyncioDatabase, createSyncioServer, ReplicationClient, applyReplicatedChange } from '../src/index.js';

const allowAll = [{ effect:'allow' }];
async function makeDb(name, options) { const root=await fs.mkdtemp(path.join(os.tmpdir(),`syncio-${name}-`)); const file=path.join(root,'db.json'); return {root,file,db:await SyncioDatabase.open(file,options)}; }

async function cleanup(...states) { for (const state of states) { await state.db?.close().catch(()=>{}); await fs.rm(state.root,{recursive:true,force:true}); } }

test('server refuses an expired replication cursor instead of silently skipping history', async (t) => {
  const source = await makeDb('expired-source',{changeRetention:2});
  t.after(()=>cleanup(source));
  const items=source.db.collection('items');
  await items.upsert({id:'a',value:1});
  await items.upsert({id:'b',value:2});
  await items.upsert({id:'c',value:3});
  assert.deepEqual(source.db.changesSince(0).map(c=>c.sequence),[2,3]);
  const service=createSyncioServer({db:source.db,policies:allowAll});
  t.after(()=>service.close());
  const address=await service.listen();
  const response=await fetch(`${address.url}/replicate/pull`,{method:'POST',headers:{'content-type':'application/json'},body:'{"cursor":0}'});
  assert.equal(response.status,409);
  const body=await response.json();
  assert.equal(body.error,'snapshot_required');
  assert.equal(body.oldestRetained,2);
  assert.equal(body.sequence,3);
});

test('expired client can verified-reseed and then resume incremental replication', async (t) => {
  const source=await makeDb('reseed-source',{changeRetention:2});
  const replica=await makeDb('reseed-replica');
  t.after(()=>cleanup(source,replica));
  const originalReplicaId=replica.db.databaseId;
  await source.db.collection('items').upsert({id:'a',value:1});
  await source.db.collection('items').upsert({id:'b',value:2});
  await source.db.collection('items').upsert({id:'c',value:3});
  const service=createSyncioServer({db:source.db,policies:allowAll});
  t.after(()=>service.close());
  const address=await service.listen();
  const client=new ReplicationClient({baseUrl:address.url,cursor:0});
  const pulled=await client.pull(
    async(change)=>applyReplicatedChange(replica.db,change),
    { reseed: async(state)=>replica.db.replaceState(state) }
  );
  assert.equal(pulled,0);
  assert.equal(client.cursor,3);
  assert.equal(replica.db.databaseId,originalReplicaId);
  assert.deepEqual(replica.db.collection('items').all().map(r=>r.id).sort(),['a','b','c']);

  await source.db.collection('items').upsert({id:'d',value:4});
  const incremental=await client.pull(async(change)=>applyReplicatedChange(replica.db,change));
  assert.equal(incremental,1);
  assert.equal(client.cursor,4);
  assert.equal(replica.db.collection('items').get('d').value,4);
});

test('snapshot packet tampering is rejected by client before reseed', async () => {
  const fakeFetch=async(url) => {
    if (String(url).endsWith('/replicate/pull')) return new Response(JSON.stringify({error:'snapshot_required'}),{status:409,headers:{'content-type':'application/json'}});
    return new Response(JSON.stringify({protocol:'syncio-snapshot/1',from:'x',cursor:1,state:{version:1,collections:{}},digest:'bad'}),{status:200,headers:{'content-type':'application/json'}});
  };
  const client=new ReplicationClient({baseUrl:'http://syncio.invalid',fetchImpl:fakeFetch});
  let reseeded=false;
  await assert.rejects(client.pull(async()=>{}, {reseed:async()=>{reseeded=true;}}),/invalid snapshot packet/);
  assert.equal(reseeded,false);
});
