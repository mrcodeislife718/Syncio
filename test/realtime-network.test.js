import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { SyncioDatabase, createSyncioServer } from '../src/index.js';

const policies=[{effect:'allow',collection:'items',action:'read'},{effect:'allow',collection:'items',action:'write'}];

async function setup(maxSubscriptions=10,options={}) {
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-sse-'));
  const db=await SyncioDatabase.open(path.join(root,'db.json'),options);
  const service=createSyncioServer({db,policies,maxSubscriptions});
  const address=await service.listen();
  return {root,db,service,address};
}

async function cleanup(state){await state.service.close();await state.db.close();await fs.rm(state.root,{recursive:true,force:true});}

async function readUntil(reader, pattern, timeoutMs=3000) {
  let text='';
  const deadline=Date.now()+timeoutMs;
  while(Date.now()<deadline) {
    const remaining=deadline-Date.now();
    const result=await Promise.race([reader.read(),new Promise((_,reject)=>setTimeout(()=>reject(new Error('stream timeout')),remaining))]);
    if(result.done) break;
    text+=new TextDecoder().decode(result.value);
    if(pattern.test(text)) return text;
  }
  throw new Error(`pattern not received: ${pattern}`);
}

test('network subscriber receives committed durable change event',async(t)=>{
  const state=await setup(); t.after(()=>cleanup(state));
  const controller=new AbortController(); t.after(()=>controller.abort());
  const response=await fetch(`${state.address.url}/subscribe/items`,{signal:controller.signal});
  assert.equal(response.status,200);
  assert.match(response.headers.get('content-type'),/text\/event-stream/);
  const reader=response.body.getReader();
  const ready=await readUntil(reader,/event: ready/);
  assert.match(ready,/"collection":"items"/);
  const write=await fetch(`${state.address.url}/collections/items/a`,{method:'PUT',headers:{'content-type':'application/json'},body:'{"value":42}'});
  assert.equal(write.status,200);
  const event=await readUntil(reader,/event: change/);
  assert.match(event,/id: 1/);
  assert.match(event,/"value":42/);
  controller.abort();
});

test('realtime subscriber replays missed durable changes then continues live',async(t)=>{
  const state=await setup(); t.after(()=>cleanup(state));
  await state.db.collection('items').upsert({id:'a',value:1});
  const cursor=state.db.sequence;
  await state.db.collection('items').upsert({id:'b',value:2});
  await state.db.collection('items').upsert({id:'c',value:3});

  const controller=new AbortController(); t.after(()=>controller.abort());
  const response=await fetch(`${state.address.url}/subscribe/items?after=${cursor}`,{signal:controller.signal});
  assert.equal(response.status,200);
  const reader=response.body.getReader();
  const replay=await readUntil(reader,/"id":"c"/);
  assert.match(replay,/id: 2/);
  assert.match(replay,/"id":"b"/);
  assert.match(replay,/id: 3/);
  assert.match(replay,/"id":"c"/);

  await state.db.collection('items').upsert({id:'d',value:4});
  const live=await readUntil(reader,/"id":"d"/);
  assert.match(live,/id: 4/);
  controller.abort();
});

test('Last-Event-ID resumes a disconnected realtime stream',async(t)=>{
  const state=await setup(); t.after(()=>cleanup(state));
  await state.db.collection('items').upsert({id:'a',value:1});
  await state.db.collection('items').upsert({id:'b',value:2});
  await state.db.collection('items').upsert({id:'c',value:3});

  const controller=new AbortController(); t.after(()=>controller.abort());
  const response=await fetch(`${state.address.url}/subscribe/items`,{headers:{'last-event-id':'1'},signal:controller.signal});
  assert.equal(response.status,200);
  const replay=await readUntil(response.body.getReader(),/"id":"c"/);
  assert.equal(replay.includes('"id":"a"'),false);
  assert.match(replay,/id: 2/);
  assert.match(replay,/id: 3/);
  controller.abort();
});

test('expired realtime cursor is rejected explicitly instead of silently losing changes',async(t)=>{
  const state=await setup(10,{changeRetention:2}); t.after(()=>cleanup(state));
  for(let i=1;i<=4;i++) await state.db.collection('items').upsert({id:`i${i}`,value:i});
  const response=await fetch(`${state.address.url}/subscribe/items?after=1`);
  assert.equal(response.status,409);
  const problem=await response.json();
  assert.equal(problem.error,'stream_resume_expired');
  assert.equal(problem.oldestRetained,3);
  assert.equal(problem.sequence,4);
});

test('transaction changes feed the same realtime stream in durable sequence order',async(t)=>{
  const state=await setup(); t.after(()=>cleanup(state));
  const seen=[];
  const stop=state.db.watchChanges({collection:'items',after:0},(change)=>seen.push(change));
  t.after(stop);
  await state.db.transaction(async(tx)=>{
    tx.collection('items').put({id:'a',value:1});
    tx.collection('items').put({id:'b',value:2});
  });
  await new Promise((resolve)=>setImmediate(resolve));
  assert.deepEqual(seen.map((change)=>change.sequence),[1,2]);
  assert.deepEqual(seen.map((change)=>change.record.id),['a','b']);
});

test('network subscription capacity is bounded rather than unbounded fanout',async(t)=>{
  const state=await setup(1); t.after(()=>cleanup(state));
  const firstController=new AbortController();t.after(()=>firstController.abort());
  const first=await fetch(`${state.address.url}/subscribe/items`,{signal:firstController.signal});
  assert.equal(first.status,200);
  await readUntil(first.body.getReader(),/event: ready/);
  const second=await fetch(`${state.address.url}/subscribe/items`);
  assert.equal(second.status,429);
  assert.equal((await second.json()).error,'subscription_capacity_exceeded');
  firstController.abort();
});
