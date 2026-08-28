import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { SyncioDatabase, createSyncioServer } from '../src/index.js';

const policies=[{effect:'allow',collection:'items',action:'read'},{effect:'allow',collection:'items',action:'write'}];

async function setup(maxSubscriptions=10) {
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-sse-'));
  const db=await SyncioDatabase.open(path.join(root,'db.json'));
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
  assert.match(event,/"value":42/);
  controller.abort();
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
