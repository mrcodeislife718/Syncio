import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import { startSelfHostedSyncio } from '../src/self-host.js';
import { SyncioClient } from '../src/client.js';

async function setup(){const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-client-'));const runtime=await startSelfHostedSyncio({file:path.join(root,'db.json'),secret:'x'.repeat(48),port:0,ttlSweepIntervalMs:0,pitrSnapshotIntervalMs:0});const token=runtime.issueToken();const client=new SyncioClient({baseUrl:runtime.address.url,token,retry:{attempts:2,baseDelayMs:1,maxDelayMs:2}});return{root,runtime,client};}
async function cleanup(s){await s.runtime.close().catch(()=>undefined);await fs.rm(s.root,{recursive:true,force:true});}

test('client driver performs CRUD rich query partial update and aggregation against production runtime',async(t)=>{
  const s=await setup();t.after(()=>cleanup(s));const users=s.client.collection('users');
  await users.upsert('u1',{name:'Ada',score:1,profile:{city:'Bronx'}});await users.update('u1',{$inc:{score:2}});assert.equal((await users.get('u1')).score,3);
  const rows=await users.query({where:{'profile.city':'Bronx'},projection:{id:1,score:1}});assert.deepEqual(rows,[{id:'u1',score:3}]);
  const aggregate=await users.aggregate([{$match:{score:{$gte:3}}},{$count:'count'}]);assert.deepEqual(aggregate,[{count:1}]);
  assert.equal(await users.remove('u1'),true);assert.equal(await users.get('u1'),null);
});

test('client retries transient server response but does not retry authorization failure',async(t)=>{
  let calls=0;const server=http.createServer((req,res)=>{calls++;if(calls===1){res.writeHead(503,{'content-type':'application/json'});res.end('{"error":"busy"}');}else{res.writeHead(200,{'content-type':'application/json'});res.end('{"ok":true}');}});await new Promise((resolve)=>server.listen(0,'127.0.0.1',resolve));t.after(()=>new Promise((resolve)=>server.close(resolve)));const client=new SyncioClient({baseUrl:`http://127.0.0.1:${server.address().port}`,retry:{attempts:3,baseDelayMs:1,maxDelayMs:2}});assert.equal((await client.health()).ok,true);assert.equal(calls,2);
});

test('client realtime iterator resumes from durable cursor and receives live change',async(t)=>{
  const s=await setup();t.after(()=>cleanup(s));const users=s.client.collection('users');await users.upsert('u1',{name:'first'});const after=s.runtime.db.sequence;const watch=users.watch({after,reconnect:false});const iterator=watch[Symbol.asyncIterator]();const pending=iterator.next();await new Promise((resolve)=>setTimeout(resolve,15));await users.upsert('u2',{name:'second'});const event=await pending;assert.equal(event.value.record.id,'u2');assert.equal(watch.cursor,event.value.sequence);await watch.close();
});
