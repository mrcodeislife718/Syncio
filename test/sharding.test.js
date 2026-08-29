import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { ProductionSyncioDatabase } from '../src/production-db.js';
import { ConsistentHashRing, ShardedSyncioDatabase } from '../src/sharding.js';

async function setup(){const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-shards-'));const a=await ProductionSyncioDatabase.open(path.join(root,'a.json'));const b=await ProductionSyncioDatabase.open(path.join(root,'b.json'));return{root,a,b,router:new ShardedSyncioDatabase({a,b},{virtualNodes:64})};}
async function cleanup(s){await s.router.close().catch(()=>undefined);await fs.rm(s.root,{recursive:true,force:true});}

test('consistent hash ring routes deterministically and distributes keys',()=>{
  const ring=new ConsistentHashRing(['a','b','c'],{virtualNodes:128});
  assert.equal(ring.locate('same-key'),ring.locate('same-key'));
  const distribution=ring.distribution(Array.from({length:300},(_,i)=>`k-${i}`));
  assert.deepEqual(Object.keys(distribution),['a','b','c']);
  assert.equal(Object.values(distribution).reduce((a,b)=>a+b,0),300);
  assert.ok(Object.values(distribution).every((count)=>count>40));
});

test('sharded CRUD routes each record to one deterministic shard and scatter query preserves query semantics',async(t)=>{
  const s=await setup();t.after(()=>cleanup(s));
  for(let i=0;i<30;i++)await s.router.collection('items').upsert({id:`id-${i}`,group:i%2?'odd':'even',value:i});
  assert.equal(s.a.collection('items').all().length+s.b.collection('items').all().length,30);
  const rows=s.router.collection('items').query({where:{group:'even'},orderBy:{field:'value',direction:'desc'},limit:3,projection:{id:1,value:1}});
  assert.deepEqual(rows.map((row)=>row.value),[28,26,24]);
  const item=s.router.collection('items').get('id-7');
  assert.equal(item.value,7);
  await s.router.collection('items').remove('id-7');
  assert.equal(s.router.collection('items').get('id-7'),null);
});

test('online record move verifies target before deleting source',async(t)=>{
  const s=await setup();t.after(()=>cleanup(s));
  await s.a.collection('items').upsert({id:'forced',value:1});
  const result=await s.router.moveRecord('items','forced',{fromShard:'a',toShard:'b'});
  assert.equal(result.moved,true);
  assert.equal(s.a.collection('items').get('forced'),null);
  assert.equal(s.b.collection('items').get('forced').value,1);
});

test('rebalance migrates misplaced records to ring-selected owners without loss',async(t)=>{
  const s=await setup();t.after(()=>cleanup(s));
  const records=Array.from({length:20},(_,i)=>({id:`r-${i}`,value:i}));
  for(const record of records)await s.a.collection('items').upsert(record);
  const plan=await s.router.rebalanceCollection('items',{dryRun:true});
  assert.ok(plan.planned>0);
  const moved=await s.router.rebalanceCollection('items');
  assert.equal(moved.moved,plan.planned);
  for(const record of records){const owner=s.router.locate(record);assert.equal(s.router.shard(owner).collection('items').get(record.id).value,record.value);}
  assert.equal(s.a.collection('items').all().length+s.b.collection('items').all().length,20);
});
