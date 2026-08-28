import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { SegmentedDocumentStore, TieredStatePlane } from '../src/tiered-store.js';

async function root(){return fs.mkdtemp(path.join(os.tmpdir(),'syncio-tiered-'));}

test('segmented store persists records while bounding hot-record cache bytes',async(t)=>{
  const dir=await root();t.after(()=>fs.rm(dir,{recursive:true,force:true}));
  let store=await SegmentedDocumentStore.open(dir,{segmentBytes:64*1024,cacheBytes:2048});
  for(let i=0;i<100;i++)await store.put({id:`id-${i}`,payload:'x'.repeat(512),n:i});
  assert.equal(store.size,100);
  assert.ok(store.stats().segments>1);
  assert.ok(store.stats().cache.bytes<=2048);
  await store.close();
  store=await SegmentedDocumentStore.open(dir,{segmentBytes:64*1024,cacheBytes:2048});
  assert.equal((await store.get('id-73')).n,73);
  assert.equal(store.size,100);
  assert.ok(store.stats().cache.bytes<=2048);
  await store.close();
});

test('segmented store tombstones survive restart and compaction preserves only live values',async(t)=>{
  const dir=await root();t.after(()=>fs.rm(dir,{recursive:true,force:true}));
  let store=await SegmentedDocumentStore.open(dir,{segmentBytes:64*1024,cacheBytes:1024});
  await store.put({id:'a',value:1});await store.put({id:'b',value:2});await store.put({id:'a',value:3});await store.remove('b');
  assert.equal(await store.get('b'),null);assert.equal((await store.get('a')).value,3);
  const before=store.stats();assert.ok(before.tombstones>=1);
  await store.compact();
  assert.equal((await store.get('a')).value,3);assert.equal(await store.get('b'),null);assert.equal(store.size,1);
  await store.close();
  store=await SegmentedDocumentStore.open(dir,{segmentBytes:64*1024,cacheBytes:1024});
  assert.equal((await store.get('a')).value,3);assert.equal(store.size,1);await store.close();
});

test('streaming scan does not require materializing whole dataset into caller memory',async(t)=>{
  const dir=await root();t.after(()=>fs.rm(dir,{recursive:true,force:true}));
  const store=await SegmentedDocumentStore.open(dir,{segmentBytes:64*1024,cacheBytes:1024});
  for(let i=0;i<50;i++)await store.put({id:`r-${i}`,value:i,payload:'y'.repeat(256)});
  let count=0,sum=0;for await(const record of store.scan({batchSize:7})){count++;sum+=record.value;assert.ok(store.stats().cache.bytes<=1024);}
  assert.equal(count,50);assert.equal(sum,1225);await store.close();
});

test('tiered state plane isolates collection segment stores',async(t)=>{
  const dir=await root();t.after(()=>fs.rm(dir,{recursive:true,force:true}));
  const plane=await TieredStatePlane.open(dir,{cacheBytes:1024,segmentBytes:64*1024});
  const users=await plane.collection('users');const orders=await plane.collection('orders');
  await users.put({id:'u1',name:'Ada'});await orders.put({id:'o1',total:42});
  assert.equal((await users.get('u1')).name,'Ada');assert.equal((await orders.get('o1')).total,42);
  const stats=await plane.stats();assert.equal(stats.users.records,1);assert.equal(stats.orders.records,1);await plane.close();
});
