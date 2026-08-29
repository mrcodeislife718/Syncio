import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { StorageBackedSyncioDatabase } from '../src/storage-core.js';
import { SuperiorIndexedSyncioDatabase } from '../src/superior-indexed.js';
import { openSuperiorProduction } from '../src/superior-production.js';
import { ReplicatedPartitionGroup, DurablePartitionParticipant, DurableTransactionCoordinator, RegionalTopology } from '../src/distributed.js';
import { GlobalCommitLedger, SubscriptionRouter, RegionalDistributedDatabase } from '../src/distributed-runtime.js';
import { ShardedSyncioDatabase } from '../src/sharding.js';
import { AdaptiveScheduler, ExecutionCostModel, PRIORITY, planTransfer } from '../src/scheduler.js';
import { compileQueryIR, verifyQueryIR, encodeCommitProtocol, decodeCommitProtocol, encodeSyncProtocol, decodeSyncProtocol, negotiateProtocols } from '../src/protocol.js';
import { ReactiveQueryGraph } from '../src/reactive.js';

async function temp(prefix='syncio-superiority-'){const root=await fs.mkdtemp(path.join(os.tmpdir(),prefix));return{root,file:path.join(root,'data.syncio.json')};}
async function openCore(root,name){return StorageBackedSyncioDatabase.open(path.join(root,`${name}.syncio.json`),{checkpointEvery:1000,storage:{cacheBytes:64*1024,indexCacheBuckets:2}});}

test('versioned query commit and sync protocols detect tampering and negotiate compatibility',()=>{
  assert.deepEqual(negotiateProtocols({query:[1],commit:[1],sync:[1]}),{query:1,commit:1,sync:1});
  const q=compileQueryIR({collection:'orders',where:{tenant:'a'},policyConstraints:{owner:'u'}});assert.equal(verifyQueryIR(q),true);assert.equal(verifyQueryIR({...q,collection:'other'}),false);
  const commit={commitId:'c1',sequence:3,mutations:[{type:'put',collection:'orders',id:'1',record:{id:'1'}}]};const cp=encodeCommitProtocol(commit);assert.deepEqual(decodeCommitProtocol(cp),commit);assert.throws(()=>decodeCommitProtocol({...cp,digest:'0'.repeat(64)}));
  const sp=encodeSyncProtocol({view:'mine',cursor:1,sequence:2,changes:[{id:'1'}]});assert.equal(decodeSyncProtocol(sp).view,'mine');assert.throws(()=>decodeSyncProtocol({...sp,sequence:99}));
});

test('reactive materialization incrementally handles insert update and delete then falls back on incomplete deltas',async()=>{
  let source=[{id:'a',status:'open'}],evaluations=0;const g=new ReactiveQueryGraph();g.register('q',{collection:'orders',query:{status:'open'},initial:source,evaluate:async()=>{evaluations++;return source.filter(x=>x.status==='open');}});
  let out=await g.applyCommit({commitId:'c1',mutations:[{collection:'orders',id:'a',fields:['status'],record:{id:'a',status:'closed'}}]});assert.deepEqual(out[0].value,[]);assert.equal(out[0].mode,'incremental');
  out=await g.applyCommit({commitId:'c2',mutations:[{collection:'orders',id:'b',fields:['status'],record:{id:'b',status:'open'}}]});assert.deepEqual(out[0].value,[{id:'b',status:'open'}]);
  out=await g.applyCommit({commitId:'c3',mutations:[{collection:'orders',id:'b',fields:['status'],type:'remove'}]});assert.deepEqual(out[0].value,[]);
  source=[{id:'z',status:'open'}];out=await g.applyCommit({commitId:'c4',mutations:[{collection:'orders',fields:['status']}]});assert.deepEqual(out[0].value,source);assert.equal(out[0].mode,'recompute');assert.equal(evaluations,1);
});

test('persistent secondary index preserves typed unique identity and bounded bucket cache across restart',async(t)=>{
  const s=await temp();t.after(()=>fs.rm(s.root,{recursive:true,force:true}));let db=await SuperiorIndexedSyncioDatabase.open(s.file,{indexCacheBuckets:1});await db.defineIndex('items','key',{name:'key_unique',unique:true});
  await db.collection('items').upsert({id:'missing'});await db.collection('items').upsert({id:'null',key:null});await db.collection('items').upsert({id:'number',key:1});await db.collection('items').upsert({id:'string',key:'1'});await db.collection('items').upsert({id:'object',key:{a:1,b:2}});
  await assert.rejects(()=>db.collection('items').upsert({id:'object2',key:{b:2,a:1}}),e=>e.code==='SYNCIO_UNIQUE_INDEX_VIOLATION');assert.equal(db.collection('items').query({where:{key:1}})[0].id,'number');
  const before=db.storageStatus().secondaryIndexes;assert.ok(Object.values(before)[0].maxCachedBuckets===1);await db.close();db=await SuperiorIndexedSyncioDatabase.open(s.file,{indexCacheBuckets:1});assert.equal(db.collection('items').query({where:{key:'1'}})[0].id,'string');assert.ok(Object.values(db.storageStatus().secondaryIndexes)[0].cachedBuckets<=1);await db.close();
});

test('bounded production text and geo indexes persist and update without heap-wide index state',async(t)=>{
  const s=await temp();t.after(()=>fs.rm(s.root,{recursive:true,force:true}));let db=await openSuperiorProduction(s.file,{specialIndexCacheBuckets:1,lease:{heartbeatMs:1000,staleMs:5000}});await db.defineTextIndex('places','name',{name:'names'});await db.defineGeoIndex('places','location',{name:'geo'});await db.collection('places').upsert({id:'a',name:'Alpha Cafe',location:[-73.99,40.75]});await db.collection('places').upsert({id:'b',name:'Beta Hall',location:[-74.1,40.8]});assert.equal(db.search('places','alpha',{index:'names'})[0].record.id,'a');assert.equal(db.near('places',[-73.99,40.75],{index:'geo',maxDistanceMeters:1000})[0].record.id,'a');await db.collection('places').upsert({id:'a',name:'Gamma Cafe',location:[-73.98,40.75]});assert.equal(db.search('places','alpha',{index:'names'}).length,0);assert.equal(db.search('places','gamma',{index:'names'})[0].record.id,'a');const status=db.storageStatus();assert.ok(Object.values(status.specialIndexes.text)[0].cachedBuckets<=1);await db.close();db=await openSuperiorProduction(s.file,{specialIndexCacheBuckets:1});assert.equal(db.search('places','gamma',{index:'names'})[0].record.id,'a');await db.close();
});

test('hierarchical scheduler enforces parent and child budgets and transfer planner chooses cheaper representation',()=>{
  const root=new AdaptiveScheduler({budgets:{cpu:2,memory:100}}),realtime=root.child('realtime',{budgets:{cpu:1,memory:50}});const a=realtime.admit({priority:PRIORITY.realtime,cost:{cpu:1,memory:20}});assert.equal(a.decision,'admit');assert.equal(realtime.admit({priority:PRIORITY.realtime,cost:{cpu:1}}).decision,'reject');a.release();assert.equal(realtime.admit({priority:PRIORITY.realtime,cost:{cpu:1}}).decision,'admit');assert.equal(planTransfer({deltaBytes:10,snapshotBytes:100}).strategy,'delta');assert.equal(planTransfer({deltaBytes:1000,snapshotBytes:10}).strategy,'snapshot');const cost=new ExecutionCostModel();assert.equal(cost.choose([{id:'near',networkBytes:10},{id:'far',networkBytes:1000}]).id,'near');
});

test('three-node replicated partition acknowledges overlapping quorum and repairs a lagging replica',async(t)=>{
  const s=await temp();t.after(()=>fs.rm(s.root,{recursive:true,force:true}));const a=await openCore(s.root,'a'),b=await openCore(s.root,'b'),c=await openCore(s.root,'c');const group=new ReplicatedPartitionGroup({a,b,c},{writeQuorum:2,readQuorum:2});const write=await group.put('items',{id:'x',value:1},{sessionId:'s'});assert.ok(write.replicas.length>=2);assert.equal((await group.get('items','x',{consistency:'quorum',sessionId:'s'})).value,1);await c.collection('items').remove('x');assert.equal((await group.get('items','x',{consistency:'quorum'})).value,1);await new Promise(r=>setTimeout(r,10));assert.equal(c.collection('items').get('x').value,1);await Promise.all([a.close(),b.close(),c.close()]);
});

test('durable two-phase coordinator commits cross-partition transaction and survives reopen',async(t)=>{
  const s=await temp();t.after(()=>fs.rm(s.root,{recursive:true,force:true}));const a=await openCore(s.root,'pa'),b=await openCore(s.root,'pb');const pa=await DurablePartitionParticipant.open('a',a,path.join(s.root,'pa.json')),pb=await DurablePartitionParticipant.open('b',b,path.join(s.root,'pb.json'));let coordinator=await DurableTransactionCoordinator.open(path.join(s.root,'coord.json'),{a:pa,b:pb});const result=await coordinator.execute({a:[{type:'put',collection:'items',record:{id:'a',v:1}}],b:[{type:'put',collection:'items',record:{id:'b',v:2}}]},{txId:'tx-1'});assert.equal(result.status,'committed');assert.equal(a.collection('items').get('a').v,1);assert.equal(b.collection('items').get('b').v,2);coordinator=await DurableTransactionCoordinator.open(path.join(s.root,'coord.json'),{a:pa,b:pb});assert.deepEqual(await coordinator.recover(),[]);assert.equal(coordinator.get('tx-1').status,'committed');await Promise.all([a.close(),b.close()]);
});

test('sharded database targets shard-key equality and executes durable cross-shard mutations',async(t)=>{
  const s=await temp();t.after(()=>fs.rm(s.root,{recursive:true,force:true}));const a=await openCore(s.root,'sa'),b=await openCore(s.root,'sb');const db=await ShardedSyncioDatabase.open({a,b},{journalDirectory:path.join(s.root,'dtx'),virtualNodes:16,shardKey:'tenant'});let one={id:'1',tenant:'one'},two={id:'2',tenant:'two'};if(db.locate(one)===db.locate(two)){two={id:'2',tenant:'three'};if(db.locate(one)===db.locate(two))two={id:'2',tenant:'four'};}assert.notEqual(db.locate(one),db.locate(two));const result=await db.distributedMutate([{type:'put',collection:'items',record:one},{type:'put',collection:'items',record:two}],{txId:'cross-1'});assert.equal(result.status,'committed');assert.equal(db.explainQuery({where:{tenant:one.tenant}}).strategy,'targeted_shard');assert.equal(db.collection('items').query({where:{tenant:one.tenant}})[0].id,'1');await db.close();
});

test('global commit ledger is tamper evident and subscription router enforces bounded capacity',async(t)=>{
  const s=await temp();t.after(()=>fs.rm(s.root,{recursive:true,force:true}));let ledger=await GlobalCommitLedger.open(path.join(s.root,'global.json'));await ledger.append({region:'east',partition:'p1',commitId:'c1',sequence:1});assert.equal(ledger.verify(),true);await ledger.close();ledger=await GlobalCommitLedger.open(path.join(s.root,'global.json'));assert.equal(ledger.length,1);const db=await openCore(s.root,'sub');const router=new SubscriptionRouter({p:db},{maxSubscriptions:1});const first=router.subscribe({partition:'p',collection:'items',listener:()=>{}});assert.throws(()=>router.subscribe({partition:'p',collection:'items',listener:()=>{}}),e=>e.code==='SYNCIO_SUBSCRIPTION_CAPACITY');first.close();assert.equal(router.status().subscriptions,0);router.closeAll();await db.close();await ledger.close();
});

test('regional database fails over writes to healthy region and records global commit metadata',async(t)=>{
  const s=await temp();t.after(()=>fs.rm(s.root,{recursive:true,force:true}));const nodes=[];for(const n of ['e1','e2','w1','w2'])nodes.push(await openCore(s.root,n));const east=new ReplicatedPartitionGroup({e1:nodes[0],e2:nodes[1]},{writeQuorum:2,readQuorum:1}),west=new ReplicatedPartitionGroup({w1:nodes[2],w2:nodes[3]},{writeQuorum:2,readQuorum:1});const ledger=await GlobalCommitLedger.open(path.join(s.root,'regional-ledger.json'));const regional=new RegionalDistributedDatabase({east,west},{primary:'east',ledger});await regional.put('items',{id:'a',value:1});regional.markRegion('east',{healthy:false,latencyMs:100});assert.equal(regional.status().topology.primary,'west');await regional.put('items',{id:'b',value:2});assert.equal(regional.status().globalCommits,2);assert.equal((await regional.get('items','b',{preferredRegion:'west'})).value,2);await regional.close();await Promise.all(nodes.map(x=>x.close()));
});
