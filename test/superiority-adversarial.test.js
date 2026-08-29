import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { StorageBackedSyncioDatabase } from '../src/storage-core.js';
import { ReplicatedPartitionGroup, DurableTransactionCoordinator } from '../src/distributed.js';
import { ShardedSyncioDatabase } from '../src/sharding.js';
import { openSuperiorProduction } from '../src/superior-production.js';
import { createResourcePolicy } from '../src/resource-policy.js';

async function temp(){const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-adversarial-'));return{root,file:path.join(root,'data.syncio.json')};}
async function core(root,name,options={}){return StorageBackedSyncioDatabase.open(path.join(root,`${name}.syncio.json`),{checkpointEvery:1000,storage:{cacheBytes:64*1024,indexCacheBuckets:2},...options});}

test('wildcard authority cannot bypass a narrower collection deny rule',()=>{
  const policy=createResourcePolicy([{effect:'allow',collection:'*',action:'read'},{effect:'deny',collection:'secret',action:'read'}]);
  assert.equal(policy.hasScope({action:'read',collection:'public'}),true);
  assert.equal(policy.hasScope({action:'read',collection:'secret'}),false);
  assert.equal(policy.hasScope({action:'read',collection:'*'}),false);
});

test('failed write quorum rolls back the successful minority replica',async(t)=>{
  const s=await temp();t.after(()=>fs.rm(s.root,{recursive:true,force:true}));
  const healthy=await core(s.root,'healthy');
  const blocked1=await core(s.root,'blocked1',{schedulerOptions:{budgets:{cpu:0}}});
  const blocked2=await core(s.root,'blocked2',{schedulerOptions:{budgets:{cpu:0}}});
  const group=new ReplicatedPartitionGroup({healthy,blocked1,blocked2},{writeQuorum:2,readQuorum:2});
  await assert.rejects(()=>group.put('items',{id:'x',value:1}),e=>e.code==='SYNCIO_QUORUM_UNAVAILABLE'&&e.details.rolledBack.includes('healthy'));
  assert.equal(healthy.collection('items').get('x'),null);
  assert.equal(group.status().metrics.failedQuorums,1);
  await Promise.all([healthy.close(),blocked1.close(),blocked2.close()]);
});

test('prepared distributed transaction reserves keys against normal sharded writes until abort',async(t)=>{
  const s=await temp();t.after(()=>fs.rm(s.root,{recursive:true,force:true}));
  const a=await core(s.root,'a'),b=await core(s.root,'b');
  const db=await ShardedSyncioDatabase.open({a,b},{journalDirectory:path.join(s.root,'journal'),virtualNodes:16,shardKey:'tenant'});
  const record={id:'reserved',tenant:'alpha',value:1},shardId=db.locate(record),participant=db.participants[shardId];
  await participant.prepare('hold-1',[{type:'put',collection:'items',record}]);
  await assert.rejects(()=>db.collection('items').upsert(record),e=>e.code==='SYNCIO_DISTRIBUTED_LOCKED');
  await participant.abort('hold-1');
  await db.collection('items').upsert(record);
  assert.equal(db.collection('items').get(record.id,record).value,1);
  await db.close();
});

test('coordinator recovery completes an already-decided cross-partition commit after restart',async(t)=>{
  const s=await temp();t.after(()=>fs.rm(s.root,{recursive:true,force:true}));
  const a=await core(s.root,'pa'),b=await core(s.root,'pb');
  const router=await ShardedSyncioDatabase.open({a,b},{journalDirectory:path.join(s.root,'journal'),virtualNodes:16,shardKey:'tenant'});
  let one={id:'a',tenant:'one',value:1},two={id:'b',tenant:'two',value:2};for(const candidate of ['three','four','five','six']){if(router.locate(one)!==router.locate(two))break;two={...two,tenant:candidate};}
  assert.notEqual(router.locate(one),router.locate(two));
  const sid1=router.locate(one),sid2=router.locate(two),plan={};plan[sid1]=[{type:'put',collection:'items',record:one}];plan[sid2]=[{type:'put',collection:'items',record:two}];
  await router.participants[sid1].prepare('recover-1',plan[sid1]);await router.participants[sid2].prepare('recover-1',plan[sid2]);
  await router.participants[sid1].commit('recover-1');
  const coordinatorFile=path.join(s.root,'journal','coordinator.json');
  await fs.writeFile(coordinatorFile,JSON.stringify({version:1,transactions:{'recover-1':{txId:'recover-1',status:'committing',digest:'fault-injected-after-decision',plan,prepared:[sid1,sid2],committed:[sid1],createdAt:Date.now(),updatedAt:Date.now()}}}));
  const coordinator=await DurableTransactionCoordinator.open(coordinatorFile,router.participants);
  const recovered=await coordinator.recover();assert.deepEqual(recovered,[{txId:'recover-1',status:'committed'}]);
  assert.equal(a.collection('items').get(one.id)?.value??b.collection('items').get(one.id)?.value,1);
  assert.equal(a.collection('items').get(two.id)?.value??b.collection('items').get(two.id)?.value,2);
  await router.close();
});

test('corrupt secondary index falls back to authoritative SSD state and can be repaired',async(t)=>{
  const s=await temp();t.after(()=>fs.rm(s.root,{recursive:true,force:true}));
  let db=await openSuperiorProduction(s.file,{indexCacheBuckets:1});await db.defineIndex('items','key',{name:'key_idx'});await db.collection('items').upsert({id:'a',key:'alpha',value:7});await db.close();
  const bucketDir=path.join(`${s.file}.secondary-indexes`,'items','key_idx','buckets'),files=await fs.readdir(bucketDir);assert.ok(files.length);await fs.writeFile(path.join(bucketDir,files[0]),'{corrupt');
  db=await openSuperiorProduction(s.file,{indexCacheBuckets:1});const result=db.collection('items').query({where:{key:'alpha'}});assert.equal(result[0].value,7);assert.equal(db.storageStatus().resilience.authoritativeFallback,true);
  const repair=await db.repairIndexes();assert.equal(repair.repaired,true);assert.equal(db.storageStatus().resilience.authoritativeFallback,false);assert.equal(db.collection('items').query({where:{key:'alpha'}})[0].id,'a');await db.close();
});

test('corrupt text index falls back to authoritative scan instead of serving stale results',async(t)=>{
  const s=await temp();t.after(()=>fs.rm(s.root,{recursive:true,force:true}));
  let db=await openSuperiorProduction(s.file,{specialIndexCacheBuckets:1});await db.defineTextIndex('docs','body',{name:'body_text'});await db.collection('docs').upsert({id:'a',body:'alpha beta'});await db.close();
  const bucketDir=path.join(`${s.file}.special-indexes`,'text','docs','body_text','buckets'),files=await fs.readdir(bucketDir);assert.ok(files.length);for(const file of files)await fs.writeFile(path.join(bucketDir,file),'{corrupt');
  db=await openSuperiorProduction(s.file,{specialIndexCacheBuckets:1});const result=db.search('docs','alpha',{index:'body_text'});assert.equal(result[0].record.id,'a');assert.equal(db.storageStatus().resilience.authoritativeFallback,true);await db.repairIndexes();assert.equal(db.search('docs','alpha',{index:'body_text'})[0].record.id,'a');await db.close();
});
