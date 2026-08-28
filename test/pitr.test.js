import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { ProductionSyncioDatabase } from '../src/production-db.js';
import { PointInTimeRecovery } from '../src/pitr.js';

async function setup(){const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-pitr-'));const db=await ProductionSyncioDatabase.open(path.join(root,'db.json'));const pitr=await PointInTimeRecovery.open(db,path.join(root,'pitr'));return{root,db,pitr};}
async function cleanup(s){await s.pitr.close().catch(()=>undefined);await s.db.close().catch(()=>undefined);await fs.rm(s.root,{recursive:true,force:true});}

test('PITR restores exact historical sequence without mutating live database by default',async(t)=>{
  const s=await setup();t.after(()=>cleanup(s));
  await s.db.collection('items').upsert({id:'a',value:1});
  const first=s.db.sequence;
  await s.db.collection('items').upsert({id:'a',value:2});
  await s.db.collection('items').upsert({id:'b',value:3});
  await s.pitr.flush();
  const restored=await s.pitr.restoreToSequence(first);
  assert.equal(restored.state.collections.items.a.value,1);
  assert.equal(restored.state.collections.items.b,undefined);
  assert.equal(s.db.collection('items').get('a').value,2);
});

test('PITR can replace live state then database continues from restored sequence',async(t)=>{
  const s=await setup();t.after(()=>cleanup(s));
  await s.db.collection('items').upsert({id:'a',value:1});const first=s.db.sequence;
  await s.db.collection('items').upsert({id:'a',value:2});
  await s.pitr.flush();
  await s.pitr.restoreToSequence(first,{replaceLive:true});
  assert.equal(s.db.collection('items').get('a').value,1);
  await s.db.collection('items').upsert({id:'c',value:4});
  assert.equal(s.db.collection('items').get('c').value,4);
});

test('PITR journal and snapshots are integrity checked',async(t)=>{
  const s=await setup();t.after(()=>cleanup(s));
  await s.db.collection('items').upsert({id:'a',value:1});
  await s.pitr.flush();
  const snapshot=await s.pitr.createSnapshot({reason:'test'});
  assert.deepEqual(await s.pitr.verify(),{ok:true,snapshots:2,lastSequence:s.db.sequence});
  const envelope=JSON.parse(await fs.readFile(snapshot.file,'utf8'));envelope.state.collections.items.a.value=999;await fs.writeFile(snapshot.file,JSON.stringify(envelope));
  await assert.rejects(s.pitr.verify(),(error)=>error.code==='SYNCIO_PITR_CORRUPT');
});

test('PITR restart reconciles committed changes still retained by durable database history',async(t)=>{
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-pitr-reconcile-'));t.after(()=>fs.rm(root,{recursive:true,force:true}));
  const file=path.join(root,'db.json');const dir=path.join(root,'pitr');
  let db=await ProductionSyncioDatabase.open(file);let pitr=await PointInTimeRecovery.open(db,dir);
  await db.collection('items').upsert({id:'a',value:1});await pitr.flush();await pitr.close();
  await db.collection('items').upsert({id:'b',value:2});await db.close();
  db=await ProductionSyncioDatabase.open(file);pitr=await PointInTimeRecovery.open(db,dir,{createInitialSnapshot:false});
  assert.equal(pitr.status().lastSequence,db.sequence);
  await pitr.close();await db.close();
});
