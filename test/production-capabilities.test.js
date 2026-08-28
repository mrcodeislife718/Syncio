import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { ProductionSyncioDatabase } from '../src/production-db.js';

async function setup(){
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-production-cap-'));
  const file=path.join(root,'db.json');
  const db=await ProductionSyncioDatabase.open(file);
  return {root,file,db};
}
async function cleanup(state){await state.db.close().catch(()=>undefined);await fs.rm(state.root,{recursive:true,force:true});}

test('enforced schema rejects invalid normal transaction and replicated writes before commit',async(t)=>{
  const state=await setup();t.after(()=>cleanup(state));
  await state.db.defineSchema('users',{type:'object',required:['id','email'],properties:{id:{type:'string'},email:{type:'string',pattern:'@'},age:{type:'integer',minimum:0}}});
  await state.db.collection('users').upsert({id:'a',email:'a@example.com',age:4});
  const before=state.db.sequence;
  await assert.rejects(state.db.collection('users').upsert({id:'b',email:'invalid'}),(error)=>error.code==='SYNCIO_SCHEMA_VALIDATION_FAILED');
  await assert.rejects(state.db.transaction(async(tx)=>tx.collection('users').put({id:'c',email:'c@example.com',age:-1})),(error)=>error.code==='SYNCIO_SCHEMA_VALIDATION_FAILED');
  await assert.rejects(state.db.applyReplicationChange({changeId:'r1',collection:'users',type:'upsert',record:{id:'d',email:'bad'}},(_local,remote)=>remote),(error)=>error.code==='SYNCIO_SCHEMA_VALIDATION_FAILED');
  assert.equal(state.db.sequence,before);
  assert.deepEqual(state.db.collection('users').all().map((row)=>row.id),['a']);
});

test('schema metadata persists across reopen',async(t)=>{
  const state=await setup();t.after(()=>cleanup(state));
  await state.db.defineSchema('items',{type:'object',required:['id','name'],properties:{id:{type:'string'},name:{type:'string'}}});
  await state.db.close();
  state.db=await ProductionSyncioDatabase.open(state.file);
  assert.equal(state.db.getSchema('items').mode,'enforced');
  await assert.rejects(state.db.collection('items').upsert({id:'x'}),(error)=>error.code==='SYNCIO_SCHEMA_VALIDATION_FAILED');
});

test('TTL sweep removes expired records through normal durable transaction change spine',async(t)=>{
  const state=await setup();t.after(()=>cleanup(state));
  await state.db.defineTTLIndex('sessions','expiresAt');
  await state.db.collection('sessions').upsert({id:'old',expiresAt:'2026-01-01T00:00:00.000Z'});
  await state.db.collection('sessions').upsert({id:'new',expiresAt:'2099-01-01T00:00:00.000Z'});
  const afterWrites=state.db.sequence;
  const result=await state.db.sweepExpired({now:Date.parse('2026-08-28T00:00:00.000Z')});
  assert.equal(result.removed,1);
  assert.equal(state.db.collection('sessions').get('old'),null);
  assert.ok(state.db.sequence>afterWrites);
  assert.equal(state.db.changesSince(afterWrites).at(-1).type,'remove');
});

test('persistent text index returns ranked documents and updates after mutation',async(t)=>{
  const state=await setup();t.after(()=>cleanup(state));
  await state.db.defineTextIndex('docs',['title','body'],{name:'docs_text'});
  await state.db.collection('docs').upsert({id:'a',title:'Realtime database',body:'durable realtime sync realtime'});
  await state.db.collection('docs').upsert({id:'b',title:'Database notes',body:'storage engine'});
  assert.deepEqual(state.db.search('docs','realtime database',{index:'docs_text'}).map((row)=>row.record.id),['a','b']);
  await state.db.collection('docs').update('b',{$set:{body:'realtime realtime realtime'}});
  assert.equal(state.db.search('docs','realtime',{index:'docs_text'})[0].record.id,'b');
  await state.db.close();
  state.db=await ProductionSyncioDatabase.open(state.file);
  assert.equal(state.db.search('docs','storage',{index:'docs_text'}).length,0);
});

test('geospatial index performs deterministic radius and nearest-neighbor search',async(t)=>{
  const state=await setup();t.after(()=>cleanup(state));
  await state.db.defineGeoIndex('places','location',{name:'location_geo'});
  await state.db.collection('places').upsert({id:'bronx',location:[-73.8648,40.8448]});
  await state.db.collection('places').upsert({id:'brooklyn',location:[-73.9442,40.6782]});
  const rows=state.db.near('places',[-73.87,40.85],{index:'location_geo',maxDistanceMeters:5000});
  assert.deepEqual(rows.map((row)=>row.record.id),['bronx']);
  assert.ok(rows[0].distanceMeters<2000);
});
