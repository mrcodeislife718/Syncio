import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { IndexedSyncioDatabase } from '../src/indexed.js';

async function setup(){
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-compound-index-'));
  const file=path.join(root,'db.json');
  const db=await IndexedSyncioDatabase.open(file);
  return {root,file,db};
}
async function cleanup(state){await state.db.close().catch(()=>undefined);await fs.rm(state.root,{recursive:true,force:true});}

test('compound index persists and is selected for multi-field equality query',async(t)=>{
  const state=await setup();t.after(()=>cleanup(state));
  await state.db.defineIndex('users',['tenantId','profile.email'],{name:'tenant_email'});
  await state.db.collection('users').upsert({id:'a',tenantId:'t1',profile:{email:'a@example.com'},active:true});
  await state.db.collection('users').upsert({id:'b',tenantId:'t1',profile:{email:'b@example.com'},active:true});
  const plan=state.db.collection('users').explain({where:{tenantId:'t1','profile.email':'b@example.com'}});
  assert.deepEqual(plan,{strategy:'index',index:'tenant_email',fields:['tenantId','profile.email'],values:['t1','b@example.com']});
  assert.deepEqual(state.db.collection('users').query({where:{tenantId:'t1','profile.email':'b@example.com'}}).map((row)=>row.id),['b']);
  await state.db.close();
  state.db=await IndexedSyncioDatabase.open(state.file);
  assert.equal(state.db.listIndexes().some((index)=>index.name==='tenant_email'&&index.fields.length===2),true);
  assert.equal(state.db.collection('users').explain({where:{tenantId:'t1','profile.email':'a@example.com'}}).strategy,'index');
});

test('unique index rejects duplicate before persistence and leaves durable state unchanged',async(t)=>{
  const state=await setup();t.after(()=>cleanup(state));
  await state.db.defineIndex('users','email',{unique:true});
  await state.db.collection('users').upsert({id:'a',email:'same@example.com'});
  const sequence=state.db.sequence;
  await assert.rejects(state.db.collection('users').upsert({id:'b',email:'same@example.com'}),(error)=>error.code==='SYNCIO_UNIQUE_INDEX_VIOLATION');
  assert.equal(state.db.sequence,sequence);
  assert.equal(state.db.collection('users').get('b'),null);
  await state.db.close();
  state.db=await IndexedSyncioDatabase.open(state.file);
  assert.equal(state.db.collection('users').get('b'),null);
});

test('concurrent writes cannot race through a unique index',async(t)=>{
  const state=await setup();t.after(()=>cleanup(state));
  await state.db.defineIndex('users','email',{unique:true});
  const results=await Promise.allSettled([
    state.db.collection('users').upsert({id:'a',email:'race@example.com'}),
    state.db.collection('users').upsert({id:'b',email:'race@example.com'})
  ]);
  assert.equal(results.filter((result)=>result.status==='fulfilled').length,1);
  assert.equal(results.filter((result)=>result.status==='rejected'&&result.reason?.code==='SYNCIO_UNIQUE_INDEX_VIOLATION').length,1);
  assert.equal(state.db.collection('users').all().length,1);
  await state.db.close();
  state.db=await IndexedSyncioDatabase.open(state.file);
  assert.equal(state.db.collection('users').all().length,1);
});

test('compound unique index permits same value in different compound partition',async(t)=>{
  const state=await setup();t.after(()=>cleanup(state));
  await state.db.defineIndex('users',['tenantId','email'],{name:'tenant_email_unique',unique:true});
  await state.db.collection('users').upsert({id:'a',tenantId:'t1',email:'person@example.com'});
  await state.db.collection('users').upsert({id:'b',tenantId:'t2',email:'person@example.com'});
  await assert.rejects(state.db.collection('users').upsert({id:'c',tenantId:'t1',email:'person@example.com'}),(error)=>error.code==='SYNCIO_UNIQUE_INDEX_VIOLATION');
  assert.deepEqual(state.db.collection('users').all().map((row)=>row.id).sort(),['a','b']);
});

test('unique constraints are enforced inside multi-document transaction before commit',async(t)=>{
  const state=await setup();t.after(()=>cleanup(state));
  await state.db.defineIndex('users','username',{unique:true});
  const before=state.db.sequence;
  await assert.rejects(state.db.transaction(async(tx)=>{
    tx.collection('users').put({id:'a',username:'duplicate'});
    tx.collection('users').put({id:'b',username:'duplicate'});
  }),(error)=>error.code==='SYNCIO_UNIQUE_INDEX_VIOLATION');
  assert.equal(state.db.sequence,before);
  assert.deepEqual(state.db.collection('users').all(),[]);
});

test('unique constraint rejects conflicting replicated document before database mutation',async(t)=>{
  const state=await setup();t.after(()=>cleanup(state));
  await state.db.defineIndex('users','email',{unique:true});
  await state.db.collection('users').upsert({id:'local',email:'unique@example.com'});
  const sequence=state.db.sequence;
  await assert.rejects(state.db.applyReplicationChange({changeId:'remote-1',collection:'users',type:'upsert',record:{id:'remote',email:'unique@example.com'}},(_local,remote)=>remote),(error)=>error.code==='SYNCIO_UNIQUE_INDEX_VIOLATION');
  assert.equal(state.db.sequence,sequence);
  assert.equal(state.db.collection('users').get('remote'),null);
});

test('sparse unique index allows documents without indexed field but rejects duplicate present values',async(t)=>{
  const state=await setup();t.after(()=>cleanup(state));
  await state.db.defineIndex('users','externalId',{unique:true,sparse:true});
  await state.db.collection('users').upsert({id:'a',name:'A'});
  await state.db.collection('users').upsert({id:'b',name:'B'});
  await state.db.collection('users').upsert({id:'c',externalId:'x'});
  await assert.rejects(state.db.collection('users').upsert({id:'d',externalId:'x'}),(error)=>error.code==='SYNCIO_UNIQUE_INDEX_VIOLATION');
});
