import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { IndexedSyncioDatabase } from '../src/indexed.js';

test('persistent index survives reopen and is selected for equality query',async(t)=>{
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-indexed-'));t.after(()=>fs.rm(root,{recursive:true,force:true}));
  const file=path.join(root,'db.json');
  let db=await IndexedSyncioDatabase.open(file);
  await db.defineIndex('users','email');
  const users=db.collection('users');
  await users.upsert({id:'1',email:'a@example.com',age:10});
  await users.upsert({id:'2',email:'b@example.com',age:20});
  assert.deepEqual(users.explain({where:{email:'b@example.com'}}),{strategy:'index',field:'email',value:'b@example.com'});
  assert.deepEqual(users.query({where:{email:'b@example.com'}}).map(r=>r.id),['2']);
  assert.deepEqual(users.explain({where:{age:{$gte:15}}}),{strategy:'scan'});
  await db.close();
  db=await IndexedSyncioDatabase.open(file);
  t.after(()=>db.close());
  assert.deepEqual(db.listIndexes(),[{collection:'users',field:'email'}]);
  assert.deepEqual(db.collection('users').query({where:{email:'a@example.com'}}).map(r=>r.id),['1']);
});

test('index remains correct across upsert remove transaction and replication rebuild',async(t)=>{
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-index-maint-'));t.after(()=>fs.rm(root,{recursive:true,force:true}));
  const db=await IndexedSyncioDatabase.open(path.join(root,'db.json'));t.after(()=>db.close());
  await db.defineIndex('items','group');
  const items=db.collection('items');
  await items.upsert({id:'a',group:'x'});
  await items.upsert({id:'a',group:'y'});
  assert.equal(items.query({where:{group:'x'}}).length,0);
  assert.equal(items.query({where:{group:'y'}}).length,1);
  await items.remove('a');
  assert.equal(items.query({where:{group:'y'}}).length,0);
  await db.transaction(async(tx)=>{tx.collection('items').put({id:'b',group:'z'});tx.collection('items').put({id:'c',group:'z'});});
  assert.deepEqual(items.query({where:{group:'z'}}).map(r=>r.id).sort(),['b','c']);
});
