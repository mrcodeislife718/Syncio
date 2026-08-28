import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { IndexedSyncioDatabase } from '../src/indexed.js';
import { createSyncioServer } from '../src/server.js';

test('HTTP equality query delegates through persistent indexed planner',async(t)=>{
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-index-http-'));
  const db=await IndexedSyncioDatabase.open(path.join(root,'db.json'));
  await db.defineIndex('users','email');
  await db.collection('users').upsert({id:'u1',email:'a@example.com'});
  await db.collection('users').upsert({id:'u2',email:'b@example.com'});
  let planned=false;
  const original=db.query.bind(db);
  db.query=(collection,spec)=>{planned=true;assert.deepEqual(db.explainQuery(collection,spec),{strategy:'index',field:'email',value:'b@example.com'});return original(collection,spec);};
  const service=createSyncioServer({db,policies:[{effect:'allow'}]});
  const address=await service.listen();
  t.after(async()=>{await service.close();await db.close();await fs.rm(root,{recursive:true,force:true});});
  const where=encodeURIComponent(JSON.stringify({email:'b@example.com'}));
  const response=await fetch(`${address.url}/collections/users?where=${where}`);
  assert.equal(response.status,200);
  assert.equal(planned,true);
  assert.deepEqual((await response.json()).records.map(item=>item.id),['u2']);
});
