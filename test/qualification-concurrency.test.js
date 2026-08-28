import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { SyncioDatabase } from '../src/index.js';

test('serialized transactions preserve all concurrent increments without starvation', async (t) => {
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-tx-contention-'));
  const file=path.join(root,'db.json');
  const db=await SyncioDatabase.open(file);
  t.after(async()=>{await db.close();await fs.rm(root,{recursive:true,force:true});});
  await db.collection('counters').upsert({id:'main',value:0});
  const completions=[];
  await Promise.all(Array.from({length:100},(_,index)=>db.transaction(async(tx)=>{
    const counters=tx.collection('counters');
    const current=counters.get('main');
    counters.put({...current,value:current.value+1});
    completions.push(index);
  })));
  assert.equal(db.collection('counters').get('main').value,100);
  assert.equal(completions.length,100);
  await db.close();
  const reopened=await SyncioDatabase.open(file);
  assert.equal(reopened.collection('counters').get('main').value,100);
  await reopened.close();
});

test('failed transaction under contention does not poison later queued transactions', async (t) => {
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-tx-recovery-'));
  const db=await SyncioDatabase.open(path.join(root,'db.json'));
  t.after(async()=>{await db.close();await fs.rm(root,{recursive:true,force:true});});
  await db.collection('items').upsert({id:'base',value:0});
  const jobs=[];
  for(let i=0;i<50;i++)jobs.push(db.transaction(async(tx)=>{const c=tx.collection('items');c.put({id:`ok-${i}`,value:i});}));
  jobs.splice(20,0,db.transaction(async(tx)=>{tx.collection('items').put({id:'never',value:1});throw new Error('expected failure');}));
  const results=await Promise.allSettled(jobs);
  assert.equal(results.filter(result=>result.status==='rejected').length,1);
  assert.equal(db.collection('items').get('never'),null);
  for(let i=0;i<50;i++)assert.equal(db.collection('items').get(`ok-${i}`).value,i);
});
