import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { open, createSyncioServer, ReplicationClient, applyReplicatedChange, queryRecords, TransactionManager, ChangeLog, OfflineQueue, createPolicyEngine, migrate } from '../src/index.js';

test('queries, transactions, policies, migrations, and change logs operate', async () => {
  const records = queryRecords([{id:'1',age:2},{id:'2',age:5}], { where: { age: { $gte: 3 } } });
  assert.deepEqual(records.map(r=>r.id), ['2']);
  let state = { version: 1, collections: { users: {} } };
  const tx = new TransactionManager(() => state, async (next) => { state = next; });
  await tx.run(async (draft) => draft.collection('users').put({ id: 'a', name: 'A' }));
  assert.equal(state.collections.users.a.name, 'A');
  const log = new ChangeLog('node'); assert.equal(log.append({type:'x'}).clock, 1);
  const policy = createPolicyEngine([{ effect:'allow', collection:'users', action:'read' }]);
  assert.equal(policy.authorize({collection:'users',action:'read'}), true);
  assert.equal(policy.authorize({collection:'users',action:'write'}), false);
  const migrated = migrate({version:1,value:1}, [{version:2,up:(s)=>({...s,value:2})}]);
  assert.equal(migrated.version, 2);
});

test('self-host server performs CRUD and replication endpoints', async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'syncio-'));
  const db = await open(path.join(dir, 'db.json'));
  const policies = [
    { effect:'allow', collection:'users', action:'read' },
    { effect:'allow', collection:'users', action:'write' },
    { effect:'allow', collection:'users', action:'delete' },
    { effect:'allow', collection:'*', action:'replicate' }
  ];
  const service = createSyncioServer({ db, policies });
  const address = await service.listen();
  let response = await fetch(`${address.url}/collections/users`, { method:'POST', headers:{'content-type':'application/json'}, body:JSON.stringify({id:'u1',name:'Charles'}) });
  assert.equal(response.status, 201);
  response = await fetch(`${address.url}/collections/users/u1`);
  assert.equal((await response.json()).name, 'Charles');
  const client = new ReplicationClient({ baseUrl: address.url });
  const pulled = [];
  await client.pull(async (change) => pulled.push(change));
  assert.ok(pulled.some((change) => change.type === 'insert'));
  await service.close(); await db.close();
});

test('offline queue preserves failed work', async () => {
  const queue = new OfflineQueue(); queue.enqueue({ type:'write' });
  const result = await queue.flush(async () => { throw new Error('offline'); });
  assert.equal(result.pending, 1);
  assert.equal(result.failures.length, 1);
});
