import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { SyncioDatabase, createSyncioServer } from '../src/index.js';

test('write policies can inspect bounded parsed request bodies', async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'syncio-policy-body-'));
  const db = await SyncioDatabase.open(path.join(root, 'db.json'));
  const service = createSyncioServer({
    db,
    policies: [{ effect: 'allow', collection: 'profiles', action: 'write', when: ({ body }) => body?.role !== 'admin' }]
  });
  t.after(async () => { await service.close(); await db.close(); await fs.rm(root, { recursive: true, force: true }); });
  const address = await service.listen();

  let response = await fetch(`${address.url}/collections/profiles/u1`, {
    method: 'PUT', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ role: 'member' })
  });
  assert.equal(response.status, 200);
  assert.equal(db.collection('profiles').get('u1').role, 'member');

  response = await fetch(`${address.url}/collections/profiles/u2`, {
    method: 'PUT', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ role: 'admin' })
  });
  assert.equal(response.status, 403);
  assert.equal(db.collection('profiles').get('u2'), null);
});
