import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { SyncioDatabase } from '../src/index.js';
import { createSyncioServer } from '../src/server.js';

async function database(name, options = {}) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), `syncio-${name}-`));
  const file = path.join(dir, 'db.json');
  return { dir, file, db: await SyncioDatabase.open(file, options) };
}

async function closeAndRemove(state) {
  await state.db?.close().catch(() => undefined);
  await fs.rm(state.dir, { recursive: true, force: true });
}

const allowAll = [{ effect: 'allow', collection: '*', action: '*' }];

test('durable change feed survives database and server restart', async (t) => {
  const state = await database('durable-change');
  t.after(() => closeAndRemove(state));
  await state.db.collection('items').insert({ id: 'a', value: 1 });
  const first = state.db.changesSince(0)[0];
  assert.equal(first.collection, 'items');
  assert.equal(first.type, 'insert');
  await state.db.close();
  state.db = await SyncioDatabase.open(state.file);
  assert.deepEqual(state.db.changesSince(0).map((change) => change.changeId), [first.changeId]);

  const service = createSyncioServer({ db: state.db, policies: allowAll });
  t.after(() => service.close());
  const address = await service.listen();
  const response = await fetch(`${address.url}/replicate/pull`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ cursor: 0 }) });
  assert.equal(response.status, 200);
  const packet = await response.json();
  assert.equal(packet.changes[0].changeId, first.changeId);
});

test('replication push is idempotent for the same changeId', async (t) => {
  const state = await database('idempotency');
  t.after(() => closeAndRemove(state));
  const service = createSyncioServer({ db: state.db, policies: allowAll });
  t.after(() => service.close());
  const address = await service.listen();
  const change = { changeId: 'fixed-change', collection: 'items', type: 'upsert', record: { id: 'a', value: 1 }, sequence: 1, originDatabaseId: 'remote' };
  const packet = { protocol: 'syncio-replication/1', from: 'remote', cursor: 1, changes: [change] };
  const crypto = await import('node:crypto');
  const payload = { from: packet.from, cursor: packet.cursor, changes: packet.changes };
  packet.digest = crypto.createHash('sha256').update(JSON.stringify(payload)).digest('hex');

  for (let i = 0; i < 2; i++) {
    const response = await fetch(`${address.url}/replicate/push`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(packet) });
    assert.equal(response.status, 200);
  }
  assert.equal(state.db.collection('items').all().length, 1);
  assert.equal(state.db.changesSince(0).length, 1);
});

test('HTTP boundary rejects oversized and malformed request bodies', async (t) => {
  const state = await database('http-boundary');
  t.after(() => closeAndRemove(state));
  const service = createSyncioServer({ db: state.db, policies: allowAll, maxBodyBytes: 32 });
  t.after(() => service.close());
  const address = await service.listen();

  const oversized = await fetch(`${address.url}/collections/items`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ id: 'a', value: 'x'.repeat(100) }) });
  assert.equal(oversized.status, 413);
  assert.equal((await oversized.json()).error, 'payload_too_large');

  const malformed = await fetch(`${address.url}/collections/items`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{' });
  assert.equal(malformed.status, 400);
  assert.equal((await malformed.json()).error, 'invalid_json');
});

test('internal exceptions are observable without leaking exception messages to clients', async (t) => {
  const state = await database('error-redaction');
  t.after(() => closeAndRemove(state));
  const events = [];
  const service = createSyncioServer({ db: state.db, policies: allowAll, authenticate: async () => { throw new Error('credential-secret-should-not-leak'); }, observe: (event) => events.push(event) });
  t.after(() => service.close());
  const address = await service.listen();
  const response = await fetch(`${address.url}/collections/items`);
  assert.equal(response.status, 500);
  const body = await response.json();
  assert.equal(body.error, 'internal_error');
  assert.equal(JSON.stringify(body).includes('credential-secret-should-not-leak'), false);
  assert.ok(response.headers.get('x-syncio-request-id'));
  assert.ok(events.some((event) => event.type === 'request_error' && event.status === 500));
});

test('corrupt primary file recovers latest durable checkpoint from backup', async (t) => {
  const state = await database('backup-recovery');
  t.after(() => closeAndRemove(state));
  const items = state.db.collection('items');
  await items.upsert({ id: 'a', value: 1 });
  await items.upsert({ id: 'b', value: 2 });
  await state.db.close();
  await fs.writeFile(state.file, '{corrupt', 'utf8');
  state.db = await SyncioDatabase.open(state.file);
  assert.equal(state.db.collection('items').get('a').value, 1);
  assert.equal(state.db.collection('items').get('b').value, 2);
});

test('integrated transaction commits multiple records atomically and emits durable changes', async (t) => {
  const state = await database('transaction');
  t.after(() => closeAndRemove(state));
  await state.db.transaction(async (tx) => {
    tx.collection('accounts').put({ id: 'a', balance: 90 });
    tx.collection('accounts').put({ id: 'b', balance: 10 });
  });
  assert.equal(state.db.collection('accounts').get('a').balance, 90);
  assert.equal(state.db.collection('accounts').get('b').balance, 10);
  assert.equal(state.db.changesSince(0).length, 2);
  await state.db.close();
  state.db = await SyncioDatabase.open(state.file);
  assert.equal(state.db.collection('accounts').get('a').balance, 90);
  assert.equal(state.db.collection('accounts').get('b').balance, 10);
});

test('failed transaction leaves memory and disk unchanged', async (t) => {
  const state = await database('transaction-rollback');
  t.after(() => closeAndRemove(state));
  await state.db.collection('accounts').insert({ id: 'a', balance: 100 });
  const before = JSON.stringify(state.db.snapshot());
  await assert.rejects(state.db.transaction(async (tx) => {
    tx.collection('accounts').put({ id: 'a', balance: 0 });
    tx.collection('accounts').put({ id: 'b', balance: 100 });
    throw new Error('abort');
  }), /abort/);
  assert.equal(JSON.stringify(state.db.snapshot()), before);
  await state.db.close();
  state.db = await SyncioDatabase.open(state.file);
  assert.equal(state.db.collection('accounts').get('a').balance, 100);
  assert.equal(state.db.collection('accounts').get('b'), null);
});

test('records reject values that would change type or meaning after JSON persistence', async (t) => {
  const state = await database('json-consistency');
  t.after(() => closeAndRemove(state));
  await assert.rejects(state.db.collection('items').insert({ id: 'nan', value: Number.NaN }), /non-finite/);
  await assert.rejects(state.db.collection('items').insert({ id: 'undefined', value: undefined }), /non-JSON/);
  await assert.rejects(state.db.collection('items').insert({ id: 'date', value: new Date() }), /non-plain object/);
  const circular = { id: 'circle' };
  circular.self = circular;
  await assert.rejects(state.db.collection('items').insert(circular), /circular/);
  assert.deepEqual(state.db.collection('items').all(), []);
});
