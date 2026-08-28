import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { SyncioDatabase, createSyncioServer, createReplicationPacket } from '../src/index.js';

const allowAll = [{ effect: 'allow' }];

async function database(name, options) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), `syncio-${name}-`));
  const file = path.join(root, 'db.json');
  return { root, file, db: await SyncioDatabase.open(file, options) };
}

async function closeAndRemove(state) {
  await state.db?.close().catch(() => undefined);
  await fs.rm(state.root, { recursive: true, force: true });
}

test('durable change feed survives database and server restart', async (t) => {
  const state = await database('durable-feed');
  t.after(() => closeAndRemove(state));
  const firstServer = createSyncioServer({ db: state.db, policies: allowAll });
  const firstAddress = await firstServer.listen();
  const write = await fetch(`${firstAddress.url}/collections/items/a`, { method: 'PUT', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ value: 1, updatedAt: '2026-08-28T12:00:00.000Z' }) });
  assert.equal(write.status, 200);
  await firstServer.close();
  await state.db.close();
  state.db = await SyncioDatabase.open(state.file);
  const secondServer = createSyncioServer({ db: state.db, policies: allowAll });
  t.after(() => secondServer.close());
  const secondAddress = await secondServer.listen();
  const pulled = await fetch(`${secondAddress.url}/replicate/pull`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ cursor: 0 }) });
  assert.equal(pulled.status, 200);
  const packet = await pulled.json();
  assert.equal(packet.changes.length, 1);
  assert.equal(packet.changes[0].record.id, 'a');
  assert.equal(packet.changes[0].record.value, 1);
  assert.equal(packet.cursor, packet.changes[0].sequence);
});

test('replication push is idempotent for the same changeId', async (t) => {
  const state = await database('idempotent-replication');
  t.after(() => closeAndRemove(state));
  const service = createSyncioServer({ db: state.db, policies: allowAll });
  t.after(() => service.close());
  const address = await service.listen();
  const change = { changeId: crypto.randomUUID(), originDatabaseId: 'remote-a', collection: 'items', type: 'upsert', record: { id: 'same', value: 7, updatedAt: '2026-08-28T12:00:00.000Z' } };
  const packet = createReplicationPacket({ from: 'remote-a', changes: [change] });
  let response = await fetch(`${address.url}/replicate/push`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(packet) });
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { ok: true, accepted: 1, duplicates: 0, sequence: 1 });
  response = await fetch(`${address.url}/replicate/push`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(packet) });
  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), { ok: true, accepted: 0, duplicates: 1, sequence: 1 });
  assert.equal(state.db.collection('items').get('same').value, 7);
  assert.equal(state.db.changesSince(0).length, 1);
});

test('HTTP boundary rejects oversized and malformed request bodies', async (t) => {
  const state = await database('http-boundary');
  t.after(() => closeAndRemove(state));
  const service = createSyncioServer({ db: state.db, policies: allowAll, maxBodyBytes: 64 });
  t.after(() => service.close());
  const address = await service.listen();
  let response = await fetch(`${address.url}/collections/items`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ payload: 'x'.repeat(100) }) });
  assert.equal(response.status, 413);
  assert.equal((await response.json()).error, 'payload_too_large');
  response = await fetch(`${address.url}/collections/items`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{not-json' });
  assert.equal(response.status, 400);
  assert.equal((await response.json()).error, 'invalid_json');
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

test('corrupt primary file recovers from last durable backup', async (t) => {
  const state = await database('backup-recovery');
  t.after(() => closeAndRemove(state));
  const items = state.db.collection('items');
  await items.upsert({ id: 'a', value: 1 });
  await items.upsert({ id: 'b', value: 2 });
  await state.db.close();
  await fs.writeFile(state.file, '{corrupt', 'utf8');
  state.db = await SyncioDatabase.open(state.file);
  assert.equal(state.db.collection('items').get('a').value, 1);
  assert.equal(state.db.collection('items').get('b'), null);
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
  assert.equal(state.db.changesSince(0).length, 2);
});

test('failed transaction leaves memory and disk unchanged', async (t) => {
  const state = await database('transaction-rollback');
  t.after(() => closeAndRemove(state));
  await state.db.collection('accounts').upsert({ id: 'a', balance: 100 });
  await assert.rejects(state.db.transaction(async (tx) => {
    tx.collection('accounts').put({ id: 'a', balance: 0 });
    tx.collection('accounts').put({ id: 'b', balance: 100 });
    throw new Error('abort');
  }), /abort/);
  assert.equal(state.db.collection('accounts').get('a').balance, 100);
  assert.equal(state.db.collection('accounts').get('b'), null);
  assert.equal(state.db.changesSince(0).length, 1);
  await state.db.close();
  state.db = await SyncioDatabase.open(state.file);
  assert.equal(state.db.collection('accounts').get('a').balance, 100);
  assert.equal(state.db.collection('accounts').get('b'), null);
});

test('records reject values that would change type or meaning after JSON persistence', async (t) => {
  const state = await database('json-consistency');
  t.after(() => closeAndRemove(state));
  const items = state.db.collection('items');
  await assert.rejects(items.insert({ value: new Date() }), /non-plain object/);
  await assert.rejects(items.insert({ value: Number.NaN }), /non-finite number/);
  await assert.rejects(items.insert({ value: undefined }), /non-JSON value/);
  assert.equal(items.all().length, 0);
});
