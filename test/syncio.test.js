import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { open } from '../src/index.js';

test('Syncio persists records across reopen', async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'syncio-'));
  const file = path.join(dir, 'db.json');
  const db = await open(file);
  const users = db.collection('users');
  const saved = await users.insert({ name: 'Charles' });
  await db.close();

  const reopened = await open(file);
  assert.equal(reopened.collection('users').get(saved.id).name, 'Charles');
  await reopened.close();
  await fs.rm(dir, { recursive: true, force: true });
});

test('Syncio emits live collection events after durable writes', async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'syncio-'));
  const file = path.join(dir, 'db.json');
  const db = await open(file);
  const users = db.collection('users');
  const eventPromise = new Promise((resolve) => {
    const stop = users.watch((event) => {
      stop();
      resolve(event);
    });
  });

  const saved = await users.insert({ name: 'Ada' });
  const event = await eventPromise;
  assert.equal(event.type, 'insert');
  assert.equal(event.id, saved.id);
  assert.equal(event.record.name, 'Ada');
  await db.close();
  await fs.rm(dir, { recursive: true, force: true });
});

test('Syncio serializes concurrent writes without losing records', async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'syncio-'));
  const file = path.join(dir, 'db.json');
  const db = await open(file);
  const items = db.collection('items');
  await Promise.all(Array.from({ length: 20 }, (_, index) => items.insert({ index })));
  assert.equal(items.all().length, 20);
  await db.close();
  await fs.rm(dir, { recursive: true, force: true });
});
