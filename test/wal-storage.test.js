import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { SyncioDatabase } from '../src/index.js';

async function tempDatabase(name) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), `syncio-wal-${name}-`));
  return { root, file: path.join(root, 'data.json') };
}

test('normal commit is durable in WAL without rewriting checkpoint and replays after crash-style reopen', async (t) => {
  const state = await tempDatabase('replay');
  t.after(async () => fs.rm(state.root, { recursive: true, force: true }));
  const db = await SyncioDatabase.open(state.file, { checkpointEvery: 100 });
  await db.collection('items').insert({ id: 'a', value: 1 });

  const checkpoint = JSON.parse(await fs.readFile(state.file, 'utf8'));
  assert.equal(checkpoint._syncio.sequence, 0);
  const walBefore = await fs.readFile(`${state.file}.wal`, 'utf8');
  assert.match(walBefore, /syncio-wal\/1/);
  assert.equal(db.storageStatus().commitsSinceCheckpoint, 1);

  const recovered = await SyncioDatabase.open(state.file, { checkpointEvery: 100 });
  assert.deepEqual(recovered.collection('items').get('a'), { id: 'a', value: 1 });
  assert.equal(recovered.sequence, 1);
  await recovered.close();
  assert.equal(await fs.readFile(`${state.file}.wal`, 'utf8'), '');
  assert.equal(JSON.parse(await fs.readFile(state.file, 'utf8'))._syncio.sequence, 1);
  await db.close();
});

test('checkpoint threshold atomically checkpoints state and compacts WAL', async (t) => {
  const state = await tempDatabase('checkpoint');
  t.after(async () => fs.rm(state.root, { recursive: true, force: true }));
  const db = await SyncioDatabase.open(state.file, { checkpointEvery: 2 });
  await db.collection('items').insert({ id: 'a' });
  assert.notEqual(await fs.readFile(`${state.file}.wal`, 'utf8'), '');
  await db.collection('items').insert({ id: 'b' });
  assert.equal(await fs.readFile(`${state.file}.wal`, 'utf8'), '');
  const checkpoint = JSON.parse(await fs.readFile(state.file, 'utf8'));
  assert.equal(checkpoint._syncio.sequence, 2);
  assert.deepEqual(Object.keys(checkpoint.collections.items).sort(), ['a', 'b']);
  assert.equal(db.storageStatus().degraded, false);
  await db.close();
});

test('recovery ignores a truncated final WAL record but rejects corruption in a completed record', async (t) => {
  const truncated = await tempDatabase('truncated');
  const corrupt = await tempDatabase('corrupt');
  t.after(async () => {
    await fs.rm(truncated.root, { recursive: true, force: true });
    await fs.rm(corrupt.root, { recursive: true, force: true });
  });

  const db = await SyncioDatabase.open(truncated.file, { checkpointEvery: 100 });
  await db.collection('items').insert({ id: 'safe', value: 7 });
  await fs.appendFile(`${truncated.file}.wal`, '{"format":"syncio-wal/1"', 'utf8');
  const recovered = await SyncioDatabase.open(truncated.file, { checkpointEvery: 100 });
  assert.equal(recovered.collection('items').get('safe').value, 7);
  await recovered.close();
  await db.close();

  const broken = await SyncioDatabase.open(corrupt.file, { checkpointEvery: 100 });
  await broken.collection('items').insert({ id: 'safe' });
  const lines = (await fs.readFile(`${corrupt.file}.wal`, 'utf8')).trimEnd().split('\n');
  const entry = JSON.parse(lines[0]);
  entry.events[0].record.id = 'tampered';
  await fs.writeFile(`${corrupt.file}.wal`, `${JSON.stringify(entry)}\n`, 'utf8');
  await assert.rejects(SyncioDatabase.open(corrupt.file), (error) => error?.code === 'SYNCIO_CORRUPT_WAL');
});

test('recovery ignores stale WAL already represented by a durable checkpoint', async (t) => {
  const state = await tempDatabase('stale');
  t.after(async () => fs.rm(state.root, { recursive: true, force: true }));
  const db = await SyncioDatabase.open(state.file, { checkpointEvery: 100 });
  await db.collection('items').insert({ id: 'a', value: 1 });
  const staleWal = await fs.readFile(`${state.file}.wal`, 'utf8');
  await db.checkpoint();
  assert.equal(await fs.readFile(`${state.file}.wal`, 'utf8'), '');

  await fs.writeFile(`${state.file}.wal`, staleWal, 'utf8');
  const recovered = await SyncioDatabase.open(state.file, { checkpointEvery: 100 });
  assert.deepEqual(recovered.collection('items').all(), [{ id: 'a', value: 1 }]);
  assert.equal(recovered.sequence, 1);
  await recovered.close();
  await db.close();
});
