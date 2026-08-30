import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { WriteAheadLog, readWalEntries } from '../src/wal.js';

function ioWithOpenFailure({ code, failWhen }) {
  let armed = true;
  return {
    ...fs,
    async open(target, flags, mode) {
      if (armed && failWhen(target, flags)) {
        armed = false;
        const error = new Error(code);
        error.code = code;
        throw error;
      }
      return fs.open(target, flags, mode);
    }
  };
}

test('WAL ENOSPC failure does not publish an in-memory durable entry and queue recovers', async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'syncio-wal-enospc-'));
  const file = path.join(dir, 'db.wal');
  const io = ioWithOpenFailure({ code: 'ENOSPC', failWhen: (_target, flags) => flags === 'a' });
  const wal = await WriteAheadLog.open(file, { io });

  await assert.rejects(
    wal.append({ databaseId: 'db1', baseSequence: 0, resultSequence: 1, events: [{ type: 'put', id: '1' }] }),
    (error) => error?.code === 'ENOSPC'
  );
  assert.deepEqual(wal.listAfter(0), []);

  await wal.append({ databaseId: 'db1', baseSequence: 0, resultSequence: 1, events: [{ type: 'put', id: '1' }] });
  assert.equal(wal.listAfter(0).length, 1);
  assert.equal((await readWalEntries(file)).length, 1);
  await wal.close();
});

test('WAL read-only compaction failure preserves the previously durable log', async () => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'syncio-wal-erofs-'));
  const file = path.join(dir, 'db.wal');
  const seed = await WriteAheadLog.open(file);
  await seed.append({ databaseId: 'db1', baseSequence: 0, resultSequence: 1, events: [{ type: 'put', id: '1' }] });
  await seed.append({ databaseId: 'db1', baseSequence: 1, resultSequence: 2, events: [{ type: 'put', id: '2' }] });
  await seed.close();

  const io = ioWithOpenFailure({ code: 'EROFS', failWhen: (target, flags) => flags === 'r' && target.includes('.tmp') });
  const wal = await WriteAheadLog.open(file, { io });
  await assert.rejects(wal.compactThrough(1), (error) => error?.code === 'EROFS');

  const durable = await readWalEntries(file);
  assert.equal(durable.length, 2);
  assert.equal(durable[0].resultSequence, 1);
  assert.equal(durable[1].resultSequence, 2);
  assert.equal(wal.listAfter(0).length, 2);
  await wal.close();
});
