import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { openSuperiorProduction } from '../src/superior-production.js';
import { runWithWriteProvenance } from '../src/write-provenance.js';

test('write provenance is durably attached to authoritative change history', async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'syncio-provenance-'));
  const file = path.join(dir, 'db.json');
  let db = await openSuperiorProduction(file, { lease: { heartbeatMs: 60_000 } });
  t.after(async () => {
    await db?.close().catch(() => undefined);
    await fs.rm(dir, { recursive: true, force: true });
  });

  await runWithWriteProvenance({
    actorId: 'agent:invoice-worker',
    actorKind: 'ai_agent',
    executionId: 'session:run-123',
    requestId: 'request:456',
    authorityRef: 'grant:789',
    causeRef: 'task:invoice-reconcile'
  }, () => db.collection('items').insert({ id: 'a', value: 1 }));

  const first = db.changesSince(0)[0];
  assert.deepEqual(first.provenance, {
    actorId: 'agent:invoice-worker',
    actorKind: 'ai_agent',
    executionId: 'session:run-123',
    requestId: 'request:456',
    authorityRef: 'grant:789',
    causeRef: 'task:invoice-reconcile'
  });

  await db.close();
  db = await openSuperiorProduction(file, { lease: { heartbeatMs: 60_000 } });
  const recovered = db.changesSince(0)[0];
  assert.deepEqual(recovered.provenance, first.provenance);
});

test('provenance rejects secrets and unbounded nested metadata shapes', async () => {
  assert.throws(() => runWithWriteProvenance({ actorId: 'agent', metadata: { nested: { secret: true } } }, () => undefined), /metadata values must be scalar/);
  assert.throws(() => runWithWriteProvenance({ actorId: 'contains spaces' }, () => undefined), /invalid write provenance actorId/);
});
