import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { DurableOfflineQueue, createTokenAuthority, createEntitlementGate, MetricsRegistry, AuditLog, createEncryptedBackupManager } from '../src/operations.js';

async function temp(name) { return fs.mkdtemp(path.join(os.tmpdir(), `syncio-${name}-`)); }

test('durable offline queue survives restart and preserves retry state', async (t) => {
  const dir = await temp('queue'); t.after(()=>fs.rm(dir,{recursive:true,force:true}));
  const file = path.join(dir,'queue.json');
  let queue = await DurableOfflineQueue.open(file);
  await queue.enqueue({ type:'upsert', id:'a' }, { idempotencyKey:'k1' });
  await queue.enqueue({ type:'upsert', id:'a' }, { idempotencyKey:'k1' });
  assert.equal(queue.size,1);
  let result = await queue.flush(async()=>{ throw new Error('offline'); });
  assert.equal(result.pending,1);
  assert.equal(queue.list()[0].attempts,1);
  queue = await DurableOfflineQueue.open(file);
  assert.equal(queue.size,1);
  assert.equal(queue.list()[0].attempts,1);
  result = await queue.flush(async(item)=>assert.equal(item.idempotencyKey,'k1'));
  assert.equal(result.delivered,1);
  assert.equal(queue.size,0);
  const reopened = await DurableOfflineQueue.open(file);
  assert.equal(reopened.size,0);
});

test('token authority rejects tampering and expiry and preserves project boundary', () => {
  const authority = createTokenAuthority(Buffer.alloc(32,7));
  const token = authority.issue({ subject:'u1', projectId:'p1', role:'owner', entitlements:['realtime'] });
  const payload = authority.verify(token);
  assert.equal(payload.sub,'u1');
  assert.equal(payload.projectId,'p1');
  assert.deepEqual(payload.entitlements,['realtime']);
  const [body,sig] = token.split('.');
  assert.equal(authority.verify(`${body}.${sig.slice(0,-1)}x`),null);
  const expired = authority.issue({ subject:'u1', projectId:'p1', expiresInSeconds:-1 });
  assert.equal(authority.verify(expired),null);
});

test('entitlement gate is deny-capable without coupling billing provider into data plane', () => {
  const gate = createEntitlementGate({ requiredByAction: { replicate:'realtime', backup:'backup' } });
  assert.equal(gate.authorize({ action:'replicate', user:{entitlements:['realtime']} }),true);
  assert.equal(gate.authorize({ action:'backup', user:{entitlements:['realtime']} }),false);
  assert.equal(gate.authorize({ action:'read', user:{entitlements:[]} }),true);
});

test('encrypted backup round trips and detects ciphertext tampering', async (t) => {
  const dir = await temp('backup'); t.after(()=>fs.rm(dir,{recursive:true,force:true}));
  const file = path.join(dir,'backup.syncio');
  const manager = createEncryptedBackupManager({ key: crypto.randomBytes(32) });
  await manager.backup({ state:{version:1,collections:{users:{u1:{id:'u1',name:'Ada'}}}}, file, metadata:{projectId:'p1'} });
  const restored = await manager.restore(file);
  assert.equal(restored.state.collections.users.u1.name,'Ada');
  const envelope = JSON.parse(await fs.readFile(file,'utf8'));
  const bytes = Buffer.from(envelope.ciphertext,'base64'); bytes[0] ^= 1; envelope.ciphertext = bytes.toString('base64');
  await fs.writeFile(file,JSON.stringify(envelope),'utf8');
  await assert.rejects(manager.restore(file),/digest mismatch/);
});

test('audit log persists append-only events across reopen', async (t) => {
  const dir = await temp('audit'); t.after(()=>fs.rm(dir,{recursive:true,force:true}));
  const file = path.join(dir,'audit.ndjson');
  const log = new AuditLog(file);
  await Promise.all([log.append({type:'project.created',projectId:'p1'}),log.append({type:'token.issued',projectId:'p1'})]);
  const reopened = new AuditLog(file);
  const events = await reopened.readAll();
  assert.equal(events.length,2);
  assert.ok(events.every(event=>event.id && event.at));
});

test('metrics registry records request counts errors and latency percentiles', () => {
  const metrics = new MetricsRegistry();
  const observe = metrics.observer();
  observe({type:'request_complete',status:200,durationMs:10});
  observe({type:'request_complete',status:200,durationMs:20});
  observe({type:'request_error',status:500,durationMs:30});
  const snapshot = metrics.snapshot();
  assert.equal(snapshot.counters['requests_total:200'],2);
  assert.equal(snapshot.counters['requests_total:500'],1);
  assert.equal(snapshot.counters.request_errors_total,1);
  assert.equal(snapshot.histograms.request_duration_ms.count,3);
  assert.equal(snapshot.histograms.request_duration_ms.p95,20);
});
