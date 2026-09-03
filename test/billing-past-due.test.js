import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { SyncioControlPlane, BillingStateProcessor } from '../src/control-plane.js';

test('past-due billing preserves paid plan identity but suspends paid grants until recovery', async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'syncio-past-due-'));
  const control = await SyncioControlPlane.open(path.join(root, 'control.json'), {
    tokenSecret: Buffer.alloc(32, 7),
    storageRoot: path.join(root, 'projects'),
  });
  t.after(async () => {
    await control.close();
    await fs.rm(root, { recursive: true, force: true });
  });

  const account = await control.createAccount({ email: 'billing-recovery@example.com' });
  const project = await control.createProject({ accountId: account.id, name: 'Revenue Protected', plan: 'business' });
  const billing = new BillingStateProcessor(control);

  assert.ok(control.entitlements(project.id).includes('audit'));

  await billing.process({
    id: 'evt-past-due',
    projectId: project.id,
    type: 'subscription.updated',
    plan: 'business',
    status: 'past_due',
    provider: 'stripe',
  });

  assert.equal(control.project(project.id).plan, 'business');
  assert.equal(control.project(project.id).billingStatus, 'past_due');
  assert.deepEqual(control.entitlements(project.id), ['database', 'realtime:basic']);
  assert.equal(control.entitlements(project.id).includes('audit'), false);
  assert.equal(control.db.collection('_entitlements').get(project.id).source, 'billing:stripe:past_due');

  await billing.process({
    id: 'evt-recovered',
    projectId: project.id,
    type: 'subscription.updated',
    plan: 'business',
    status: 'active',
    provider: 'stripe',
  });

  assert.equal(control.project(project.id).billingStatus, 'active');
  assert.ok(control.entitlements(project.id).includes('audit'));
  assert.equal(control.db.collection('_entitlements').get(project.id).source, 'billing:stripe');
});
