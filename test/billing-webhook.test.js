import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { SyncioControlPlane } from '../src/control-plane.js';
import { createBillingWebhookProcessor } from '../src/billing.js';

async function setup() {
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-billing-webhook-'));
  const control=await SyncioControlPlane.open(path.join(root,'control.json'),{tokenSecret:Buffer.alloc(32,8),storageRoot:path.join(root,'projects')});
  const account=await control.createAccount({email:'billing-webhook@example.com'});
  const project=await control.createProject({accountId:account.id,name:'Billing'});
  return {root,control,project};
}

test('verified billing webhook atomically applies entitlement and rejects replay mutation',async(t)=>{
  const state=await setup();t.after(async()=>{await state.control.close();await fs.rm(state.root,{recursive:true,force:true});});
  let now=1_800_000_000_000;
  const webhook=createBillingWebhookProcessor({controlPlane:state.control,secret:Buffer.alloc(32,5),now:()=>now});
  const raw=JSON.stringify({id:'evt-webhook-1',projectId:state.project.id,type:'subscription.updated',plan:'business',provider:'test'});
  const signature=webhook.sign(raw);
  let result=await webhook.process({rawBody:raw,signature});
  assert.deepEqual(result,{duplicate:false,applied:true,plan:'business'});
  assert.ok(state.control.entitlements(state.project.id).includes('audit'));
  const replayBody=JSON.stringify({id:'evt-webhook-1',projectId:state.project.id,type:'subscription.updated',plan:'enterprise',provider:'test'});
  result=await webhook.process({rawBody:replayBody,signature:webhook.sign(replayBody)});
  assert.deepEqual(result,{duplicate:true,applied:false});
  assert.equal(state.control.project(state.project.id).plan,'business');
});

test('billing webhook rejects tampering stale timestamps and malformed signature encoding',async(t)=>{
  const state=await setup();t.after(async()=>{await state.control.close();await fs.rm(state.root,{recursive:true,force:true});});
  let now=1_800_000_000_000;
  const webhook=createBillingWebhookProcessor({controlPlane:state.control,secret:Buffer.alloc(32,6),now:()=>now,toleranceSeconds:60});
  const raw=JSON.stringify({id:'evt-2',projectId:state.project.id,type:'subscription.updated',plan:'pro'});
  const signature=webhook.sign(raw);
  await assert.rejects(webhook.process({rawBody:`${raw} `,signature}),/billing_webhook_invalid_signature/);
  now+=61_000;
  await assert.rejects(webhook.process({rawBody:raw,signature}),/billing_webhook_expired/);
  now=1_800_000_000_000;
  await assert.rejects(webhook.process({rawBody:raw,signature:'t=1800000000,v1=zz'}),/billing_webhook_invalid_signature/);
});
