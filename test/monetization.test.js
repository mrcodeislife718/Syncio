import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { SyncioControlPlane } from '../src/control-plane.js';
import { PLAN_CATALOG, UsageMeter, RevenueEngine, canonicalPlan, serviceCatalog } from '../src/monetization.js';

async function setup(plan='pro'){
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-money-'));
  const file=path.join(root,'control.json');
  const control=await SyncioControlPlane.open(file,{tokenSecret:'m'.repeat(48)});
  const account=await control.createAccount({email:`${plan}-${Date.now()}@example.com`});
  const project=await control.createProject({accountId:account.id,name:'Revenue',plan});
  return{root,file,control,account,project};
}
async function cleanup(s){await s.control.close().catch(()=>undefined);await fs.rm(s.root,{recursive:true,force:true});}

test('commercial catalog has free pro scale enterprise and legacy business maps to scale',()=>{
  assert.equal(PLAN_CATALOG.free.monthlyBaseCents,0);
  assert.equal(PLAN_CATALOG.pro.monthlyBaseCents,4900);
  assert.equal(PLAN_CATALOG.scale.monthlyBaseCents,49900);
  assert.equal(PLAN_CATALOG.enterprise.monthlyBaseCents,null);
  assert.equal(canonicalPlan('business'),'scale');
  assert.ok(serviceCatalog().migrationServices.sources.includes('mongodb'));
});

test('usage recording is durable and idempotent',async(t)=>{
  const s=await setup();t.after(()=>cleanup(s));const meter=new UsageMeter(s.control);
  let result=await meter.record({projectId:s.project.id,metric:'reads',quantity:40,idempotencyKey:'r1',at:'2026-08-05T00:00:00Z'});assert.equal(result.recorded,true);
  result=await meter.record({projectId:s.project.id,metric:'reads',quantity:40,idempotencyKey:'r1',at:'2026-08-05T00:00:00Z'});assert.equal(result.duplicate,true);
  await meter.record({projectId:s.project.id,metric:'writes',quantity:3,idempotencyKey:'w1',at:'2026-08-05T00:00:00Z'});
  assert.equal(meter.summary(s.project.id,'2026-08').billable.reads,40);
  await s.control.close();s.control=await SyncioControlPlane.open(s.file,{tokenSecret:'m'.repeat(48)});
  assert.equal(new UsageMeter(s.control).summary(s.project.id,'2026-08').billable.writes,3);
});

test('usage units normalize storage realtime egress backup PITR and replicas',async(t)=>{
  const s=await setup();t.after(()=>cleanup(s));const meter=new UsageMeter(s.control);const GB=1024**3;
  const rows=[['storage_byte_hours',2*GB*730],['realtime_seconds',7200],['egress_bytes',3*GB],['backup_byte_hours',4*GB*730],['pitr_byte_hours',5*GB*730],['replica_region_hours',100]];
  for(const [metric,quantity] of rows)await meter.record({projectId:s.project.id,metric,quantity,idempotencyKey:metric,at:'2026-08-10T00:00:00Z'});
  const u=meter.summary(s.project.id,'2026-08').billable;assert.equal(u.storageGbMonths,2);assert.equal(u.realtimeHours,2);assert.equal(u.egressGb,3);assert.equal(u.backupGbMonths,4);assert.equal(u.pitrGbMonths,5);assert.equal(u.replicaRegionHours,100);
});

test('Pro invoice charges base plus only usage above included allowances',async(t)=>{
  const s=await setup('pro');t.after(()=>cleanup(s));const revenue=new RevenueEngine({controlPlane:s.control});
  await revenue.usage.record({projectId:s.project.id,metric:'reads',quantity:3_000_000,idempotencyKey:'reads',at:'2026-08-01T00:00:00Z'});
  await revenue.usage.record({projectId:s.project.id,metric:'writes',quantity:1_250_000,idempotencyKey:'writes',at:'2026-08-01T00:00:00Z'});
  const estimate=revenue.estimateInvoice(s.project.id,'2026-08');
  assert.equal(estimate.plan,'pro');assert.equal(estimate.lines[0].metric,'base');assert.equal(estimate.lines[0].amountCents,4900);
  assert.ok(estimate.totalCents>4900);assert.equal(estimate.lines.some((l)=>l.metric==='reads'),true);assert.equal(estimate.lines.some((l)=>l.metric==='writes'),true);
});

test('quota decisions require upgrades when a plan hard limit is crossed',async(t)=>{
  const s=await setup('free');t.after(()=>cleanup(s));const revenue=new RevenueEngine({controlPlane:s.control});
  await revenue.usage.record({projectId:s.project.id,metric:'reads',quantity:100_000,idempotencyKey:'limit',at:'2026-08-01T00:00:00Z'});
  const free=revenue.quotaDecision(s.project.id,'monthlyReads',1,'2026-08');assert.equal(free.allowed,false);assert.equal(free.upgradeRequired,true);
  await s.control.changePlan(s.project.id,'scale');const scale=revenue.quotaDecision(s.project.id,'monthlyReads',1,'2026-08');assert.equal(scale.allowed,true);
});

test('finalized invoice is idempotent durable and enterprise is contract-priced',async(t)=>{
  const s=await setup('pro');t.after(()=>cleanup(s));let revenue=new RevenueEngine({controlPlane:s.control});
  const a=await revenue.finalizeInvoice(s.project.id,'2026-08');const b=await revenue.finalizeInvoice(s.project.id,'2026-08');assert.deepEqual(a,b);
  await s.control.close();s.control=await SyncioControlPlane.open(s.file,{tokenSecret:'m'.repeat(48)});revenue=new RevenueEngine({controlPlane:s.control});assert.equal(revenue.invoices(s.project.id).length,1);
  await s.control.changePlan(s.project.id,'enterprise');const enterprise=revenue.estimateInvoice(s.project.id,'2026-09');assert.equal(enterprise.contractPriced,true);assert.equal(enterprise.totalCents,null);
});
