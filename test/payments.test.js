import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { SyncioControlPlane } from '../src/control-plane.js';
import { PaymentOrchestrator, createPaymentProviderAdapter } from '../src/payments.js';

async function setup(){
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-payments-'));const control=await SyncioControlPlane.open(path.join(root,'control.json'),{tokenSecret:'z'.repeat(48)});const account=await control.createAccount({email:'pay@example.com'});const project=await control.createProject({accountId:account.id,name:'Paid',plan:'pro'});const calls=[];
  const provider=createPaymentProviderAdapter({name:'testpay',createCheckoutSession:async(input)=>{calls.push(['checkout',input]);return{id:'checkout-1',url:'https://pay.invalid/c/1'};},createPortalSession:async(input)=>{calls.push(['portal',input]);return{id:'portal-1',url:'https://pay.invalid/p/1'};},cancelSubscription:async(input)=>{calls.push(['cancel',input]);return{id:'sub-1',status:'cancelling'};},refundPayment:async(input)=>{calls.push(['refund',input]);return{id:'refund-1',status:'succeeded'};}});return{root,control,account,project,calls,payments:new PaymentOrchestrator({controlPlane:control,provider})};
}
async function cleanup(s){await s.control.close().catch(()=>undefined);await fs.rm(s.root,{recursive:true,force:true});}

test('commercial actions are provider-neutral durable and idempotent',async(t)=>{
  const s=await setup();t.after(()=>cleanup(s));const first=await s.payments.createCheckout({projectId:s.project.id,plan:'business',successUrl:'https://app/success',cancelUrl:'https://app/cancel',idempotencyKey:'same'});const second=await s.payments.createCheckout({projectId:s.project.id,plan:'business',successUrl:'https://app/success',cancelUrl:'https://app/cancel',idempotencyKey:'same'});assert.deepEqual(first,second);assert.equal(s.calls.filter(([name])=>name==='checkout').length,1);
  await s.payments.createPortal({projectId:s.project.id,returnUrl:'https://app/account'});await s.payments.cancelSubscription({projectId:s.project.id,subscriptionId:'sub'});await s.payments.refund({projectId:s.project.id,paymentId:'pay',amount:500});assert.equal((await s.payments.actionHistory(s.project.id)).length,4);
});

test('failed payment restricts paid entitlement and recovery restores current plan',async(t)=>{
  const s=await setup();t.after(()=>cleanup(s));assert.ok(s.control.entitlements(s.project.id).includes('higher-limits'));
  const failed=await s.payments.processEvent({id:'evt-fail',projectId:s.project.id,type:'payment.failed'});assert.equal(failed.restricted,true);assert.deepEqual(s.control.entitlements(s.project.id),['database','realtime:basic']);
  const recovered=await s.payments.processEvent({id:'evt-recover',projectId:s.project.id,type:'payment.recovered'});assert.equal(recovered.plan,'pro');assert.ok(s.control.entitlements(s.project.id).includes('higher-limits'));
  const duplicate=await s.payments.processEvent({id:'evt-recover',projectId:s.project.id,type:'payment.recovered'});assert.equal(duplicate.duplicate,true);
});

test('subscription cancellation and refund lifecycle are durable billing events',async(t)=>{
  const s=await setup();t.after(()=>cleanup(s));await s.payments.processEvent({id:'evt-cancel',projectId:s.project.id,type:'subscription.cancelled',status:'cancelled'});assert.equal(s.control.project(s.project.id).plan,'free');assert.deepEqual(s.control.entitlements(s.project.id),['database','realtime:basic']);const refund=await s.payments.processEvent({id:'evt-refund',projectId:s.project.id,type:'refund.completed'});assert.equal(refund.status,'refunded');assert.equal(s.control.db.collection('_billing_events').get('evt-refund').type,'refund.completed');
});
