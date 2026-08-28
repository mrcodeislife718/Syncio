import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { SyncioControlPlane } from '../src/control-plane.js';
import { createHostedControlServer } from '../src/hosted-control.js';
import { PaymentOrchestrator, createPaymentProviderAdapter } from '../src/payments.js';

async function setup(){
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-cloud-commerce-'));
  const control=await SyncioControlPlane.open(path.join(root,'control.json'),{tokenSecret:'c'.repeat(48)});
  const calls=[];
  const provider=createPaymentProviderAdapter({name:'qualified',createCheckoutSession:async(input)=>{calls.push(['checkout',input]);return{id:'checkout-1',url:'https://billing.example/checkout'};},createPortalSession:async(input)=>{calls.push(['portal',input]);return{id:'portal-1',url:'https://billing.example/portal'};},cancelSubscription:async(input)=>{calls.push(['cancel',input]);return{id:'subscription-1',status:'cancelling'};},refundPayment:async()=>({id:'refund-1',status:'succeeded'})});
  const payments=new PaymentOrchestrator({controlPlane:control,provider});
  const hosted=createHostedControlServer({controlPlane:control,sessionSecret:'s'.repeat(48),payments});const address=await hosted.listen();
  return{root,control,hosted,address,calls};
}
async function cleanup(s){await s.hosted.close();await s.control.close();await fs.rm(s.root,{recursive:true,force:true});}
async function signup(s,email){const response=await fetch(`${s.address.url}/v1/signup`,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({email})});assert.equal(response.status,201);return response.json();}
async function project(s,token,name='App'){const response=await fetch(`${s.address.url}/v1/projects`,{method:'POST',headers:{authorization:`Bearer ${token}`,'content-type':'application/json'},body:JSON.stringify({name})});assert.equal(response.status,201);return(await response.json()).project;}

test('pricing catalog is public and describes the cloud revenue ladder',async(t)=>{const s=await setup();t.after(()=>cleanup(s));const response=await fetch(`${s.address.url}/v1/plans`);assert.equal(response.status,200);const body=await response.json();assert.equal(body.plans.pro.monthlyBaseCents,4900);assert.equal(body.plans.scale.monthlyBaseCents,49900);assert.ok(body.services.enterpriseSelfHosted);assert.ok(body.services.migrationServices);});

test('customer can inspect only owned project usage and invoice estimates',async(t)=>{const s=await setup();t.after(()=>cleanup(s));const a=await signup(s,'a@example.com');const b=await signup(s,'b@example.com');const p=await project(s,a.token);await s.hosted.revenue.usage.record({projectId:p.id,metric:'reads',quantity:2_000_000,idempotencyKey:'reads',at:'2026-08-01T00:00:00Z'});
  let response=await fetch(`${s.address.url}/v1/projects/${p.id}/usage?period=2026-08`,{headers:{authorization:`Bearer ${a.token}`}});assert.equal(response.status,200);assert.equal((await response.json()).billable.reads,2_000_000);
  response=await fetch(`${s.address.url}/v1/projects/${p.id}/invoice-estimate?period=2026-08`,{headers:{authorization:`Bearer ${a.token}`}});assert.equal(response.status,200);assert.equal((await response.json()).plan,'free');
  response=await fetch(`${s.address.url}/v1/projects/${p.id}/usage?period=2026-08`,{headers:{authorization:`Bearer ${b.token}`}});assert.equal(response.status,404);
});

test('checkout portal and cancellation are account-scoped and idempotency-aware',async(t)=>{const s=await setup();t.after(()=>cleanup(s));const a=await signup(s,'pay@example.com');const p=await project(s,a.token);const headers={authorization:`Bearer ${a.token}`,'content-type':'application/json','idempotency-key':'upgrade-1'};
  let response=await fetch(`${s.address.url}/v1/projects/${p.id}/checkout`,{method:'POST',headers,body:JSON.stringify({plan:'pro',successUrl:'https://app.example/success',cancelUrl:'https://app.example/cancel'})});assert.equal(response.status,200);assert.equal((await response.json()).id,'checkout-1');
  response=await fetch(`${s.address.url}/v1/projects/${p.id}/checkout`,{method:'POST',headers,body:JSON.stringify({plan:'pro',successUrl:'https://app.example/success',cancelUrl:'https://app.example/cancel'})});assert.equal(response.status,200);assert.equal(s.calls.filter(([name])=>name==='checkout').length,1);
  response=await fetch(`${s.address.url}/v1/projects/${p.id}/portal`,{method:'POST',headers:{...headers,'idempotency-key':'portal-1'},body:JSON.stringify({returnUrl:'https://app.example/account'})});assert.equal(response.status,200);
  response=await fetch(`${s.address.url}/v1/projects/${p.id}/cancel`,{method:'POST',headers:{...headers,'idempotency-key':'cancel-1'},body:JSON.stringify({subscriptionId:'subscription-1'})});assert.equal(response.status,200);
});
