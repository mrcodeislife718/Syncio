import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { SyncioControlPlane } from '../src/control-plane.js';
import { createHostedControlServer } from '../src/hosted-control.js';
import { createBillingWebhookProcessor } from '../src/billing.js';

async function setup(){
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-hosted-control-'));
  const tokenSecret=Buffer.alloc(32,4);const billingSecret=Buffer.alloc(32,3);
  const control=await SyncioControlPlane.open(path.join(root,'control.json'),{tokenSecret,storageRoot:path.join(root,'projects')});
  const hosted=createHostedControlServer({controlPlane:control,sessionSecret:Buffer.alloc(32,2),billingWebhookSecret:billingSecret});
  const address=await hosted.listen();
  return{root,control,hosted,address,billingSecret};
}
async function cleanup(state){await state.hosted.close();await state.control.close();await fs.rm(state.root,{recursive:true,force:true});}

test('hosted control API supports signup project token export and deletion lifecycle',async(t)=>{
  const state=await setup();t.after(()=>cleanup(state));
  let response=await fetch(`${state.address.url}/v1/signup`,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({email:'customer@example.com',name:'Customer'})});
  assert.equal(response.status,201);
  const signup=await response.json();
  assert.ok(signup.token);
  const auth={authorization:`Bearer ${signup.token}`,'content-type':'application/json'};
  response=await fetch(`${state.address.url}/v1/projects`,{method:'POST',headers:auth,body:JSON.stringify({name:'Production'})});
  assert.equal(response.status,201);
  const {project}=await response.json();
  response=await fetch(`${state.address.url}/v1/projects/${encodeURIComponent(project.id)}/token`,{method:'POST',headers:auth});
  assert.equal(response.status,200);
  const projectToken=await response.json();assert.ok(projectToken.token);assert.ok(projectToken.entitlements.includes('database'));
  response=await fetch(`${state.address.url}/v1/account/export`,{headers:auth});
  assert.equal(response.status,200);assert.equal((await response.json()).projects.length,1);
  response=await fetch(`${state.address.url}/v1/account`,{method:'DELETE',headers:auth});
  assert.equal(response.status,200);
  response=await fetch(`${state.address.url}/v1/projects`,{headers:auth});
  assert.equal(response.status,401);
});

test('hosted control isolates project token issuance across accounts',async(t)=>{
  const state=await setup();t.after(()=>cleanup(state));
  const signup=async(email)=>{const r=await fetch(`${state.address.url}/v1/signup`,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({email})});return r.json();};
  const a=await signup('a@example.com');const b=await signup('b@example.com');
  let response=await fetch(`${state.address.url}/v1/projects`,{method:'POST',headers:{authorization:`Bearer ${a.token}`,'content-type':'application/json'},body:'{"name":"A"}'});
  const project=(await response.json()).project;
  response=await fetch(`${state.address.url}/v1/projects/${project.id}/token`,{method:'POST',headers:{authorization:`Bearer ${b.token}`}});
  assert.equal(response.status,404);
});

test('hosted billing endpoint requires verified raw-body signature before plan mutation',async(t)=>{
  const state=await setup();t.after(()=>cleanup(state));
  let response=await fetch(`${state.address.url}/v1/signup`,{method:'POST',headers:{'content-type':'application/json'},body:'{"email":"paying@example.com"}'});
  const signup=await response.json();
  response=await fetch(`${state.address.url}/v1/projects`,{method:'POST',headers:{authorization:`Bearer ${signup.token}`,'content-type':'application/json'},body:'{"name":"Paid"}'});
  const project=(await response.json()).project;
  const signer=createBillingWebhookProcessor({controlPlane:state.control,secret:state.billingSecret});
  const raw=JSON.stringify({id:'evt-hosted-1',projectId:project.id,type:'subscription.updated',plan:'pro',provider:'test'});
  response=await fetch(`${state.address.url}/v1/billing/webhook`,{method:'POST',headers:{'x-syncio-signature':signer.sign(raw),'content-type':'application/json'},body:raw});
  assert.equal(response.status,200);assert.equal(state.control.project(project.id).plan,'pro');
  response=await fetch(`${state.address.url}/v1/billing/webhook`,{method:'POST',headers:{'x-syncio-signature':'t=1,v1=bad','content-type':'application/json'},body:raw});
  assert.equal(response.status,400);assert.equal(state.control.project(project.id).plan,'pro');
});
