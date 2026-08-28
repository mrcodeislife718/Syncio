import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { SyncioControlPlane, BillingStateProcessor } from '../src/control-plane.js';

async function openControl(name) {
  const root=await fs.mkdtemp(path.join(os.tmpdir(),`syncio-control-${name}-`));
  const control=await SyncioControlPlane.open(path.join(root,'control.json'),{tokenSecret:Buffer.alloc(32,9),storageRoot:path.join(root,'projects')});
  return {root,control};
}

test('account and project lifecycle persists with project-scoped signed identity', async (t) => {
  const state=await openControl('lifecycle'); t.after(async()=>{await state.control.close();await fs.rm(state.root,{recursive:true,force:true});});
  const account=await state.control.createAccount({email:'Owner@Example.com',name:'Owner'});
  const project=await state.control.createProject({accountId:account.id,name:'Primary',plan:'pro'});
  assert.equal(project.plan,'pro');
  assert.ok(state.control.entitlements(project.id).includes('backups'));
  const token=state.control.issueProjectToken({accountId:account.id,projectId:project.id});
  const req={headers:{authorization:`Bearer ${token}`}};
  const user=state.control.authenticateProjectRequest(project.id,req);
  assert.equal(user.sub,account.id);
  assert.equal(user.projectId,project.id);
  assert.equal(state.control.authenticateProjectRequest('wrong-project',req),null);
  assert.ok(state.control.projectStorageFile(project.id).endsWith(`${project.id}/data.syncio.json`));
});

test('billing state is idempotent and changes durable plan entitlements', async (t) => {
  const state=await openControl('billing'); t.after(async()=>{await state.control.close();await fs.rm(state.root,{recursive:true,force:true});});
  const account=await state.control.createAccount({email:'billing@example.com'});
  const project=await state.control.createProject({accountId:account.id,name:'Paid'});
  const billing=new BillingStateProcessor(state.control);
  let result=await billing.process({id:'evt-1',projectId:project.id,type:'subscription.updated',plan:'business',provider:'test-provider'});
  assert.deepEqual(result,{duplicate:false,applied:true,plan:'business'});
  assert.ok(state.control.entitlements(project.id).includes('audit'));
  result=await billing.process({id:'evt-1',projectId:project.id,type:'subscription.updated',plan:'enterprise',provider:'test-provider'});
  assert.deepEqual(result,{duplicate:true,applied:false});
  assert.equal(state.control.project(project.id).plan,'business');
  await state.control.close();
  state.control=await SyncioControlPlane.open(path.join(state.root,'control.json'),{tokenSecret:Buffer.alloc(32,9),storageRoot:path.join(state.root,'projects')});
  assert.equal(state.control.project(project.id).plan,'business');
  assert.ok(state.control.entitlements(project.id).includes('audit'));
});

test('account export precedes privacy deletion and deletion revokes project access', async (t) => {
  const state=await openControl('delete'); t.after(async()=>{await state.control.close();await fs.rm(state.root,{recursive:true,force:true});});
  const account=await state.control.createAccount({email:'delete@example.com',name:'Delete Me'});
  const project=await state.control.createProject({accountId:account.id,name:'Data'});
  const exported=await state.control.exportAccount(account.id);
  assert.equal(exported.account.email,'delete@example.com');
  assert.equal(exported.projects.length,1);
  assert.ok(exported.entitlements[project.id].length>0);
  assert.equal(await state.control.deleteAccount(account.id),true);
  assert.equal(state.control.project(project.id),null);
  assert.deepEqual(state.control.entitlements(project.id),[]);
  const redacted=state.control.db.collection('_accounts').get(account.id);
  assert.equal(redacted.status,'deleted');
  assert.equal(redacted.name,null);
  assert.equal(redacted.email.includes('delete@example.com'),false);
});

test('duplicate active account email and cross-account token issuance are rejected', async (t) => {
  const state=await openControl('security'); t.after(async()=>{await state.control.close();await fs.rm(state.root,{recursive:true,force:true});});
  const a=await state.control.createAccount({email:'same@example.com'});
  await assert.rejects(state.control.createAccount({email:'SAME@example.com'}),/account_email_exists/);
  const b=await state.control.createAccount({email:'other@example.com'});
  const project=await state.control.createProject({accountId:a.id,name:'A'});
  assert.throws(()=>state.control.issueProjectToken({accountId:b.id,projectId:project.id}),/project_not_found/);
});
