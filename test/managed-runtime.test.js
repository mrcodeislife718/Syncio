import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { SyncioControlPlane } from '../src/control-plane.js';
import { ManagedRuntimeManager } from '../src/managed-runtime.js';
import { SyncioClient } from '../src/client.js';

async function setup(){
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-managed-runtime-'));
  const control=await SyncioControlPlane.open(path.join(root,'control.json'),{tokenSecret:'c'.repeat(48),storageRoot:path.join(root,'projects')});
  const account=await control.createAccount({email:'owner@example.com'});const project=await control.createProject({accountId:account.id,name:'Managed',plan:'pro'});
  const manager=new ManagedRuntimeManager({controlPlane:control,tokenSecret:'m'.repeat(48),storageRoot:path.join(root,'managed'),regions:['east','west'],maxProjects:2,startupTimeoutMs:10000});
  return{root,control,account,project,manager};
}
async function cleanup(s){await s.manager.close().catch(()=>undefined);await s.control.close().catch(()=>undefined);await fs.rm(s.root,{recursive:true,force:true});}

test('managed runtime starts project in isolated child process and accepts project-scoped token',async(t)=>{
  const s=await setup();t.after(()=>cleanup(s));const state=await s.manager.startProject(s.project.id,{region:'east'});assert.equal(state.status,'running');assert.notEqual(state.pid,process.pid);assert.equal(state.region,'east');
  const token=s.manager.issueDataToken({projectId:s.project.id,subject:s.account.id});const client=new SyncioClient({baseUrl:state.address.url,token});await client.collection('items').upsert('a',{value:1});assert.equal((await client.collection('items').get('a')).value,1);
  const status=await s.manager.status(s.project.id);assert.equal(status.runtime.sequence,1);assert.equal(status.runtime.projectId,s.project.id);
});

test('managed runtime enforces project capacity and supports clean restart with durable data',async(t)=>{
  const s=await setup();t.after(()=>cleanup(s));const second=await s.control.createProject({accountId:s.account.id,name:'Second'});const third=await s.control.createProject({accountId:s.account.id,name:'Third'});
  const first=await s.manager.startProject(s.project.id,{region:'west'});await s.manager.startProject(second.id,{region:'east'});await assert.rejects(s.manager.startProject(third.id),(error)=>error.code==='managed_capacity_exceeded');
  const client=new SyncioClient({baseUrl:first.address.url,token:s.manager.issueDataToken({projectId:s.project.id})});await client.collection('items').upsert('persisted',{value:7});
  const restarted=await s.manager.restartProject(s.project.id);const client2=new SyncioClient({baseUrl:restarted.address.url,token:s.manager.issueDataToken({projectId:s.project.id})});assert.equal((await client2.collection('items').get('persisted')).value,7);
});

test('managed runtime rejects unsupported region and unknown project',async(t)=>{
  const s=await setup();t.after(()=>cleanup(s));await assert.rejects(s.manager.startProject(s.project.id,{region:'moon'}),(error)=>error.code==='unsupported_region');await assert.rejects(s.manager.startProject('missing'),(error)=>error.code==='project_not_found');
});
