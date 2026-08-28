import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { ReleaseRegistry, RollingDeploymentController } from '../src/deployment.js';

const digest=(value)=>crypto.createHash('sha256').update(value).digest('hex');

test('release registry makes version artifact identity immutable and requires qualification proof to promote',async(t)=>{
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-release-'));t.after(()=>fs.rm(root,{recursive:true,force:true}));const registry=await ReleaseRegistry.open(path.join(root,'releases.json'));await registry.register({version:'1.0.0',artifactDigest:digest('one'),sourceCommit:'abcdef1'});await assert.rejects(registry.register({version:'1.0.0',artifactDigest:digest('different'),sourceCommit:'abcdef1'}),(error)=>error.code==='SYNCIO_RELEASE_IMMUTABLE');await assert.rejects(registry.promote('1.0.0',{}),(error)=>error.code==='SYNCIO_RELEASE_UNQUALIFIED');await registry.promote('1.0.0',{healthProof:{ok:true,checks:['health','auth']}});assert.equal(registry.current().version,'1.0.0');assert.equal(registry.verifyArtifact('1.0.0','one').ok,true);
});

test('rolling deployment keeps old version active when candidate health fails then can rollback healthy release',async(t)=>{
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-deploy-'));t.after(()=>fs.rm(root,{recursive:true,force:true}));const registry=await ReleaseRegistry.open(path.join(root,'releases.json'));for(const [version,data]of [['1.0.0','one'],['2.0.0','two']])await registry.register({version,artifactDigest:digest(data),sourceCommit:version==='1.0.0'?'abcdef1':'abcdef2'});const stopped=[];const controller=new RollingDeploymentController({registry,start:async(version)=>({version}),stop:async(instance)=>stopped.push(instance.version),health:async(instance)=>({ok:instance.version!=='2.0.0-bad',version:instance.version})});await controller.deploy('1.0.0');await controller.deploy('2.0.0');assert.equal(registry.current().version,'2.0.0');const rollback=await controller.rollback('1.0.0');assert.equal(rollback.version,'1.0.0');assert.equal(registry.current().version,'1.0.0');assert.ok(stopped.includes('2.0.0'));
});

test('failed candidate never becomes current release',async(t)=>{
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-deploy-fail-'));t.after(()=>fs.rm(root,{recursive:true,force:true}));const registry=await ReleaseRegistry.open(path.join(root,'releases.json'));await registry.register({version:'bad',artifactDigest:digest('bad'),sourceCommit:'abcdef3'});const stopped=[];const controller=new RollingDeploymentController({registry,start:async(version)=>({version}),stop:async(instance)=>stopped.push(instance.version),health:async()=>({ok:false})});await assert.rejects(controller.deploy('bad'),(error)=>error.code==='SYNCIO_DEPLOY_HEALTH_FAILED');assert.equal(registry.current(),null);assert.deepEqual(stopped,['bad']);
});
