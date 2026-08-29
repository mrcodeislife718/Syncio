import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { SyncioControlPlane } from '../src/control-plane.js';
import { RevenueEngine } from '../src/monetization.js';
import { RuntimeUsageSampler, pathBytes } from '../src/usage-sampler.js';

async function setup(){const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-usage-sampler-'));const control=await SyncioControlPlane.open(path.join(root,'control.json'),{tokenSecret:'u'.repeat(48)});const account=await control.createAccount({email:'usage-sampler@example.com'});const project=await control.createProject({accountId:account.id,name:'Usage',plan:'scale'});return{root,control,project};}
async function cleanup(s){await s.control.close();await fs.rm(s.root,{recursive:true,force:true});}

test('pathBytes measures files and nested directories without following symlinks',async(t)=>{const s=await setup();t.after(()=>cleanup(s));const dir=path.join(s.root,'bytes');await fs.mkdir(path.join(dir,'nested'),{recursive:true});await fs.writeFile(path.join(dir,'a'),Buffer.alloc(10));await fs.writeFile(path.join(dir,'nested','b'),Buffer.alloc(15));assert.equal(await pathBytes(dir),25);assert.equal(await pathBytes(path.join(dir,'missing')),0);});

test('runtime sampler converts authoritative file sizes and elapsed time into byte-hours',async(t)=>{const s=await setup();t.after(()=>cleanup(s));const data=path.join(s.root,'data.bin'),backup=path.join(s.root,'backup.bin'),pitr=path.join(s.root,'pitr.bin');await fs.writeFile(data,Buffer.alloc(100));await fs.writeFile(backup,Buffer.alloc(40));await fs.writeFile(pitr,Buffer.alloc(20));let now=Date.UTC(2026,7,1,0,0,0);const revenue=new RevenueEngine({controlPlane:s.control});const sampler=new RuntimeUsageSampler({meter:revenue.usage,projectId:s.project.id,dataPaths:[data],backupPaths:[backup],pitrPaths:[pitr],intervalMs:60_000,now:()=>now});let result=await sampler.sample();assert.equal(result.recorded,false);now+=3_600_000;result=await sampler.sample();assert.equal(result.recorded,true);const usage=revenue.usageSummary(s.project.id,'2026-08').raw;assert.equal(usage.storage_byte_hours,100);assert.equal(usage.backup_byte_hours,40);assert.equal(usage.pitr_byte_hours,20);assert.equal(usage.compute_milliseconds,3_600_000);await sampler.close();});
