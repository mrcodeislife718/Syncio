import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { openSuperiorProduction } from '../src/superior-production.js';
import { DatabaseLease } from '../src/database-lock.js';

test('production database refuses a second live owner and releases lease on close',async(t)=>{const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-lock-'));t.after(()=>fs.rm(root,{recursive:true,force:true}));const file=path.join(root,'data.syncio.json');const db=await openSuperiorProduction(file);await assert.rejects(openSuperiorProduction(file),(error)=>error.code==='SYNCIO_DATABASE_LOCKED');await db.close();const reopened=await openSuperiorProduction(file);await reopened.close();});

test('lease recovers an abandoned lock whose process is definitely dead',async(t)=>{const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-stale-lock-'));t.after(()=>fs.rm(root,{recursive:true,force:true}));const file=path.join(root,'data.syncio.json'),lockFile=`${file}.lock`;await fs.writeFile(lockFile,JSON.stringify({version:1,instanceId:'dead',pid:2147483647,hostname:process.env.HOSTNAME??null,startedAt:1,heartbeatAt:1}));const lease=await DatabaseLease.acquire(file);assert.notEqual(lease.owner.instanceId,'dead');await lease.release();});
