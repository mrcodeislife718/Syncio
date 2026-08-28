import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { startSelfHostedSyncio } from '../src/self-host.js';
import { SyncioClient } from '../src/client.js';

async function setup(){const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-network-special-'));const runtime=await startSelfHostedSyncio({file:path.join(root,'db.json'),secret:'q'.repeat(48),port:0,ttlSweepIntervalMs:0,pitrSnapshotIntervalMs:0});const client=new SyncioClient({baseUrl:runtime.address.url,token:runtime.issueToken()});return{root,runtime,client};}
async function cleanup(s){await s.runtime.close().catch(()=>undefined);await fs.rm(s.root,{recursive:true,force:true});}

test('HTTP query uses production text index through normal authenticated collection endpoint',async(t)=>{const s=await setup();t.after(()=>cleanup(s));await s.runtime.db.defineTextIndex('docs',['title','body'],{name:'search'});await s.client.collection('docs').upsert('a',{title:'Realtime database',body:'sync realtime realtime'});await s.client.collection('docs').upsert('b',{title:'Storage',body:'segments'});const rows=await s.client.collection('docs').query({where:{$text:{query:'realtime',index:'search'}}});assert.deepEqual(rows.map((row)=>row.id),['a']);});

test('HTTP query uses production geo index and can combine proximity with normal filters',async(t)=>{const s=await setup();t.after(()=>cleanup(s));await s.runtime.db.defineGeoIndex('places','location',{name:'geo'});await s.client.collection('places').upsert('bronx',{location:[-73.8648,40.8448],open:true});await s.client.collection('places').upsert('brooklyn',{location:[-73.9442,40.6782],open:true});const rows=await s.client.collection('places').query({where:{$near:{point:[-73.87,40.85],index:'geo',maxDistanceMeters:5000},open:true}});assert.deepEqual(rows.map((row)=>row.id),['bronx']);});
