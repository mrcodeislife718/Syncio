import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import { DurableTelemetryExporter, SloMonitor } from '../src/telemetry.js';

async function collector(handler){const server=http.createServer(handler);await new Promise((resolve)=>server.listen(0,'127.0.0.1',resolve));const {port}=server.address();return{url:`http://127.0.0.1:${port}`,close:()=>new Promise((resolve)=>server.close(resolve))};}

test('telemetry spool survives endpoint failure and restart then delivers exactly queued batch',async(t)=>{
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-telemetry-'));t.after(()=>fs.rm(root,{recursive:true,force:true}));const file=path.join(root,'spool.json');
  let exporter=await DurableTelemetryExporter.open(file,{endpoint:'http://127.0.0.1:1',batchSize:10});await exporter.enqueue({metric:'a'});await exporter.enqueue({metric:'b'});const failed=await exporter.flush();assert.equal(failed.pending,2);await exporter.close();
  const received=[];const c=await collector(async(req,res)=>{let body='';for await(const chunk of req)body+=chunk;received.push(...JSON.parse(body).items);res.writeHead(200);res.end('ok');});t.after(()=>c.close());
  exporter=await DurableTelemetryExporter.open(file,{endpoint:c.url,batchSize:10});const result=await exporter.flush();assert.equal(result.delivered,2);assert.equal(exporter.size,0);assert.deepEqual(received.map((item)=>item.payload.metric),['a','b']);await exporter.close();
});

test('telemetry spool bounds disk queue and accounts for dropped oldest events',async(t)=>{
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-telemetry-bound-'));t.after(()=>fs.rm(root,{recursive:true,force:true}));const exporter=await DurableTelemetryExporter.open(path.join(root,'spool.json'),{endpoint:'http://127.0.0.1:1',batchSize:2,maxItems:3});for(let i=0;i<5;i++)await exporter.enqueue({i});assert.equal(exporter.status().pending,3);assert.equal(exporter.status().dropped,2);await exporter.close();
});

test('SLO monitor reports latency or availability breach and remaining error budget',()=>{
  const slo=new SloMonitor({availability:.9,p95LatencyMs:100,errorBudgetWindow:10});for(let i=0;i<9;i++)slo.record({ok:true,durationMs:20});const state=slo.record({ok:false,durationMs:200});assert.equal(state.status,'breached');assert.equal(state.samples,10);assert.equal(state.availability,.9);assert.equal(state.p95LatencyMs,200);assert.ok(state.errorBudget.remaining<=0.0000001);
});
