import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { startSelfHostedSyncio } from '../src/self-host.js';

test('self-host runtime denies anonymous data access and persists authenticated writes across restart', async (t) => {
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-selfhost-'));
  t.after(()=>fs.rm(root,{recursive:true,force:true}));
  const file=path.join(root,'data.json');
  const secret='0123456789abcdef0123456789abcdef';
  let runtime=await startSelfHostedSyncio({file,secret,projectId:'p1',port:0});
  const token=runtime.issueToken();
  let response=await fetch(`${runtime.address.url}/collections/items`);
  assert.equal(response.status,403);
  response=await fetch(`${runtime.address.url}/collections/items/a`,{method:'PUT',headers:{authorization:`Bearer ${token}`,'content-type':'application/json'},body:'{"value":7}'});
  assert.equal(response.status,200);
  assert.equal((await response.json()).value,7);
  const firstMetrics=runtime.metrics.snapshot();
  assert.ok(firstMetrics.counters['requests_total:403']>=1);
  assert.ok(firstMetrics.counters['requests_total:200']>=1);
  await runtime.close();

  runtime=await startSelfHostedSyncio({file,secret,projectId:'p1',port:0});
  t.after(()=>runtime.close());
  const token2=runtime.issueToken();
  response=await fetch(`${runtime.address.url}/collections/items/a`,{headers:{authorization:`Bearer ${token2}`}});
  assert.equal(response.status,200);
  assert.equal((await response.json()).value,7);
});

test('self-host token without realtime entitlement cannot replicate', async (t) => {
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-selfhost-entitlement-'));
  t.after(()=>fs.rm(root,{recursive:true,force:true}));
  const runtime=await startSelfHostedSyncio({file:path.join(root,'data.json'),secret:'0123456789abcdef0123456789abcdef',projectId:'p2',port:0});
  t.after(()=>runtime.close());
  const token=runtime.issueToken({entitlements:['database']});
  const response=await fetch(`${runtime.address.url}/replicate/pull`,{method:'POST',headers:{authorization:`Bearer ${token}`,'content-type':'application/json'},body:'{"cursor":0}'});
  assert.equal(response.status,403);
});
