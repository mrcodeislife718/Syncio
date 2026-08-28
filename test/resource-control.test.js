import test from 'node:test';
import assert from 'node:assert/strict';
import { TokenBucketLimiter, ConcurrencyAdmissionController } from '../src/resource-control.js';

test('token bucket rate limiter denies bursts and deterministically refills',()=>{
  let now=0;
  const limiter=new TokenBucketLimiter({capacity:2,refillPerSecond:1,maxKeys:10,now:()=>now});
  assert.equal(limiter.consume('client').allowed,true);
  assert.equal(limiter.consume('client').allowed,true);
  const denied=limiter.consume('client');
  assert.equal(denied.allowed,false);
  assert.equal(denied.retryAfterMs,1000);
  now=1000;
  assert.equal(limiter.consume('client').allowed,true);
});

test('rate limiter bounds identity-memory growth by evicting old keys',()=>{
  let now=0;
  const limiter=new TokenBucketLimiter({capacity:1,refillPerSecond:1,maxKeys:2,now:()=>now});
  limiter.consume('a'); now+=1; limiter.consume('b'); now+=1; limiter.consume('c');
  assert.equal(limiter.trackedKeys,2);
  assert.equal(limiter.buckets.has('a'),false);
});

test('concurrency admission controller refuses excess and releases idempotently',()=>{
  const admission=new ConcurrencyAdmissionController({maxConcurrent:1});
  const release=admission.enter();
  assert.equal(typeof release,'function');
  assert.equal(admission.enter(),null);
  release(); release();
  assert.equal(admission.active,0);
  assert.equal(typeof admission.enter(),'function');
});
