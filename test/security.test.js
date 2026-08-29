import test from 'node:test';
import assert from 'node:assert/strict';
import { RotatingTokenAuthority, createSecretProvider } from '../src/security.js';

const keyA=Buffer.alloc(32,1);const keyB=Buffer.alloc(32,2);

test('rotating token authority accepts old key during rotation then rejects it after retirement',()=>{
  let now=1_800_000_000_000;const authority=new RotatingTokenAuthority({keys:{a:keyA},activeKeyId:'a',issuer:'syncio:test',now:()=>now});
  const old=authority.issue({subject:'u1',projectId:'p1'});assert.equal(authority.verify(old).kid,'a');
  authority.rotate('b',keyB);const fresh=authority.issue({subject:'u1',projectId:'p1'});assert.equal(authority.verify(old).kid,'a');assert.equal(authority.verify(fresh).kid,'b');
  authority.retire('a');assert.equal(authority.verify(old),null);assert.equal(authority.verify(fresh).kid,'b');
});

test('token and subject revocation reject previously valid authority without changing unrelated subjects',()=>{
  let now=1_800_000_000_000;const authority=new RotatingTokenAuthority({keys:{a:keyA},activeKeyId:'a',now:()=>now});
  const one=authority.issue({subject:'u1',projectId:'p1'});const two=authority.issue({subject:'u2',projectId:'p1'});const onePayload=authority.verify(one);authority.revokeToken(onePayload.jti,{expiresAt:onePayload.exp});assert.equal(authority.verify(one),null);assert.ok(authority.verify(two));
  authority.revokeSubject('u2',{before:Math.floor(now/1000)});assert.equal(authority.verify(two),null);now+=1000;const newer=authority.issue({subject:'u2',projectId:'p1'});assert.ok(authority.verify(newer));
});

test('global revoke-before invalidates previously issued tokens and preserves later tokens',()=>{
  let now=1_800_000_000_000;const authority=new RotatingTokenAuthority({keys:{a:keyA},activeKeyId:'a',now:()=>now});const old=authority.issue({subject:'u',projectId:'p'});authority.revokeAll({before:Math.floor(now/1000)});assert.equal(authority.verify(old),null);now+=1000;assert.ok(authority.verify(authority.issue({subject:'u',projectId:'p'})));
});

test('secret provider caches refreshes and coalesces concurrent loads',async()=>{
  let calls=0;const provider=createSecretProvider({refreshIntervalMs:1000,load:async()=>{calls++;await new Promise((resolve)=>setTimeout(resolve,5));return{token:`v${calls}`};}});
  const [a,b]=await Promise.all([provider.get(),provider.get()]);assert.equal(calls,1);assert.equal(a.token,'v1');assert.equal(b.token,'v1');a.token='mutated';assert.equal((await provider.get()).token,'v1');provider.invalidate();assert.equal((await provider.get()).token,'v2');assert.equal(calls,2);
});
