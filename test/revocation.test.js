import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { RotatingTokenAuthority } from '../src/security.js';
import { DurableRevocationLedger } from '../src/revocation.js';

const key=Buffer.alloc(32,7);

test('token revocation survives authority restart through integrity-checked ledger',async(t)=>{
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-revoke-'));t.after(()=>fs.rm(root,{recursive:true,force:true}));const file=path.join(root,'revocations.ndjson');let authority=new RotatingTokenAuthority({keys:{a:key},activeKeyId:'a'});const token=authority.issue({subject:'u',projectId:'p'});const payload=authority.verify(token);let ledger=await DurableRevocationLedger.open(file,authority);await ledger.revokeToken(payload.jti,{expiresAt:payload.exp});assert.equal(authority.verify(token),null);await ledger.close();authority=new RotatingTokenAuthority({keys:{a:key},activeKeyId:'a'});ledger=await DurableRevocationLedger.open(file,authority);assert.equal(authority.verify(token),null);assert.equal(ledger.status().events,1);await ledger.close();
});

test('revocation ledger detects tampering instead of silently accepting lost authority',async(t)=>{
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-revoke-tamper-'));t.after(()=>fs.rm(root,{recursive:true,force:true}));const file=path.join(root,'revocations.ndjson');const authority=new RotatingTokenAuthority({keys:{a:key},activeKeyId:'a'});const ledger=await DurableRevocationLedger.open(file,authority);await ledger.revokeSubject('u');await ledger.close();const lines=(await fs.readFile(file,'utf8')).trim().split('\n');const event=JSON.parse(lines[0]);event.event.subject='attacker';await fs.writeFile(file,`${JSON.stringify(event)}\n`);await assert.rejects(DurableRevocationLedger.open(file,new RotatingTokenAuthority({keys:{a:key},activeKeyId:'a'})),(error)=>error.code==='SYNCIO_REVOCATION_CORRUPT');
});
