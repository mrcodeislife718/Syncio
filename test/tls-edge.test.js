import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import http from 'node:http';
import https from 'node:https';
import { execFileSync } from 'node:child_process';
import { createTlsEdge } from '../src/tls-edge.js';

async function certPair(root,name){const key=path.join(root,`${name}.key`);const cert=path.join(root,`${name}.crt`);execFileSync('openssl',['req','-x509','-newkey','rsa:2048','-nodes','-keyout',key,'-out',cert,'-days','1','-subj',`/CN=${name}`],{stdio:'ignore'});return{key:await fs.readFile(key),cert:await fs.readFile(cert)};}
function request(url){return new Promise((resolve,reject)=>{const req=https.get(url,{rejectUnauthorized:false},(res)=>{let body='';res.on('data',(chunk)=>body+=chunk);res.on('end',()=>resolve({status:res.statusCode,headers:res.headers,body}));});req.on('error',reject);});}

test('TLS edge terminates HTTPS and streams request to Syncio-compatible HTTP upstream',async(t)=>{
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-tls-'));t.after(()=>fs.rm(root,{recursive:true,force:true}));const upstream=http.createServer((req,res)=>{res.writeHead(200,{'content-type':'application/json'});res.end(JSON.stringify({proto:req.headers['x-forwarded-proto'],host:req.headers['x-forwarded-host']}));});await new Promise((resolve)=>upstream.listen(0,'127.0.0.1',resolve));t.after(()=>new Promise((resolve)=>upstream.close(resolve)));const pair=await certPair(root,'one');const edge=createTlsEdge({targetUrl:`http://127.0.0.1:${upstream.address().port}`,key:pair.key,cert:pair.cert});const address=await edge.listen({host:'127.0.0.1',port:0});t.after(()=>edge.close());const response=await request(address.url);assert.equal(response.status,200);const body=JSON.parse(response.body);assert.equal(body.proto,'https');assert.ok(body.host.includes(String(address.port)));
});

test('TLS edge reload accepts a new validated certificate context without restart',async(t)=>{
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-tls-reload-'));t.after(()=>fs.rm(root,{recursive:true,force:true}));const upstream=http.createServer((_req,res)=>res.end('ok'));await new Promise((resolve)=>upstream.listen(0,'127.0.0.1',resolve));t.after(()=>new Promise((resolve)=>upstream.close(resolve)));const one=await certPair(root,'one');const two=await certPair(root,'two');const edge=createTlsEdge({targetUrl:`http://127.0.0.1:${upstream.address().port}`,key:one.key,cert:one.cert});const address=await edge.listen({host:'127.0.0.1',port:0});t.after(()=>edge.close());assert.equal((await request(address.url)).body,'ok');assert.equal(edge.reload({key:two.key,cert:two.cert}),true);assert.equal((await request(address.url)).body,'ok');
});
