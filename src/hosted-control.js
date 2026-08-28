import http from 'node:http';
import crypto from 'node:crypto';
import { createTokenAuthority } from './operations.js';
import { TokenBucketLimiter, rateLimitError } from './resource-control.js';
import { createBillingWebhookProcessor } from './billing.js';

const MAX_BODY_BYTES=256*1024;

export function createHostedControlServer({ controlPlane, sessionSecret, billingWebhookSecret, rateLimit={capacity:120,refillPerSecond:2,maxKeys:10_000}, observe=()=>undefined }={}) {
  if(!controlPlane)throw new TypeError('hosted control server requires controlPlane');
  const sessions=createTokenAuthority(sessionSecret,{issuer:'syncio-control',ttlSeconds:86_400});
  const limiter=new TokenBucketLimiter(rateLimit);
  const billing=billingWebhookSecret?createBillingWebhookProcessor({controlPlane,secret:billingWebhookSecret}):null;

  const server=http.createServer(async(req,res)=>{
    const requestId=crypto.randomUUID();
    const started=process.hrtime.bigint();
    res.setHeader('x-syncio-request-id',requestId);
    try{
      const decision=limiter.consume(req.socket.remoteAddress??'unknown');
      if(!decision.allowed)throw rateLimitError(decision);
      const url=new URL(req.url,'http://syncio-control.local');
      if(req.method==='GET'&&url.pathname==='/health')return reply(res,200,{ok:true},observe,{requestId,req,started});

      if(req.method==='POST'&&url.pathname==='/v1/signup'){
        const body=await readJson(req);
        const account=await controlPlane.createAccount(body);
        const token=issueAccountSession(sessions,account.id);
        return reply(res,201,{account,token},observe,{requestId,req,started});
      }

      if(req.method==='POST'&&url.pathname==='/v1/billing/webhook'){
        if(!billing)return reply(res,503,{error:'billing_webhook_not_configured',requestId},observe,{requestId,req,started});
        const rawBody=await readRaw(req);
        const result=await billing.process({rawBody,signature:req.headers['x-syncio-signature']});
        return reply(res,200,result,observe,{requestId,req,started});
      }

      const account=authenticateAccount(sessions,controlPlane,req);
      if(!account)return reply(res,401,{error:'unauthorized',requestId},observe,{requestId,req,started});

      if(req.method==='GET'&&url.pathname==='/v1/account')return reply(res,200,account,observe,{requestId,req,started});
      if(req.method==='GET'&&url.pathname==='/v1/account/export'){
        return reply(res,200,await controlPlane.exportAccount(account.id),observe,{requestId,req,started});
      }
      if(req.method==='DELETE'&&url.pathname==='/v1/account'){
        await controlPlane.deleteAccount(account.id);
        return reply(res,200,{deleted:true},observe,{requestId,req,started});
      }

      if(req.method==='GET'&&url.pathname==='/v1/projects'){
        const projects=controlPlane.db.collection('_projects').all().filter((project)=>project.accountId===account.id&&project.status==='active');
        return reply(res,200,{projects},observe,{requestId,req,started});
      }
      if(req.method==='POST'&&url.pathname==='/v1/projects'){
        const body=await readJson(req);
        const project=await controlPlane.createProject({accountId:account.id,name:body.name,plan:body.plan??'free'});
        return reply(res,201,{project},observe,{requestId,req,started});
      }

      const tokenMatch=url.pathname.match(/^\/v1\/projects\/([^/]+)\/token$/);
      if(req.method==='POST'&&tokenMatch){
        const projectId=decodeURIComponent(tokenMatch[1]);
        const project=controlPlane.project(projectId);
        if(!project||project.accountId!==account.id)return reply(res,404,{error:'project_not_found',requestId},observe,{requestId,req,started});
        const token=controlPlane.issueProjectToken({accountId:account.id,projectId});
        return reply(res,200,{token,projectId,entitlements:controlPlane.entitlements(projectId)},observe,{requestId,req,started});
      }

      return reply(res,404,{error:'not_found',requestId},observe,{requestId,req,started});
    }catch(error){
      const status=error.statusCode??500;
      const code=status>=500?'internal_error':(error.code??'bad_request');
      safeObserve(observe,{type:'control_request_error',requestId,status,code,error:status>=500?error:undefined});
      if(!res.headersSent)json(res,status,{error:code,requestId});else if(!res.writableEnded)res.end();
    }
  });
  server.requestTimeout=15_000;
  server.headersTimeout=10_000;
  server.keepAliveTimeout=5_000;

  return Object.freeze({
    server,
    sessions,
    billing,
    async listen({host='127.0.0.1',port=0}={}){
      await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(port,host,resolve);});
      const address=server.address();return{host,port:address.port,url:`http://${host}:${address.port}`};
    },
    async close(){if(server.listening)await new Promise((resolve,reject)=>server.close((error)=>error?reject(error):resolve()));}
  });
}

function issueAccountSession(sessions,accountId){return sessions.issue({subject:accountId,projectId:'__account__',role:'owner',entitlements:['account:manage']});}
function authenticateAccount(sessions,controlPlane,req){const session=sessions.authenticateRequest(req);if(!session||session.projectId!=='__account__')return null;const account=controlPlane.db.collection('_accounts').get(session.sub);return account?.status==='active'?account:null;}
async function readRaw(req){const chunks=[];let bytes=0;for await(const chunk of req){bytes+=chunk.length;if(bytes>MAX_BODY_BYTES)throw httpError(413,'payload_too_large');chunks.push(chunk);}return Buffer.concat(chunks).toString('utf8');}
async function readJson(req){const raw=await readRaw(req);if(!raw)throw httpError(400,'json_object_required');let value;try{value=JSON.parse(raw);}catch{throw httpError(400,'invalid_json');}if(!value||typeof value!=='object'||Array.isArray(value))throw httpError(400,'json_object_required');return value;}
function httpError(statusCode,code){const error=new Error(code);error.statusCode=statusCode;error.code=code;return error;}
function reply(res,status,body,observe,context){json(res,status,body);safeObserve(observe,{type:'control_request_complete',requestId:context.requestId,method:context.req.method,path:context.req.url,status,durationMs:Number(process.hrtime.bigint()-context.started)/1_000_000});}
function safeObserve(observe,event){try{observe(Object.freeze({...event}));}catch{}}
function json(res,status,body){if(res.writableEnded)return;res.statusCode=status;res.setHeader('content-type','application/json; charset=utf-8');res.end(JSON.stringify(body));}
