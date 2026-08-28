import { fork } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { RotatingTokenAuthority } from './security.js';
import { RevenueEngine, createUsageObserver } from './monetization.js';

const workerFile=fileURLToPath(new URL('../bin/syncio-tenant.js',import.meta.url));

export class ManagedRuntimeManager {
  constructor({ controlPlane, tokenSecret, storageRoot, regions=['local'], maxProjects=100, startupTimeoutMs=15000, revenueEngine }={}) {
    if(!controlPlane||typeof controlPlane.project!=='function')throw new TypeError('managed runtime requires controlPlane');
    const key=Buffer.isBuffer(tokenSecret)?Buffer.from(tokenSecret):Buffer.from(String(tokenSecret??''));if(key.length<32)throw new TypeError('managed runtime token secret requires at least 32 bytes');
    if(!Array.isArray(regions)||!regions.length||regions.some((r)=>typeof r!=='string'||!r))throw new TypeError('regions required');
    if(!Number.isSafeInteger(maxProjects)||maxProjects<1)throw new TypeError('maxProjects must be positive');
    this.controlPlane=controlPlane;this.secret=key.toString('base64url');this.storageRoot=path.resolve(storageRoot??'./syncio-managed-projects');this.regions=[...new Set(regions)];this.maxProjects=maxProjects;this.startupTimeoutMs=startupTimeoutMs;this.instances=new Map();this.revenue=revenueEngine??new RevenueEngine({controlPlane});this.usageObservers=new Map();
  }
  list(){return [...this.instances.values()].map((item)=>publicState(item));}
  get(projectId){const item=this.instances.get(projectId);return item?publicState(item):null;}
  issueDataToken({projectId,subject,role='owner',ttlSeconds=3600}={}){const project=this.controlPlane.project(projectId);if(!project)throw notFound('project_not_found');const authority=new RotatingTokenAuthority({keys:{legacy:Buffer.from(this.secret)},activeKeyId:'legacy',issuer:`syncio:${projectId}`});return authority.issue({subject:subject??project.accountId,projectId,role,entitlements:this.controlPlane.entitlements(projectId),expiresInSeconds:ttlSeconds});}
  async startProject(projectId,{region=this.regions[0],databaseOptions={}}={}){
    const project=this.controlPlane.project(projectId);if(!project)throw notFound('project_not_found');if(!this.regions.includes(region))throw Object.assign(new Error('unsupported_region'),{code:'unsupported_region'});const regionDecision=this.revenue.quotaDecision(projectId,'regions',this.#activeRegionCount(projectId,region));if(!regionDecision.allowed)throw quotaError(regionDecision);if(this.instances.has(projectId))return publicState(this.instances.get(projectId));if(this.instances.size>=this.maxProjects)throw Object.assign(new Error('managed_capacity_exceeded'),{code:'managed_capacity_exceeded',statusCode:503});
    const file=path.join(this.storageRoot,region,projectId,'data.syncio.json');const config={projectId,file,secret:this.secret,region,port:0,databaseOptions};const child=fork(workerFile,[],{stdio:['ignore','pipe','pipe','ipc'],env:{...process.env,SYNCIO_TENANT_CONFIG:Buffer.from(JSON.stringify(config)).toString('base64url')}});
    const state={projectId,region,status:'starting',pid:child.pid,address:null,child,startedAt:new Date().toISOString(),lastStatus:null};this.instances.set(projectId,state);
    const usage=this.#usageObserver(projectId);const onUsage=(message)=>{if(message?.type==='usage'&&message.projectId===projectId)usage(message.event);};child.on('message',onUsage);state.onUsage=onUsage;
    try{const ready=await waitFor(child,(message)=>message?.type==='ready'||message?.type==='failed',this.startupTimeoutMs);if(ready.type==='failed')throw Object.assign(new Error(ready.message),{code:ready.code});state.status='running';state.address=ready.address;state.pid=ready.pid;child.once('exit',(code,signal)=>{state.status=code===0?'stopped':'failed';state.exit={code,signal,at:new Date().toISOString()};if(this.instances.get(projectId)===state&&state.status==='stopped')this.instances.delete(projectId);});return publicState(state);}catch(error){this.instances.delete(projectId);child.off('message',onUsage);child.kill('SIGKILL');throw error;}
  }
  async status(projectId){const state=this.instances.get(projectId);if(!state)throw notFound('runtime_not_found');if(state.status!=='running')return publicState(state);const message=await requestStatus(state.child,5000);state.lastStatus=message;return {...publicState(state),runtime:message,usage:this.revenue.usageSummary(projectId)};}
  async stopProject(projectId,{forceAfterMs=5000}={}){const state=this.instances.get(projectId);if(!state)return false;if(state.status==='stopped'){this.instances.delete(projectId);return true;}state.status='stopping';state.child.send({type:'shutdown'});const result=await Promise.race([waitForExit(state.child),new Promise((resolve)=>setTimeout(()=>resolve(null),forceAfterMs))]);if(!result)state.child.kill('SIGKILL');if(state.onUsage)state.child.off('message',state.onUsage);this.instances.delete(projectId);return true;}
  async restartProject(projectId,options={}){const current=this.instances.get(projectId);const region=options.region??current?.region??this.regions[0];await this.stopProject(projectId).catch(()=>undefined);return this.startProject(projectId,{...options,region});}
  async close(){await Promise.all([...this.instances.keys()].map((id)=>this.stopProject(id).catch(()=>undefined)));}
  #usageObserver(projectId){if(!this.usageObservers.has(projectId))this.usageObservers.set(projectId,createUsageObserver({meter:this.revenue.usage,projectId}));return this.usageObservers.get(projectId);}
  #activeRegionCount(projectId,targetRegion){const regions=new Set([...this.instances.values()].filter((item)=>item.projectId===projectId).map((item)=>item.region));regions.add(targetRegion);return regions.size;}
}

function publicState(state){return{projectId:state.projectId,region:state.region,status:state.status,pid:state.pid,address:state.address,startedAt:state.startedAt,exit:state.exit??null};}
function waitFor(child,predicate,timeoutMs){return new Promise((resolve,reject)=>{const timer=setTimeout(()=>cleanup(()=>reject(Object.assign(new Error('tenant startup timeout'),{code:'TENANT_START_TIMEOUT'}))),timeoutMs);const onMessage=(message)=>{if(predicate(message))cleanup(()=>resolve(message));};const onExit=(code,signal)=>cleanup(()=>reject(Object.assign(new Error(`tenant exited before ready (${code??signal})`),{code:'TENANT_EXIT_EARLY'})));const cleanup=(finish)=>{clearTimeout(timer);child.off('message',onMessage);child.off('exit',onExit);finish();};child.on('message',onMessage);child.once('exit',onExit);});}
function requestStatus(child,timeoutMs){return new Promise((resolve,reject)=>{const timer=setTimeout(()=>cleanup(()=>reject(Object.assign(new Error('tenant status timeout'),{code:'TENANT_STATUS_TIMEOUT'}))),timeoutMs);const onMessage=(message)=>{if(message?.type==='status')cleanup(()=>resolve(message));};const onExit=()=>cleanup(()=>reject(Object.assign(new Error('tenant stopped'),{code:'TENANT_STOPPED'})));const cleanup=(finish)=>{clearTimeout(timer);child.off('message',onMessage);child.off('exit',onExit);finish();};child.on('message',onMessage);child.once('exit',onExit);child.send({type:'status'});});}
function waitForExit(child){return new Promise((resolve)=>{if(child.exitCode!==null||child.signalCode!==null)return resolve({code:child.exitCode,signal:child.signalCode});child.once('exit',(code,signal)=>resolve({code,signal}));});}
function notFound(code){const error=new Error(code);error.code=code;error.statusCode=404;return error;}
function quotaError(decision){const error=new Error(`plan quota exceeded: ${decision.resource}`);error.code='plan_quota_exceeded';error.statusCode=402;error.details=decision;return error;}
