import fsSync from 'node:fs';
import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { RegionalTopology } from './distributed.js';
import { PRIORITY } from './scheduler.js';

const clone=v=>structuredClone(v);

export class GlobalCommitLedger {
  constructor(file,state){this.file=path.resolve(file);this.journalFile=`${this.file}.ndjson`;this.state=state;this.queue=Promise.resolve();}
  static async open(file){const target=path.resolve(file),journal=`${target}.ndjson`;let state={version:2,length:0,head:null};try{const raw=JSON.parse(await fs.readFile(target,'utf8'));if(raw.version===1&&Array.isArray(raw.entries)){await migrateLegacyLedger(target,journal,raw);state={version:2,length:raw.entries.length,head:raw.head??null};}else state=raw;}catch(error){if(error.code!=='ENOENT')throw error;}validateLedgerState(state);const ledger=new GlobalCommitLedger(target,state);const verified=await ledger.verify();if(!verified.ok)throw corruption('global commit ledger integrity failure');if(state.length!==verified.length||state.head!==verified.head){state={version:2,length:verified.length,head:verified.head};ledger.state=state;await ledger.#persistManifest();}else if(!(await exists(target)))await ledger.#persistManifest();return ledger;}
  append({region,partition,commitId,sequence,causalParents=[]}={}){return this.#enqueue(async()=>{if(typeof region!=='string'||!region||typeof partition!=='string'||!partition||typeof commitId!=='string'||!commitId||!Number.isSafeInteger(sequence)||sequence<0)throw new TypeError('invalid global commit');const body={version:1,index:this.state.length+1,region,partition,commitId,sequence,causalParents:[...new Set(causalParents)].sort(),previous:this.state.head,at:new Date().toISOString()};const entry={...body,hash:digest(body)};await appendLine(this.journalFile,entry);this.state.length=entry.index;this.state.head=entry.hash;await this.#persistManifest();return clone(entry);});}
  async verify(){let previous=null,length=0;for await(const entry of readLines(this.journalFile)){length++;const {hash,...body}=entry;if(body.index!==length||body.previous!==previous||hash!==digest(body))return{ok:false,length,head:previous};previous=hash;}return{ok:true,length,head:previous};}
  since(index=0,{limit=1000}={}){if(!Number.isSafeInteger(index)||index<0||!Number.isSafeInteger(limit)||limit<1)throw new TypeError('invalid ledger cursor');const out=[];if(!fsSync.existsSync(this.journalFile))return out;const text=fsSync.readFileSync(this.journalFile,'utf8');for(const line of text.split('\n')){if(!line)continue;const entry=JSON.parse(line);if(entry.index>index){out.push(entry);if(out.length>=limit)break;}}return out.map(clone);}
  get head(){return this.state.head;}
  get length(){return this.state.length;}
  status(){return{format:'append-only-global-commit-ledger/2',length:this.length,head:this.head,residentHistoryEntries:0};}
  #enqueue(fn){const op=this.queue.then(fn);this.queue=op.catch(()=>undefined);return op;}
  async #persistManifest(){await atomicJson(this.file,this.state);}
  async close(){await this.queue;}
}

export class SubscriptionRouter {
  constructor(partitions,{maxSubscriptions=10000,scheduler=null}={}){if(!partitions||typeof partitions!=='object'||Array.isArray(partitions)||!Object.keys(partitions).length)throw new TypeError('partitions required');if(!Number.isSafeInteger(maxSubscriptions)||maxSubscriptions<1)throw new TypeError('invalid subscription capacity');this.partitions=new Map(Object.entries(partitions));for(const[id,db]of this.partitions)if(!db?.watchChanges)throw new TypeError(`partition ${id} lacks change stream`);this.maxSubscriptions=maxSubscriptions;this.scheduler=scheduler?.child?.('subscription-router')??scheduler;this.subscriptions=new Map();this.metrics={opened:0,closed:0,rejected:0,events:0};}
  subscribe({partition,collection=null,after=0,listener,id=crypto.randomUUID()}={}){if(!this.partitions.has(partition))throw new TypeError('unknown partition');if(typeof listener!=='function')throw new TypeError('subscription listener required');if(this.subscriptions.size>=this.maxSubscriptions){this.metrics.rejected++;throw capacity('subscription router capacity exceeded');}const admission=this.scheduler?.admit?.({priority:PRIORITY.realtime,cost:{memory:8192,network:1,egress:4096,coordination:1}});if(admission?.decision&&admission.decision!=='admit'){this.metrics.rejected++;throw capacity('subscription scheduler capacity exceeded');}const db=this.partitions.get(partition);let stopped=false;const stop=db.watchChanges({collection,after},event=>{if(stopped)return;this.metrics.events++;listener(clone(event));});const close=()=>{if(stopped)return false;stopped=true;stop?.();admission?.release?.();this.subscriptions.delete(id);this.metrics.closed++;return true;};this.subscriptions.set(id,{id,partition,collection,after,close});this.metrics.opened++;return Object.freeze({id,close});}
  close(id){return this.subscriptions.get(id)?.close()??false;}
  closeAll(){for(const entry of[...this.subscriptions.values()])entry.close();}
  status(){return{subscriptions:this.subscriptions.size,maxSubscriptions:this.maxSubscriptions,metrics:{...this.metrics},partitions:[...this.partitions.keys()].sort()};}
}

export class RegionalDistributedDatabase {
  constructor(regions,{primary=null,ledger=null}={}){this.topology=new RegionalTopology(regions,{primary});this.ledger=ledger;this.metrics={writes:0,reads:0,failovers:0};}
  async put(collection,record,{sessionId=null,partition=collection,causalParents=[]}={}){const region=this.topology.primary,target=this.topology.route({write:true});const result=await target.put(collection,record,{consistency:'quorum',sessionId});this.metrics.writes++;await this.ledger?.append({region,partition,commitId:result.operationId,sequence:result.sequence,causalParents});return{...result,region,globalHead:this.ledger?.head??null};}
  async remove(collection,id,{sessionId=null,partition=collection,causalParents=[]}={}){const region=this.topology.primary,target=this.topology.route({write:true});const result=await target.remove(collection,id,{consistency:'quorum',sessionId});this.metrics.writes++;await this.ledger?.append({region,partition,commitId:result.operationId,sequence:result.sequence,causalParents});return{...result,region,globalHead:this.ledger?.head??null};}
  async get(collection,id,{preferredRegion=null,consistency='quorum',sessionId=null,maxStalenessMs=5000}={}){const target=this.topology.route({preferred:preferredRegion});this.metrics.reads++;return target.get(collection,id,{consistency,sessionId,maxStalenessMs});}
  markRegion(region,status){const before=this.topology.primary;const result=this.topology.mark(region,status);if(before!==this.topology.primary)this.metrics.failovers++;return result;}
  status(){return{topology:this.topology.status(),globalCommits:this.ledger?.length??0,globalHead:this.ledger?.head??null,ledger:this.ledger?.status?.()??null,metrics:{...this.metrics}};}
  async close(){await this.ledger?.close?.();}
}

async function migrateLegacyLedger(target,journal,legacy){await fs.rm(journal,{force:true});for(const entry of legacy.entries)await appendLine(journal,entry);await atomicJson(target,{version:2,length:legacy.entries.length,head:legacy.head??legacy.entries.at(-1)?.hash??null});}
function validateLedgerState(state){if(state?.version!==2||!Number.isSafeInteger(state.length)||state.length<0||!(state.head===null||typeof state.head==='string'))throw corruption('invalid global commit ledger manifest');}
function digest(value){return crypto.createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');}
function canonical(v){if(Array.isArray(v))return v.map(canonical);if(v&&typeof v==='object'){const o={};for(const k of Object.keys(v).sort())o[k]=canonical(v[k]);return o;}return v;}
async function appendLine(file,value){await fs.mkdir(path.dirname(file),{recursive:true});const h=await fs.open(file,'a',0o600);try{await h.writeFile(`${JSON.stringify(value)}\n`,'utf8');await h.sync();}finally{await h.close();}}
async function* readLines(file){let text='';try{text=await fs.readFile(file,'utf8');}catch(error){if(error.code==='ENOENT')return;throw error;}for(const line of text.split('\n'))if(line)yield JSON.parse(line);}
async function atomicJson(file,value){await fs.mkdir(path.dirname(file),{recursive:true});const temp=`${file}.${crypto.randomUUID()}.tmp`;try{await fs.writeFile(temp,`${JSON.stringify(value)}\n`,{mode:0o600});const h=await fs.open(temp,'r');try{await h.sync();}finally{await h.close();}await fs.rename(temp,file);const d=await fs.open(path.dirname(file),'r');try{await d.sync();}finally{await d.close();}}finally{await fs.rm(temp,{force:true}).catch(()=>undefined);}}
async function exists(file){try{await fs.access(file);return true;}catch{return false;}}
function corruption(message){const e=new Error(message);e.code='SYNCIO_GLOBAL_LEDGER_CORRUPT';return e;}
function capacity(message){const e=new Error(message);e.code='SYNCIO_SUBSCRIPTION_CAPACITY';e.statusCode=429;return e;}
