import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { RegionalTopology } from './distributed.js';
import { PRIORITY } from './scheduler.js';

const clone=v=>structuredClone(v);

export class GlobalCommitLedger {
  constructor(file,state){this.file=path.resolve(file);this.state=state;this.queue=Promise.resolve();}
  static async open(file){let state={version:1,entries:[],head:null};try{state=JSON.parse(await fs.readFile(file,'utf8'));}catch(error){if(error.code!=='ENOENT')throw error;}validateLedger(state);const ledger=new GlobalCommitLedger(file,state);if(!ledger.verify())throw corruption('global commit ledger integrity failure');return ledger;}
  append({region,partition,commitId,sequence,causalParents=[]}={}){return this.#enqueue(async()=>{if(typeof region!=='string'||!region||typeof partition!=='string'||!partition||typeof commitId!=='string'||!commitId||!Number.isSafeInteger(sequence)||sequence<0)throw new TypeError('invalid global commit');const previous=this.state.head;const body={version:1,index:this.state.entries.length+1,region,partition,commitId,sequence,causalParents:[...new Set(causalParents)].sort(),previous,at:new Date().toISOString()};const hash=digest(body);const entry={...body,hash};this.state.entries.push(entry);this.state.head=hash;await this.#persist();return clone(entry);});}
  verify(){let previous=null,index=0;for(const entry of this.state.entries){index++;const {hash,...body}=entry;if(body.index!==index||body.previous!==previous||hash!==digest(body))return false;previous=hash;}return this.state.head===previous;}
  since(index=0,{limit=1000}={}){if(!Number.isSafeInteger(index)||index<0||!Number.isSafeInteger(limit)||limit<1)throw new TypeError('invalid ledger cursor');return this.state.entries.filter(x=>x.index>index).slice(0,limit).map(clone);}
  get head(){return this.state.head;}
  get length(){return this.state.entries.length;}
  #enqueue(fn){const op=this.queue.then(fn);this.queue=op.catch(()=>undefined);return op;}
  async #persist(){await atomicJson(this.file,this.state);}
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
  async put(collection,record,{sessionId=null}={}){const region=this.topology.primary,target=this.topology.route({write:true});const result=await target.put(collection,record,{consistency:'quorum',sessionId});this.metrics.writes++;await this.ledger?.append({region,partition:collection,commitId:result.operationId,sequence:result.sequence,causalParents:[]});return{...result,region,globalHead:this.ledger?.head??null};}
  async remove(collection,id,{sessionId=null}={}){const region=this.topology.primary,target=this.topology.route({write:true});const result=await target.remove(collection,id,{consistency:'quorum',sessionId});this.metrics.writes++;await this.ledger?.append({region,partition:collection,commitId:result.operationId,sequence:result.sequence,causalParents:[]});return{...result,region,globalHead:this.ledger?.head??null};}
  async get(collection,id,{preferredRegion=null,consistency='quorum',sessionId=null,maxStalenessMs=5000}={}){const target=this.topology.route({preferred:preferredRegion});this.metrics.reads++;return target.get(collection,id,{consistency,sessionId,maxStalenessMs});}
  markRegion(region,status){const before=this.topology.primary;const result=this.topology.mark(region,status);if(before!==this.topology.primary)this.metrics.failovers++;return result;}
  status(){return{topology:this.topology.status(),globalCommits:this.ledger?.length??0,globalHead:this.ledger?.head??null,metrics:{...this.metrics}};}
  async close(){await this.ledger?.close?.();}
}

function validateLedger(state){if(state?.version!==1||!Array.isArray(state.entries)||!(state.head===null||typeof state.head==='string'))throw corruption('invalid global commit ledger');}
function digest(value){return crypto.createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');}
function canonical(v){if(Array.isArray(v))return v.map(canonical);if(v&&typeof v==='object'){const o={};for(const k of Object.keys(v).sort())o[k]=canonical(v[k]);return o;}return v;}
async function atomicJson(file,value){await fs.mkdir(path.dirname(file),{recursive:true});const temp=`${file}.${crypto.randomUUID()}.tmp`;try{await fs.writeFile(temp,`${JSON.stringify(value)}\n`,{mode:0o600});const h=await fs.open(temp,'r');try{await h.sync();}finally{await h.close();}await fs.rename(temp,file);const d=await fs.open(path.dirname(file),'r');try{await d.sync();}finally{await d.close();}}finally{await fs.rm(temp,{force:true}).catch(()=>undefined);}}
function corruption(message){const e=new Error(message);e.code='SYNCIO_GLOBAL_LEDGER_CORRUPT';return e;}
function capacity(message){const e=new Error(message);e.code='SYNCIO_SUBSCRIPTION_CAPACITY';e.statusCode=429;return e;}
