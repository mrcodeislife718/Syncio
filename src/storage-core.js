import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { EventEmitter } from 'node:events';
import { WriteAheadLog } from './wal.js';
import { TieredStatePlane } from './tiered-store.js';
import { queryRecords } from './advanced.js';
import { createCommitFabric, verifyCommitFabric } from './commit-fabric.js';
import { ReactiveQueryGraph } from './reactive.js';
import { DurableIntentLog } from './sync-plane.js';
import { AdaptiveScheduler, PRIORITY } from './scheduler.js';
import { CONSISTENCY, consistencyContract } from './consistency.js';
import { createRecoveryManifest } from './recovery-manifest.js';

const FORMAT='syncio-storage-core/1';
const clone=value=>structuredClone(value);

export class StorageBackedSyncioDatabase {
  constructor(file,meta,wal,state,intents,options,recovery){this.file=file;this.meta=meta;this.wal=wal;this.state=state;this.intents=intents;this.changeRetention=options.changeRetention;this.checkpointEvery=options.checkpointEvery;this.scheduler=options.scheduler??new AdaptiveScheduler(options.schedulerOptions);this.reactive=new ReactiveQueryGraph(options.reactiveOptions);this.events=new EventEmitter();this.events.setMaxListeners(0);this.queue=Promise.resolve();this.commitsSinceCheckpoint=0;this.degraded=false;this.checkpointError=null;this.recovery=recovery;this.syncViews=new Map();}

  static async open(file,options={}){
    const target=path.resolve(file),cfg=normalizeOptions(options),loaded=await readCheckpoint(target);
    const legacy=loaded.value&&!isMeta(loaded.value)?loaded.value:null;
    const meta=isMeta(loaded.value)?normalizeMeta(loaded.value):freshMeta(legacy?.databaseId);
    const state=await TieredStatePlane.open(`${target}.segments`,cfg.storage);
    if(legacy)await importLegacy(state,meta,legacy);
    for(const name of meta.collections)await state.collection(name);
    const wal=await WriteAheadLog.open(`${target}.wal`);
    const intents=await DurableIntentLog.open(`${target}.intents.json`);
    const replay=await replayWal(state,meta,wal,cfg.changeRetention);
    let recovery=null;
    if(legacy||loaded.source==='backup'||replay.count){recovery=createRecoveryManifest({failure:legacy?'legacy_state_migration':loaded.source==='backup'?'primary_checkpoint_unavailable':'wal_replay',lastDurableCommit:meta.lastCommitId,checkpoint:{source:loaded.source,sequence:loaded.sequence},walAccepted:replay.accepted,walRejected:[],invariants:['wal_contiguous','segment_state_authoritative','commit_fabric_verified'],finalSequence:meta.sequence,stateDigest:await digestState(state,meta.collections)});await atomicJson(`${target}.recovery.json`,recovery);}
    const db=new StorageBackedSyncioDatabase(target,meta,wal,state,intents,cfg,recovery);
    if(!loaded.value||legacy||loaded.source==='backup'||replay.count)await db.checkpointNow();
    return db;
  }

  get databaseId(){return this.meta.databaseId;}
  get sequence(){return this.meta.sequence;}
  consistency(profile=CONSISTENCY.SERIALIZABLE,options={}){return consistencyContract(profile,options);}
  storageStatus(){const collections={};for(const name of this.meta.collections){const s=this.state.getOpened(name);if(s)collections[name]=s.stats();}return{mode:'commit-fabric-segmented-mvcc',authoritativeState:'ssd-segments',fullStateResidentInRam:false,sequence:this.sequence,walEntries:this.wal.listAfter(0).length,checkpointEvery:this.checkpointEvery,commitsSinceCheckpoint:this.commitsSinceCheckpoint,degraded:this.degraded,checkpointError:this.checkpointError,scheduler:this.scheduler.snapshot(),collections};}

  collection(name){validateCollection(name);const db=this;return Object.freeze({
    async insert(value){const record=recordValue({...value,id:value?.id??crypto.randomUUID()});await db.transaction(tx=>{const c=tx.collection(name);if(c.get(record.id))throw duplicate(record.id);c.put(record);});return clone(record);},
    async upsert(value){const record=recordValue(value);await db.transaction(tx=>tx.collection(name).put(record));return clone(record);},
    get(id){return db.readRecord(name,id);},all(){return db.readAll(name);},scan(){return db.scan(name);},query(spec={}){return db.query(name,spec);},
    async remove(id){let removed=false;await db.transaction(tx=>{removed=tx.collection(name).remove(id);});return removed;},
    watch(listener){return db.watchChanges({collection:name,after:db.sequence},listener);}
  });}

  readRecord(collection,id){validateCollection(collection);validateId(id);return this.state.getOpened(collection)?.getSync(id)??null;}
  readAll(collection){validateCollection(collection);return this.state.getOpened(collection)?.allSync()??[];}
  scan(collection){validateCollection(collection);return this.state.getOpened(collection)?.scanSync()??[][Symbol.iterator]();}
  query(collection,spec={},options={}){this.consistency(options.consistency??CONSISTENCY.SERIALIZABLE,options);return queryRecords(this.readAll(collection),spec);}

  async transaction(work,{maxRetries=8,origin='local',causalParents=[],partitionId='local',sourceChangeId=null}={}){
    if(typeof work!=='function')throw new TypeError('transaction requires function');if(!Number.isSafeInteger(maxRetries)||maxRetries<0||maxRetries>100)throw new TypeError('invalid transaction retry limit');
    for(let attempt=0;attempt<=maxRetries;attempt++){const tx=new WriteSetTransaction(this);const result=await work(tx.api());if(!tx.hasWrites())return result;try{await this.enqueue(()=>this.commitTransaction(tx,{origin,causalParents,partitionId,sourceChangeId}));return result;}catch(error){if(error.code!=='SYNCIO_TRANSACTION_CONFLICT'||attempt===maxRetries)throw error;}}
    throw conflict('transaction retries exhausted');
  }

  async commitTransaction(tx,{origin,causalParents,partitionId,sourceChangeId}){
    tx.validate();const ops=tx.operations();if(!ops.length)return null;
    const admission=this.scheduler.admit({priority:PRIORITY.foreground,cost:commitCost(ops)});if(admission.decision!=='admit')throw busy(admission);
    try{
      const base=this.sequence,result=base+ops.length,transactionId=crypto.randomUUID();
      const mutations=ops.map(op=>({type:op.after?'put':'remove',collection:op.collection,id:op.id,fields:changedFields(op.before,op.after),...(op.after?{record:clone(op.after)}:{})}));
      const fabric=createCommitFabric({databaseId:this.databaseId,partitionId,sequence:result,transactionId,origin,mutations,causalParents});if(!verifyCommitFabric(fabric))throw corrupt('commit fabric verification failed');
      const events=ops.map((op,i)=>({type:op.after?(op.before?'upsert':'insert'):'remove',collection:op.collection,id:op.id,...(op.after?{record:clone(op.after)}:{}),fields:mutations[i].fields,changeId:i===0&&sourceChangeId?sourceChangeId:crypto.randomUUID(),originDatabaseId:this.databaseId,sequence:base+i+1,at:new Date(fabric.logicalTime).toISOString(),commitId:fabric.commitId,commitChecksum:fabric.checksum,transactionId,partitionId,logicalTime:fabric.logicalTime,causalParents:[...fabric.causalParents],sourceChangeId:i===0?sourceChangeId:null});
      await this.wal.append({databaseId:this.databaseId,baseSequence:base,resultSequence:result,events});
      await applyEvents(this.state,events);
      this.meta.sequence=result;this.meta.lastCommitId=fabric.commitId;
      for(const event of events){this.meta.recordVersions[keyOf(event.collection,event.id)]=event.sequence;this.meta.collectionVersions[event.collection]=event.sequence;if(!this.meta.collections.includes(event.collection))this.meta.collections.push(event.collection);this.meta.changes.push(clone(event));if(event.sourceChangeId)this.meta.appliedChangeIds=bounded([...this.meta.appliedChangeIds,event.sourceChangeId],this.changeRetention*2);}
      this.meta.collections.sort();trimChanges(this.meta,this.changeRetention);this.commitsSinceCheckpoint++;
      for(const event of events)this.events.emit('change',clone(event));this.events.emit('commit',fabric);
      if(this.commitsSinceCheckpoint>=this.checkpointEvery)this.scheduleCheckpoint();
      return fabric;
    }finally{admission.release?.();}
  }

  changesSince(cursor=0,{limit=Infinity}={}){sequence(cursor);return this.meta.changes.filter(x=>x.sequence>cursor).slice(0,limit).map(clone);}
  resumeStatus(cursor=0){sequence(cursor);const oldest=this.meta.changes[0]?.sequence??this.sequence+1;return{resumable:cursor===this.sequence||cursor>=oldest-1,cursor,oldestRetained:oldest,sequence:this.sequence};}
  watchChanges({collection=null,after=this.sequence}={},listener){if(typeof listener!=='function')throw new TypeError('listener required');const status=this.resumeStatus(after);if(!status.resumable)throw streamExpired(status);let cursor=after;for(const e of this.changesSince(after)){cursor=e.sequence;if(!collection||e.collection===collection)listener(clone(e));}const handler=e=>{if(e.sequence<=cursor)return;cursor=e.sequence;if(!collection||e.collection===collection)listener(clone(e));};this.events.on('change',handler);return()=>this.events.off('change',handler);}
  watchCommits(listener){if(typeof listener!=='function')throw new TypeError('listener required');this.events.on('commit',listener);return()=>this.events.off('commit',listener);}

  subscribeQuery(collection,spec,listener,{id=crypto.randomUUID(),emitInitial=true}={}){validateCollection(collection);if(typeof listener!=='function')throw new TypeError('listener required');const evaluate=()=>this.query(collection,spec);const initial=evaluate();const unregister=this.reactive.register(id,{collection,query:spec?.where??spec??{},evaluate,initial});const onCommit=async commit=>{for(const result of await this.reactive.applyCommit(commit))if(result.queryId===id)listener({type:'query_result',queryId:id,commitId:commit.commitId,sequence:commit.sequence,value:clone(result.value)});};this.events.on('commit',onCommit);if(emitInitial)queueMicrotask(()=>listener({type:'query_result',queryId:id,commitId:this.meta.lastCommitId,sequence:this.sequence,initial:true,value:clone(initial)}));return()=>{this.events.off('commit',onCommit);unregister();};}

  defineSyncView(name,{collection,where={},maxDocuments=100000}={}){if(typeof name!=='string'||!/^[A-Za-z0-9_.-]+$/.test(name)||name.length>128)throw new TypeError('invalid sync view name');validateCollection(collection);if(!Number.isSafeInteger(maxDocuments)||maxDocuments<1)throw new TypeError('invalid maxDocuments');const view=Object.freeze({name,collection,where:clone(where),maxDocuments});this.syncViews.set(name,view);return clone(view);}
  materializeSyncView(name){const view=this.syncViews.get(name);if(!view)throw codeError('SYNCIO_SYNC_VIEW_NOT_FOUND','sync view not found');const records=queryRecords(this.readAll(view.collection),{where:view.where});if(records.length>view.maxDocuments)throw codeError('SYNCIO_RESOURCE_LIMIT','sync view expansion exceeds limit');return{name,collection:view.collection,sequence:this.sequence,records:clone(records)};}
  enqueueOfflineIntent(intent){return this.intents.enqueue(intent);}
  listOfflineIntents(){return this.intents.list();}
  reconcileOfflineIntents(){return this.intents.reconcile(async intent=>{await this.transaction(tx=>{for(const p of intent.preconditions??[]){const current=tx.collection(p.collection).get(p.id);if(!precondition(p,current)&&intent.conflictPolicy!=='overwrite')throw conflict('offline intent precondition failed');}applyIntent(tx,intent.mutation);});return{status:'committed'};});}

  hasAppliedChange(id){return this.meta.appliedChangeIds.includes(id);}
  async applyReplicationChange(change,resolver=(_local,remote)=>remote){if(!change?.changeId||!change.collection)throw new TypeError('replication change identity required');if(this.hasAppliedChange(change.changeId))return{applied:false,duplicate:true};let resolved=null;await this.transaction(tx=>{const c=tx.collection(change.collection);if(change.type==='remove'){c.remove(change.id);return;}const incoming=recordValue(change.record);resolved=resolver(c.get(incoming.id),incoming);if(resolved)c.put(recordValue(resolved));},{origin:change.originDatabaseId??'replication',causalParents:change.commitId?[change.commitId]:[],sourceChangeId:change.changeId});return{applied:true,duplicate:false,record:resolved?clone(resolved):null};}

  snapshot(){const collections={};for(const name of this.meta.collections)collections[name]=Object.fromEntries(this.readAll(name).map(r=>[r.id,r]));return{version:1,databaseId:this.databaseId,sequence:this.sequence,collections,changes:this.meta.changes.map(clone),appliedChangeIds:[...this.meta.appliedChangeIds]};}
  async replaceState(snapshot){validateSnapshot(snapshot);return this.enqueue(async()=>{for(const name of new Set([...this.meta.collections,...Object.keys(snapshot.collections)])){const store=await this.openStore(name),desired=snapshot.collections[name]??{},ops=[];for(const id of store.ids())if(!Object.hasOwn(desired,id))ops.push({type:'remove',id});for(const record of Object.values(desired))ops.push({type:'put',record:recordValue(record)});if(ops.length)await store.applyBatch(ops);}this.meta.sequence=snapshot.sequence;this.meta.collections=Object.keys(snapshot.collections).sort();this.meta.changes=(snapshot.changes??[]).slice(-this.changeRetention).map(clone);this.meta.appliedChangeIds=bounded(snapshot.appliedChangeIds??[],this.changeRetention*2);this.meta.recordVersions={};this.meta.collectionVersions={};for(const[name,records]of Object.entries(snapshot.collections)){this.meta.collectionVersions[name]=snapshot.sequence;for(const id of Object.keys(records))this.meta.recordVersions[keyOf(name,id)]=snapshot.sequence;}await this.wal.reset();await this.persistCheckpoint();return true;});}

  async openStore(name){validateCollection(name);if(!this.meta.collections.includes(name)){this.meta.collections.push(name);this.meta.collections.sort();}return this.state.collection(name);}
  enqueue(work){const op=this.queue.then(work);this.queue=op.catch(()=>undefined);return op;}
  checkpoint(){return this.checkpointNow();}
  checkpointNow(){return this.enqueue(async()=>{await this.persistCheckpoint();await this.wal.compactThrough(this.sequence);this.commitsSinceCheckpoint=0;this.degraded=false;this.checkpointError=null;return this.sequence;});}
  async persistCheckpoint(){const value={...clone(this.meta),format:FORMAT,version:1};await atomicJson(this.file,value);await atomicJson(`${this.file}.bak`,value);}
  scheduleCheckpoint(){const seq=this.sequence;queueMicrotask(()=>this.enqueue(async()=>{if(this.commitsSinceCheckpoint<this.checkpointEvery)return;try{await this.persistCheckpoint();await this.wal.compactThrough(seq);this.commitsSinceCheckpoint=0;this.degraded=false;this.checkpointError=null;}catch(error){this.degraded=true;this.checkpointError=error.code??error.message;}}));}
  lastRecoveryManifest(){return this.recovery?clone(this.recovery):null;}
  async close(){await this.queue;if(this.commitsSinceCheckpoint>0)await this.checkpointNow();await this.state.close();await this.wal.close();this.events.removeAllListeners();}
}

export class WriteSetTransaction{
  constructor(db){this.db=db;this.reads=new Map();this.scans=new Map();this.writes=new Map();}
  api(){const tx=this;return Object.freeze({readVersion:this.db.sequence,collection:name=>tx.collection(name)});}
  collection(name){validateCollection(name);const tx=this;return Object.freeze({
    get(id){validateId(id);const key=keyOf(name,id);if(tx.writes.has(key))return clone(tx.writes.get(key).after);const value=tx.db.readRecord(name,id);tx.reads.set(key,tx.db.meta.recordVersions[key]??0);return clone(value);},
    put(record){const value=recordValue(record),key=keyOf(name,value.id),prior=tx.writes.has(key)?tx.writes.get(key).before:tx.db.readRecord(name,value.id);tx.writes.set(key,{collection:name,id:value.id,before:clone(prior),after:clone(value)});},
    remove(id){validateId(id);const key=keyOf(name,id),current=tx.writes.has(key)?tx.writes.get(key).after:tx.db.readRecord(name,id);if(!current)return false;const before=tx.writes.has(key)?tx.writes.get(key).before:current;tx.writes.set(key,{collection:name,id,before:clone(before),after:null});return true;},
    all(){tx.scans.set(name,tx.db.meta.collectionVersions[name]??0);const map=new Map(tx.db.readAll(name).map(r=>[r.id,r]));for(const w of tx.writes.values())if(w.collection===name){if(w.after)map.set(w.id,clone(w.after));else map.delete(w.id);}return[...map.values()].map(clone);}
  });}
  hasWrites(){return this.writes.size>0;}
  operations(){return[...this.writes.values()].filter(w=>JSON.stringify(w.before)!==JSON.stringify(w.after));}
  validate(){for(const[key,v]of this.reads)if((this.db.meta.recordVersions[key]??0)!==v)throw conflict('record changed during transaction');for(const[name,v]of this.scans)if((this.db.meta.collectionVersions[name]??0)!==v)throw conflict('collection changed during transaction scan');}
}

async function replayWal(state,meta,wal,retention){let count=0;const accepted=[];for(const entry of wal.listAfter(meta.sequence)){if(entry.databaseId!==meta.databaseId||entry.baseSequence!==meta.sequence)throw corrupt('WAL identity or sequence mismatch');verifyEntryFabric(meta,entry);await applyEvents(state,entry.events);for(const e of entry.events){meta.recordVersions[keyOf(e.collection,e.id)]=e.sequence;meta.collectionVersions[e.collection]=e.sequence;if(!meta.collections.includes(e.collection))meta.collections.push(e.collection);meta.changes.push(clone(e));if(e.sourceChangeId)meta.appliedChangeIds=bounded([...meta.appliedChangeIds,e.sourceChangeId],retention*2);meta.lastCommitId=e.commitId??meta.lastCommitId;}meta.sequence=entry.resultSequence;accepted.push({baseSequence:entry.baseSequence,resultSequence:entry.resultSequence,digest:entry.digest});count++;}meta.collections.sort();trimChanges(meta,retention);return{count,accepted};}
function verifyEntryFabric(meta,entry){const groups=new Map();for(const e of entry.events)if(e.commitId){const a=groups.get(e.commitId)??[];a.push(e);groups.set(e.commitId,a);}for(const events of groups.values()){const e=events[0],mutations=events.map(x=>({type:x.type==='remove'?'remove':'put',collection:x.collection,id:x.id,fields:x.fields??['*'],...(x.record?{record:x.record}:{})}));const fabric={version:1,databaseId:meta.databaseId,partitionId:e.partitionId??'local',sequence:entry.resultSequence,logicalTime:e.logicalTime,transactionId:e.transactionId??null,origin:e.originDatabaseId??'recovery',mutations,schemaVersion:1,policyVersion:1,causalParents:e.causalParents??[],commitId:e.commitId,checksum:e.commitChecksum};if(!verifyCommitFabric(fabric))throw corrupt('commit fabric mismatch during recovery');}}
async function applyEvents(state,events){const groups=new Map();for(const e of events){const a=groups.get(e.collection)??[];a.push(e.type==='remove'?{type:'remove',id:e.id}:{type:'put',record:recordValue(e.record)});groups.set(e.collection,a);}for(const[name,ops]of groups){const store=await state.collection(name);await store.applyBatch(ops);}}
async function importLegacy(state,meta,legacy){meta.databaseId=legacy.databaseId??meta.databaseId;meta.sequence=legacy.sequence??0;meta.changes=(legacy.changes??[]).map(clone);meta.appliedChangeIds=[...(legacy.appliedChangeIds??[])];for(const[name,records]of Object.entries(legacy.collections??{})){const store=await state.collection(name),ops=Object.values(records).map(r=>({type:'put',record:recordValue(r)}));if(ops.length)await store.applyBatch(ops);meta.collections.push(name);meta.collectionVersions[name]=meta.sequence;for(const r of Object.values(records))meta.recordVersions[keyOf(name,r.id)]=meta.sequence;}meta.collections=[...new Set(meta.collections)].sort();}

function normalizeOptions(o){const changeRetention=o.changeRetention??10000,checkpointEvery=o.checkpointEvery??256;if(!Number.isSafeInteger(changeRetention)||changeRetention<1||!Number.isSafeInteger(checkpointEvery)||checkpointEvery<1)throw new TypeError('invalid storage options');return{changeRetention,checkpointEvery,storage:o.storage??{},scheduler:o.scheduler,schedulerOptions:o.schedulerOptions??{},reactiveOptions:o.reactiveOptions??{}};}
function freshMeta(id=crypto.randomUUID()){return{format:FORMAT,version:1,databaseId:id,sequence:0,lastCommitId:null,collections:[],changes:[],appliedChangeIds:[],recordVersions:{},collectionVersions:{}};}
function isMeta(v){return v?.format===FORMAT&&v?.version===1;}
function normalizeMeta(v){if(typeof v.databaseId!=='string'||!Number.isSafeInteger(v.sequence))throw corrupt('invalid storage checkpoint');return{...freshMeta(v.databaseId),...clone(v),collections:[...new Set(v.collections??[])].sort(),recordVersions:{...(v.recordVersions??{})},collectionVersions:{...(v.collectionVersions??{})}};}
async function readCheckpoint(file){try{const v=JSON.parse(await fs.readFile(file,'utf8'));return{value:v,source:'primary',sequence:v.sequence??0};}catch(primary){if(primary.code==='ENOENT')return{value:null,source:'new',sequence:0};try{const v=JSON.parse(await fs.readFile(`${file}.bak`,'utf8'));return{value:v,source:'backup',sequence:v.sequence??0};}catch(backup){const e=corrupt('checkpoint and backup unreadable');e.cause={primary,backup};throw e;}}}
async function digestState(state,names){const h=crypto.createHash('sha256');for(const n of [...names].sort()){h.update(n);const s=await state.collection(n);for(const id of s.ids()){h.update(id);h.update(JSON.stringify(s.getSync(id)));}}return h.digest('hex');}
async function atomicJson(file,value){await fs.mkdir(path.dirname(file),{recursive:true});const tmp=`${file}.${crypto.randomUUID()}.tmp`;try{await fs.writeFile(tmp,`${JSON.stringify(value)}\n`,{mode:0o600});const h=await fs.open(tmp,'r');try{await h.sync();}finally{await h.close();}await fs.rename(tmp,file);const d=await fs.open(path.dirname(file),'r');try{await d.sync();}finally{await d.close();}}finally{await fs.rm(tmp,{force:true}).catch(()=>undefined);}}
function trimChanges(meta,n){if(meta.changes.length>n)meta.changes.splice(0,meta.changes.length-n);}
function bounded(v,n){return[...new Set(v)].slice(-n);}
function commitCost(ops){const bytes=ops.reduce((n,o)=>n+Buffer.byteLength(JSON.stringify(o.after??o.before??{})),0);return{cpu:ops.length,memory:Math.min(bytes,8*1024*1024),ssdIo:bytes,coordination:1,recoveryRisk:1,latency:1};}
function changedFields(a,b){if(!a||!b)return['*'];const keys=new Set([...Object.keys(a),...Object.keys(b)]),out=[...keys].filter(k=>JSON.stringify(a[k])!==JSON.stringify(b[k]));return out.length?out:['*'];}
function keyOf(c,id){return`${c}\u0000${id}`;}
function recordValue(v){if(!v||typeof v!=='object'||Array.isArray(v)||typeof v.id!=='string'||!v.id)throw new TypeError('record requires id');const text=JSON.stringify(v);if(text===undefined)throw new TypeError('record not JSON serializable');return JSON.parse(text);}
function validateCollection(v){if(typeof v!=='string'||!/^[A-Za-z0-9_-]+$/.test(v)||v.length>128)throw new TypeError('invalid collection');}
function validateId(v){if(typeof v!=='string'||!v||v.length>512)throw new TypeError('invalid id');}
function sequence(v){if(!Number.isSafeInteger(v)||v<0)throw new TypeError('invalid sequence');}
function validateSnapshot(s){if(!s||typeof s.databaseId!=='string'||!Number.isSafeInteger(s.sequence)||!s.collections||typeof s.collections!=='object')throw new TypeError('invalid snapshot');}
function precondition(p,r){if(p.exists===true&&!r)return false;if(p.exists===false&&r)return false;if(p.equals!==undefined&&JSON.stringify(p.equals)!==JSON.stringify(r))return false;return true;}
function applyIntent(tx,m){if(!m||!m.collection)throw new TypeError('invalid intent mutation');const c=tx.collection(m.collection);if(m.type==='remove')return c.remove(m.id);if(['put','upsert','insert'].includes(m.type))return c.put(m.record);throw new TypeError('unsupported intent mutation');}
function duplicate(id){const e=codeError('SYNCIO_DUPLICATE_ID',`duplicate id ${id}`);e.statusCode=409;return e;}
function conflict(m){const e=codeError('SYNCIO_TRANSACTION_CONFLICT',m);e.statusCode=409;return e;}
function corrupt(m){return codeError('SYNCIO_CORRUPT_DATABASE',m);}
function busy(d){const e=codeError('SYNCIO_RESOURCE_BUSY','resource admission refused');e.statusCode=503;e.details=d;return e;}
function streamExpired(d){const e=codeError('SYNCIO_STREAM_RESUME_EXPIRED','resume cursor expired');e.details=d;return e;}
function codeError(code,message){const e=new Error(message);e.code=code;return e;}
