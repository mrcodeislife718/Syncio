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

const FORMAT='syncio-storage-core/2';
const clone=(value)=>structuredClone(value);

export class StorageBackedSyncioDatabase {
  constructor(file,meta,wal,state,intents,options,recovery){
    this.file=file;this.meta=meta;this.wal=wal;this.state=state;this.intents=intents;
    this.changeRetention=options.changeRetention;this.checkpointEvery=options.checkpointEvery;this.maxSyncViews=options.maxSyncViews;
    this.scheduler=options.scheduler??new AdaptiveScheduler(options.schedulerOptions);this.reactive=new ReactiveQueryGraph(options.reactiveOptions);
    this.events=new EventEmitter();this.events.setMaxListeners(0);this.queue=Promise.resolve();this.commitsSinceCheckpoint=0;
    this.degraded=false;this.recoveryRequired=false;this.checkpointError=null;this.recovery=recovery;this.syncViews=new Map(Object.entries(meta.syncViews??{}));
    this.txMetrics={commits:0,conflicts:0,retries:0,aborts:0};
  }

  static async open(file,options={}){
    const target=path.resolve(file),cfg=normalizeOptions(options),loaded=await readCheckpoint(target);
    const legacy=loaded.value&&!isMeta(loaded.value)?loaded.value:null;
    const meta=isMeta(loaded.value)?normalizeMeta(loaded.value):freshMeta(legacy?.databaseId);
    const state=await TieredStatePlane.open(`${target}.segments`,cfg.storage);
    if(legacy)await importLegacy(state,meta,legacy);
    for(const name of meta.collections)await state.collection(name);
    const wal=await WriteAheadLog.open(`${target}.wal`),intents=await DurableIntentLog.open(`${target}.intents.json`);
    const replay=await replayWal(state,meta,wal,cfg.changeRetention);let recovery=null;
    if(legacy||loaded.source==='backup'||replay.count){
      recovery=createRecoveryManifest({failure:legacy?'legacy_state_migration':loaded.source==='backup'?'primary_checkpoint_unavailable':'wal_replay',lastDurableCommit:meta.lastCommitId,checkpoint:{source:loaded.source,sequence:loaded.sequence},walAccepted:replay.accepted,walRejected:[],invariants:['wal_contiguous','segment_state_authoritative','commit_fabric_verified','occ_versions_on_disk'],finalSequence:meta.sequence,stateDigest:await digestState(state,meta.collections)});
      await atomicJson(`${target}.recovery.json`,recovery);
    }
    const db=new StorageBackedSyncioDatabase(target,meta,wal,state,intents,cfg,recovery);
    if(!loaded.value||legacy||loaded.source==='backup'||replay.count||loaded.value?.format!==FORMAT)await db.checkpointNow();
    return db;
  }

  get databaseId(){return this.meta.databaseId;}
  get sequence(){return this.meta.sequence;}
  consistency(profile=CONSISTENCY.SERIALIZABLE,options={}){return consistencyContract(profile,options);}
  storageStatus(){const collections={};for(const name of this.meta.collections){const store=this.state.getOpened(name);if(store)collections[name]=store.stats();}return{mode:'commit-fabric-segmented-mvcc',authoritativeState:'ssd-segments',fullStateResidentInRam:false,recordVersionState:'offset-index',sequence:this.sequence,walEntries:this.wal.listAfter(0).length,checkpointEvery:this.checkpointEvery,commitsSinceCheckpoint:this.commitsSinceCheckpoint,degraded:this.degraded,recoveryRequired:this.recoveryRequired,checkpointError:this.checkpointError,transactions:{...this.txMetrics},scheduler:this.scheduler.snapshot(),collections};}

  collection(name){validateCollection(name);const db=this;return Object.freeze({
    async insert(value){const record=recordValue({...value,id:value?.id??crypto.randomUUID()});await db.transaction(tx=>{const c=tx.collection(name);if(c.get(record.id))throw duplicate(record.id);c.put(record);});return clone(record);},
    async upsert(value){const record=recordValue(value);await db.transaction(tx=>tx.collection(name).put(record));return clone(record);},
    get:id=>db.readRecord(name,id),all:()=>db.readAll(name),scan:()=>db.scan(name),query:(spec={})=>db.query(name,spec),
    async remove(id){let removed=false;await db.transaction(tx=>{removed=tx.collection(name).remove(id);});return removed;},
    watch:listener=>db.watchChanges({collection:name,after:db.sequence},listener)
  });}
  readRecord(collection,id){validateCollection(collection);validateId(id);return this.state.getOpened(collection)?.getSync(id)??null;}
  readRecordVersion(collection,id){validateCollection(collection);validateId(id);return this.state.getOpened(collection)?.version(id)??0;}
  readAll(collection){validateCollection(collection);return this.state.getOpened(collection)?.allSync()??[];}
  scan(collection){validateCollection(collection);return this.state.getOpened(collection)?.scanSync()??[][Symbol.iterator]();}
  query(collection,spec={},options={}){this.consistency(options.consistency??CONSISTENCY.SERIALIZABLE,options);return queryRecords(this.readAll(collection),spec);}

  async transaction(work,{maxRetries=8,origin='local',causalParents=[],partitionId='local',sourceChangeId=null}={}){
    if(this.recoveryRequired)throw recoveryNeeded();if(typeof work!=='function')throw new TypeError('transaction requires function');if(!Number.isSafeInteger(maxRetries)||maxRetries<0||maxRetries>100)throw new TypeError('invalid transaction retry limit');
    for(let attempt=0;attempt<=maxRetries;attempt++){
      const tx=new WriteSetTransaction(this);let result;try{result=await work(tx.api());}catch(error){this.txMetrics.aborts++;throw error;}if(!tx.hasWrites())return result;
      try{await this.enqueue(()=>this.commitTransaction(tx,{origin,causalParents,partitionId,sourceChangeId}));this.txMetrics.commits++;return result;}catch(error){if(error.code!=='SYNCIO_TRANSACTION_CONFLICT'||attempt===maxRetries){if(error.code==='SYNCIO_TRANSACTION_CONFLICT')this.txMetrics.conflicts++;throw error;}this.txMetrics.conflicts++;this.txMetrics.retries++;await new Promise(resolve=>setTimeout(resolve,Math.min(16,1<<Math.min(attempt,4))));}
    }
    throw conflict('transaction retries exhausted');
  }

  async commitTransaction(tx,{origin,causalParents,partitionId,sourceChangeId}){
    if(this.recoveryRequired)throw recoveryNeeded();tx.validate();const ops=tx.operations();if(!ops.length){if(sourceChangeId)await this.persistAppliedChangeIdUnsafe(sourceChangeId);return null;}
    const admission=this.scheduler.admit({priority:PRIORITY.foreground,cost:commitCost(ops)});if(admission.decision!=='admit')throw busy(admission);
    try{
      const baseSequence=this.sequence,resultSequence=baseSequence+ops.length,transactionId=crypto.randomUUID();
      const mutations=ops.map(op=>({type:op.after?'put':'remove',collection:op.collection,id:op.id,fields:changedFields(op.before,op.after),...(op.after?{record:clone(op.after)}:{})}));
      const fabric=createCommitFabric({databaseId:this.databaseId,partitionId,sequence:resultSequence,transactionId,origin,mutations,causalParents});if(!verifyCommitFabric(fabric))throw corrupt('commit fabric verification failed');
      const events=ops.map((op,index)=>({type:op.after?(op.before?'upsert':'insert'):'remove',collection:op.collection,id:op.id,...(op.after?{record:clone(op.after)}:{}),...(op.before?{before:clone(op.before)}:{}),fields:mutations[index].fields,changeId:index===0&&sourceChangeId?sourceChangeId:crypto.randomUUID(),sourceChangeId:index===0?sourceChangeId:null,originDatabaseId:this.databaseId,commitOrigin:fabric.origin,sequence:baseSequence+index+1,at:new Date(fabric.logicalTime).toISOString(),commitId:fabric.commitId,commitChecksum:fabric.checksum,transactionId,partitionId,logicalTime:fabric.logicalTime,schemaVersion:fabric.schemaVersion,policyVersion:fabric.policyVersion,causalParents:[...fabric.causalParents] }));
      await this.wal.append({databaseId:this.databaseId,baseSequence,resultSequence,events});
      try{await applyEvents(this.state,events);}catch(error){this.degraded=true;this.recoveryRequired=true;const durable=codeError('SYNCIO_DURABLE_COMMIT_RESTART_REQUIRED','commit is durable in WAL but state application failed');durable.cause=error;durable.statusCode=503;throw durable;}
      this.meta.sequence=resultSequence;this.meta.lastCommitId=fabric.commitId;
      for(const event of events){this.meta.collectionVersions[event.collection]=event.sequence;if(!this.meta.collections.includes(event.collection))this.meta.collections.push(event.collection);this.meta.changes.push(clone(event));if(event.sourceChangeId)this.meta.appliedChangeIds=bounded([...this.meta.appliedChangeIds,event.sourceChangeId],this.changeRetention*2);}
      this.meta.collections.sort();trimChanges(this.meta,this.changeRetention);this.commitsSinceCheckpoint++;
      for(const event of events)this.events.emit('change',clone(event));this.events.emit('commit',fabric);if(this.commitsSinceCheckpoint>=this.checkpointEvery)this.scheduleCheckpoint();return fabric;
    }finally{admission.release?.();}
  }

  async persistAppliedChangeIdUnsafe(changeId){if(this.meta.appliedChangeIds.includes(changeId))return;this.meta.appliedChangeIds=bounded([...this.meta.appliedChangeIds,changeId],this.changeRetention*2);await this.persistCheckpoint();}
  changesSince(cursor=0,{limit=Infinity}={}){sequence(cursor);return this.meta.changes.filter(event=>event.sequence>cursor).slice(0,limit).map(clone);}
  resumeStatus(cursor=0){sequence(cursor);const oldest=this.meta.changes[0]?.sequence??this.sequence+1;return{resumable:cursor===this.sequence||cursor>=oldest-1,cursor,oldestRetained:oldest,sequence:this.sequence};}
  watchChanges({collection=null,after=this.sequence}={},listener){if(typeof listener!=='function')throw new TypeError('listener required');const status=this.resumeStatus(after);if(!status.resumable)throw streamExpired(status);let cursor=after;for(const event of this.changesSince(after)){cursor=event.sequence;if(!collection||event.collection===collection)listener(clone(event));}const handler=event=>{if(event.sequence<=cursor)return;cursor=event.sequence;if(!collection||event.collection===collection)listener(clone(event));};this.events.on('change',handler);return()=>this.events.off('change',handler);}
  watchCommits(listener){if(typeof listener!=='function')throw new TypeError('listener required');this.events.on('commit',listener);return()=>this.events.off('commit',listener);}

  subscribeQuery(collection,spec,listener,{id=crypto.randomUUID(),emitInitial=true}={}){validateCollection(collection);if(typeof listener!=='function')throw new TypeError('listener required');const evaluate=()=>this.query(collection,spec),initial=evaluate();const unregister=this.reactive.register(id,{collection,query:spec?.where??spec??{},evaluate,initial});let delivery=Promise.resolve();const onCommit=commit=>{delivery=delivery.then(async()=>{for(const result of await this.reactive.applyCommit(commit))if(result.queryId===id)listener({type:'query_result',queryId:id,commitId:commit.commitId,sequence:commit.sequence,value:clone(result.value)});}).catch(()=>undefined);};this.events.on('commit',onCommit);if(emitInitial)queueMicrotask(()=>listener({type:'query_result',queryId:id,commitId:this.meta.lastCommitId,sequence:this.sequence,initial:true,value:clone(initial)}));return()=>{this.events.off('commit',onCommit);unregister();};}

  async defineSyncView(name,{collection,where={},maxDocuments=100000}={}){if(typeof name!=='string'||!/^[A-Za-z0-9_.-]+$/.test(name)||name.length>128)throw new TypeError('invalid sync view name');validateCollection(collection);if(!Number.isSafeInteger(maxDocuments)||maxDocuments<1)throw new TypeError('invalid maxDocuments');if(!this.syncViews.has(name)&&this.syncViews.size>=this.maxSyncViews)throw codeError('SYNCIO_RESOURCE_LIMIT','sync view definition limit exceeded');const view={name,collection,where:clone(where),maxDocuments};this.syncViews.set(name,view);this.meta.syncViews[name]=clone(view);await this.enqueue(()=>this.persistCheckpoint());return clone(view);}
  async dropSyncView(name){return this.enqueue(async()=>{const removed=this.syncViews.delete(name);delete this.meta.syncViews[name];if(removed)await this.persistCheckpoint();return removed;});}
  listSyncViews(){return[...this.syncViews.values()].map(clone);}
  materializeSyncView(name){const view=this.syncViews.get(name);if(!view)throw codeError('SYNCIO_SYNC_VIEW_NOT_FOUND','sync view not found');const records=queryRecords(this.readAll(view.collection),{where:view.where});if(records.length>view.maxDocuments)throw codeError('SYNCIO_RESOURCE_LIMIT','sync view expansion exceeds limit');return{name,collection:view.collection,sequence:this.sequence,records:clone(records)};}
  syncViewChanges(name,after=0){const view=this.syncViews.get(name);if(!view)throw codeError('SYNCIO_SYNC_VIEW_NOT_FOUND','sync view not found');const status=this.resumeStatus(after);if(!status.resumable)throw streamExpired(status);const changes=[];for(const event of this.changesSince(after)){if(event.collection!==view.collection)continue;const beforeMatches=event.before?matchesWhere(event.before,view.where):false,afterMatches=event.record?matchesWhere(event.record,view.where):false;if(!beforeMatches&&!afterMatches)continue;changes.push({sequence:event.sequence,commitId:event.commitId,type:afterMatches?'put':'remove',id:event.id,...(afterMatches?{record:clone(event.record)}:{})});}return{name,after,sequence:this.sequence,changes};}

  enqueueOfflineIntent(intent){return this.intents.enqueue(intent);}listOfflineIntents(){return this.intents.list();}
  reconcileOfflineIntents(){return this.intents.reconcile(async intent=>{await this.transaction(tx=>{for(const condition of intent.preconditions??[]){const current=tx.collection(condition.collection).get(condition.id);if(!precondition(condition,current)&&intent.conflictPolicy!=='overwrite')throw codeError('SYNCIO_INTENT_CONFLICT','offline intent precondition failed');}for(const mutation of intent.mutations??[])applyIntent(tx,mutation);});return{status:'committed'};});}

  hasAppliedChange(id){return this.meta.appliedChangeIds.includes(id);}
  async applyReplicationChange(change,resolver=(_local,remote)=>remote){if(!change?.changeId||!change.collection)throw new TypeError('replication change identity required');if(this.hasAppliedChange(change.changeId))return{applied:false,duplicate:true};let resolved=null;await this.transaction(tx=>{const collection=tx.collection(change.collection);if(change.type==='remove'){collection.remove(change.id);return;}const incoming=recordValue(change.record);resolved=resolver(collection.get(incoming.id),incoming);if(resolved)collection.put(recordValue(resolved));},{origin:change.originDatabaseId??'replication',causalParents:change.commitId?[change.commitId]:[],sourceChangeId:change.changeId});if(!this.hasAppliedChange(change.changeId))await this.enqueue(()=>this.persistAppliedChangeIdUnsafe(change.changeId));return{applied:true,duplicate:false,record:resolved?clone(resolved):null};}

  snapshot(){const collections={};for(const name of this.meta.collections)collections[name]=Object.fromEntries(this.readAll(name).map(record=>[record.id,record]));return{version:1,databaseId:this.databaseId,sequence:this.sequence,collections,changes:this.meta.changes.map(clone),appliedChangeIds:[...this.meta.appliedChangeIds]};}
  async replaceState(snapshot){validateSnapshot(snapshot);return this.enqueue(async()=>{for(const name of new Set([...this.meta.collections,...Object.keys(snapshot.collections)])){const store=await this.openStore(name),desired=snapshot.collections[name]??{},operations=[];for(const id of store.ids())if(!Object.hasOwn(desired,id))operations.push({type:'remove',id});for(const record of Object.values(desired))operations.push({type:'put',record:recordValue(record)});if(operations.length)await store.applyBatch(operations);}this.meta.sequence=snapshot.sequence;this.meta.collections=Object.keys(snapshot.collections).sort();this.meta.changes=(snapshot.changes??[]).slice(-this.changeRetention).map(clone);this.meta.appliedChangeIds=bounded(snapshot.appliedChangeIds??[],this.changeRetention*2);this.meta.collectionVersions=Object.fromEntries(this.meta.collections.map(name=>[name,snapshot.sequence]));await this.wal.reset();await this.persistCheckpoint();return true;});}

  async openStore(name){validateCollection(name);if(!this.meta.collections.includes(name)){this.meta.collections.push(name);this.meta.collections.sort();}return this.state.collection(name);}
  enqueue(work){const operation=this.queue.then(work);this.queue=operation.catch(()=>undefined);return operation;}
  checkpoint(){return this.checkpointNow();}
  checkpointNow(){return this.enqueue(async()=>{const checkpointSequence=this.sequence;await this.persistCheckpoint();await this.wal.compactThrough(checkpointSequence);this.commitsSinceCheckpoint=0;this.degraded=false;this.checkpointError=null;return checkpointSequence;});}
  async persistCheckpoint(){const value={...clone(this.meta),format:FORMAT,version:2};delete value.recordVersions;await atomicJson(this.file,value);await atomicJson(`${this.file}.bak`,value);}
  scheduleCheckpoint(){queueMicrotask(()=>this.enqueue(async()=>{if(this.commitsSinceCheckpoint<this.checkpointEvery)return;try{const checkpointSequence=this.sequence;await this.persistCheckpoint();await this.wal.compactThrough(checkpointSequence);this.commitsSinceCheckpoint=Math.max(0,this.sequence-checkpointSequence);this.degraded=false;this.checkpointError=null;}catch(error){this.degraded=true;this.checkpointError=error.code??error.message;}}));}
  lastRecoveryManifest(){return this.recovery?clone(this.recovery):null;}
  async close(){await this.queue;if(this.commitsSinceCheckpoint>0)await this.checkpointNow();await this.intents.close();await this.state.close();await this.wal.close();this.events.removeAllListeners();}
}

export class WriteSetTransaction {
  constructor(db){this.db=db;this.reads=new Map();this.scans=new Map();this.writes=new Map();}
  api(){const tx=this;return Object.freeze({readVersion:this.db.sequence,collection:name=>tx.collection(name)});}
  collection(name){validateCollection(name);const tx=this;return Object.freeze({
    get(id){validateId(id);const key=keyOf(name,id);if(tx.writes.has(key))return clone(tx.writes.get(key).after);const value=tx.db.readRecord(name,id);tx.reads.set(key,{collection:name,id,version:tx.db.readRecordVersion(name,id)});return clone(value);},
    put(record){const value=recordValue(record),key=keyOf(name,value.id);if(!tx.reads.has(key))tx.reads.set(key,{collection:name,id:value.id,version:tx.db.readRecordVersion(name,value.id)});const prior=tx.writes.has(key)?tx.writes.get(key).before:tx.db.readRecord(name,value.id);tx.writes.set(key,{collection:name,id:value.id,before:clone(prior),after:clone(value)});},
    remove(id){validateId(id);const key=keyOf(name,id);if(!tx.reads.has(key))tx.reads.set(key,{collection:name,id,version:tx.db.readRecordVersion(name,id)});const current=tx.writes.has(key)?tx.writes.get(key).after:tx.db.readRecord(name,id);if(!current)return false;const before=tx.writes.has(key)?tx.writes.get(key).before:current;tx.writes.set(key,{collection:name,id,before:clone(before),after:null});return true;},
    all(){tx.scans.set(name,tx.db.meta.collectionVersions[name]??0);const records=new Map(tx.db.readAll(name).map(record=>[record.id,record]));for(const write of tx.writes.values()){if(write.collection!==name)continue;if(write.after)records.set(write.id,clone(write.after));else records.delete(write.id);}return[...records.values()].map(clone);}
  });}
  hasWrites(){return this.writes.size>0;}operations(){return[...this.writes.values()].filter(write=>JSON.stringify(write.before)!==JSON.stringify(write.after));}
  validate(){for(const read of this.reads.values())if(this.db.readRecordVersion(read.collection,read.id)!==read.version)throw conflict('record changed during transaction');for(const[name,version]of this.scans)if((this.db.meta.collectionVersions[name]??0)!==version)throw conflict('collection changed during transaction scan');}
}

async function replayWal(state,meta,wal,retention){let count=0;const accepted=[];for(const entry of wal.listAfter(meta.sequence)){if(entry.databaseId!==meta.databaseId||entry.baseSequence!==meta.sequence)throw corrupt('WAL identity or sequence mismatch');verifyEntryFabric(meta,entry);await applyEvents(state,entry.events);for(const event of entry.events){meta.collectionVersions[event.collection]=event.sequence;if(!meta.collections.includes(event.collection))meta.collections.push(event.collection);meta.changes.push(clone(event));if(event.sourceChangeId)meta.appliedChangeIds=bounded([...meta.appliedChangeIds,event.sourceChangeId],retention*2);meta.lastCommitId=event.commitId??meta.lastCommitId;}meta.sequence=entry.resultSequence;accepted.push({baseSequence:entry.baseSequence,resultSequence:entry.resultSequence,digest:entry.digest});count++;}meta.collections.sort();trimChanges(meta,retention);return{count,accepted};}
function verifyEntryFabric(meta,entry){const groups=new Map();for(const event of entry.events){if(!event.commitId)continue;const group=groups.get(event.commitId)??[];group.push(event);groups.set(event.commitId,group);}for(const events of groups.values()){const first=events[0],mutations=events.map(event=>({type:event.type==='remove'?'remove':'put',collection:event.collection,id:event.id,fields:event.fields??['*'],...(event.record?{record:event.record}:{})}));const fabric={version:1,databaseId:meta.databaseId,partitionId:first.partitionId??'local',sequence:entry.resultSequence,logicalTime:first.logicalTime,transactionId:first.transactionId??null,origin:first.commitOrigin??first.originDatabaseId??'recovery',mutations,schemaVersion:first.schemaVersion??1,policyVersion:first.policyVersion??1,causalParents:first.causalParents??[],commitId:first.commitId,checksum:first.commitChecksum};if(!verifyCommitFabric(fabric))throw corrupt('commit fabric mismatch during recovery');}}
async function applyEvents(state,events){const groups=new Map();for(const event of events){const operations=groups.get(event.collection)??[];operations.push(event.type==='remove'?{type:'remove',id:event.id}:{type:'put',record:recordValue(event.record)});groups.set(event.collection,operations);}for(const[name,operations]of groups){const store=await state.collection(name);await store.applyBatch(operations);}}
async function importLegacy(state,meta,legacy){meta.databaseId=legacy.databaseId??meta.databaseId;meta.sequence=legacy.sequence??0;meta.changes=(legacy.changes??[]).map(clone);meta.appliedChangeIds=[...(legacy.appliedChangeIds??[])];for(const[name,records]of Object.entries(legacy.collections??{})){const store=await state.collection(name),operations=Object.values(records).map(record=>({type:'put',record:recordValue(record)}));if(operations.length)await store.applyBatch(operations);meta.collections.push(name);meta.collectionVersions[name]=meta.sequence;}meta.collections=[...new Set(meta.collections)].sort();}
function normalizeOptions(options){const changeRetention=options.changeRetention??10000,checkpointEvery=options.checkpointEvery??256,maxSyncViews=options.maxSyncViews??1024;if(!Number.isSafeInteger(changeRetention)||changeRetention<1||!Number.isSafeInteger(checkpointEvery)||checkpointEvery<1||!Number.isSafeInteger(maxSyncViews)||maxSyncViews<1)throw new TypeError('invalid storage options');return{changeRetention,checkpointEvery,maxSyncViews,storage:options.storage??{},scheduler:options.scheduler,schedulerOptions:options.schedulerOptions??{},reactiveOptions:options.reactiveOptions??{}};}
function freshMeta(databaseId=crypto.randomUUID()){return{format:FORMAT,version:2,databaseId,sequence:0,lastCommitId:null,collections:[],changes:[],appliedChangeIds:[],collectionVersions:{},syncViews:{}};}
function isMeta(value){return value?.format?.startsWith('syncio-storage-core/')&&[1,2].includes(value?.version);}
function normalizeMeta(value){if(typeof value.databaseId!=='string'||!Number.isSafeInteger(value.sequence))throw corrupt('invalid storage checkpoint');return{...freshMeta(value.databaseId),...clone(value),format:FORMAT,version:2,collections:[...new Set(value.collections??[])].sort(),collectionVersions:{...(value.collectionVersions??{})},syncViews:{...(value.syncViews??{})},recordVersions:undefined};}
async function readCheckpoint(file){try{const value=JSON.parse(await fs.readFile(file,'utf8'));return{value,source:'primary',sequence:value.sequence??0};}catch(primary){if(primary.code==='ENOENT')return{value:null,source:'new',sequence:0};try{const value=JSON.parse(await fs.readFile(`${file}.bak`,'utf8'));return{value,source:'backup',sequence:value.sequence??0};}catch(backup){const error=corrupt('checkpoint and backup unreadable');error.cause={primary,backup};throw error;}}}
async function digestState(state,names){const hash=crypto.createHash('sha256');for(const name of [...names].sort()){hash.update(name);const store=await state.collection(name);for(const id of store.ids()){hash.update(id);hash.update(JSON.stringify(store.getSync(id)));}}return hash.digest('hex');}
async function atomicJson(file,value){await fs.mkdir(path.dirname(file),{recursive:true});const temp=`${file}.${crypto.randomUUID()}.tmp`;try{await fs.writeFile(temp,`${JSON.stringify(value)}\n`,{mode:0o600});const handle=await fs.open(temp,'r');try{await handle.sync();}finally{await handle.close();}await fs.rename(temp,file);const directory=await fs.open(path.dirname(file),'r');try{await directory.sync();}finally{await directory.close();}}finally{await fs.rm(temp,{force:true}).catch(()=>undefined);}}
function trimChanges(meta,count){if(meta.changes.length>count)meta.changes.splice(0,meta.changes.length-count);}function bounded(values,count){return[...new Set(values)].slice(-count);}
function commitCost(ops){const bytes=ops.reduce((sum,op)=>sum+Buffer.byteLength(JSON.stringify(op.after??op.before??{})),0);return{cpu:ops.length,memory:Math.min(bytes,8*1024*1024),ssdIo:bytes,coordination:1,recoveryRisk:1,latency:1};}
function changedFields(before,after){if(!before||!after)return['*'];const keys=new Set([...Object.keys(before),...Object.keys(after)]),changed=[...keys].filter(key=>JSON.stringify(before[key])!==JSON.stringify(after[key]));return changed.length?changed:['*'];}
function keyOf(collection,id){return`${collection}\u0000${id}`;}function recordValue(value){if(!value||typeof value!=='object'||Array.isArray(value)||typeof value.id!=='string'||!value.id)throw new TypeError('record requires id');const text=JSON.stringify(value);if(text===undefined)throw new TypeError('record not JSON serializable');return JSON.parse(text);}
function validateCollection(value){if(typeof value!=='string'||!/^[A-Za-z0-9_-]+$/.test(value)||value.length>128)throw new TypeError('invalid collection');}function validateId(value){if(typeof value!=='string'||!value||value.length>512)throw new TypeError('invalid id');}function sequence(value){if(!Number.isSafeInteger(value)||value<0)throw new TypeError('invalid sequence');}
function validateSnapshot(value){if(!value||typeof value.databaseId!=='string'||!Number.isSafeInteger(value.sequence)||!value.collections||typeof value.collections!=='object')throw new TypeError('invalid snapshot');}
function precondition(condition,record){if(condition.exists===true&&!record)return false;if(condition.exists===false&&record)return false;if(condition.equals!==undefined&&JSON.stringify(condition.equals)!==JSON.stringify(record))return false;return true;}
function applyIntent(tx,mutation){if(!mutation?.collection)throw new TypeError('intent mutation is not executable by Syncio');const collection=tx.collection(mutation.collection);if(mutation.type==='remove')return collection.remove(mutation.id);if(['put','upsert','insert'].includes(mutation.type))return collection.put(mutation.record);throw new TypeError('unsupported intent mutation');}
function matchesWhere(record,where){return queryRecords([record],{where}).length===1;}function duplicate(id){const error=codeError('SYNCIO_DUPLICATE_ID',`duplicate id ${id}`);error.statusCode=409;return error;}function conflict(message){const error=codeError('SYNCIO_TRANSACTION_CONFLICT',message);error.statusCode=409;return error;}function corrupt(message){return codeError('SYNCIO_CORRUPT_DATABASE',message);}function busy(details){const error=codeError('SYNCIO_RESOURCE_BUSY','resource admission refused');error.statusCode=503;error.details=details;return error;}function recoveryNeeded(){const error=codeError('SYNCIO_RECOVERY_REQUIRED','database requires restart recovery before accepting more writes');error.statusCode=503;return error;}function streamExpired(details){const error=codeError('SYNCIO_STREAM_RESUME_EXPIRED','resume cursor expired');error.details=details;return error;}function codeError(code,message){const error=new Error(message);error.code=code;return error;}
