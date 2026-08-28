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
import { consistencyContract, CONSISTENCY } from './consistency.js';
import { createRecoveryManifest, verifyRecoveryManifest } from './recovery-manifest.js';

const FORMAT = 'syncio-superior/1';
const DEFAULT_CHANGE_RETENTION = 10_000;
const DEFAULT_CHECKPOINT_EVERY = 256;
const clone = value => structuredClone(value);

export class SuperiorSyncioDatabase {
  constructor(file, metadata, wal, statePlane, intentLog, options, recoveryManifest = null) {
    this.file = path.resolve(file);
    this.metadata = metadata;
    this.wal = wal;
    this.statePlane = statePlane;
    this.intentLog = intentLog;
    this.changeRetention = options.changeRetention;
    this.checkpointEvery = options.checkpointEvery;
    this.commitsSinceCheckpoint = options.commitsSinceCheckpoint ?? 0;
    this.scheduler = options.scheduler ?? new AdaptiveScheduler(options.schedulerOptions);
    this.reactive = new ReactiveQueryGraph(options.reactiveOptions);
    this.emitter = new EventEmitter();
    this.emitter.setMaxListeners(0);
    this.commitQueue = Promise.resolve();
    this.closed = false;
    this.degraded = false;
    this.lastCheckpointError = null;
    this.lastRecovery = recoveryManifest;
    this.syncViews = new Map();
  }

  static async open(file, options = {}) {
    const target = path.resolve(file);
    const normalized = normalizeOptions(options);
    const loaded = await readMetadataWithRecovery(target);
    const imported = loaded.metadata && !isSuperiorMetadata(loaded.metadata) ? loaded.metadata : null;
    const metadata = isSuperiorMetadata(loaded.metadata) ? normalizeMetadata(loaded.metadata) : freshMetadata(imported?.databaseId);
    const statePlane = await TieredStatePlane.open(`${target}.segments`, normalized.storage);

    if (imported) await importLegacyState(statePlane, metadata, imported);
    for (const name of metadata.collections) await statePlane.collection(name);

    const wal = await WriteAheadLog.open(`${target}.wal`);
    const intentLog = await DurableIntentLog.open(`${target}.intents.json`);
    const replayed = await replayDurableWal({ metadata, wal, statePlane, changeRetention: normalized.changeRetention });
    const recoveryNeeded = loaded.source === 'backup' || replayed > 0 || imported !== null;
    let recoveryManifest = null;
    if (recoveryNeeded) {
      recoveryManifest = createRecoveryManifest({
        failure: loaded.source === 'backup' ? 'primary_checkpoint_unavailable' : imported ? 'legacy_state_migration' : 'wal_replay',
        lastDurableCommit: metadata.lastCommitId,
        checkpoint: { source: loaded.source, sequence: loaded.checkpointSequence },
        walAccepted: wal.listAfter(loaded.checkpointSequence).map(entry => ({ baseSequence: entry.baseSequence, resultSequence: entry.resultSequence, digest: entry.digest })),
        walRejected: [],
        invariants: ['commit_fabric_verified','wal_contiguous','segmented_state_applied'],
        finalSequence: metadata.sequence,
        stateDigest: await digestState(statePlane, metadata.collections)
      });
      await atomicWriteJson(`${target}.recovery.json`, recoveryManifest);
    }

    const db = new SuperiorSyncioDatabase(target, metadata, wal, statePlane, intentLog, normalized, recoveryManifest);
    if (!loaded.metadata || imported || loaded.source === 'backup' || replayed > 0) await db.checkpointNow();
    return db;
  }

  get databaseId() { return this.metadata.databaseId; }
  get sequence() { return this.metadata.sequence; }

  storageStatus() {
    const collections = {};
    for (const name of this.metadata.collections) {
      const store = this.statePlane.getOpened(name);
      if (store) collections[name] = store.stats();
    }
    return Object.freeze({
      mode: 'commit-fabric-segmented-mvcc',
      authoritativeState: 'ssd-segments',
      fullStateResidentInRam: false,
      checkpointEvery: this.checkpointEvery,
      commitsSinceCheckpoint: this.commitsSinceCheckpoint,
      walEntries: this.wal.listAfter(0).length,
      degraded: this.degraded,
      checkpointError: this.lastCheckpointError,
      scheduler: this.scheduler.snapshot(),
      collections
    });
  }

  consistency(profile = CONSISTENCY.SERIALIZABLE, options = {}) { return consistencyContract(profile, options); }

  collection(name) {
    validateCollection(name);
    const db = this;
    return Object.freeze({
      async insert(value) {
        const record = normalizeRecord({ ...value, id: value?.id ?? crypto.randomUUID() });
        await db.transaction(tx => {
          const c = tx.collection(name);
          if (c.get(record.id)) throw duplicate(record.id);
          c.put(record);
        });
        return clone(record);
      },
      async upsert(value) {
        const record = normalizeRecord(value);
        await db.transaction(tx => tx.collection(name).put(record));
        return clone(record);
      },
      get(id) { return db.#get(name, id); },
      all() { return db.#all(name); },
      scan() { return db.#scan(name); },
      query(spec = {}) { return db.query(name, spec); },
      async remove(id) { let removed = false; await db.transaction(tx => { removed = tx.collection(name).remove(id); }); return removed; },
      watch(listener) { if (typeof listener !== 'function') throw new TypeError('watch requires listener'); return db.watchChanges({ collection: name, after: db.sequence }, listener); }
    });
  }

  query(collection, spec = {}, { consistency = CONSISTENCY.SERIALIZABLE } = {}) {
    this.consistency(consistency);
    return queryRecords(this.#all(collection), spec);
  }

  subscribeQuery(collection, spec, listener, { id = crypto.randomUUID(), emitInitial = true } = {}) {
    validateCollection(collection);
    if (typeof listener !== 'function') throw new TypeError('query subscription requires listener');
    const evaluate = () => this.query(collection, spec);
    let last = evaluate();
    const unregister = this.reactive.register(id, { collection, query: spec?.where ?? spec ?? {}, evaluate });
    const onCommit = async commit => {
      const results = await this.reactive.applyCommit(commit);
      const result = results.find(item => item.queryId === id);
      if (!result) return;
      last = clone(result.value);
      listener(Object.freeze({ type: 'query_result', queryId: id, commitId: commit.commitId, sequence: commit.sequence, value: clone(last) }));
    };
    this.emitter.on('commit', onCommit);
    if (emitInitial) queueMicrotask(() => listener(Object.freeze({ type: 'query_result', queryId: id, commitId: this.metadata.lastCommitId, sequence: this.sequence, initial: true, value: clone(last) })));
    return () => { this.emitter.off('commit', onCommit); unregister(); };
  }

  defineSyncView(name, { collection, where = {}, maxDocuments = 100_000 } = {}) {
    if (typeof name !== 'string' || !/^[A-Za-z0-9_.-]+$/.test(name) || name.length > 128) throw new TypeError('invalid sync view name');
    validateCollection(collection);
    if (!Number.isSafeInteger(maxDocuments) || maxDocuments < 1) throw new TypeError('maxDocuments must be positive');
    this.syncViews.set(name, Object.freeze({ name, collection, where: clone(where), maxDocuments }));
    return clone(this.syncViews.get(name));
  }

  materializeSyncView(name) {
    const view = this.syncViews.get(name);
    if (!view) throw syncError('SYNCIO_SYNC_VIEW_NOT_FOUND', 'sync view not found');
    const records = queryRecords(this.#all(view.collection), { where: view.where });
    if (records.length > view.maxDocuments) throw syncError('SYNCIO_RESOURCE_LIMIT', 'sync view expansion limit exceeded');
    return Object.freeze({ name, collection: view.collection, sequence: this.sequence, records: clone(records) });
  }

  async enqueueOfflineIntent(intent) { return this.intentLog.enqueue(intent); }

  listOfflineIntents() { return this.intentLog.list(); }

  async reconcileOfflineIntents() {
    return this.intentLog.reconcile(async intent => {
      let outcome = { status: 'committed' };
      await this.transaction(tx => {
        for (const condition of intent.preconditions ?? []) {
          const c = tx.collection(condition.collection);
          const record = c.get(condition.id);
          const valid = preconditionMatches(condition, record);
          if (!valid && intent.conflictPolicy !== 'overwrite') throw conflict('offline intent precondition failed');
        }
        applyIntentMutation(tx, intent.mutation);
      });
      return outcome;
    });
  }

  async transaction(work, { maxRetries = 8, origin = 'local', causalParents = [], partitionId = 'local' } = {}) {
    if (typeof work !== 'function') throw new TypeError('transaction requires a function');
    if (!Number.isSafeInteger(maxRetries) || maxRetries < 0 || maxRetries > 100) throw new TypeError('maxRetries must be between 0 and 100');
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      const tx = new MvccTransaction(this);
      const result = await work(tx.publicApi());
      if (!tx.hasWrites()) return result;
      try {
        await this.#enqueueCommit(() => this.#validateAndCommit(tx, { origin, causalParents, partitionId }));
        return result;
      } catch (error) {
        if (error.code !== 'SYNCIO_TRANSACTION_CONFLICT' || attempt === maxRetries) throw error;
      }
    }
    throw conflict('transaction retry budget exhausted');
  }

  changesSince(sequence = 0, { limit = Infinity } = {}) {
    assertSequence(sequence, 'change cursor');
    if (!(Number.isFinite(limit) || limit === Infinity) || limit < 0) throw new TypeError('limit must be non-negative');
    return this.metadata.changes.filter(change => change.sequence > sequence).slice(0, limit).map(clone);
  }

  resumeStatus(cursor = 0) {
    assertSequence(cursor, 'resume cursor');
    const oldest = this.metadata.changes[0]?.sequence ?? this.sequence + 1;
    return Object.freeze({ resumable: cursor === this.sequence || cursor >= oldest - 1, cursor, oldestRetained: oldest, sequence: this.sequence });
  }

  watchChanges({ collection = null, after = this.sequence } = {}, listener) {
    if (typeof listener !== 'function') throw new TypeError('watchChanges requires listener');
    const status = this.resumeStatus(after);
    if (!status.resumable) throw syncError('SYNCIO_STREAM_RESUME_EXPIRED', 'change cursor is outside retained history');
    let cursor = after;
    for (const event of this.changesSince(after)) {
      if (!collection || event.collection === collection) listener(clone(event));
      cursor = Math.max(cursor, event.sequence);
    }
    const handler = event => {
      if (event.sequence <= cursor) return;
      cursor = event.sequence;
      if (!collection || event.collection === collection) listener(clone(event));
    };
    this.emitter.on('change', handler);
    return () => this.emitter.off('change', handler);
  }

  watchCommits(listener) {
    if (typeof listener !== 'function') throw new TypeError('watchCommits requires listener');
    this.emitter.on('commit', listener);
    return () => this.emitter.off('commit', listener);
  }

  hasAppliedChange(changeId) { return this.metadata.appliedChangeIds.includes(changeId); }

  async applyReplicationChange(change, resolver = (_local, remote) => remote) {
    if (!change?.changeId || !change.collection) throw new TypeError('replication change requires changeId and collection');
    if (this.hasAppliedChange(change.changeId)) return { applied: false, duplicate: true };
    const result = await this.transaction(tx => {
      const c = tx.collection(change.collection);
      if (change.type === 'remove') { c.remove(change.id); return null; }
      const incoming = normalizeRecord(change.record);
      const resolved = resolver(c.get(incoming.id), incoming);
      if (resolved) c.put(normalizeRecord(resolved));
      return resolved;
    }, { origin: change.originDatabaseId ?? 'replication', causalParents: change.commitId ? [change.commitId] : [] });
    this.metadata.appliedChangeIds = boundedIds([...this.metadata.appliedChangeIds, change.changeId], this.changeRetention * 2);
    return { applied: true, duplicate: false, record: result ? clone(result) : null };
  }

  async snapshot() {
    const collections = {};
    for (const name of this.metadata.collections) collections[name] = Object.fromEntries(this.#all(name).map(record => [record.id, record]));
    return { version: 1, databaseId: this.databaseId, sequence: this.sequence, collections, changes: this.metadata.changes.map(clone), appliedChangeIds: [...this.metadata.appliedChangeIds] };
  }

  async replaceState(state) {
    validateReplacementState(state);
    return this.#enqueueCommit(async () => {
      for (const name of new Set([...this.metadata.collections, ...Object.keys(state.collections ?? {})])) {
        const store = await this.#store(name);
        const desired = state.collections?.[name] ?? {};
        const operations = [];
        for (const id of store.ids()) if (!Object.hasOwn(desired, id)) operations.push({ type: 'remove', id });
        for (const record of Object.values(desired)) operations.push({ type: 'put', record: normalizeRecord(record) });
        if (operations.length) await store.applyBatch(operations);
      }
      this.metadata.sequence = state.sequence;
      this.metadata.changes = (state.changes ?? []).slice(-this.changeRetention).map(clone);
      this.metadata.appliedChangeIds = boundedIds(state.appliedChangeIds ?? [], this.changeRetention * 2);
      this.metadata.collections = [...new Set(Object.keys(state.collections ?? {}))].sort();
      this.metadata.recordVersions = {};
      this.metadata.collectionVersions = Object.fromEntries(this.metadata.collections.map(name => [name, state.sequence]));
      for (const [name, records] of Object.entries(state.collections ?? {})) for (const id of Object.keys(records)) this.metadata.recordVersions[recordKey(name, id)] = state.sequence;
      await this.wal.reset();
      await this.#persistCheckpointPair();
      return true;
    });
  }

  async checkpoint() { return this.checkpointNow(); }

  async checkpointNow() {
    return this.#enqueueCommit(async () => {
      await this.#persistCheckpointPair();
      await this.wal.compactThrough(this.sequence);
      this.commitsSinceCheckpoint = 0;
      this.degraded = false;
      this.lastCheckpointError = null;
      return this.sequence;
    });
  }

  lastRecoveryManifest() { return this.lastRecovery ? clone(this.lastRecovery) : null; }

  async close() {
    if (this.closed) return;
    this.closed = true;
    await this.commitQueue;
    await this.checkpointNow().catch(error => { this.degraded = true; this.lastCheckpointError = error.code ?? error.message; });
    await this.intentLog.reconcile(async () => ({ status: 'pending' })).catch(() => undefined);
    await this.statePlane.close();
    await this.wal.close();
    this.emitter.removeAllListeners();
  }

  #get(collection, id) {
    validateCollection(collection); validateId(id);
    const store = this.statePlane.getOpened(collection);
    return store?.getSync(id) ?? null;
  }

  #all(collection) {
    validateCollection(collection);
    const store = this.statePlane.getOpened(collection);
    return store ? store.allSync() : [];
  }

  #scan(collection) {
    validateCollection(collection);
    const store = this.statePlane.getOpened(collection);
    return store ? store.scanSync() : [][Symbol.iterator]();
  }

  async #store(collection) {
    validateCollection(collection);
    if (!this.metadata.collections.includes(collection)) this.metadata.collections = [...this.metadata.collections, collection].sort();
    return this.statePlane.collection(collection);
  }

  async #validateAndCommit(tx, { origin, causalParents, partitionId }) {
    tx.validateVersions();
    const operations = tx.operations();
    if (!operations.length) return null;
    const admission = this.scheduler.admit({ priority: PRIORITY.foreground, cost: estimateCommitCost(operations) });
    if (admission.decision !== 'admit') throw resourceBusy(admission);
    try {
      const baseSequence = this.sequence;
      const transactionId = crypto.randomUUID();
      const mutations = operations.map(op => ({ type: op.type, collection: op.collection, id: op.id, fields: changedFields(op.before, op.after), record: op.after ? clone(op.after) : undefined }));
      const resultSequence = baseSequence + mutations.length;
      const fabric = createCommitFabric({ databaseId: this.databaseId, partitionId, sequence: resultSequence, logicalTime: Date.now(), transactionId, origin, mutations, schemaVersion: 1, policyVersion: 1, causalParents });
      if (!verifyCommitFabric(fabric)) throw corruption('commit fabric self-verification failed');
      const events = mutations.map((mutation, index) => ({
        type: mutation.type === 'put' ? (operations[index].before ? 'upsert' : 'insert') : 'remove',
        collection: mutation.collection,
        id: mutation.id,
        record: mutation.record,
        fields: mutation.fields,
        changeId: crypto.randomUUID(),
        originDatabaseId: this.databaseId,
        sequence: baseSequence + index + 1,
        at: new Date(fabric.logicalTime).toISOString(),
        commitId: fabric.commitId,
        commitChecksum: fabric.checksum,
        transactionId: fabric.transactionId,
        partitionId: fabric.partitionId,
        logicalTime: fabric.logicalTime,
        causalParents: [...fabric.causalParents]
      }));
      await this.wal.append({ databaseId: this.databaseId, baseSequence, resultSequence, events });
      await applyEventsToStatePlane(this.statePlane, events);
      this.metadata.sequence = resultSequence;
      this.metadata.lastCommitId = fabric.commitId;
      for (const event of events) {
        this.metadata.recordVersions[recordKey(event.collection, event.id)] = event.sequence;
        this.metadata.collectionVersions[event.collection] = event.sequence;
        this.metadata.changes.push(clone(event));
      }
      pruneChanges(this.metadata, this.changeRetention);
      this.commitsSinceCheckpoint++;
      for (const event of events) this.emitter.emit('change', clone(event));
      this.emitter.emit('commit', fabric);
      if (this.commitsSinceCheckpoint >= this.checkpointEvery) {
        this.#persistCheckpointPair().then(() => this.wal.compactThrough(this.sequence)).then(() => { this.commitsSinceCheckpoint = 0; this.degraded = false; this.lastCheckpointError = null; }).catch(error => { this.degraded = true; this.lastCheckpointError = error.code ?? error.message; });
      }
      return fabric;
    } finally { admission.release?.(); }
  }

  #enqueueCommit(work) {
    const operation = this.commitQueue.then(work);
    this.commitQueue = operation.catch(() => undefined);
    return operation;
  }

  async #persistCheckpointPair() {
    const payload = checkpointMetadata(this.metadata);
    await atomicWriteJson(this.file, payload);
    await atomicWriteJson(`${this.file}.bak`, payload);
  }
}

class MvccTransaction {
  constructor(db) { this.db = db; this.readVersions = new Map(); this.collectionReads = new Map(); this.writes = new Map(); }
  publicApi() { const tx = this; return Object.freeze({ collection: name => tx.collection(name), readVersion: this.db.sequence }); }
  collection(name) {
    validateCollection(name); const tx = this;
    return Object.freeze({
      get(id) { validateId(id); const key = recordKey(name,id); if (tx.writes.has(key)) return clone(tx.writes.get(key).after); const record = tx.db.#get(name,id); tx.readVersions.set(key,tx.db.metadata.recordVersions[key]??0); return clone(record); },
      put(record) { const value=normalizeRecord(record); const key=recordKey(name,value.id); const prior=tx.writes.has(key)?tx.writes.get(key).before:tx.db.#get(name,value.id); tx.writes.set(key,{type:'put',collection:name,id:value.id,before:clone(prior),after:clone(value)}); },
      remove(id) { validateId(id); const key=recordKey(name,id); const current=tx.writes.has(key)?tx.writes.get(key).after:tx.db.#get(name,id); if(!current)return false; const before=tx.writes.has(key)?tx.writes.get(key).before:current; tx.writes.set(key,{type:'remove',collection:name,id,before:clone(before),after:null}); return true; },
      all() { const version=tx.db.metadata.collectionVersions[name]??0; tx.collectionReads.set(name,version); const map=new Map(tx.db.#all(name).map(r=>[r.id,r])); for(const write of tx.writes.values())if(write.collection===name){if(write.after)map.set(write.id,clone(write.after));else map.delete(write.id);} return [...map.values()].map(clone); }
    });
  }
  hasWrites(){return this.writes.size>0;}
  operations(){return [...this.writes.values()].filter(op=>JSON.stringify(op.before)!==JSON.stringify(op.after));}
  validateVersions(){for(const[key,version]of this.readVersions)if((this.db.metadata.recordVersions[key]??0)!==version)throw conflict('record changed during transaction');for(const[name,version]of this.collectionReads)if((this.db.metadata.collectionVersions[name]??0)!==version)throw conflict('collection changed during transaction scan');}
}

async function replayDurableWal({metadata,wal,statePlane,changeRetention}) {
  let count=0;
  for (const entry of wal.listAfter(metadata.sequence)) {
    if(entry.databaseId!==metadata.databaseId)throw corruption('WAL database identity mismatch');
    if(entry.baseSequence!==metadata.sequence)throw corruption('WAL sequence gap during superior recovery');
    for(const event of entry.events){
      if(event.commitId&&event.commitChecksum){
        const grouped=entry.events.filter(item=>item.commitId===event.commitId);
        const mutations=grouped.map(item=>({type:item.type==='remove'?'remove':'put',collection:item.collection,id:item.id,fields:item.fields??['*'],record:item.record}));
        const fabric={version:1,databaseId:metadata.databaseId,partitionId:event.partitionId??'local',sequence:entry.resultSequence,logicalTime:event.logicalTime??Date.parse(event.at),transactionId:event.transactionId??null,origin:event.originDatabaseId??'recovery',mutations,schemaVersion:1,policyVersion:1,causalParents:event.causalParents??[],commitId:event.commitId,checksum:event.commitChecksum};
        if(!verifyCommitFabric(fabric))throw corruption('commit fabric checksum mismatch during recovery');
      }
    }
    await applyEventsToStatePlane(statePlane,entry.events);
    for(const event of entry.events){metadata.recordVersions[recordKey(event.collection,event.id)]=event.sequence;metadata.collectionVersions[event.collection]=event.sequence;metadata.changes.push(clone(event));if(!metadata.collections.includes(event.collection))metadata.collections.push(event.collection);metadata.lastCommitId=event.commitId??metadata.lastCommitId;}
    metadata.sequence=entry.resultSequence;count++;
  }
  metadata.collections=[...new Set(metadata.collections)].sort();pruneChanges(metadata,changeRetention);return count;
}

async function applyEventsToStatePlane(statePlane,events){const grouped=new Map();for(const event of events){const ops=grouped.get(event.collection)??[];ops.push(event.type==='remove'?{type:'remove',id:event.id}:{type:'put',record:normalizeRecord(event.record)});grouped.set(event.collection,ops);}for(const[collection,operations]of grouped){const store=await statePlane.collection(collection);await store.applyBatch(operations);}}

async function importLegacyState(statePlane,metadata,legacy){metadata.databaseId=legacy.databaseId??metadata.databaseId;metadata.sequence=legacy.sequence??0;metadata.changes=(legacy.changes??[]).map(clone);metadata.appliedChangeIds=[...(legacy.appliedChangeIds??[])];for(const[name,records]of Object.entries(legacy.collections??{})){validateCollection(name);const store=await statePlane.collection(name);const ops=Object.values(records).map(record=>({type:'put',record:normalizeRecord(record)}));if(ops.length)await store.applyBatch(ops);metadata.collections.push(name);metadata.collectionVersions[name]=metadata.sequence;for(const record of Object.values(records))metadata.recordVersions[recordKey(name,record.id)]=metadata.sequence;}metadata.collections=[...new Set(metadata.collections)].sort();}

function normalizeOptions(options){const changeRetention=options.changeRetention??DEFAULT_CHANGE_RETENTION;const checkpointEvery=options.checkpointEvery??DEFAULT_CHECKPOINT_EVERY;if(!Number.isSafeInteger(changeRetention)||changeRetention<1)throw new TypeError('changeRetention must be positive');if(!Number.isSafeInteger(checkpointEvery)||checkpointEvery<1)throw new TypeError('checkpointEvery must be positive');return{changeRetention,checkpointEvery,storage:options.storage??{},scheduler:options.scheduler,schedulerOptions:options.schedulerOptions??{},reactiveOptions:options.reactiveOptions??{}};}
function freshMetadata(databaseId=crypto.randomUUID()){return{format:FORMAT,version:1,databaseId,sequence:0,lastCommitId:null,collections:[],changes:[],appliedChangeIds:[],recordVersions:{},collectionVersions:{}};}
function isSuperiorMetadata(value){return value?.format===FORMAT&&value?.version===1;}
function normalizeMetadata(value){if(!isSuperiorMetadata(value)||typeof value.databaseId!=='string'||!Number.isSafeInteger(value.sequence))throw corruption('invalid superior checkpoint');return{...freshMetadata(value.databaseId),...clone(value),collections:[...new Set(value.collections??[])].sort(),recordVersions:{...(value.recordVersions??{})},collectionVersions:{...(value.collectionVersions??{})}};}
function checkpointMetadata(metadata){return{...clone(metadata),format:FORMAT,version:1};}
async function readMetadataWithRecovery(file){let primary=null;try{primary=JSON.parse(await fs.readFile(file,'utf8'));return{metadata:primary,source:'primary',checkpointSequence:primary.sequence??0};}catch(error){if(error.code==='ENOENT')return{metadata:null,source:'new',checkpointSequence:0};}try{const backup=JSON.parse(await fs.readFile(`${file}.bak`,'utf8'));return{metadata:backup,source:'backup',checkpointSequence:backup.sequence??0};}catch(backupError){const e=new Error('Syncio checkpoint and backup are unreadable');e.code='SYNCIO_CORRUPT_DATABASE';e.cause={primary:error,backup:backupError};throw e;}}
async function digestState(statePlane,collections){const hash=crypto.createHash('sha256');for(const name of [...collections].sort()){hash.update(name);const store=await statePlane.collection(name);for(const id of store.ids()){hash.update(id);hash.update(JSON.stringify(store.getSync(id)));}}return hash.digest('hex');}
async function atomicWriteJson(file,value){await fs.mkdir(path.dirname(file),{recursive:true});const tmp=`${file}.${process.pid}.${crypto.randomUUID()}.tmp`;try{await fs.writeFile(tmp,`${JSON.stringify(value)}\n`,{encoding:'utf8',mode:0o600});const handle=await fs.open(tmp,'r');try{await handle.sync();}finally{await handle.close();}await fs.rename(tmp,file);const dir=await fs.open(path.dirname(file),'r');try{await dir.sync();}finally{await dir.close();}}finally{await fs.rm(tmp,{force:true}).catch(()=>undefined);}}
function pruneChanges(metadata,retention){if(metadata.changes.length>retention)metadata.changes.splice(0,metadata.changes.length-retention);}
function boundedIds(ids,max){return[...new Set(ids)].slice(-max);}
function estimateCommitCost(ops){const bytes=ops.reduce((n,op)=>n+Buffer.byteLength(JSON.stringify(op.after??op.before??{})),0);return{cpu:ops.length,memory:Math.min(bytes,8*1024*1024),ssdIo:bytes,network:0,egress:0,coordination:1,recoveryRisk:1,latency:1};}
function changedFields(before,after){if(!before||!after)return['*'];const keys=new Set([...Object.keys(before),...Object.keys(after)]);const changed=[...keys].filter(k=>JSON.stringify(before[k])!==JSON.stringify(after[k]));return changed.length?changed:['*'];}
function recordKey(collection,id){return`${collection}\u0000${id}`;}
function normalizeRecord(record){if(!record||typeof record!=='object'||Array.isArray(record)||typeof record.id!=='string'||!record.id)throw new TypeError('record requires non-empty string id');const text=JSON.stringify(record);if(text===undefined)throw new TypeError('record must be JSON serializable');return JSON.parse(text);}
function validateCollection(name){if(typeof name!=='string'||!/^[A-Za-z0-9_-]+$/.test(name)||name.length>128)throw new TypeError('invalid collection name');}
function validateId(id){if(typeof id!=='string'||!id||id.length>512)throw new TypeError('invalid record id');}
function assertSequence(value,label){if(!Number.isSafeInteger(value)||value<0)throw new TypeError(`${label} must be non-negative integer`);}
function duplicate(id){const e=new Error(`record ${id} already exists`);e.code='SYNCIO_DUPLICATE_ID';e.statusCode=409;return e;}
function conflict(message){const e=new Error(message);e.code='SYNCIO_TRANSACTION_CONFLICT';e.statusCode=409;return e;}
function corruption(message){const e=new Error(message);e.code='SYNCIO_CORRUPT_DATABASE';return e;}
function resourceBusy(decision){const e=new Error('resource admission refused transaction');e.code='SYNCIO_RESOURCE_BUSY';e.statusCode=503;e.details=decision;return e;}
function syncError(code,message){const e=new Error(message);e.code=code;return e;}
function preconditionMatches(condition,record){if(condition.exists===true&&!record)return false;if(condition.exists===false&&record)return false;if(condition.equals!==undefined&&JSON.stringify(record)!==JSON.stringify(condition.equals))return false;return true;}
function applyIntentMutation(tx,mutation){if(!mutation||typeof mutation!=='object')throw new TypeError('intent mutation required');const c=tx.collection(mutation.collection);if(mutation.type==='remove')return c.remove(mutation.id);if(mutation.type==='put'||mutation.type==='upsert'||mutation.type==='insert')return c.put(mutation.record);throw new TypeError('unsupported intent mutation type');}
function validateReplacementState(state){if(!state||typeof state!=='object'||typeof state.databaseId!=='string'||!Number.isSafeInteger(state.sequence)||!state.collections||typeof state.collections!=='object')throw new TypeError('invalid replacement state');}
