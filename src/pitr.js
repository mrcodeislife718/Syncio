import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';

const clone = (value) => structuredClone(value);

export class PointInTimeRecovery {
  constructor(db, directory, state) {
    this.db = db;
    this.directory = path.resolve(directory);
    this.manifestFile = path.join(this.directory, 'manifest.json');
    this.journalFile = path.join(this.directory, 'history.ndjson');
    this.state = state;
    this.queue = Promise.resolve();
    this.stopWatching = null;
  }

  static async open(db, directory, { createInitialSnapshot = true } = {}) {
    if (!db || typeof db.snapshot !== 'function' || typeof db.watchChanges !== 'function') throw new TypeError('PITR requires Syncio-compatible database');
    if (!directory) throw new TypeError('PITR directory required');
    const dir = path.resolve(directory);
    await fs.mkdir(dir, { recursive: true });
    const manifestFile = path.join(dir, 'manifest.json');
    let state = { version: 1, databaseId: db.databaseId, snapshots: [], lastSequence: 0, lastHash: null };
    try { state = JSON.parse(await fs.readFile(manifestFile, 'utf8')); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
    validateManifest(state, db.databaseId);
    const manager = new PointInTimeRecovery(db, dir, state);
    await manager.#verifyJournal();
    await manager.syncFromDatabase();
    if (createInitialSnapshot && !manager.state.snapshots.length) await manager.createSnapshot({ reason: 'initial' });
    manager.#watch();
    return manager;
  }

  status() {
    return clone({
      databaseId: this.state.databaseId,
      lastSequence: this.state.lastSequence,
      snapshots: this.state.snapshots,
      protectedFromSequence: this.state.snapshots[0]?.sequence ?? null,
      currentSequence: this.db.sequence
    });
  }

  async syncFromDatabase() {
    const last = this.state.lastSequence;
    if (last > this.db.sequence) throw pitrError('SYNCIO_PITR_AHEAD', 'PITR history is ahead of database');
    if (last === this.db.sequence) return { appended: 0 };
    const resume = this.db.resumeStatus(last);
    if (!resume.resumable) throw pitrError('SYNCIO_PITR_GAP', 'durable change retention no longer contains PITR gap', resume);
    let cursor = last;
    let appended = 0;
    while (cursor < this.db.sequence) {
      const changes = this.db.changesSince(cursor, { limit: 10_000 });
      if (!changes.length) break;
      for (const change of changes) { await this.#append(change); cursor = change.sequence; appended += 1; }
    }
    if (cursor !== this.db.sequence) throw pitrError('SYNCIO_PITR_GAP', 'PITR reconciliation did not reach database high-water mark', { cursor, sequence: this.db.sequence });
    return { appended };
  }

  async createSnapshot({ reason = 'scheduled', metadata = {} } = {}) {
    await this.flush();
    await this.syncFromDatabase();
    const state = this.db.snapshot();
    const sequence = this.db.sequence;
    const createdAt = new Date().toISOString();
    const body = { format: 'syncio-pitr-snapshot/1', databaseId: this.db.databaseId, sequence, createdAt, reason, metadata: clone(metadata), state };
    const digest = digestJson(body);
    const id = `${String(sequence).padStart(20, '0')}-${crypto.randomUUID()}`;
    const file = path.join(this.directory, `${id}.snapshot.json`);
    await atomicWriteJson(file, { ...body, digest });
    const record = { id, file, sequence, createdAt, reason, digest };
    this.state.snapshots.push(record);
    this.state.snapshots.sort((a,b)=>a.sequence-b.sequence || a.createdAt.localeCompare(b.createdAt));
    await this.#persistManifest();
    return clone(record);
  }

  async verify() {
    await this.flush();
    await this.#verifyJournal();
    for (const record of this.state.snapshots) await readVerifiedSnapshot(record.file, this.db.databaseId, record.digest);
    return { ok: true, snapshots: this.state.snapshots.length, lastSequence: this.state.lastSequence };
  }

  async restoreToSequence(sequence, { replaceLive = false } = {}) {
    if (!Number.isSafeInteger(sequence) || sequence < 0) throw new TypeError('PITR sequence must be a non-negative safe integer');
    await this.flush();
    const snapshotRecord = [...this.state.snapshots].filter((item)=>item.sequence<=sequence).sort((a,b)=>b.sequence-a.sequence)[0];
    if (!snapshotRecord) throw pitrError('SYNCIO_PITR_BEFORE_WINDOW', 'requested sequence predates earliest PITR snapshot');
    const snapshot = await readVerifiedSnapshot(snapshotRecord.file, this.db.databaseId, snapshotRecord.digest);
    const state = clone(snapshot.state);
    const entries = await readJournal(this.journalFile);
    for (const entry of entries) {
      if (entry.sequence <= snapshotRecord.sequence) continue;
      if (entry.sequence > sequence) break;
      applyChange(state, entry.event);
    }
    state._syncio ??= {};
    state._syncio.sequence = sequence;
    state._syncio.changes = (state._syncio.changes ?? []).filter((item)=>item.sequence<=sequence);
    for (const key of Object.keys(state._syncio.appliedChangeIds ?? {})) if (state._syncio.appliedChangeIds[key] > sequence) delete state._syncio.appliedChangeIds[key];
    if (replaceLive) await this.db.replaceState(state);
    return { sequence, snapshotSequence: snapshotRecord.sequence, state };
  }

  async restoreToTime(isoTime, options = {}) {
    const target = Date.parse(isoTime);
    if (!Number.isFinite(target)) throw new TypeError('valid PITR timestamp required');
    await this.flush();
    const entries = await readJournal(this.journalFile);
    let sequence = null;
    for (const entry of entries) if (Date.parse(entry.event.at) <= target) sequence = entry.sequence;
    if (sequence === null) {
      const snapshot = [...this.state.snapshots].filter((item)=>Date.parse(item.createdAt)<=target).sort((a,b)=>Date.parse(b.createdAt)-Date.parse(a.createdAt))[0];
      if (!snapshot) throw pitrError('SYNCIO_PITR_BEFORE_WINDOW', 'requested time predates PITR window');
      sequence = snapshot.sequence;
    }
    return this.restoreToSequence(sequence, options);
  }

  async prune({ keepSnapshots = 8, keepAfterSequence = 0 } = {}) {
    if (!Number.isSafeInteger(keepSnapshots) || keepSnapshots < 1) throw new TypeError('keepSnapshots must be positive');
    if (!Number.isSafeInteger(keepAfterSequence) || keepAfterSequence < 0) throw new TypeError('keepAfterSequence must be non-negative');
    await this.flush();
    const sorted = [...this.state.snapshots].sort((a,b)=>b.sequence-a.sequence);
    const keep = new Set(sorted.slice(0, keepSnapshots).map((item)=>item.id));
    for (const item of sorted) if (item.sequence >= keepAfterSequence) keep.add(item.id);
    const removed = [];
    for (const item of this.state.snapshots) if (!keep.has(item.id)) { await fs.rm(item.file, { force: true }); removed.push(item.id); }
    this.state.snapshots = this.state.snapshots.filter((item)=>keep.has(item.id));
    await this.#persistManifest();
    return { removed };
  }

  async flush() { await this.queue; }
  async close() { this.stopWatching?.(); this.stopWatching = null; await this.flush(); }

  #watch() {
    const after = this.db.sequence;
    this.stopWatching = this.db.watchChanges({ after }, (event) => {
      const operation = this.queue.then(()=>this.#append(event));
      this.queue = operation.catch(()=>undefined);
    });
  }

  async #append(event) {
    if (!event || !Number.isSafeInteger(event.sequence)) throw new TypeError('PITR event requires sequence');
    if (event.sequence <= this.state.lastSequence) return false;
    if (event.sequence !== this.state.lastSequence + 1) throw pitrError('SYNCIO_PITR_GAP', 'PITR sequence gap detected', { expected: this.state.lastSequence + 1, actual: event.sequence });
    const base = { sequence: event.sequence, event: clone(event), previousHash: this.state.lastHash };
    const hash = digestJson(base);
    const line = `${JSON.stringify({ ...base, hash })}\n`;
    const handle = await fs.open(this.journalFile, 'a', 0o600);
    try { await handle.writeFile(line, 'utf8'); await handle.sync(); } finally { await handle.close(); }
    this.state.lastSequence = event.sequence;
    this.state.lastHash = hash;
    await this.#persistManifest();
    return true;
  }

  async #verifyJournal() {
    const entries = await readJournal(this.journalFile);
    let previousHash = null;
    let previousSequence = 0;
    for (const entry of entries) {
      if (entry.previousHash !== previousHash) throw pitrError('SYNCIO_PITR_CORRUPT', 'PITR hash chain mismatch');
      if (entry.sequence !== previousSequence + 1 && previousSequence !== 0) throw pitrError('SYNCIO_PITR_CORRUPT', 'PITR sequence discontinuity');
      const expected = digestJson({ sequence: entry.sequence, event: entry.event, previousHash: entry.previousHash });
      if (entry.hash !== expected) throw pitrError('SYNCIO_PITR_CORRUPT', 'PITR entry digest mismatch');
      previousHash = entry.hash; previousSequence = entry.sequence;
    }
    if (entries.length) {
      const last = entries.at(-1);
      if (this.state.lastSequence !== last.sequence || this.state.lastHash !== last.hash) throw pitrError('SYNCIO_PITR_CORRUPT', 'PITR manifest does not match journal tail');
    } else if (this.state.lastSequence !== 0 || this.state.lastHash !== null) throw pitrError('SYNCIO_PITR_CORRUPT', 'PITR manifest references missing journal');
    return true;
  }

  #persistManifest() { return atomicWriteJson(this.manifestFile, this.state); }
}

function applyChange(state, event) {
  const collection = state.collections[event.collection] ??= {};
  if (event.type === 'remove') delete collection[event.id];
  else if (event.record?.id) collection[event.record.id] = clone(event.record);
  state._syncio ??= { sequence: 0, changes: [], appliedChangeIds: {} };
  state._syncio.sequence = event.sequence;
  state._syncio.changes ??= [];
  state._syncio.changes.push(clone(event));
  state._syncio.appliedChangeIds ??= {};
  if (event.changeId) state._syncio.appliedChangeIds[event.changeId] = event.sequence;
}
function validateManifest(state, databaseId) { if (!state || state.version !== 1 || state.databaseId !== databaseId || !Array.isArray(state.snapshots) || !Number.isSafeInteger(state.lastSequence)) throw pitrError('SYNCIO_PITR_DATABASE_MISMATCH', 'invalid PITR manifest or database identity'); }
function digestJson(value) { return crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex'); }
function pitrError(code, message, details) { const error = new Error(message); error.code = code; if (details) error.details = clone(details); return error; }
async function readJournal(file) { try { const text = await fs.readFile(file, 'utf8'); return text.split('\n').filter(Boolean).map((line)=>JSON.parse(line)).sort((a,b)=>a.sequence-b.sequence); } catch (error) { if (error.code === 'ENOENT') return []; throw error; } }
async function readVerifiedSnapshot(file, databaseId, expectedDigest) { const envelope = JSON.parse(await fs.readFile(file,'utf8')); const { digest, ...body } = envelope; if (body.format !== 'syncio-pitr-snapshot/1' || body.databaseId !== databaseId || digest !== expectedDigest || digest !== digestJson(body)) throw pitrError('SYNCIO_PITR_CORRUPT','PITR snapshot verification failed'); return body; }
async function atomicWriteJson(target,value){const file=path.resolve(target);await fs.mkdir(path.dirname(file),{recursive:true});const temp=`${file}.${process.pid}.${crypto.randomUUID()}.tmp`;try{await fs.writeFile(temp,`${JSON.stringify(value,null,2)}\n`,{encoding:'utf8',mode:0o600});const handle=await fs.open(temp,'r');try{await handle.sync();}finally{await handle.close();}await fs.rename(temp,file);const dir=await fs.open(path.dirname(file),'r');try{await dir.sync();}finally{await dir.close();}}finally{await fs.rm(temp,{force:true}).catch(()=>undefined);}}
