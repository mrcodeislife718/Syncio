import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { isDeepStrictEqual } from 'node:util';
import { WriteAheadLog } from './wal.js';
export * from './advanced.js';
export * from './server.js';
export * from './wal.js';

const DEFAULT_CHANGE_RETENTION = 10_000;
const DEFAULT_CHECKPOINT_EVERY = 256;

export class SyncioDatabase {
  constructor(file, state, { changeRetention = DEFAULT_CHANGE_RETENTION, checkpointEvery = DEFAULT_CHECKPOINT_EVERY, wal } = {}) {
    if (!Number.isSafeInteger(changeRetention) || changeRetention < 1) throw new TypeError('changeRetention must be a positive safe integer');
    if (!Number.isSafeInteger(checkpointEvery) || checkpointEvery < 1) throw new TypeError('checkpointEvery must be a positive safe integer');
    if (!(wal instanceof WriteAheadLog)) throw new TypeError('Syncio database requires a WriteAheadLog');
    this.file = file;
    this.state = normalizeState(state);
    this.listeners = new Map();
    this.changeListeners = new Set();
    this.writeQueue = Promise.resolve();
    this.changeRetention = changeRetention;
    this.checkpointEvery = checkpointEvery;
    this.wal = wal;
    this.commitsSinceCheckpoint = 0;
    this.lastCheckpointError = null;
  }

  static async open(file, options = {}) {
    const target = path.resolve(file);
    const loaded = await readStateWithRecovery(target);
    let state = normalizeState(loaded.state);
    if (!loaded.exists) await atomicWriteJson(target, state);
    const checkpointSequence = state._syncio.sequence;
    const wal = await WriteAheadLog.open(`${target}.wal`);
    const replayEntries = wal.listAfter(checkpointSequence);
    state = replayWal(state, replayEntries, options.changeRetention ?? DEFAULT_CHANGE_RETENTION);
    const db = new SyncioDatabase(target, state, { ...options, wal });
    db.commitsSinceCheckpoint = replayEntries.length;
    return db;
  }

  get databaseId() { return this.state._syncio.databaseId; }
  get sequence() { return this.state._syncio.sequence; }

  storageStatus() {
    return Object.freeze({
      mode: 'wal-checkpoint',
      checkpointEvery: this.checkpointEvery,
      commitsSinceCheckpoint: this.commitsSinceCheckpoint,
      walEntries: this.wal.entries.length,
      degraded: Boolean(this.lastCheckpointError),
      checkpointError: this.lastCheckpointError ? {
        code: this.lastCheckpointError.code ?? 'CHECKPOINT_FAILED',
        message: this.lastCheckpointError.message
      } : null
    });
  }

  collection(name) {
    validateCollectionName(name);
    const db = this;
    return Object.freeze({
      async insert(value) {
        validateRecord(value, 'insert');
        const id = value.id ?? crypto.randomUUID();
        validateId(id);
        const record = structuredClone({ ...value, id });
        await db.#mutate(name, (collection) => {
          if (collection[id]) throw new Error(`Syncio record '${id}' already exists in '${name}'`);
          collection[id] = record;
          return record;
        }, () => ({ type: 'insert', id, record }));
        return structuredClone(record);
      },
      async upsert(value) {
        validateRecord(value, 'upsert');
        if (!value.id) throw new TypeError('Syncio upsert requires value.id');
        validateId(value.id);
        const record = structuredClone(value);
        await db.#mutate(name, (collection) => {
          collection[record.id] = record;
          return record;
        }, () => ({ type: 'upsert', id: record.id, record }));
        return structuredClone(record);
      },
      get(id) {
        const value = db.state.collections[name]?.[id];
        return value ? structuredClone(value) : null;
      },
      all() {
        return Object.values(db.state.collections[name] ?? {}).map((value) => structuredClone(value));
      },
      async remove(id) {
        validateId(id);
        let existed = false;
        await db.#mutate(name, (collection) => {
          existed = Boolean(collection[id]);
          if (existed) delete collection[id];
          return existed;
        }, () => existed ? ({ type: 'remove', id }) : null);
        return existed;
      },
      watch(listener) {
        if (typeof listener !== 'function') throw new TypeError('Syncio watch requires a function');
        const set = db.listeners.get(name) ?? new Set();
        set.add(listener);
        db.listeners.set(name, set);
        return () => {
          set.delete(listener);
          if (set.size === 0) db.listeners.delete(name);
        };
      }
    });
  }

  async transaction(work) {
    if (typeof work !== 'function') throw new TypeError('Syncio transaction requires a function');
    return this.#enqueue(async () => {
      const draft = structuredClone(this.state);
      const before = structuredClone(draft.collections);
      const result = await work(createTransactionApi(draft.collections));
      const changes = diffCollections(before, draft.collections);
      const events = changes.map((change) => this.#appendChange(draft, change));
      if (events.length) {
        await this.#commitDraft(draft, events);
        for (const event of events) this.#publish(event.collection, event);
      }
      return result;
    });
  }

  changesSince(cursor = 0, { limit = 1_000, collection } = {}) {
    if (!Number.isSafeInteger(cursor) || cursor < 0) throw new TypeError('Syncio change cursor must be a non-negative safe integer');
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 10_000) throw new TypeError('Syncio change limit must be between 1 and 10000');
    if (collection !== undefined) validateCollectionName(collection);
    return this.state._syncio.changes
      .filter((change) => change.sequence > cursor && (collection === undefined || change.collection === collection))
      .slice(0, limit)
      .map((change) => structuredClone(change));
  }

  resumeStatus(cursor = 0) {
    if (!Number.isSafeInteger(cursor) || cursor < 0) throw new TypeError('Syncio change cursor must be a non-negative safe integer');
    const oldestRetained = this.state._syncio.changes[0]?.sequence ?? (this.sequence + 1);
    return Object.freeze({
      cursor,
      sequence: this.sequence,
      oldestRetained,
      resumable: cursor >= oldestRetained - 1 && cursor <= this.sequence
    });
  }

  watchChanges({ collection, after = this.sequence } = {}, listener) {
    if (collection !== undefined) validateCollectionName(collection);
    if (!Number.isSafeInteger(after) || after < 0) throw new TypeError('Syncio watch cursor must be a non-negative safe integer');
    if (typeof listener !== 'function') throw new TypeError('Syncio watchChanges requires a function');
    const status = this.resumeStatus(after);
    if (!status.resumable) {
      const error = new Error(after > this.sequence ? 'Syncio change cursor is ahead of the database' : 'Syncio change cursor has expired');
      error.code = after > this.sequence ? 'SYNCIO_CURSOR_AHEAD' : 'SYNCIO_CURSOR_EXPIRED';
      error.details = status;
      throw error;
    }

    let active = true;
    let lastSequence = after;
    const buffered = [];
    const live = (event) => {
      if (!active || event.sequence <= lastSequence) return;
      if (collection !== undefined && event.collection !== collection) return;
      buffered.push(structuredClone(event));
    };
    this.changeListeners.add(live);

    const highWater = this.sequence;
    const replay = this.changesSince(after, { limit: 10_000, collection }).filter((event) => event.sequence <= highWater);
    for (const event of replay) {
      if (!active) break;
      lastSequence = event.sequence;
      listener(structuredClone(event));
    }
    buffered.sort((a, b) => a.sequence - b.sequence);
    for (const event of buffered) {
      if (!active || event.sequence <= lastSequence) continue;
      lastSequence = event.sequence;
      listener(structuredClone(event));
    }
    buffered.length = 0;

    this.changeListeners.delete(live);
    const direct = (event) => {
      if (!active || event.sequence <= lastSequence) return;
      if (collection !== undefined && event.collection !== collection) return;
      lastSequence = event.sequence;
      listener(structuredClone(event));
    };
    this.changeListeners.add(direct);
    return () => {
      active = false;
      this.changeListeners.delete(live);
      this.changeListeners.delete(direct);
    };
  }

  hasAppliedChange(changeId) {
    return Boolean(changeId && this.state._syncio.appliedChangeIds[changeId]);
  }

  async applyReplicationChange(change, resolver) {
    if (!change || typeof change !== 'object') throw new TypeError('replication change must be an object');
    const normalizedChange = structuredClone(change);
    normalizedChange.changeId ??= legacyChangeId(normalizedChange);
    validateCollectionName(normalizedChange.collection);
    if (typeof resolver !== 'function') throw new TypeError('replication resolver must be a function');

    return this.#enqueue(async () => {
      if (this.state._syncio.appliedChangeIds[normalizedChange.changeId]) return { applied: false, duplicate: true };
      const draft = structuredClone(this.state);
      const collection = draft.collections[normalizedChange.collection] ??= {};

      if (normalizedChange.type === 'remove') {
        validateId(normalizedChange.id);
        delete collection[normalizedChange.id];
      } else {
        validateRecord(normalizedChange.record, 'replication');
        const incoming = normalizedChange.record;
        if (!incoming?.id) throw new Error('replicated record requires id');
        validateId(incoming.id);
        const current = collection[incoming.id] ?? null;
        const resolved = resolver(structuredClone(current), structuredClone(incoming));
        validateRecord(resolved, 'replication resolver');
        if (!resolved?.id) throw new Error('replication resolver must return a record with id');
        validateId(resolved.id);
        collection[resolved.id] = structuredClone(resolved);
      }

      const event = this.#appendChange(draft, {
        ...normalizedChange,
        source: 'replication',
        receivedAt: new Date().toISOString()
      }, { preserveChangeId: true });
      await this.#commitDraft(draft, [event]);
      this.#publish(normalizedChange.collection, event);
      return { applied: true, duplicate: false, event: structuredClone(event) };
    });
  }

  snapshot() { return structuredClone(this.state); }

  async checkpoint() {
    return this.#enqueue(async () => this.#checkpointNow());
  }

  async replaceState(nextState) {
    return this.#enqueue(async () => {
      const replacement = normalizeState(structuredClone(nextState), this.state._syncio.databaseId);
      await atomicWriteJson(this.file, replacement);
      this.state = replacement;
      try {
        await this.wal.reset();
        this.commitsSinceCheckpoint = 0;
        this.lastCheckpointError = null;
      } catch (error) {
        this.lastCheckpointError = error;
      }
      return this.snapshot();
    });
  }

  async close() {
    await this.writeQueue;
    if (this.commitsSinceCheckpoint > 0) await this.#checkpointNow();
    await this.wal.close();
  }

  async #mutate(collectionName, mutation, eventFactory) {
    return this.#enqueue(async () => {
      const draft = structuredClone(this.state);
      const collection = draft.collections[collectionName] ??= {};
      const result = mutation(collection, draft);
      const eventData = eventFactory?.(result) ?? null;
      const event = eventData ? this.#appendChange(draft, { collection: collectionName, ...eventData }) : null;
      if (event) {
        await this.#commitDraft(draft, [event]);
        this.#publish(collectionName, event);
      }
      return result;
    });
  }

  async #commitDraft(draft, events) {
    const baseSequence = this.state._syncio.sequence;
    await this.wal.append({
      databaseId: this.databaseId,
      baseSequence,
      resultSequence: draft._syncio.sequence,
      events
    });
    this.state = draft;
    this.commitsSinceCheckpoint += 1;
    if (this.commitsSinceCheckpoint >= this.checkpointEvery) {
      try { await this.#checkpointNow(); }
      catch (error) { this.lastCheckpointError = error; }
    }
  }

  async #checkpointNow() {
    if (this.commitsSinceCheckpoint === 0) return { sequence: this.sequence, compacted: 0 };
    const sequence = this.sequence;
    await atomicWriteJson(this.file, this.state);
    const remaining = await this.wal.compactThrough(sequence);
    this.commitsSinceCheckpoint = remaining;
    this.lastCheckpointError = null;
    return { sequence, compacted: true, remainingWalEntries: remaining };
  }

  #enqueue(work) {
    const operation = this.writeQueue.then(work);
    this.writeQueue = operation.catch(() => undefined);
    return operation;
  }

  #appendChange(state, change, { preserveChangeId = false } = {}) {
    const localSequence = ++state._syncio.sequence;
    const copied = structuredClone(change);
    const event = {
      ...copied,
      changeId: preserveChangeId ? copied.changeId : crypto.randomUUID(),
      originDatabaseId: copied.originDatabaseId ?? state._syncio.databaseId,
      ...(preserveChangeId && Number.isSafeInteger(copied.sequence) ? { sourceSequence: copied.sequence } : {}),
      sequence: localSequence,
      at: copied.at ?? new Date().toISOString()
    };
    state._syncio.changes.push(event);
    state._syncio.appliedChangeIds[event.changeId] = localSequence;
    pruneChangeMetadata(state, this.changeRetention);
    return event;
  }

  #publish(collection, event) {
    const payload = structuredClone(event);
    for (const listener of this.listeners.get(collection) ?? []) queueMicrotask(() => listener(structuredClone(payload)));
    for (const listener of this.changeListeners) queueMicrotask(() => listener(structuredClone(payload)));
  }
}

export async function open(file, options) { return SyncioDatabase.open(file, options); }

function createTransactionApi(collections) {
  return Object.freeze({
    collection(name) {
      validateCollectionName(name);
      const collection = collections[name] ??= {};
      return Object.freeze({
        get(id) {
          validateId(id);
          return collection[id] ? structuredClone(collection[id]) : null;
        },
        put(record) {
          validateRecord(record, 'transaction put');
          if (!record.id) throw new TypeError('transaction put requires id');
          validateId(record.id);
          collection[record.id] = structuredClone(record);
        },
        remove(id) {
          validateId(id);
          return delete collection[id];
        },
        all() { return Object.values(collection).map((record) => structuredClone(record)); }
      });
    }
  });
}

function diffCollections(before, after) {
  const changes = [];
  const collectionNames = [...new Set([...Object.keys(before), ...Object.keys(after)])].sort();
  for (const collection of collectionNames) {
    validateCollectionName(collection);
    const previous = before[collection] ?? {};
    const next = after[collection] ?? {};
    const ids = [...new Set([...Object.keys(previous), ...Object.keys(next)])].sort();
    for (const id of ids) {
      if (!(id in next)) changes.push({ collection, type: 'remove', id });
      else if (!(id in previous)) changes.push({ collection, type: 'insert', id, record: structuredClone(next[id]) });
      else if (!isDeepStrictEqual(previous[id], next[id])) changes.push({ collection, type: 'upsert', id, record: structuredClone(next[id]) });
    }
  }
  return changes;
}

function validateCollectionName(name) {
  if (typeof name !== 'string' || !/^[A-Za-z0-9_-]+$/.test(name)) throw new TypeError('Syncio collection names may contain letters, numbers, _ and -');
}

function validateId(id) {
  if (typeof id !== 'string' || id.length < 1 || id.length > 512) throw new TypeError('Syncio record id must be a non-empty string up to 512 characters');
}

function validateRecord(value, operation) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new TypeError(`Syncio ${operation} requires an object`);
  assertJsonCompatible(value, operation);
}

function assertJsonCompatible(value, label, seen = new Set(), location = '$') {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return;
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new TypeError(`Syncio ${label} contains a non-finite number at ${location}`);
    return;
  }
  if (typeof value !== 'object') throw new TypeError(`Syncio ${label} contains a non-JSON value at ${location}`);
  if (seen.has(value)) throw new TypeError(`Syncio ${label} contains a circular reference at ${location}`);
  seen.add(value);
  if (Array.isArray(value)) {
    for (let index = 0; index < value.length; index++) assertJsonCompatible(value[index], label, seen, `${location}[${index}]`);
  } else {
    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) throw new TypeError(`Syncio ${label} contains a non-plain object at ${location}`);
    for (const [key, nested] of Object.entries(value)) assertJsonCompatible(nested, label, seen, `${location}.${key}`);
    if (Object.getOwnPropertySymbols(value).length) throw new TypeError(`Syncio ${label} contains symbol keys at ${location}`);
  }
  seen.delete(value);
}

function normalizeState(input, databaseId) {
  const state = input && typeof input === 'object' ? structuredClone(input) : {};
  if (!state.collections || typeof state.collections !== 'object' || Array.isArray(state.collections)) state.collections = {};
  if (!Number.isSafeInteger(state.version) || state.version < 1) state.version = 1;
  const metadata = state._syncio && typeof state._syncio === 'object' ? state._syncio : {};
  metadata.databaseId = metadata.databaseId ?? databaseId ?? crypto.randomUUID();
  metadata.sequence = Number.isSafeInteger(metadata.sequence) && metadata.sequence >= 0 ? metadata.sequence : 0;
  metadata.changes = Array.isArray(metadata.changes) ? metadata.changes : [];
  metadata.appliedChangeIds = metadata.appliedChangeIds && typeof metadata.appliedChangeIds === 'object'
    ? metadata.appliedChangeIds
    : Object.fromEntries(metadata.changes.filter((change) => change?.changeId).map((change) => [change.changeId, change.sequence]));
  state._syncio = metadata;
  return state;
}

function replayWal(initialState, entries, retention) {
  const state = normalizeState(initialState);
  for (const entry of entries) {
    if (entry.databaseId !== state._syncio.databaseId) {
      const error = new Error('Syncio WAL database identity mismatch');
      error.code = 'SYNCIO_WAL_DATABASE_MISMATCH';
      throw error;
    }
    if (entry.resultSequence <= state._syncio.sequence) continue;
    if (entry.baseSequence !== state._syncio.sequence) {
      const error = new Error(`Syncio WAL sequence gap: expected ${state._syncio.sequence}, received ${entry.baseSequence}`);
      error.code = 'SYNCIO_WAL_SEQUENCE_GAP';
      throw error;
    }
    let expected = entry.baseSequence;
    for (const event of entry.events) {
      if (!event || event.sequence !== ++expected) {
        const error = new Error('Syncio WAL event sequence is invalid');
        error.code = 'SYNCIO_CORRUPT_WAL';
        throw error;
      }
      applyEventToState(state, event);
      state._syncio.changes.push(structuredClone(event));
      state._syncio.appliedChangeIds[event.changeId] = event.sequence;
    }
    if (expected !== entry.resultSequence) {
      const error = new Error('Syncio WAL result sequence does not match event sequence');
      error.code = 'SYNCIO_CORRUPT_WAL';
      throw error;
    }
    state._syncio.sequence = entry.resultSequence;
    pruneChangeMetadata(state, retention);
  }
  return state;
}

function applyEventToState(state, event) {
  validateCollectionName(event.collection);
  const collection = state.collections[event.collection] ??= {};
  if (event.type === 'remove') {
    validateId(event.id);
    delete collection[event.id];
    return;
  }
  if (event.type !== 'insert' && event.type !== 'upsert') throw new Error(`unsupported WAL event type: ${event.type}`);
  validateRecord(event.record, 'WAL replay');
  if (!event.record.id) throw new Error('WAL record requires id');
  validateId(event.record.id);
  collection[event.record.id] = structuredClone(event.record);
}

function pruneChangeMetadata(state, retention) {
  const changes = state._syncio.changes;
  if (changes.length <= retention) return;
  const removed = changes.splice(0, changes.length - retention);
  for (const change of removed) {
    if (state._syncio.appliedChangeIds[change.changeId] === change.sequence) delete state._syncio.appliedChangeIds[change.changeId];
  }
}

function legacyChangeId(change) {
  return `legacy:${crypto.createHash('sha256').update(JSON.stringify(change)).digest('hex')}`;
}

async function readStateWithRecovery(target) {
  try {
    return { state: JSON.parse(await fs.readFile(target, 'utf8')), exists: true };
  } catch (error) {
    if (error.code === 'ENOENT') return { state: { version: 1, collections: {} }, exists: false };
    if (!(error instanceof SyntaxError)) throw error;
    try {
      return { state: JSON.parse(await fs.readFile(`${target}.bak`, 'utf8')), exists: true };
    } catch (backupError) {
      if (backupError.code === 'ENOENT' || backupError instanceof SyntaxError) {
        const recoveryError = new Error(`Syncio database is corrupted and no valid backup exists: ${target}`);
        recoveryError.code = 'SYNCIO_CORRUPT_DATABASE';
        recoveryError.cause = error;
        throw recoveryError;
      }
      throw backupError;
    }
  }
}

async function atomicWriteJson(target, value) {
  await fs.mkdir(path.dirname(target), { recursive: true });
  const temp = `${target}.${process.pid}.${crypto.randomUUID()}.tmp`;
  const backupTemp = `${target}.bak.${process.pid}.${crypto.randomUUID()}.tmp`;
  const data = `${JSON.stringify(value, null, 2)}\n`;
  try {
    await fs.writeFile(temp, data, { encoding: 'utf8', mode: 0o600 });
    const tempHandle = await fs.open(temp, 'r');
    try { await tempHandle.sync(); } finally { await tempHandle.close(); }
    await fs.rename(temp, target);
    const directoryHandle = await fs.open(path.dirname(target), 'r');
    try { await directoryHandle.sync(); } finally { await directoryHandle.close(); }

    await fs.writeFile(backupTemp, data, { encoding: 'utf8', mode: 0o600 });
    const backupHandle = await fs.open(backupTemp, 'r');
    try { await backupHandle.sync(); } finally { await backupHandle.close(); }
    await fs.rename(backupTemp, `${target}.bak`);
    const backupDirectoryHandle = await fs.open(path.dirname(target), 'r');
    try { await backupDirectoryHandle.sync(); } finally { await backupDirectoryHandle.close(); }
  } finally {
    await fs.rm(temp, { force: true }).catch(() => undefined);
    await fs.rm(backupTemp, { force: true }).catch(() => undefined);
  }
}
