import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
export * from './advanced.js';
export * from './server.js';

const DEFAULT_CHANGE_RETENTION = 10_000;

export class SyncioDatabase {
  constructor(file, state, { changeRetention = DEFAULT_CHANGE_RETENTION } = {}) {
    if (!Number.isSafeInteger(changeRetention) || changeRetention < 1) throw new TypeError('changeRetention must be a positive safe integer');
    this.file = file;
    this.state = normalizeState(state);
    this.listeners = new Map();
    this.writeQueue = Promise.resolve();
    this.changeRetention = changeRetention;
  }

  static async open(file, options = {}) {
    const target = path.resolve(file);
    const state = await readStateWithRecovery(target);
    return new SyncioDatabase(target, state, options);
  }

  get databaseId() { return this.state._syncio.databaseId; }
  get sequence() { return this.state._syncio.sequence; }

  collection(name) {
    validateCollectionName(name);
    this.state.collections[name] ??= {};
    const db = this;
    return Object.freeze({
      async insert(value) {
        validateRecord(value, 'insert');
        const id = value.id ?? crypto.randomUUID();
        const record = structuredClone({ ...value, id });
        await db.#mutate(name, () => {
          if (db.state.collections[name][id]) throw new Error(`Syncio record '${id}' already exists in '${name}'`);
          db.state.collections[name][id] = record;
          return record;
        }, () => ({ type: 'insert', id, record }));
        return structuredClone(record);
      },
      async upsert(value) {
        validateRecord(value, 'upsert');
        if (!value.id) throw new TypeError('Syncio upsert requires value.id');
        const record = structuredClone(value);
        await db.#mutate(name, () => {
          db.state.collections[name][record.id] = record;
          return record;
        }, () => ({ type: 'upsert', id: record.id, record }));
        return structuredClone(record);
      },
      get(id) {
        const value = db.state.collections[name][id];
        return value ? structuredClone(value) : null;
      },
      all() {
        return Object.values(db.state.collections[name]).map((value) => structuredClone(value));
      },
      async remove(id) {
        let existed = false;
        await db.#mutate(name, () => {
          existed = Boolean(db.state.collections[name][id]);
          if (existed) delete db.state.collections[name][id];
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

  changesSince(cursor = 0, { limit = 1_000 } = {}) {
    if (!Number.isSafeInteger(cursor) || cursor < 0) throw new TypeError('Syncio change cursor must be a non-negative safe integer');
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 10_000) throw new TypeError('Syncio change limit must be between 1 and 10000');
    return this.state._syncio.changes
      .filter((change) => change.sequence > cursor)
      .slice(0, limit)
      .map((change) => structuredClone(change));
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

    let outcome;
    await this.#enqueue(async () => {
      if (this.state._syncio.appliedChangeIds[normalizedChange.changeId]) {
        outcome = { applied: false, duplicate: true };
        return;
      }

      const collection = this.state.collections[normalizedChange.collection] ??= {};
      if (normalizedChange.type === 'remove') {
        delete collection[normalizedChange.id];
      } else {
        const incoming = normalizedChange.record;
        if (!incoming?.id) throw new Error('replicated record requires id');
        const current = collection[incoming.id] ?? null;
        const resolved = resolver(structuredClone(current), structuredClone(incoming));
        if (!resolved?.id) throw new Error('replication resolver must return a record with id');
        collection[resolved.id] = structuredClone(resolved);
      }

      const event = this.#appendChange({
        ...normalizedChange,
        source: 'replication',
        receivedAt: new Date().toISOString()
      }, { preserveChangeId: true });
      await this.#persist();
      outcome = { applied: true, duplicate: false, event: structuredClone(event) };
      this.#publish(normalizedChange.collection, event);
    });
    return outcome;
  }

  snapshot() { return structuredClone(this.state); }

  async replaceState(nextState) {
    await this.#enqueue(async () => {
      this.state = normalizeState(structuredClone(nextState), this.state._syncio.databaseId);
      await this.#persist();
    });
    return this.snapshot();
  }

  async close() { await this.writeQueue; }

  async #mutate(collection, mutation, eventFactory) {
    return this.#enqueue(async () => {
      const result = mutation();
      const eventData = eventFactory?.(result) ?? null;
      const event = eventData ? this.#appendChange({ collection, ...eventData }) : null;
      await this.#persist();
      if (event) this.#publish(collection, event);
      return result;
    });
  }

  #enqueue(work) {
    const operation = this.writeQueue.then(work);
    this.writeQueue = operation.catch(() => undefined);
    return operation;
  }

  #appendChange(change, { preserveChangeId = false } = {}) {
    const localSequence = ++this.state._syncio.sequence;
    const copied = structuredClone(change);
    const event = {
      ...copied,
      changeId: preserveChangeId ? copied.changeId : crypto.randomUUID(),
      originDatabaseId: copied.originDatabaseId ?? this.state._syncio.databaseId,
      ...(preserveChangeId && Number.isSafeInteger(copied.sequence) ? { sourceSequence: copied.sequence } : {}),
      sequence: localSequence,
      at: copied.at ?? new Date().toISOString()
    };
    this.state._syncio.changes.push(event);
    this.state._syncio.appliedChangeIds[event.changeId] = localSequence;
    this.#pruneChangeMetadata();
    return event;
  }

  #pruneChangeMetadata() {
    const changes = this.state._syncio.changes;
    if (changes.length <= this.changeRetention) return;
    const removed = changes.splice(0, changes.length - this.changeRetention);
    for (const change of removed) {
      if (this.state._syncio.appliedChangeIds[change.changeId] === change.sequence) {
        delete this.state._syncio.appliedChangeIds[change.changeId];
      }
    }
  }

  #publish(collection, event) {
    const payload = structuredClone(event);
    for (const listener of this.listeners.get(collection) ?? []) {
      queueMicrotask(() => listener(structuredClone(payload)));
    }
  }

  async #persist() { await atomicWriteJson(this.file, this.state); }
}

export async function open(file, options) { return SyncioDatabase.open(file, options); }

function validateCollectionName(name) {
  if (typeof name !== 'string' || !/^[A-Za-z0-9_-]+$/.test(name)) {
    throw new TypeError('Syncio collection names may contain letters, numbers, _ and -');
  }
}

function validateRecord(value, operation) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) {
    throw new TypeError(`Syncio ${operation} requires an object`);
  }
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

function legacyChangeId(change) {
  return `legacy:${crypto.createHash('sha256').update(JSON.stringify(change)).digest('hex')}`;
}

async function readStateWithRecovery(target) {
  try {
    return JSON.parse(await fs.readFile(target, 'utf8'));
  } catch (error) {
    if (error.code === 'ENOENT') return { version: 1, collections: {} };
    if (!(error instanceof SyntaxError)) throw error;
    try {
      return JSON.parse(await fs.readFile(`${target}.bak`, 'utf8'));
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
  const data = `${JSON.stringify(value, null, 2)}\n`;
  try {
    await fs.writeFile(temp, data, { encoding: 'utf8', mode: 0o600 });
    const tempHandle = await fs.open(temp, 'r');
    try { await tempHandle.sync(); } finally { await tempHandle.close(); }

    try {
      await fs.copyFile(target, `${target}.bak`);
      const backupHandle = await fs.open(`${target}.bak`, 'r');
      try { await backupHandle.sync(); } finally { await backupHandle.close(); }
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }

    await fs.rename(temp, target);
    const directoryHandle = await fs.open(path.dirname(target), 'r');
    try { await directoryHandle.sync(); } finally { await directoryHandle.close(); }
  } finally {
    await fs.rm(temp, { force: true }).catch(() => undefined);
  }
}
