import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { SyncioDatabase } from './index.js';
import { queryRecords } from './advanced.js';
import { firstPathValue } from './document.js';

export class IndexedSyncioDatabase {
  constructor(base, catalogFile, definitions = {}) {
    this.base = base;
    this.file = base.file;
    this.catalogFile = catalogFile;
    this.definitions = normalizeDefinitions(definitions);
    this.indexes = new Map();
    this.catalogQueue = Promise.resolve();
    this.#rebuildAll();
  }

  static async open(file, options = {}) {
    const base = await SyncioDatabase.open(file, options);
    const catalogFile = path.resolve(`${base.file}.indexes.json`);
    let definitions = {};
    try { definitions = JSON.parse(await fs.readFile(catalogFile, 'utf8')).indexes ?? {}; }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
    return new IndexedSyncioDatabase(base, catalogFile, definitions);
  }

  get databaseId() { return this.base.databaseId; }
  get sequence() { return this.base.sequence; }
  storageStatus() { return this.base.storageStatus(); }
  resumeStatus(cursor) { return this.base.resumeStatus(cursor); }
  watchChanges(options, listener) { return this.base.watchChanges(options, listener); }

  async defineIndex(collection, fields, options = {}) {
    validateName(collection, 'collection');
    const normalizedFields = normalizeFields(fields);
    const definition = normalizeDefinition({
      name: options.name ?? normalizedFields.join('_'),
      fields: normalizedFields,
      unique: options.unique === true,
      sparse: options.sparse === true
    });
    const current = this.definitions[collection] ?? [];
    const collision = current.find((item) => item.name === definition.name);
    if (collision && !sameDefinition(collision, definition)) throw indexError('SYNCIO_INDEX_NAME_CONFLICT', `index '${definition.name}' already exists with different definition`);
    if (collision) return publicDefinition(collection, collision);
    const candidate = new DocumentIndex(definition).rebuild(this.base.collection(collection).all());
    const previousDefinitions = this.definitions;
    this.definitions = { ...this.definitions, [collection]: [...current, definition] };
    this.indexes.set(indexKey(collection, definition.name), candidate);
    try { await this.#persistCatalog(); }
    catch (error) { this.definitions = previousDefinitions; this.#rebuildCollection(collection); throw error; }
    return publicDefinition(collection, definition);
  }

  async dropIndex(collection, nameOrFields) {
    validateName(collection, 'collection');
    const name = Array.isArray(nameOrFields) ? normalizeFields(nameOrFields).join('_') : String(nameOrFields);
    validateName(name, 'name');
    const current = this.definitions[collection] ?? [];
    const nextDefinitions = current.filter((item) => item.name !== name && !(item.fields.length === 1 && item.fields[0] === name));
    if (nextDefinitions.length === current.length) return false;
    const previousDefinitions = this.definitions;
    const next = { ...this.definitions };
    if (nextDefinitions.length) next[collection] = nextDefinitions; else delete next[collection];
    this.definitions = next;
    this.#rebuildCollection(collection);
    try { await this.#persistCatalog(); }
    catch (error) { this.definitions = previousDefinitions; this.#rebuildCollection(collection); throw error; }
    return true;
  }

  listIndexes() {
    return Object.entries(this.definitions).flatMap(([collection, definitions]) => definitions.map((definition) => publicDefinition(collection, definition)));
  }

  collection(name) {
    const baseCollection = this.base.collection(name);
    const db = this;
    return Object.freeze({
      async insert(value) {
        if (!db.#hasUniqueIndex(name)) {
          const result = await baseCollection.insert(value);
          db.#updateIndexes(name, null, result);
          return result;
        }
        const record = structuredClone({ ...value, id: value?.id ?? crypto.randomUUID() });
        await db.transaction(async (tx) => {
          const collection = tx.collection(name);
          if (collection.get(record.id)) throw indexError('SYNCIO_DUPLICATE_ID', `record '${record.id}' already exists`);
          collection.put(record);
        });
        return structuredClone(record);
      },
      async upsert(value) {
        if (!db.#hasUniqueIndex(name)) {
          const before = value?.id ? baseCollection.get(value.id) : null;
          const result = await baseCollection.upsert(value);
          db.#updateIndexes(name, before, result);
          return result;
        }
        let result;
        await db.transaction(async (tx) => {
          const collection = tx.collection(name);
          collection.put(value);
          result = structuredClone(value);
        });
        return result;
      },
      get: (id) => baseCollection.get(id),
      all() {
        const records = baseCollection.all();
        Object.defineProperty(records, '__syncioQuery', { value: (spec) => db.query(name, spec), enumerable: false, configurable: false, writable: false });
        return records;
      },
      async remove(id) {
        const before = baseCollection.get(id);
        const result = await baseCollection.remove(id);
        if (result) db.#updateIndexes(name, before, null);
        return result;
      },
      watch: (listener) => baseCollection.watch(listener),
      query(spec = {}) { return db.query(name, spec); },
      explain(spec = {}) { return db.explainQuery(name, spec); }
    });
  }

  query(collection, spec = {}) {
    const plan = this.explainQuery(collection, spec);
    if (plan.strategy === 'index') {
      const indexName = plan.index ?? plan.field;
      const values = plan.values ?? [plan.value];
      const index = this.indexes.get(indexKey(collection, indexName));
      if (!index) return queryRecords(this.base.collection(collection).all(), spec);
      const records = index.find(values).map((id) => this.base.collection(collection).get(id)).filter(Boolean);
      return queryRecords(records, spec);
    }
    return queryRecords(this.base.collection(collection).all(), spec);
  }

  explainQuery(collection, spec = {}) {
    const where = spec.where && typeof spec.where === 'object' && !Array.isArray(spec.where) ? spec.where : {};
    const candidates = (this.definitions[collection] ?? [])
      .filter((definition) => definition.fields.every((field) => isSimpleEquality(where[field])))
      .sort((a, b) => b.fields.length - a.fields.length);
    const best = candidates[0];
    if (!best) return { strategy: 'scan' };
    if (best.fields.length === 1) {
      const field = best.fields[0];
      return { strategy: 'index', field, value: where[field] };
    }
    return { strategy: 'index', index: best.name, fields: [...best.fields], values: best.fields.map((field) => where[field]) };
  }

  async transaction(work) {
    if (typeof work !== 'function') throw new TypeError('transaction requires a function');
    const result = await this.base.transaction(async (tx) => {
      const working = structuredClone(this.base.snapshot().collections ?? {});
      return work(this.#transactionProxy(tx, working));
    });
    this.#rebuildAll();
    return result;
  }

  changesSince(...args) { return this.base.changesSince(...args); }
  hasAppliedChange(...args) { return this.base.hasAppliedChange(...args); }
  snapshot() { return this.base.snapshot(); }
  async checkpoint() { return this.base.checkpoint(); }
  async replaceState(state) { const result = await this.base.replaceState(state); this.#rebuildAll(); return result; }
  async applyReplicationChange(change, resolver) {
    const wrappedResolver = (local, remote) => {
      const resolved = resolver(local, remote);
      this.#assertUniqueCandidateAgainstRecords(change.collection, resolved, resolved?.id ?? null, this.base.collection(change.collection).all());
      return resolved;
    };
    const result = await this.base.applyReplicationChange(change, wrappedResolver);
    if (result?.applied) this.#rebuildCollection(change.collection);
    return result;
  }
  async close() { await this.catalogQueue; await this.base.close(); }

  #transactionProxy(tx, working) {
    const db = this;
    return Object.freeze({
      collection(name) {
        const delegate = tx.collection(name);
        working[name] ??= {};
        return Object.freeze({
          get(id) { return working[name][id] ? structuredClone(working[name][id]) : null; },
          put(record) {
            if (!record?.id) throw new TypeError('transaction put requires id');
            db.#assertUniqueCandidateAgainstRecords(name, record, record.id, Object.values(working[name]));
            delegate.put(record);
            working[name][record.id] = structuredClone(record);
          },
          remove(id) {
            const existed = Object.hasOwn(working[name], id);
            delegate.remove(id);
            delete working[name][id];
            return existed;
          },
          all() { return Object.values(working[name]).map((record) => structuredClone(record)); }
        });
      }
    });
  }

  #hasUniqueIndex(collection) { return (this.definitions[collection] ?? []).some((definition) => definition.unique); }

  #assertUniqueCandidateAgainstRecords(collection, candidate, excludeId, records) {
    if (!candidate || typeof candidate !== 'object' || Array.isArray(candidate)) return;
    for (const definition of this.definitions[collection] ?? []) {
      if (!definition.unique) continue;
      const values = definition.fields.map((field) => firstPathValue(candidate, field));
      if (definition.sparse && values.some((value) => value === undefined)) continue;
      for (const record of records) {
        if (excludeId && record.id === excludeId) continue;
        const existing = definition.fields.map((field) => firstPathValue(record, field));
        if (sameValues(values, existing)) throw indexError('SYNCIO_UNIQUE_INDEX_VIOLATION', `unique index '${definition.name}' rejected duplicate value`);
      }
    }
  }

  #updateIndexes(collection, before, after) {
    for (const definition of this.definitions[collection] ?? []) {
      const index = this.indexes.get(indexKey(collection, definition.name));
      if (before) index?.remove(before);
      if (after) index?.add(after);
    }
  }

  #rebuildCollection(collection) {
    for (const key of [...this.indexes.keys()]) if (key.startsWith(`${collection}\u0000`)) this.indexes.delete(key);
    const records = this.base.collection(collection).all();
    for (const definition of this.definitions[collection] ?? []) {
      this.indexes.set(indexKey(collection, definition.name), new DocumentIndex(definition).rebuild(records));
    }
  }
  #rebuildAll() { this.indexes.clear(); for (const collection of Object.keys(this.definitions)) this.#rebuildCollection(collection); }
  #persistCatalog() {
    const operation = this.catalogQueue.then(() => atomicWriteJson(this.catalogFile, { version: 2, indexes: this.definitions }));
    this.catalogQueue = operation.catch(() => undefined);
    return operation;
  }
}

class DocumentIndex {
  constructor(definition) { this.definition = definition; this.map = new Map(); }
  rebuild(records) { this.map.clear(); for (const record of records) this.add(record); return this; }
  add(record) {
    const values = this.definition.fields.map((field) => firstPathValue(record, field));
    if (this.definition.sparse && values.some((value) => value === undefined)) return;
    const key = stableKey(values);
    const set = this.map.get(key) ?? new Set();
    if (this.definition.unique && set.size && !set.has(record.id)) throw indexError('SYNCIO_UNIQUE_INDEX_VIOLATION', `unique index '${this.definition.name}' contains duplicate value`);
    set.add(record.id);
    this.map.set(key, set);
  }
  remove(record) {
    const values = this.definition.fields.map((field) => firstPathValue(record, field));
    if (this.definition.sparse && values.some((value) => value === undefined)) return;
    const key = stableKey(values);
    const set = this.map.get(key);
    if (!set) return;
    set.delete(record.id);
    if (!set.size) this.map.delete(key);
  }
  find(values) { return [...(this.map.get(stableKey(values)) ?? [])]; }
}

function publicDefinition(collection, definition) {
  if (definition.fields.length === 1 && !definition.unique && !definition.sparse && definition.name === definition.fields[0]) {
    return { collection, field: definition.fields[0] };
  }
  return { collection, name: definition.name, fields: [...definition.fields], unique: definition.unique, sparse: definition.sparse };
}

function normalizeDefinitions(definitions) {
  if (!definitions || typeof definitions !== 'object' || Array.isArray(definitions)) throw new Error('invalid Syncio index catalog');
  const output = {};
  for (const [collection, raw] of Object.entries(definitions)) {
    validateName(collection, 'collection');
    if (!Array.isArray(raw)) throw new Error('invalid Syncio index definitions');
    output[collection] = raw.map((item) => typeof item === 'string'
      ? normalizeDefinition({ name: item, fields: [item], unique: false, sparse: false })
      : normalizeDefinition(item));
    const names = new Set();
    for (const definition of output[collection]) {
      if (names.has(definition.name)) throw new Error(`duplicate index name '${definition.name}'`);
      names.add(definition.name);
    }
  }
  return output;
}

function normalizeDefinition(definition) {
  if (!definition || typeof definition !== 'object' || Array.isArray(definition)) throw new TypeError('index definition must be an object');
  const fields = normalizeFields(definition.fields ?? definition.field);
  const name = definition.name ?? fields.join('_');
  validateName(name, 'name');
  return Object.freeze({ name, fields, unique: definition.unique === true, sparse: definition.sparse === true });
}

function normalizeFields(fields) {
  const list = typeof fields === 'string' ? [fields] : fields;
  if (!Array.isArray(list) || !list.length || list.length > 16) throw new TypeError('index fields must contain 1 to 16 fields');
  const normalized = list.map((field) => { validateName(field, 'field'); return field; });
  if (new Set(normalized).size !== normalized.length) throw new TypeError('index fields must be unique');
  return normalized;
}

function isSimpleEquality(value) { return value !== undefined && !(value && typeof value === 'object' && !Array.isArray(value)); }
function sameValues(left, right) { return stableKey(left) === stableKey(right); }
function sameDefinition(a, b) { return a.name === b.name && a.unique === b.unique && a.sparse === b.sparse && sameValues(a.fields, b.fields); }
function validateName(value, label) { if (typeof value !== 'string' || !/^[A-Za-z0-9_.-]+$/.test(value) || value.length > 128) throw new TypeError(`invalid index ${label}`); }
function indexKey(collection, name) { return `${collection}\u0000${name}`; }
function stableKey(value) { return JSON.stringify(value); }
function indexError(code, message) { const error = new Error(message); error.code = code; error.statusCode = 409; return error; }
async function atomicWriteJson(file, value) { await fs.mkdir(path.dirname(file), { recursive: true }); const temp = `${file}.${process.pid}.${crypto.randomUUID()}.tmp`; try { await fs.writeFile(temp, `${JSON.stringify(value, null, 2)}\n`, { encoding: 'utf8', mode: 0o600 }); const handle = await fs.open(temp, 'r'); try { await handle.sync(); } finally { await handle.close(); } await fs.rename(temp, file); const dir = await fs.open(path.dirname(file), 'r'); try { await dir.sync(); } finally { await dir.close(); } } finally { await fs.rm(temp, { force: true }).catch(() => undefined); } }
