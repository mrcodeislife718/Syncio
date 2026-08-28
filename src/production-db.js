import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { IndexedSyncioDatabase } from './indexed.js';
import { firstPathValue, applyDocumentUpdate } from './document.js';

const clone = (value) => structuredClone(value);

export class ProductionSyncioDatabase {
  constructor(base, metadataFile, metadata) {
    this.base = base;
    this.file = base.file;
    this.metadataFile = metadataFile;
    this.metadata = normalizeMetadata(metadata);
    this.metadataQueue = Promise.resolve();
    this.textIndexes = new Map();
    this.geoIndexes = new Map();
    this.#rebuildSpecialIndexes();
  }

  static async open(file, options = {}) {
    const base = await IndexedSyncioDatabase.open(file, options);
    const metadataFile = path.resolve(`${base.file}.capabilities.json`);
    let metadata = emptyMetadata();
    try { metadata = JSON.parse(await fs.readFile(metadataFile, 'utf8')); }
    catch (error) { if (error.code !== 'ENOENT') { await base.close(); throw error; } }
    try { return new ProductionSyncioDatabase(base, metadataFile, metadata); }
    catch (error) { await base.close(); throw error; }
  }

  get databaseId() { return this.base.databaseId; }
  get sequence() { return this.base.sequence; }
  storageStatus() {
    return {
      ...this.base.storageStatus(),
      capabilities: {
        schemas: Object.keys(this.metadata.schemas).length,
        ttlIndexes: Object.keys(this.metadata.ttl).length,
        textIndexes: Object.keys(this.metadata.text).length,
        geoIndexes: Object.keys(this.metadata.geo).length
      }
    };
  }
  resumeStatus(...args) { return this.base.resumeStatus(...args); }
  watchChanges(...args) { return this.base.watchChanges(...args); }
  changesSince(...args) { return this.base.changesSince(...args); }
  hasAppliedChange(...args) { return this.base.hasAppliedChange(...args); }
  snapshot() { return this.base.snapshot(); }
  checkpoint() { return this.base.checkpoint(); }

  async defineSchema(collection, schema, { mode = 'enforced' } = {}) {
    validateCollectionName(collection);
    if (!['optional', 'enforced'].includes(mode)) throw new TypeError('schema mode must be optional or enforced');
    validateSchemaDefinition(schema);
    if (mode === 'enforced') {
      for (const record of this.base.collection(collection).all()) validateAgainstSchema(record, schema, '$');
    }
    this.metadata.schemas[collection] = { mode, schema: clone(schema) };
    await this.#persistMetadata();
    return this.getSchema(collection);
  }

  async dropSchema(collection) {
    validateCollectionName(collection);
    delete this.metadata.schemas[collection];
    await this.#persistMetadata();
    return true;
  }

  getSchema(collection) {
    validateCollectionName(collection);
    return this.metadata.schemas[collection] ? clone(this.metadata.schemas[collection]) : null;
  }

  async defineTTLIndex(collection, field, { expireAfterSeconds = 0 } = {}) {
    validateCollectionName(collection);
    validateField(field);
    if (!Number.isFinite(expireAfterSeconds) || expireAfterSeconds < 0) throw new TypeError('expireAfterSeconds must be non-negative');
    this.metadata.ttl[collection] = { field, expireAfterSeconds };
    await this.#persistMetadata();
    return clone(this.metadata.ttl[collection]);
  }

  async dropTTLIndex(collection) {
    validateCollectionName(collection);
    delete this.metadata.ttl[collection];
    await this.#persistMetadata();
    return true;
  }

  async defineTextIndex(collection, fields, { name = `text_${normalizeFields(fields).join('_')}` } = {}) {
    validateCollectionName(collection);
    const normalized = normalizeFields(fields);
    validateIndexName(name);
    const definition = { collection, name, fields: normalized };
    this.metadata.text[indexKey(collection, name)] = definition;
    this.#rebuildTextIndex(definition);
    await this.#persistMetadata();
    return clone(definition);
  }

  async defineGeoIndex(collection, field, { name = `geo_${field}` } = {}) {
    validateCollectionName(collection);
    validateField(field);
    validateIndexName(name);
    const definition = { collection, name, field };
    this.metadata.geo[indexKey(collection, name)] = definition;
    this.#rebuildGeoIndex(definition);
    await this.#persistMetadata();
    return clone(definition);
  }

  async dropSpecialIndex(collection, name) {
    validateCollectionName(collection); validateIndexName(name);
    const key = indexKey(collection, name);
    const existed = Boolean(this.metadata.text[key] || this.metadata.geo[key]);
    delete this.metadata.text[key]; delete this.metadata.geo[key];
    this.textIndexes.delete(key); this.geoIndexes.delete(key);
    await this.#persistMetadata();
    return existed;
  }

  listSpecialIndexes() {
    return {
      ttl: Object.entries(this.metadata.ttl).map(([collection, def]) => ({ collection, ...clone(def) })),
      text: Object.values(this.metadata.text).map(clone),
      geo: Object.values(this.metadata.geo).map(clone)
    };
  }

  collection(name) {
    validateCollectionName(name);
    const baseCollection = this.base.collection(name);
    const db = this;
    const refresh = () => db.#rebuildCollectionSpecialIndexes(name);
    return Object.freeze({
      async insert(value) {
        db.#validateRecord(name, value);
        const result = await baseCollection.insert(value);
        refresh();
        return result;
      },
      async upsert(value) {
        db.#validateRecord(name, value);
        const result = await baseCollection.upsert(value);
        refresh();
        return result;
      },
      get: (id) => baseCollection.get(id),
      all: () => baseCollection.all(),
      async remove(id) {
        const result = await baseCollection.remove(id);
        if (result) refresh();
        return result;
      },
      watch: (listener) => baseCollection.watch(listener),
      query: (spec = {}) => db.base.query(name, spec),
      explain: (spec = {}) => db.base.explainQuery(name, spec),
      async update(id, update) {
        const existing = baseCollection.get(id);
        if (!existing) return null;
        const next = applyDocumentUpdate(existing, update);
        db.#validateRecord(name, next);
        const result = await baseCollection.upsert(next);
        refresh();
        return result;
      }
    });
  }

  query(collection, spec = {}) { return this.base.query(collection, spec); }
  explainQuery(collection, spec = {}) { return this.base.explainQuery(collection, spec); }
  defineIndex(...args) { return this.base.defineIndex(...args); }
  dropIndex(...args) { return this.base.dropIndex(...args); }
  listIndexes(...args) { return this.base.listIndexes(...args); }

  search(collection, query, { index, limit = 20 } = {}) {
    validateCollectionName(collection);
    if (typeof query !== 'string' || !query.trim()) return [];
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) throw new TypeError('search limit must be between 1 and 1000');
    const definition = this.#resolveTextIndex(collection, index);
    const store = this.textIndexes.get(indexKey(collection, definition.name));
    const terms = tokenize(query);
    const scores = new Map();
    for (const term of terms) {
      for (const [id, weight] of store.postings.get(term) ?? []) scores.set(id, (scores.get(id) ?? 0) + weight);
    }
    return [...scores.entries()]
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .slice(0, limit)
      .map(([id, score]) => ({ score, record: this.base.collection(collection).get(id) }))
      .filter((row) => row.record);
  }

  near(collection, point, { index, maxDistanceMeters = Infinity, limit = 20 } = {}) {
    validateCollectionName(collection);
    const center = normalizePoint(point);
    if (!(Number.isFinite(maxDistanceMeters) || maxDistanceMeters === Infinity) || maxDistanceMeters < 0) throw new TypeError('maxDistanceMeters must be non-negative');
    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) throw new TypeError('geo limit must be between 1 and 1000');
    const definition = this.#resolveGeoIndex(collection, index);
    const store = this.geoIndexes.get(indexKey(collection, definition.name));
    const rows = [];
    for (const [id, candidate] of store.points) {
      const distanceMeters = haversineMeters(center, candidate);
      if (distanceMeters <= maxDistanceMeters) rows.push({ id, distanceMeters });
    }
    rows.sort((a, b) => a.distanceMeters - b.distanceMeters || a.id.localeCompare(b.id));
    return rows.slice(0, limit)
      .map(({ id, distanceMeters }) => ({ distanceMeters, record: this.base.collection(collection).get(id) }))
      .filter((row) => row.record);
  }

  async sweepExpired({ now = Date.now(), limit = 10_000 } = {}) {
    if (!Number.isFinite(now)) throw new TypeError('TTL sweep time must be finite');
    if (!Number.isSafeInteger(limit) || limit < 1) throw new TypeError('TTL sweep limit must be positive');
    const expired = [];
    outer: for (const [collection, definition] of Object.entries(this.metadata.ttl)) {
      for (const record of this.base.collection(collection).all()) {
        const at = timestamp(firstPathValue(record, definition.field));
        if (at !== null && at + definition.expireAfterSeconds * 1000 <= now) {
          expired.push({ collection, id: record.id });
          if (expired.length >= limit) break outer;
        }
      }
    }
    if (!expired.length) return { removed: 0, records: [] };
    await this.transaction(async (tx) => {
      for (const item of expired) tx.collection(item.collection).remove(item.id);
    });
    return { removed: expired.length, records: expired };
  }

  async transaction(work) {
    if (typeof work !== 'function') throw new TypeError('transaction requires function');
    const db = this;
    const result = await this.base.transaction(async (tx) => work(Object.freeze({
      collection(name) {
        const collection = tx.collection(name);
        return Object.freeze({
          get: (id) => collection.get(id),
          all: () => collection.all(),
          put(record) { db.#validateRecord(name, record); return collection.put(record); },
          remove: (id) => collection.remove(id)
        });
      }
    })));
    this.#rebuildSpecialIndexes();
    return result;
  }

  async applyReplicationChange(change, resolver) {
    if (typeof resolver !== 'function') throw new TypeError('replication resolver must be a function');
    const db = this;
    const result = await this.base.applyReplicationChange(change, (local, remote) => {
      const resolved = resolver(local, remote);
      if (resolved) db.#validateRecord(change.collection, resolved);
      return resolved;
    });
    if (result?.applied) this.#rebuildCollectionSpecialIndexes(change.collection);
    return result;
  }

  async replaceState(state) {
    for (const [collection, records] of Object.entries(state?.collections ?? {})) {
      for (const record of Object.values(records ?? {})) this.#validateRecord(collection, record);
    }
    const result = await this.base.replaceState(state);
    this.#rebuildSpecialIndexes();
    return result;
  }

  async close() {
    await this.metadataQueue;
    await this.base.close();
  }

  #validateRecord(collection, record) {
    const definition = this.metadata.schemas[collection];
    if (!definition || definition.mode === 'optional') return true;
    validateAgainstSchema(record, definition.schema, '$');
    return true;
  }

  #resolveTextIndex(collection, name) {
    const candidates = Object.values(this.metadata.text).filter((item) => item.collection === collection && (!name || item.name === name));
    if (candidates.length !== 1) {
      const error = new Error(name ? 'text index not found' : 'text index name required when collection has zero or multiple text indexes');
      error.code = 'SYNCIO_TEXT_INDEX_NOT_FOUND';
      throw error;
    }
    return candidates[0];
  }

  #resolveGeoIndex(collection, name) {
    const candidates = Object.values(this.metadata.geo).filter((item) => item.collection === collection && (!name || item.name === name));
    if (candidates.length !== 1) {
      const error = new Error(name ? 'geo index not found' : 'geo index name required when collection has zero or multiple geo indexes');
      error.code = 'SYNCIO_GEO_INDEX_NOT_FOUND';
      throw error;
    }
    return candidates[0];
  }

  #rebuildSpecialIndexes() {
    this.textIndexes.clear();
    this.geoIndexes.clear();
    for (const definition of Object.values(this.metadata.text)) this.#rebuildTextIndex(definition);
    for (const definition of Object.values(this.metadata.geo)) this.#rebuildGeoIndex(definition);
  }

  #rebuildCollectionSpecialIndexes(collection) {
    for (const definition of Object.values(this.metadata.text).filter((item) => item.collection === collection)) this.#rebuildTextIndex(definition);
    for (const definition of Object.values(this.metadata.geo).filter((item) => item.collection === collection)) this.#rebuildGeoIndex(definition);
  }

  #rebuildTextIndex(definition) {
    const postings = new Map();
    for (const record of this.base.collection(definition.collection).all()) {
      const counts = new Map();
      for (const field of definition.fields) {
        for (const term of tokenize(flattenText(firstPathValue(record, field)))) counts.set(term, (counts.get(term) ?? 0) + 1);
      }
      for (const [term, weight] of counts) {
        const ids = postings.get(term) ?? new Map();
        ids.set(record.id, weight);
        postings.set(term, ids);
      }
    }
    this.textIndexes.set(indexKey(definition.collection, definition.name), { postings });
  }

  #rebuildGeoIndex(definition) {
    const points = new Map();
    for (const record of this.base.collection(definition.collection).all()) {
      const raw = firstPathValue(record, definition.field);
      if (raw === undefined || raw === null) continue;
      try { points.set(record.id, normalizePoint(raw)); } catch {}
    }
    this.geoIndexes.set(indexKey(definition.collection, definition.name), { points });
  }

  #persistMetadata() {
    const operation = this.metadataQueue.then(() => atomicWriteJson(this.metadataFile, this.metadata));
    this.metadataQueue = operation.catch(() => undefined);
    return operation;
  }
}

export async function openProduction(file, options) {
  return ProductionSyncioDatabase.open(file, options);
}

function emptyMetadata() { return { version: 1, schemas: {}, ttl: {}, text: {}, geo: {} }; }
function normalizeMetadata(value) {
  if (!value || value.version !== 1) throw new Error('invalid Syncio capability metadata');
  const metadata = { version: 1, schemas: value.schemas ?? {}, ttl: value.ttl ?? {}, text: value.text ?? {}, geo: value.geo ?? {} };
  for (const [collection, def] of Object.entries(metadata.schemas)) {
    validateCollectionName(collection);
    if (!['optional', 'enforced'].includes(def.mode)) throw new Error('invalid schema mode');
    validateSchemaDefinition(def.schema);
  }
  for (const [collection, def] of Object.entries(metadata.ttl)) {
    validateCollectionName(collection); validateField(def.field);
    if (!Number.isFinite(def.expireAfterSeconds) || def.expireAfterSeconds < 0) throw new Error('invalid TTL definition');
  }
  for (const def of Object.values(metadata.text)) { validateCollectionName(def.collection); validateIndexName(def.name); normalizeFields(def.fields); }
  for (const def of Object.values(metadata.geo)) { validateCollectionName(def.collection); validateIndexName(def.name); validateField(def.field); }
  return metadata;
}
function validateCollectionName(value) { if (typeof value !== 'string' || !/^[A-Za-z0-9_-]+$/.test(value) || value.length > 128) throw new TypeError('invalid collection name'); }
function validateField(value) { if (typeof value !== 'string' || !value || value.length > 256 || value.split('.').some((part) => !part)) throw new TypeError('invalid field path'); }
function validateIndexName(value) { if (typeof value !== 'string' || !/^[A-Za-z0-9_.-]+$/.test(value) || value.length > 128) throw new TypeError('invalid index name'); }
function normalizeFields(fields) { const result = Array.isArray(fields) ? fields : [fields]; if (!result.length) throw new TypeError('index fields required'); for (const field of result) validateField(field); return [...new Set(result)]; }
function indexKey(collection, name) { return `${collection}\u0000${name}`; }
function timestamp(value) { if (typeof value === 'number' && Number.isFinite(value)) return value; if (typeof value === 'string') { const parsed = Date.parse(value); return Number.isFinite(parsed) ? parsed : null; } return null; }
function flattenText(value) { if (Array.isArray(value)) return value.map(flattenText).join(' '); if (value && typeof value === 'object') return Object.values(value).map(flattenText).join(' '); return typeof value === 'string' || typeof value === 'number' ? String(value) : ''; }
function tokenize(value) { return String(value ?? '').normalize('NFKC').toLocaleLowerCase('en-US').match(/[\p{L}\p{N}]+/gu) ?? []; }
function normalizePoint(value) {
  let lon; let lat;
  if (Array.isArray(value) && value.length === 2) [lon, lat] = value;
  else if (value && typeof value === 'object') { lon = value.lon ?? value.lng ?? value.longitude; lat = value.lat ?? value.latitude; }
  if (!Number.isFinite(lon) || !Number.isFinite(lat) || lon < -180 || lon > 180 || lat < -90 || lat > 90) throw new TypeError('geo point requires valid longitude and latitude');
  return [lon, lat];
}
function haversineMeters([lon1, lat1], [lon2, lat2]) { const r = 6371008.8; const rad = Math.PI / 180; const a = Math.sin((lat2-lat1)*rad/2)**2 + Math.cos(lat1*rad)*Math.cos(lat2*rad)*Math.sin((lon2-lon1)*rad/2)**2; return 2*r*Math.asin(Math.sqrt(a)); }

function validateSchemaDefinition(schema) {
  if (!schema || typeof schema !== 'object' || Array.isArray(schema)) throw new TypeError('schema must be an object');
  if (schema.type) normalizeTypes(schema.type);
  if (schema.required && (!Array.isArray(schema.required) || schema.required.some((item) => typeof item !== 'string'))) throw new TypeError('schema.required must be string array');
  if (schema.properties) {
    if (typeof schema.properties !== 'object' || Array.isArray(schema.properties)) throw new TypeError('schema.properties must be object');
    for (const child of Object.values(schema.properties)) validateSchemaDefinition(child);
  }
  if (schema.items) validateSchemaDefinition(schema.items);
  if (schema.enum && !Array.isArray(schema.enum)) throw new TypeError('schema.enum must be array');
}
function normalizeTypes(type) { const types = Array.isArray(type) ? type : [type]; const allowed = new Set(['object','array','string','number','integer','boolean','null']); if (!types.length || types.some((item) => !allowed.has(item))) throw new TypeError('invalid schema type'); return types; }
export function validateAgainstSchema(value, schema, location = '$') {
  validateSchemaDefinition(schema);
  if (schema.type && !normalizeTypes(schema.type).some((type) => matchesType(value, type))) throw schemaError(location, `expected ${[].concat(schema.type).join('|')}`);
  if (schema.enum && !schema.enum.some((candidate) => JSON.stringify(candidate) === JSON.stringify(value))) throw schemaError(location, 'value is not in enum');
  if (typeof value === 'string') {
    if (Number.isSafeInteger(schema.minLength) && value.length < schema.minLength) throw schemaError(location, 'string shorter than minLength');
    if (Number.isSafeInteger(schema.maxLength) && value.length > schema.maxLength) throw schemaError(location, 'string longer than maxLength');
    if (schema.pattern && !(new RegExp(schema.pattern, 'u')).test(value)) throw schemaError(location, 'string does not match pattern');
  }
  if (typeof value === 'number') {
    if (Number.isFinite(schema.minimum) && value < schema.minimum) throw schemaError(location, 'number below minimum');
    if (Number.isFinite(schema.maximum) && value > schema.maximum) throw schemaError(location, 'number above maximum');
  }
  if (Array.isArray(value)) {
    if (Number.isSafeInteger(schema.minItems) && value.length < schema.minItems) throw schemaError(location, 'array shorter than minItems');
    if (Number.isSafeInteger(schema.maxItems) && value.length > schema.maxItems) throw schemaError(location, 'array longer than maxItems');
    if (schema.items) value.forEach((item, index) => validateAgainstSchema(item, schema.items, `${location}[${index}]`));
  }
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    for (const required of schema.required ?? []) if (!(required in value)) throw schemaError(`${location}.${required}`, 'required property missing');
    for (const [key, child] of Object.entries(schema.properties ?? {})) if (key in value) validateAgainstSchema(value[key], child, `${location}.${key}`);
    if (schema.additionalProperties === false) for (const key of Object.keys(value)) if (!(key in (schema.properties ?? {}))) throw schemaError(`${location}.${key}`, 'additional property not allowed');
  }
  return true;
}
function matchesType(value, type) { if (type === 'null') return value === null; if (type === 'array') return Array.isArray(value); if (type === 'object') return value !== null && typeof value === 'object' && !Array.isArray(value); if (type === 'integer') return Number.isInteger(value); if (type === 'number') return typeof value === 'number' && Number.isFinite(value); return typeof value === type; }
function schemaError(pathname, message) { const error = new Error(`schema validation failed at ${pathname}: ${message}`); error.code = 'SYNCIO_SCHEMA_VALIDATION_FAILED'; error.path = pathname; return error; }
async function atomicWriteJson(target, value) { const file = path.resolve(target); await fs.mkdir(path.dirname(file), { recursive: true }); const temp = `${file}.${process.pid}.${crypto.randomUUID()}.tmp`; try { await fs.writeFile(temp, `${JSON.stringify(value, null, 2)}\n`, { encoding: 'utf8', mode: 0o600 }); const handle = await fs.open(temp, 'r'); try { await handle.sync(); } finally { await handle.close(); } await fs.rename(temp, file); const dir = await fs.open(path.dirname(file), 'r'); try { await dir.sync(); } finally { await dir.close(); } } finally { await fs.rm(temp, { force: true }).catch(() => undefined); } }
