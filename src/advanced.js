import fs from 'node:fs/promises';
import crypto from 'node:crypto';

const clone = (value) => structuredClone(value);

export class QueryIndex {
  constructor(field) { this.field = field; this.map = new Map(); }
  rebuild(records) { this.map.clear(); for (const record of records) this.add(record); return this; }
  add(record) { const key = stableKey(record[this.field]); const set = this.map.get(key) ?? new Set(); set.add(record.id); this.map.set(key, set); }
  remove(record) { const key = stableKey(record[this.field]); const set = this.map.get(key); if (!set) return; set.delete(record.id); if (!set.size) this.map.delete(key); }
  find(value) { return [...(this.map.get(stableKey(value)) ?? [])]; }
}

export function queryRecords(records, spec = {}) {
  let output = [...records];
  if (spec.where) output = output.filter((record) => matchWhere(record, spec.where));
  if (spec.orderBy) {
    const [{ field, direction = 'asc' }] = Array.isArray(spec.orderBy) ? spec.orderBy : [spec.orderBy];
    output.sort((a, b) => compare(a[field], b[field]) * (direction === 'desc' ? -1 : 1));
  }
  if (Number.isInteger(spec.offset) && spec.offset > 0) output = output.slice(spec.offset);
  if (Number.isInteger(spec.limit) && spec.limit >= 0) output = output.slice(0, spec.limit);
  return output.map(clone);
}

export class TransactionManager {
  constructor(snapshot, commit) { this.snapshot = snapshot; this.commit = commit; this.queue = Promise.resolve(); }
  run(work) {
    const operation = this.queue.then(async () => {
      const draft = structuredClone(this.snapshot());
      const api = createDraftApi(draft);
      const result = await work(api);
      await this.commit(draft);
      return result;
    });
    this.queue = operation.catch(() => {});
    return operation;
  }
}

export class ChangeLog {
  constructor(nodeId = crypto.randomUUID()) { this.nodeId = nodeId; this.clock = 0; this.entries = []; }
  append(change) { const entry = { id: `${this.nodeId}:${++this.clock}`, nodeId: this.nodeId, clock: this.clock, at: new Date().toISOString(), ...structuredClone(change) }; this.entries.push(entry); return structuredClone(entry); }
  since(cursor = 0) { return this.entries.filter((entry) => entry.clock > cursor).map(clone); }
  merge(entries) { for (const entry of entries) { if (!this.entries.some((item) => item.id === entry.id)) this.entries.push(structuredClone(entry)); } this.entries.sort((a,b) => a.clock-b.clock || a.nodeId.localeCompare(b.nodeId)); return this.entries.length; }
}

export function resolveConflict(local, remote, strategy = 'last-write-wins') {
  if (!local) return structuredClone(remote);
  if (!remote) return structuredClone(local);
  if (typeof strategy === 'function') return strategy(structuredClone(local), structuredClone(remote));
  if (strategy === 'local') return structuredClone(local);
  if (strategy === 'remote') return structuredClone(remote);
  if (strategy === 'merge') return { ...structuredClone(local), ...structuredClone(remote) };
  const localTs = Date.parse(local.updatedAt ?? local._updatedAt ?? 0) || 0;
  const remoteTs = Date.parse(remote.updatedAt ?? remote._updatedAt ?? 0) || 0;
  return structuredClone(remoteTs >= localTs ? remote : local);
}

export class OfflineQueue {
  constructor() { this.items = []; }
  enqueue(change) { const item = { queueId: crypto.randomUUID(), attempts: 0, ...structuredClone(change) }; this.items.push(item); return structuredClone(item); }
  async flush(send) {
    const failures = [];
    while (this.items.length) {
      const item = this.items[0]; item.attempts++;
      try { await send(structuredClone(item)); this.items.shift(); }
      catch (error) { failures.push({ item: structuredClone(item), error }); break; }
    }
    return { pending: this.items.length, failures };
  }
}

export function createPolicyEngine(rules = []) {
  const normalized = rules.map((rule) => ({ effect: rule.effect ?? 'deny', ...rule }));
  return {
    authorize(context) {
      for (const rule of normalized) {
        if (rule.collection && rule.collection !== context.collection) continue;
        if (rule.action && rule.action !== context.action) continue;
        if (typeof rule.when === 'function' && !rule.when(context)) continue;
        return rule.effect === 'allow';
      }
      return false;
    }
  };
}

export async function exportDatabase(state, file) { await fs.writeFile(file, JSON.stringify(state, null, 2), 'utf8'); return file; }
export async function importDatabase(file) { return JSON.parse(await fs.readFile(file, 'utf8')); }

export function migrate(state, migrations = []) {
  let current = structuredClone(state);
  for (const migration of [...migrations].sort((a,b) => a.version-b.version)) {
    if ((current.version ?? 0) >= migration.version) continue;
    current = migration.up(structuredClone(current)); current.version = migration.version;
  }
  return current;
}

export function createReplicationPacket({ from, cursor = 0, changes = [] }) {
  return { protocol: 'syncio-replication/1', from, cursor, changes: changes.map(clone), digest: digest({ from, cursor, changes }) };
}
export function verifyReplicationPacket(packet) { return packet?.protocol === 'syncio-replication/1' && packet.digest === digest({ from: packet.from, cursor: packet.cursor, changes: packet.changes }); }

function createDraftApi(draft) {
  return {
    collection(name) {
      draft.collections ??= {}; draft.collections[name] ??= {};
      return {
        get: (id) => structuredClone(draft.collections[name][id] ?? null),
        put: (record) => { if (!record?.id) throw new Error('transaction put requires id'); draft.collections[name][record.id] = structuredClone(record); },
        remove: (id) => delete draft.collections[name][id],
        all: () => Object.values(draft.collections[name]).map(clone)
      };
    }
  };
}
function matchWhere(record, where) { return Object.entries(where).every(([field, expected]) => { const actual = record[field]; if (expected && typeof expected === 'object' && !Array.isArray(expected)) { if ('$gt' in expected && !(actual > expected.$gt)) return false; if ('$gte' in expected && !(actual >= expected.$gte)) return false; if ('$lt' in expected && !(actual < expected.$lt)) return false; if ('$lte' in expected && !(actual <= expected.$lte)) return false; if ('$in' in expected && !expected.$in.includes(actual)) return false; return true; } return Object.is(actual, expected); }); }
function compare(a,b) { if (Object.is(a,b)) return 0; return a > b ? 1 : -1; }
function stableKey(value) { return JSON.stringify(value); }
function digest(value) { return crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex'); }
