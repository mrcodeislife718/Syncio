import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
export * from './advanced.js';

export class SyncioDatabase {
  constructor(file, state) {
    this.file = file;
    this.state = state;
    this.listeners = new Map();
    this.writeQueue = Promise.resolve();
  }

  static async open(file) {
    const target = path.resolve(file);
    let state = { version: 1, collections: {} };
    try {
      const text = await fs.readFile(target, 'utf8');
      state = JSON.parse(text);
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
    return new SyncioDatabase(target, state);
  }

  collection(name) {
    if (!/^[A-Za-z0-9_-]+$/.test(name)) throw new TypeError('Syncio collection names may contain letters, numbers, _ and -');
    if (!this.state.collections[name]) this.state.collections[name] = {};
    const db = this;

    return Object.freeze({
      async insert(value) {
        if (!value || typeof value !== 'object' || Array.isArray(value)) throw new TypeError('Syncio insert requires an object');
        const id = value.id ?? crypto.randomUUID();
        const record = structuredClone({ ...value, id });
        await db.#mutate(name, () => {
          if (db.state.collections[name][id]) throw new Error(`Syncio record '${id}' already exists in '${name}'`);
          db.state.collections[name][id] = record;
        }, { type: 'insert', id, record });
        return structuredClone(record);
      },
      async upsert(value) {
        if (!value || typeof value !== 'object' || Array.isArray(value)) throw new TypeError('Syncio upsert requires an object');
        if (!value.id) throw new TypeError('Syncio upsert requires value.id');
        const record = structuredClone(value);
        await db.#mutate(name, () => { db.state.collections[name][record.id] = record; }, { type: 'upsert', id: record.id, record });
        return structuredClone(record);
      },
      get(id) { const value = db.state.collections[name][id]; return value ? structuredClone(value) : null; },
      all() { return Object.values(db.state.collections[name]).map((value) => structuredClone(value)); },
      async remove(id) {
        let existed = false;
        await db.#mutate(name, () => { existed = Boolean(db.state.collections[name][id]); delete db.state.collections[name][id]; }, { type: 'remove', id });
        return existed;
      },
      watch(listener) {
        if (typeof listener !== 'function') throw new TypeError('Syncio watch requires a function');
        const set = db.listeners.get(name) ?? new Set(); set.add(listener); db.listeners.set(name, set);
        return () => { set.delete(listener); if (set.size === 0) db.listeners.delete(name); };
      },
    });
  }

  snapshot() { return structuredClone(this.state); }
  async replaceState(nextState) { this.state = structuredClone(nextState); await this.#persist(); return this.snapshot(); }
  async close() { await this.writeQueue; }

  async #mutate(collection, mutation, event) {
    this.writeQueue = this.writeQueue.then(async () => {
      mutation(); await this.#persist();
      const payload = structuredClone({ collection, ...event });
      for (const listener of this.listeners.get(collection) ?? []) queueMicrotask(() => listener(payload));
    });
    return this.writeQueue;
  }

  async #persist() {
    await fs.mkdir(path.dirname(this.file), { recursive: true });
    const temp = `${this.file}.${process.pid}.${crypto.randomUUID()}.tmp`;
    await fs.writeFile(temp, JSON.stringify(this.state, null, 2), 'utf8');
    await fs.rename(temp, this.file);
  }
}

export async function open(file) { return SyncioDatabase.open(file); }
