import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';

const FORMAT = 'syncio-wal/1';

export class WriteAheadLog {
  constructor(file, entries = []) {
    this.file = path.resolve(file);
    this.entries = entries;
    this.queue = Promise.resolve();
  }

  static async open(file) {
    const target = path.resolve(file);
    const entries = await readWalEntries(target);
    return new WriteAheadLog(target, entries);
  }

  listAfter(sequence = 0) {
    assertSequence(sequence, 'WAL cursor');
    return this.entries.filter((entry) => entry.resultSequence > sequence).map((entry) => structuredClone(entry));
  }

  async append({ databaseId, baseSequence, resultSequence, events } = {}) {
    if (typeof databaseId !== 'string' || !databaseId) throw new TypeError('WAL databaseId is required');
    assertSequence(baseSequence, 'WAL baseSequence');
    assertSequence(resultSequence, 'WAL resultSequence');
    if (resultSequence <= baseSequence) throw new TypeError('WAL resultSequence must advance baseSequence');
    if (!Array.isArray(events) || !events.length) throw new TypeError('WAL events must be a non-empty array');
    const payload = {
      format: FORMAT,
      databaseId,
      baseSequence,
      resultSequence,
      events: structuredClone(events)
    };
    const entry = { ...payload, digest: digest(payload) };
    return this.#enqueue(async () => {
      await fs.mkdir(path.dirname(this.file), { recursive: true });
      const handle = await fs.open(this.file, 'a', 0o600);
      try {
        await handle.writeFile(`${JSON.stringify(entry)}\n`, 'utf8');
        await handle.sync();
      } finally {
        await handle.close();
      }
      this.entries.push(entry);
      return structuredClone(entry);
    });
  }

  async compactThrough(sequence) {
    assertSequence(sequence, 'WAL compaction sequence');
    return this.#enqueue(async () => {
      const remaining = this.entries.filter((entry) => entry.resultSequence > sequence);
      await atomicReplace(this.file, remaining.map((entry) => `${JSON.stringify(entry)}\n`).join(''));
      this.entries = remaining;
      return remaining.length;
    });
  }

  async reset() {
    return this.#enqueue(async () => {
      await atomicReplace(this.file, '');
      this.entries = [];
    });
  }

  async close() { await this.queue; }

  #enqueue(work) {
    const operation = this.queue.then(work);
    this.queue = operation.catch(() => undefined);
    return operation;
  }
}

export async function readWalEntries(file) {
  let text;
  try { text = await fs.readFile(file, 'utf8'); }
  catch (error) { if (error.code === 'ENOENT') return []; throw error; }
  const terminated = text.endsWith('\n');
  const lines = text.split('\n');
  if (!terminated && lines.length) lines.pop();
  const entries = [];
  for (let index = 0; index < lines.length; index++) {
    const line = lines[index];
    if (!line) continue;
    let entry;
    try { entry = JSON.parse(line); }
    catch (error) { throw corruptWal(file, index + 1, error); }
    validateEntry(entry, file, index + 1);
    if (entries.length && entry.baseSequence !== entries.at(-1).resultSequence) {
      throw corruptWal(file, index + 1, new Error('non-contiguous WAL sequence'));
    }
    entries.push(entry);
  }
  return entries;
}

function validateEntry(entry, file, line) {
  if (!entry || entry.format !== FORMAT || typeof entry.databaseId !== 'string' || !entry.databaseId) {
    throw corruptWal(file, line, new Error('invalid WAL envelope'));
  }
  assertSequence(entry.baseSequence, 'WAL baseSequence');
  assertSequence(entry.resultSequence, 'WAL resultSequence');
  if (entry.resultSequence <= entry.baseSequence || !Array.isArray(entry.events) || !entry.events.length) {
    throw corruptWal(file, line, new Error('invalid WAL sequence/events'));
  }
  const payload = {
    format: entry.format,
    databaseId: entry.databaseId,
    baseSequence: entry.baseSequence,
    resultSequence: entry.resultSequence,
    events: entry.events
  };
  if (typeof entry.digest !== 'string' || entry.digest !== digest(payload)) {
    throw corruptWal(file, line, new Error('WAL digest mismatch'));
  }
}

function corruptWal(file, line, cause) {
  const error = new Error(`Syncio WAL is corrupted at ${path.resolve(file)}:${line}`);
  error.code = 'SYNCIO_CORRUPT_WAL';
  error.cause = cause;
  return error;
}

function assertSequence(value, label) {
  if (!Number.isSafeInteger(value) || value < 0) throw new TypeError(`${label} must be a non-negative safe integer`);
}

function digest(value) {
  return crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
}

async function atomicReplace(target, data) {
  await fs.mkdir(path.dirname(target), { recursive: true });
  const temp = `${target}.${process.pid}.${crypto.randomUUID()}.tmp`;
  try {
    await fs.writeFile(temp, data, { encoding: 'utf8', mode: 0o600 });
    const handle = await fs.open(temp, 'r');
    try { await handle.sync(); } finally { await handle.close(); }
    await fs.rename(temp, target);
    const directory = await fs.open(path.dirname(target), 'r');
    try { await directory.sync(); } finally { await directory.close(); }
  } finally {
    await fs.rm(temp, { force: true }).catch(() => undefined);
  }
}
