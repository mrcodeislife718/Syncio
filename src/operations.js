import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';

const clone = (value) => structuredClone(value);

export class DurableOfflineQueue {
  constructor(file, state = { version: 1, items: [] }) {
    this.file = path.resolve(file);
    this.state = state;
    this.queue = Promise.resolve();
  }

  static async open(file) {
    const target = path.resolve(file);
    let state = { version: 1, items: [] };
    try { state = JSON.parse(await fs.readFile(target, 'utf8')); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
    if (!state || state.version !== 1 || !Array.isArray(state.items)) throw new Error('invalid durable offline queue state');
    return new DurableOfflineQueue(target, state);
  }

  get size() { return this.state.items.length; }
  list() { return clone(this.state.items); }

  async enqueue(change, { idempotencyKey = crypto.randomUUID() } = {}) {
    if (!change || typeof change !== 'object' || Array.isArray(change)) throw new TypeError('queue change must be an object');
    return this.#mutate((draft) => {
      if (draft.items.some((item) => item.idempotencyKey === idempotencyKey)) return clone(draft.items.find((item) => item.idempotencyKey === idempotencyKey));
      const item = { queueId: crypto.randomUUID(), idempotencyKey, attempts: 0, enqueuedAt: new Date().toISOString(), change: clone(change) };
      draft.items.push(item);
      return clone(item);
    });
  }

  async flush(send, { maxAttempts = 20 } = {}) {
    if (typeof send !== 'function') throw new TypeError('queue flush requires send function');
    const failures = [];
    let delivered = 0;
    while (true) {
      const item = this.state.items[0];
      if (!item) break;
      if (item.attempts >= maxAttempts) {
        failures.push({ item: clone(item), code: 'max_attempts_exceeded' });
        break;
      }
      await this.#mutate((draft) => { draft.items[0].attempts += 1; draft.items[0].lastAttemptAt = new Date().toISOString(); });
      try {
        await send(clone(this.state.items[0]));
        await this.#mutate((draft) => { draft.items.shift(); });
        delivered += 1;
      } catch (error) {
        failures.push({ item: clone(this.state.items[0]), error });
        break;
      }
    }
    return { delivered, pending: this.size, failures };
  }

  async #mutate(work) {
    const operation = this.queue.then(async () => {
      const draft = clone(this.state);
      const result = work(draft);
      await atomicWriteJson(this.file, draft);
      this.state = draft;
      return result;
    });
    this.queue = operation.catch(() => undefined);
    return operation;
  }
}

export function createTokenAuthority(secret, { issuer = 'syncio', ttlSeconds = 3600 } = {}) {
  const key = Buffer.isBuffer(secret) ? secret : Buffer.from(String(secret ?? ''));
  if (key.length < 32) throw new TypeError('token authority requires at least 32 bytes of secret material');
  if (!Number.isSafeInteger(ttlSeconds) || ttlSeconds < 1) throw new TypeError('ttlSeconds must be a positive safe integer');

  function sign(payload) {
    const body = base64url(JSON.stringify(payload));
    const sig = crypto.createHmac('sha256', key).update(body).digest('base64url');
    return `${body}.${sig}`;
  }

  return Object.freeze({
    issue({ subject, projectId, role = 'member', entitlements = [], expiresInSeconds = ttlSeconds } = {}) {
      if (!subject || !projectId) throw new TypeError('subject and projectId are required');
      const now = Math.floor(Date.now() / 1000);
      return sign({ iss: issuer, sub: subject, projectId, role, entitlements: [...new Set(entitlements)], iat: now, exp: now + expiresInSeconds, jti: crypto.randomUUID() });
    },
    verify(token) {
      if (typeof token !== 'string' || token.length > 8192) return null;
      const [body, sig, extra] = token.split('.');
      if (!body || !sig || extra) return null;
      const expected = crypto.createHmac('sha256', key).update(body).digest();
      let supplied;
      try { supplied = Buffer.from(sig, 'base64url'); } catch { return null; }
      if (supplied.toString('base64url') !== sig) return null;
      if (supplied.length !== expected.length || !crypto.timingSafeEqual(supplied, expected)) return null;
      let payload;
      try { payload = JSON.parse(Buffer.from(body, 'base64url').toString('utf8')); } catch { return null; }
      if (Buffer.from(body, 'base64url').toString('base64url') !== body) return null;
      const now = Math.floor(Date.now() / 1000);
      if (payload.iss !== issuer || !payload.sub || !payload.projectId || !Number.isSafeInteger(payload.exp) || payload.exp <= now) return null;
      return clone(payload);
    },
    authenticateRequest(req) {
      const header = req?.headers?.authorization;
      if (typeof header !== 'string' || !header.startsWith('Bearer ')) return null;
      return this.verify(header.slice(7));
    }
  });
}

export function createEntitlementGate({ requiredByAction = {} } = {}) {
  return Object.freeze({
    authorize(context) {
      const required = requiredByAction[context.action] ?? requiredByAction['*'];
      if (!required) return true;
      const grants = new Set(context.user?.entitlements ?? []);
      return grants.has('*') || grants.has(required);
    }
  });
}

export class MetricsRegistry {
  constructor() { this.counters = new Map(); this.gauges = new Map(); this.histograms = new Map(); }
  inc(name, value = 1) { assertMetric(name, value); this.counters.set(name, (this.counters.get(name) ?? 0) + value); }
  gauge(name, value) { assertMetric(name, value); this.gauges.set(name, value); }
  observe(name, value) { assertMetric(name, value); const values = this.histograms.get(name) ?? []; values.push(value); this.histograms.set(name, values); }
  snapshot() {
    const histograms = {};
    for (const [name, values] of this.histograms) {
      const sorted = [...values].sort((a,b)=>a-b);
      histograms[name] = { count: values.length, sum: values.reduce((a,b)=>a+b,0), p50: percentile(sorted, .5), p95: percentile(sorted, .95), p99: percentile(sorted, .99) };
    }
    return { counters: Object.fromEntries(this.counters), gauges: Object.fromEntries(this.gauges), histograms };
  }
  observer() {
    return (event) => {
      this.inc(`requests_total:${event.status ?? 'unknown'}`);
      if (Number.isFinite(event.durationMs)) this.observe('request_duration_ms', event.durationMs);
      if (event.type === 'request_error') this.inc('request_errors_total');
    };
  }
}

export class AuditLog {
  constructor(file) { this.file = path.resolve(file); this.queue = Promise.resolve(); }
  async append(event) {
    if (!event || typeof event !== 'object' || Array.isArray(event)) throw new TypeError('audit event must be an object');
    const record = { id: crypto.randomUUID(), at: new Date().toISOString(), ...clone(event) };
    const line = `${JSON.stringify(record)}\n`;
    const operation = this.queue.then(async () => {
      await fs.mkdir(path.dirname(this.file), { recursive: true });
      const handle = await fs.open(this.file, 'a', 0o600);
      try { await handle.writeFile(line, 'utf8'); await handle.sync(); } finally { await handle.close(); }
      return clone(record);
    });
    this.queue = operation.catch(() => undefined);
    return operation;
  }
  async readAll() {
    try { return (await fs.readFile(this.file, 'utf8')).split('\n').filter(Boolean).map((line) => JSON.parse(line)); }
    catch (error) { if (error.code === 'ENOENT') return []; throw error; }
  }
}

export function createEncryptedBackupManager({ key }) {
  const material = Buffer.isBuffer(key) ? key : Buffer.from(key ?? '');
  if (material.length !== 32) throw new TypeError('backup encryption key must be exactly 32 bytes');
  return Object.freeze({
    async backup({ state, file, metadata = {} }) {
      const iv = crypto.randomBytes(12);
      const cipher = crypto.createCipheriv('aes-256-gcm', material, iv);
      const plaintext = Buffer.from(JSON.stringify({ version: 1, createdAt: new Date().toISOString(), metadata, state }));
      const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()]);
      const tag = cipher.getAuthTag();
      const envelope = { format: 'syncio-backup/1', iv: iv.toString('base64'), tag: tag.toString('base64'), ciphertext: ciphertext.toString('base64'), digest: crypto.createHash('sha256').update(ciphertext).digest('hex') };
      await atomicWriteJson(file, envelope);
      return { file: path.resolve(file), bytes: ciphertext.length, digest: envelope.digest };
    },
    async restore(file) {
      const envelope = JSON.parse(await fs.readFile(file, 'utf8'));
      if (envelope.format !== 'syncio-backup/1') throw new Error('unsupported backup format');
      const ciphertext = Buffer.from(envelope.ciphertext, 'base64');
      const digest = crypto.createHash('sha256').update(ciphertext).digest('hex');
      if (digest !== envelope.digest) throw new Error('backup digest mismatch');
      const decipher = crypto.createDecipheriv('aes-256-gcm', material, Buffer.from(envelope.iv, 'base64'));
      decipher.setAuthTag(Buffer.from(envelope.tag, 'base64'));
      const plaintext = Buffer.concat([decipher.update(ciphertext), decipher.final()]);
      const payload = JSON.parse(plaintext.toString('utf8'));
      if (payload.version !== 1 || !payload.state) throw new Error('invalid backup payload');
      return clone(payload);
    }
  });
}

function base64url(value) { return Buffer.from(value).toString('base64url'); }
function percentile(sorted, p) { if (!sorted.length) return null; return sorted[Math.min(sorted.length - 1, Math.floor((sorted.length - 1) * p))]; }
function assertMetric(name, value) { if (typeof name !== 'string' || !name || !Number.isFinite(value)) throw new TypeError('metric name and finite value are required'); }

async function atomicWriteJson(target, value) {
  const file = path.resolve(target);
  await fs.mkdir(path.dirname(file), { recursive: true });
  const temp = `${file}.${process.pid}.${crypto.randomUUID()}.tmp`;
  try {
    await fs.writeFile(temp, `${JSON.stringify(value, null, 2)}\n`, { encoding: 'utf8', mode: 0o600 });
    const handle = await fs.open(temp, 'r');
    try { await handle.sync(); } finally { await handle.close(); }
    await fs.rename(temp, file);
    const dir = await fs.open(path.dirname(file), 'r');
    try { await dir.sync(); } finally { await dir.close(); }
  } finally { await fs.rm(temp, { force: true }).catch(() => undefined); }
}
