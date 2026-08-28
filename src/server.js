import http from 'node:http';
import crypto from 'node:crypto';
import { OfflineQueue, createPolicyEngine, createReplicationPacket, verifyReplicationPacket, createSnapshotPacket, verifySnapshotPacket, resolveConflict, queryRecords } from './advanced.js';

const DEFAULT_MAX_BODY_BYTES = 1_048_576;
const DEFAULT_REPLICATION_LIMIT = 1_000;

export function createSyncioServer({
  db,
  policies = [],
  authenticate = async () => null,
  nodeId = db?.databaseId ?? crypto.randomUUID(),
  conflictStrategy = 'last-write-wins',
  maxBodyBytes = DEFAULT_MAX_BODY_BYTES,
  replicationLimit = DEFAULT_REPLICATION_LIMIT,
  observe = () => undefined
} = {}) {
  if (!db) throw new Error('Syncio server requires a database');
  if (!Number.isSafeInteger(maxBodyBytes) || maxBodyBytes < 1) throw new TypeError('maxBodyBytes must be a positive safe integer');
  if (!Number.isSafeInteger(replicationLimit) || replicationLimit < 1 || replicationLimit > 10_000) throw new TypeError('replicationLimit must be between 1 and 10000');

  const policy = createPolicyEngine(policies);
  const subscriptions = new Map();

  const server = http.createServer(async (req, res) => {
    const requestId = requestIdFor(req);
    const startedAt = process.hrtime.bigint();
    res.setHeader('x-syncio-request-id', requestId);

    try {
      const url = new URL(req.url, 'http://syncio.local');
      if (req.method === 'GET' && url.pathname === '/health') {
        return respond(res, 200, { ok: true, nodeId, sequence: db.sequence }, { observe, requestId, req, startedAt });
      }

      const user = await authenticate(req);
      const parts = url.pathname.split('/').filter(Boolean);

      if (req.method === 'POST' && url.pathname === '/replicate/pull') {
        if (!authorize(policy, { user, action: 'replicate', collection: '*' }, res, requestId)) return finishObserved(observe, requestId, req, res, startedAt);
        const body = await readJson(req, { maxBodyBytes });
        const cursor = normalizeCursor(body?.cursor ?? 0);
        const retained = db.changesSince(0, { limit: 10_000 });
        const oldestRetained = retained[0]?.sequence ?? (db.sequence + 1);
        if (cursor < oldestRetained - 1) {
          safeObserve(observe, { type: 'replication_cursor_expired', requestId, cursor, oldestRetained, sequence: db.sequence });
          return respond(res, 409, { error: 'snapshot_required', requestId, cursor, oldestRetained, sequence: db.sequence }, { observe, requestId, req, startedAt });
        }
        const changes = db.changesSince(cursor, { limit: replicationLimit });
        const nextCursor = changes.at(-1)?.sequence ?? cursor;
        return respond(res, 200, createReplicationPacket({ from: nodeId, cursor: nextCursor, changes }), { observe, requestId, req, startedAt });
      }

      if (req.method === 'POST' && url.pathname === '/replicate/snapshot') {
        if (!authorize(policy, { user, action: 'replicate', collection: '*' }, res, requestId)) return finishObserved(observe, requestId, req, res, startedAt);
        return respond(res, 200, createSnapshotPacket({ from: nodeId, cursor: db.sequence, state: db.snapshot() }), { observe, requestId, req, startedAt });
      }

      if (req.method === 'POST' && url.pathname === '/replicate/push') {
        const body = await readJson(req, { maxBodyBytes, requireObject: true });
        if (!authorize(policy, { user, action: 'replicate', collection: '*', body }, res, requestId)) return finishObserved(observe, requestId, req, res, startedAt);
        if (!verifyReplicationPacket(body) || !Array.isArray(body.changes)) return respond(res, 400, { error: 'invalid_replication_packet', requestId }, { observe, requestId, req, startedAt });
        let accepted = 0;
        let duplicates = 0;
        for (const change of body.changes) {
          const result = await applyReplicatedChange(db, change, { conflictStrategy });
          if (result?.duplicate) duplicates++;
          else {
            accepted++;
            publish(subscriptions, change.collection, result?.event ?? change);
          }
        }
        return respond(res, 200, { ok: true, accepted, duplicates, sequence: db.sequence }, { observe, requestId, req, startedAt });
      }

      if (parts[0] !== 'collections' || !parts[1]) {
        return respond(res, 404, { error: 'not_found', requestId }, { observe, requestId, req, startedAt });
      }

      const collectionName = decodeURIComponent(parts[1]);
      const id = parts[2] ? decodeURIComponent(parts[2]) : null;
      const collection = db.collection(collectionName);

      if (req.method === 'GET') {
        if (!authorize(policy, { user, action: 'read', collection: collectionName, id }, res, requestId)) return finishObserved(observe, requestId, req, res, startedAt);
        if (!id) {
          const query = parseQuery(url);
          return respond(res, 200, { records: queryRecords(collection.all(), query) }, { observe, requestId, req, startedAt });
        }
        const record = collection.get(id);
        return record
          ? respond(res, 200, record, { observe, requestId, req, startedAt })
          : respond(res, 404, { error: 'not_found', requestId }, { observe, requestId, req, startedAt });
      }

      if (req.method === 'POST' && !id) {
        const body = await readJson(req, { maxBodyBytes, requireObject: true });
        if (!authorize(policy, { user, action: 'write', collection: collectionName, id, body }, res, requestId)) return finishObserved(observe, requestId, req, res, startedAt);
        const record = await collection.insert(body);
        publishLatest(db, subscriptions, collectionName);
        return respond(res, 201, record, { observe, requestId, req, startedAt });
      }

      if (req.method === 'PUT' && id) {
        const body = await readJson(req, { maxBodyBytes, requireObject: true });
        if (!authorize(policy, { user, action: 'write', collection: collectionName, id, body }, res, requestId)) return finishObserved(observe, requestId, req, res, startedAt);
        const record = await collection.upsert({ ...body, id });
        publishLatest(db, subscriptions, collectionName);
        return respond(res, 200, record, { observe, requestId, req, startedAt });
      }

      if (req.method === 'DELETE' && id) {
        if (!authorize(policy, { user, action: 'delete', collection: collectionName, id }, res, requestId)) return finishObserved(observe, requestId, req, res, startedAt);
        const removed = await collection.remove(id);
        if (removed) publishLatest(db, subscriptions, collectionName);
        return respond(res, removed ? 200 : 404, { removed }, { observe, requestId, req, startedAt });
      }

      return respond(res, 405, { error: 'method_not_allowed', requestId }, { observe, requestId, req, startedAt });
    } catch (error) {
      const status = error.statusCode ?? 500;
      const code = error.code ?? (status === 500 ? 'internal_error' : 'bad_request');
      safeObserve(observe, { type: 'request_error', requestId, method: req.method, path: req.url, status, code, error: status >= 500 ? error : undefined });
      if (!res.headersSent) json(res, status, { error: code, requestId });
      else if (!res.writableEnded) res.end();
    }
  });

  server.requestTimeout = 30_000;
  server.headersTimeout = 10_000;
  server.keepAliveTimeout = 5_000;

  return {
    server,
    async listen({ port = 0, host = '127.0.0.1' } = {}) {
      await new Promise((resolve, reject) => { server.once('error', reject); server.listen(port, host, resolve); });
      const address = server.address();
      return { host, port: address.port, url: `http://${host}:${address.port}` };
    },
    async close() {
      if (server.listening) await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    },
    subscribe(collection, listener) {
      if (typeof listener !== 'function') throw new TypeError('Syncio subscribe requires a function');
      const set = subscriptions.get(collection) ?? new Set();
      set.add(listener);
      subscriptions.set(collection, set);
      return () => { set.delete(listener); if (!set.size) subscriptions.delete(collection); };
    }
  };
}

export class ReplicationClient {
  constructor({ baseUrl, nodeId = crypto.randomUUID(), fetchImpl = globalThis.fetch, cursor = 0 } = {}) {
    if (!baseUrl) throw new TypeError('ReplicationClient requires baseUrl');
    if (typeof fetchImpl !== 'function') throw new TypeError('ReplicationClient requires fetch');
    this.baseUrl = baseUrl.replace(/\/$/, '');
    this.nodeId = nodeId;
    this.fetch = fetchImpl;
    this.offline = new OfflineQueue();
    this.cursor = normalizeCursor(cursor);
  }

  async pull(apply, { reseed } = {}) {
    if (typeof apply !== 'function') throw new TypeError('ReplicationClient.pull requires apply callback');
    let response = await this.fetch(`${this.baseUrl}/replicate/pull`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ cursor: this.cursor }) });
    if (response.status === 409) {
      const problem = await response.json().catch(() => ({}));
      if (problem.error !== 'snapshot_required') throw new Error('replication pull conflict');
      if (typeof reseed !== 'function') {
        const error = new Error('replication cursor expired; snapshot reseed required');
        error.code = 'SYNCIO_SNAPSHOT_REQUIRED';
        error.details = problem;
        throw error;
      }
      response = await this.fetch(`${this.baseUrl}/replicate/snapshot`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' });
      if (!response.ok) throw new Error(`replication snapshot failed: ${response.status}`);
      const snapshot = await response.json();
      if (!verifySnapshotPacket(snapshot)) throw new Error('server returned invalid snapshot packet');
      await reseed(cloneSnapshotState(snapshot.state));
      this.cursor = normalizeCursor(snapshot.cursor);
      return 0;
    }
    if (!response.ok) throw new Error(`replication pull failed: ${response.status}`);
    const packet = await response.json();
    if (!verifyReplicationPacket(packet)) throw new Error('server returned invalid replication packet');
    for (const change of packet.changes ?? []) await apply(change);
    this.cursor = normalizeCursor(packet.cursor ?? this.cursor);
    return packet.changes?.length ?? 0;
  }

  async reseedDatabase(db) {
    if (!db || typeof db.replaceState !== 'function') throw new TypeError('reseedDatabase requires Syncio-compatible database');
    const response = await this.fetch(`${this.baseUrl}/replicate/snapshot`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' });
    if (!response.ok) throw new Error(`replication snapshot failed: ${response.status}`);
    const snapshot = await response.json();
    if (!verifySnapshotPacket(snapshot)) throw new Error('server returned invalid snapshot packet');
    await db.replaceState(cloneSnapshotState(snapshot.state));
    this.cursor = normalizeCursor(snapshot.cursor);
    return this.cursor;
  }

  queue(change) { return this.offline.enqueue(change); }

  async flush() {
    return this.offline.flush(async (item) => {
      const change = { ...item };
      delete change.queueId;
      delete change.attempts;
      change.changeId ??= crypto.randomUUID();
      const packet = createReplicationPacket({ from: this.nodeId, cursor: this.cursor, changes: [change] });
      const response = await this.fetch(`${this.baseUrl}/replicate/push`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(packet) });
      if (!response.ok) throw new Error(`replication push failed: ${response.status}`);
    });
  }
}

export async function applyReplicatedChange(db, change, { conflictStrategy = 'last-write-wins' } = {}) {
  if (typeof db.applyReplicationChange === 'function') return db.applyReplicationChange(change, (local, remote) => resolveConflict(local, remote, conflictStrategy));
  const collection = db.collection(change.collection);
  if (change.type === 'remove') {
    const applied = await collection.remove(change.id);
    return { applied, duplicate: false };
  }
  const incoming = change.record;
  if (!incoming?.id) throw new Error('replicated record requires id');
  const current = collection.get(incoming.id);
  const resolved = resolveConflict(current, incoming, conflictStrategy);
  await collection.upsert(resolved);
  return { applied: true, duplicate: false };
}

function cloneSnapshotState(state) {
  if (!state || typeof state !== 'object' || Array.isArray(state)) throw new TypeError('snapshot state must be an object');
  return { version: state.version ?? 1, collections: structuredClone(state.collections ?? {}) };
}

function authorize(policy, context, res, requestId) {
  const allowed = policy.authorize(context);
  if (!allowed) json(res, 403, { error: 'forbidden', requestId });
  return allowed;
}

function publishLatest(db, subscriptions, collection) {
  const change = db.changesSince(Math.max(0, db.sequence - 1), { limit: 1 })[0];
  publish(subscriptions, collection, change);
}

function publish(subscriptions, collection, change) {
  if (!change) return;
  for (const listener of subscriptions.get(collection) ?? []) queueMicrotask(() => listener(structuredClone(change)));
}

function parseQuery(url) {
  let where;
  if (url.searchParams.has('where')) {
    try { where = JSON.parse(url.searchParams.get('where')); }
    catch { throw clientError(400, 'invalid_where'); }
  }
  let limit;
  if (url.searchParams.has('limit')) {
    limit = Number(url.searchParams.get('limit'));
    if (!Number.isSafeInteger(limit) || limit < 0 || limit > 10_000) throw clientError(400, 'invalid_limit');
  }
  return { where, limit };
}

async function readJson(req, { maxBodyBytes, requireObject = false } = {}) {
  const contentLength = Number(req.headers['content-length']);
  if (Number.isFinite(contentLength) && contentLength > maxBodyBytes) throw clientError(413, 'payload_too_large');
  const chunks = [];
  let bytes = 0;
  for await (const chunk of req) {
    bytes += chunk.length;
    if (bytes > maxBodyBytes) throw clientError(413, 'payload_too_large');
    chunks.push(chunk);
  }
  if (!chunks.length) {
    if (requireObject) throw clientError(400, 'json_object_required');
    return null;
  }
  const text = Buffer.concat(chunks).toString('utf8');
  let value;
  try { value = JSON.parse(text); }
  catch { throw clientError(400, 'invalid_json'); }
  if (requireObject && (!value || typeof value !== 'object' || Array.isArray(value))) throw clientError(400, 'json_object_required');
  return value;
}

function normalizeCursor(value) {
  const cursor = Number(value);
  if (!Number.isSafeInteger(cursor) || cursor < 0) throw clientError(400, 'invalid_cursor');
  return cursor;
}

function requestIdFor(req) {
  const supplied = req.headers['x-request-id'];
  if (typeof supplied === 'string' && /^[A-Za-z0-9._:-]{1,128}$/.test(supplied)) return supplied;
  return crypto.randomUUID();
}

function clientError(statusCode, code) {
  const error = new Error(code);
  error.statusCode = statusCode;
  error.code = code;
  return error;
}

function respond(res, status, body, context) {
  json(res, status, body);
  finishObserved(context.observe, context.requestId, context.req, res, context.startedAt);
}

function finishObserved(observe, requestId, req, res, startedAt) {
  const durationMs = Number(process.hrtime.bigint() - startedAt) / 1_000_000;
  safeObserve(observe, { type: 'request_complete', requestId, method: req.method, path: req.url, status: res.statusCode, durationMs });
}

function safeObserve(observe, event) {
  try { observe(Object.freeze({ ...event })); } catch { /* observability must not take down the data plane */ }
}

function json(res, status, body) {
  if (res.writableEnded) return;
  res.statusCode = status;
  res.setHeader('content-type', 'application/json; charset=utf-8');
  res.end(JSON.stringify(body));
}
