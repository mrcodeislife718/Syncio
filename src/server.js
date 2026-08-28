import http from 'node:http';
import crypto from 'node:crypto';
import { OfflineQueue, createReplicationPacket, verifyReplicationPacket, createSnapshotPacket, verifySnapshotPacket, resolveConflict, queryRecords } from './advanced.js';
import { atomicUpdateDocument } from './document-api.js';
import { aggregateDocuments } from './document.js';
import { createResourcePolicy } from './resource-policy.js';

const DEFAULT_MAX_BODY_BYTES = 1_048_576;
const DEFAULT_REPLICATION_LIMIT = 1_000;
const DEFAULT_MAX_SUBSCRIPTIONS = 1_000;

export function createSyncioServer({ db, policies = [], authenticate = async () => null, nodeId = db?.databaseId ?? crypto.randomUUID(), conflictStrategy = 'last-write-wins', maxBodyBytes = DEFAULT_MAX_BODY_BYTES, replicationLimit = DEFAULT_REPLICATION_LIMIT, maxSubscriptions = DEFAULT_MAX_SUBSCRIPTIONS, observe = () => undefined } = {}) {
  if (!db) throw new Error('Syncio server requires a database');
  if (typeof db.watchChanges !== 'function' || typeof db.resumeStatus !== 'function') throw new TypeError('Syncio server requires resumable database change streams');
  if (!Number.isSafeInteger(maxBodyBytes) || maxBodyBytes < 1) throw new TypeError('maxBodyBytes must be a positive safe integer');
  if (!Number.isSafeInteger(replicationLimit) || replicationLimit < 1 || replicationLimit > 10_000) throw new TypeError('replicationLimit must be between 1 and 10000');
  if (!Number.isSafeInteger(maxSubscriptions) || maxSubscriptions < 1) throw new TypeError('maxSubscriptions must be a positive safe integer');

  const policy = createResourcePolicy(policies);
  const networkStreams = new Set();

  const server = http.createServer(async (req, res) => {
    const requestId = requestIdFor(req);
    const startedAt = process.hrtime.bigint();
    res.setHeader('x-syncio-request-id', requestId);
    try {
      const url = new URL(req.url, 'http://syncio.local');
      if (req.method === 'GET' && url.pathname === '/health') return respond(res, 200, { ok: true, nodeId, sequence: db.sequence, subscriptions: networkStreams.size, storage: db.storageStatus?.() }, { observe, requestId, req, startedAt });
      const user = await authenticate(req);
      const parts = url.pathname.split('/').filter(Boolean);

      if (req.method === 'POST' && url.pathname === '/replicate/pull') {
        if (!authorize(policy, { user, action: 'replicate', collection: '*' }, res, requestId)) return finishObserved(observe, requestId, req, res, startedAt);
        const body = await readJson(req, { maxBodyBytes });
        const cursor = normalizeCursor(body?.cursor ?? 0);
        const resume = db.resumeStatus(cursor);
        if (!resume.resumable) {
          safeObserve(observe, { type: 'replication_cursor_expired', requestId, cursor, oldestRetained: resume.oldestRetained, sequence: db.sequence });
          return respond(res, 409, { error: cursor > db.sequence ? 'cursor_ahead' : 'snapshot_required', requestId, cursor, oldestRetained: resume.oldestRetained, sequence: db.sequence }, { observe, requestId, req, startedAt });
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
        let accepted = 0; let duplicates = 0;
        for (const change of body.changes) { const result = await applyReplicatedChange(db, change, { conflictStrategy }); if (result?.duplicate) duplicates++; else accepted++; }
        return respond(res, 200, { ok: true, accepted, duplicates, sequence: db.sequence }, { observe, requestId, req, startedAt });
      }

      if (req.method === 'GET' && parts[0] === 'subscribe' && parts[1] && !parts[2]) {
        const collectionName = decodeURIComponent(parts[1]);
        if (!authorizeScope(policy, { user, action: 'read', collection: collectionName }, res, requestId)) return finishObserved(observe, requestId, req, res, startedAt);
        if (networkStreams.size >= maxSubscriptions) return respond(res, 429, { error: 'subscription_capacity_exceeded', requestId }, { observe, requestId, req, startedAt });
        const after = subscriptionCursor(req, url, db.sequence);
        const resume = db.resumeStatus(after);
        if (!resume.resumable) {
          const code = after > db.sequence ? 'stream_cursor_ahead' : 'stream_resume_expired';
          safeObserve(observe, { type: code, requestId, collection: collectionName, cursor: after, oldestRetained: resume.oldestRetained, sequence: db.sequence });
          return respond(res, 409, { error: code, requestId, cursor: after, oldestRetained: resume.oldestRetained, sequence: db.sequence }, { observe, requestId, req, startedAt });
        }
        return openEventStream({ req, res, db, collectionName, after, requestId, startedAt, networkStreams, observe, policy, user });
      }

      if (parts[0] !== 'collections' || !parts[1]) return respond(res, 404, { error: 'not_found', requestId }, { observe, requestId, req, startedAt });
      const collectionName = decodeURIComponent(parts[1]);
      const aggregateRoute = parts[2] === 'aggregate' && !parts[3];
      const id = aggregateRoute ? null : (parts[2] ? decodeURIComponent(parts[2]) : null);
      const collection = db.collection(collectionName);

      if (req.method === 'POST' && aggregateRoute) {
        const body = await readJson(req, { maxBodyBytes, requireObject: true });
        if (!authorizeScope(policy, { user, action: 'read', collection: collectionName, body }, res, requestId)) return finishObserved(observe, requestId, req, res, startedAt);
        if (!Array.isArray(body.pipeline)) throw clientError(400, 'aggregation_pipeline_required');
        const visible = visibleRecords(policy, { user, action: 'read', collection: collectionName }, collectionCandidates(collection, {}));
        return respond(res, 200, { records: aggregateDocuments(visible, body.pipeline) }, { observe, requestId, req, startedAt });
      }

      if (req.method === 'GET') {
        if (!authorizeScope(policy, { user, action: 'read', collection: collectionName, id }, res, requestId)) return finishObserved(observe, requestId, req, res, startedAt);
        if (!id) {
          const query = parseQuery(url);
          const candidates = collectionCandidates(collection, { where: query.where });
          const visible = visibleRecords(policy, { user, action: 'read', collection: collectionName }, candidates);
          return respond(res, 200, { records: queryRecords(visible, { ...query, where: undefined }) }, { observe, requestId, req, startedAt });
        }
        const record = collection.get(id);
        if (!record) return respond(res, 404, { error: 'not_found', requestId }, { observe, requestId, req, startedAt });
        const visible = policy.read({ user, action: 'read', collection: collectionName, id }, record);
        return visible ? respond(res, 200, visible, { observe, requestId, req, startedAt }) : respond(res, 404, { error: 'not_found', requestId }, { observe, requestId, req, startedAt });
      }

      if (req.method === 'POST' && !id) {
        const body = await readJson(req, { maxBodyBytes, requireObject: true });
        if (!authorizeWrite(policy, { user, action: 'write', collection: collectionName, id }, body, res, requestId)) return finishObserved(observe, requestId, req, res, startedAt);
        const record = await collection.insert(body);
        return respond(res, 201, policy.read({ user, action: 'read', collection: collectionName, id: record.id }, record) ?? { id: record.id }, { observe, requestId, req, startedAt });
      }

      if (req.method === 'PUT' && id) {
        const body = await readJson(req, { maxBodyBytes, requireObject: true });
        const current = collection.get(id);
        if (!authorizeWrite(policy, { user, action: 'write', collection: collectionName, id, record: current }, { ...body, id }, res, requestId)) return finishObserved(observe, requestId, req, res, startedAt);
        const record = await collection.upsert({ ...body, id });
        return respond(res, 200, policy.read({ user, action: 'read', collection: collectionName, id }, record) ?? { id }, { observe, requestId, req, startedAt });
      }

      if (req.method === 'PATCH' && id) {
        const body = await readJson(req, { maxBodyBytes, requireObject: true });
        const current = collection.get(id);
        if (!authorizeWrite(policy, { user, action: 'write', collection: collectionName, id, record: current }, body, res, requestId)) return finishObserved(observe, requestId, req, res, startedAt);
        const record = await atomicUpdateDocument(db, collectionName, id, body, { upsert: url.searchParams.get('upsert') === 'true' });
        return respond(res, 200, policy.read({ user, action: 'read', collection: collectionName, id }, record) ?? { id }, { observe, requestId, req, startedAt });
      }

      if (req.method === 'DELETE' && id) {
        const current = collection.get(id);
        if (!authorize(policy, { user, action: 'delete', collection: collectionName, id, record: current }, res, requestId)) return finishObserved(observe, requestId, req, res, startedAt);
        const removed = await collection.remove(id);
        return respond(res, removed ? 200 : 404, { removed }, { observe, requestId, req, startedAt });
      }
      return respond(res, 405, { error: 'method_not_allowed', requestId }, { observe, requestId, req, startedAt });
    } catch (error) {
      const status = error.statusCode ?? 500;
      const code = error.code ?? (status === 500 ? 'internal_error' : 'bad_request');
      safeObserve(observe, { type: 'request_error', requestId, method: req.method, path: req.url, status, code, error: status >= 500 ? error : undefined });
      if (!res.headersSent) json(res, status, { error: code, requestId }); else if (!res.writableEnded) res.end();
    }
  });

  server.requestTimeout = 30_000; server.headersTimeout = 10_000; server.keepAliveTimeout = 5_000;
  return {
    server,
    async listen({ port = 0, host = '127.0.0.1' } = {}) { await new Promise((resolve, reject) => { server.once('error', reject); server.listen(port, host, resolve); }); const address = server.address(); return { host, port: address.port, url: `http://${host}:${address.port}` }; },
    async close() { for (const stream of [...networkStreams]) stream.end(); networkStreams.clear(); if (server.listening) await new Promise((resolve, reject) => server.close((error) => error ? reject(error) : resolve())); },
    subscribe(collection, listener, { after = db.sequence } = {}) { if (typeof listener !== 'function') throw new TypeError('Syncio subscribe requires a function'); return db.watchChanges({ collection, after }, listener); }
  };
}

export class ReplicationClient {
  constructor({ baseUrl, nodeId = crypto.randomUUID(), fetchImpl = globalThis.fetch, cursor = 0, offlineQueue = new OfflineQueue() } = {}) { if (!baseUrl) throw new TypeError('ReplicationClient requires baseUrl'); if (typeof fetchImpl !== 'function') throw new TypeError('ReplicationClient requires fetch'); if (!offlineQueue || typeof offlineQueue.enqueue !== 'function' || typeof offlineQueue.flush !== 'function') throw new TypeError('ReplicationClient offlineQueue must implement enqueue and flush'); this.baseUrl = baseUrl.replace(/\/$/, ''); this.nodeId = nodeId; this.fetch = fetchImpl; this.offline = offlineQueue; this.cursor = normalizeCursor(cursor); }
  async pull(apply, { reseed } = {}) {
    if (typeof apply !== 'function') throw new TypeError('ReplicationClient.pull requires apply callback');
    let response = await this.fetch(`${this.baseUrl}/replicate/pull`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ cursor: this.cursor }) });
    if (response.status === 409) { const problem = await response.json().catch(() => ({})); if (problem.error !== 'snapshot_required') throw new Error('replication pull conflict'); if (typeof reseed !== 'function') { const error = new Error('replication cursor expired; snapshot reseed required'); error.code = 'SYNCIO_SNAPSHOT_REQUIRED'; error.details = problem; throw error; } response = await this.fetch(`${this.baseUrl}/replicate/snapshot`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' }); if (!response.ok) throw new Error(`replication snapshot failed: ${response.status}`); const snapshot = await response.json(); if (!verifySnapshotPacket(snapshot)) throw new Error('server returned invalid snapshot packet'); await reseed(cloneSnapshotState(snapshot.state)); this.cursor = normalizeCursor(snapshot.cursor); return 0; }
    if (!response.ok) throw new Error(`replication pull failed: ${response.status}`); const packet = await response.json(); if (!verifyReplicationPacket(packet)) throw new Error('server returned invalid replication packet'); for (const change of packet.changes ?? []) await apply(change); this.cursor = normalizeCursor(packet.cursor ?? this.cursor); return packet.changes?.length ?? 0;
  }
  async reseedDatabase(db) { if (!db || typeof db.replaceState !== 'function') throw new TypeError('reseedDatabase requires Syncio-compatible database'); const response = await this.fetch(`${this.baseUrl}/replicate/snapshot`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}' }); if (!response.ok) throw new Error(`replication snapshot failed: ${response.status}`); const snapshot = await response.json(); if (!verifySnapshotPacket(snapshot)) throw new Error('server returned invalid snapshot packet'); await db.replaceState(cloneSnapshotState(snapshot.state)); this.cursor = normalizeCursor(snapshot.cursor); return this.cursor; }
  queue(change) { return this.offline.enqueue(change); }
  async flush() { return this.offline.flush(async (item) => { const change = item.change ? { ...item.change, changeId: item.change.changeId ?? item.idempotencyKey } : { ...item }; delete change.queueId; delete change.attempts; delete change.idempotencyKey; delete change.enqueuedAt; delete change.lastAttemptAt; change.changeId ??= crypto.randomUUID(); const packet = createReplicationPacket({ from: this.nodeId, cursor: this.cursor, changes: [change] }); const response = await this.fetch(`${this.baseUrl}/replicate/push`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(packet) }); if (!response.ok) throw new Error(`replication push failed: ${response.status}`); }); }
}

export async function applyReplicatedChange(db, change, { conflictStrategy = 'last-write-wins' } = {}) {
  if (typeof db.applyReplicationChange === 'function') return db.applyReplicationChange(change, (local, remote) => resolveConflict(local, remote, conflictStrategy));
  const collection = db.collection(change.collection);
  if (change.type === 'remove') { const applied = await collection.remove(change.id); return { applied, duplicate: false }; }
  const incoming = change.record; if (!incoming?.id) throw new Error('replicated record requires id'); const current = collection.get(incoming.id); const resolved = resolveConflict(current, incoming, conflictStrategy); await collection.upsert(resolved); return { applied: true, duplicate: false };
}

function openEventStream({ req, res, db, collectionName, after, requestId, startedAt, networkStreams, observe, policy, user }) {
  res.statusCode = 200; res.setHeader('content-type', 'text/event-stream; charset=utf-8'); res.setHeader('cache-control', 'no-cache, no-transform'); res.setHeader('connection', 'keep-alive'); res.flushHeaders?.(); networkStreams.add(res);
  let closed = false; let stop; let heartbeat;
  const cleanup = (reason = 'closed') => { if (closed) return; closed = true; if (heartbeat) clearInterval(heartbeat); stop?.(); networkStreams.delete(res); safeObserve(observe, { type: 'subscription_closed', requestId, collection: collectionName, reason, durationMs: Number(process.hrtime.bigint() - startedAt) / 1_000_000 }); };
  const send = (eventName, data, id) => { if (closed || res.writableEnded) return false; const idLine = id === undefined ? '' : `id: ${id}\n`; const ok = res.write(`${idLine}event: ${eventName}\ndata: ${JSON.stringify(data)}\n\n`); if (!ok) { safeObserve(observe, { type: 'subscription_backpressure', requestId, collection: collectionName }); cleanup('backpressure'); res.end(); return false; } return true; };
  const sendChange = (change) => { const resource = change.record ?? { id: change.id }; const visible = policy.read({ user, action: 'read', collection: collectionName, id: resource.id }, resource); if (!visible) return true; const filtered = { ...change, ...(change.record ? { record: visible } : {}) }; return send('change', filtered, change.sequence); };
  heartbeat = setInterval(() => send('heartbeat', { sequence: db.sequence }), 15_000); heartbeat.unref?.(); req.once('close', () => cleanup('client_closed')); res.once('close', () => cleanup('response_closed')); send('ready', { collection: collectionName, sequence: db.sequence, resumeFrom: after, requestId }); stop = db.watchChanges({ collection: collectionName, after }, sendChange); safeObserve(observe, { type: 'subscription_opened', requestId, collection: collectionName, resumeFrom: after });
}

function collectionCandidates(collection, spec) { return typeof collection.query === 'function' ? collection.query(spec) : queryRecords(collection.all(), spec); }
function visibleRecords(policy, context, records) { return records.map((record) => policy.read({ ...context, id: record.id }, record)).filter(Boolean); }
function subscriptionCursor(req, url, currentSequence) { if (url.searchParams.has('after')) return normalizeCursor(url.searchParams.get('after')); const lastEventId = req.headers['last-event-id']; if (typeof lastEventId === 'string' && lastEventId.length) return normalizeCursor(lastEventId); return currentSequence; }
function cloneSnapshotState(state) { if (!state || typeof state !== 'object' || Array.isArray(state)) throw new TypeError('snapshot state must be an object'); return { version: state.version ?? 1, collections: structuredClone(state.collections ?? {}) }; }
function authorize(policy, context, res, requestId) { const allowed = policy.authorize(context); if (!allowed) json(res, 403, { error: 'forbidden', requestId }); return allowed; }
function authorizeScope(policy, context, res, requestId) { const allowed = policy.hasScope(context); if (!allowed) json(res, 403, { error: 'forbidden', requestId }); return allowed; }
function authorizeWrite(policy, context, body, res, requestId) { const result = policy.write(context, body); if (!result.allowed) json(res, 403, { error: result.code ?? 'forbidden', field: result.field, requestId }); return result.allowed; }
function parseQuery(url) { const where = parseJsonParam(url, 'where'); const projection = parseJsonParam(url, 'projection'); const orderBy = parseJsonParam(url, 'orderBy'); let limit; let offset; if (url.searchParams.has('limit')) { limit = Number(url.searchParams.get('limit')); if (!Number.isSafeInteger(limit) || limit < 0 || limit > 10_000) throw clientError(400, 'invalid_limit'); } if (url.searchParams.has('offset')) { offset = Number(url.searchParams.get('offset')); if (!Number.isSafeInteger(offset) || offset < 0) throw clientError(400, 'invalid_offset'); } return { where, projection, orderBy, limit, offset }; }
function parseJsonParam(url, name) { if (!url.searchParams.has(name)) return undefined; try { return JSON.parse(url.searchParams.get(name)); } catch { throw clientError(400, `invalid_${name}`); } }
async function readJson(req, { maxBodyBytes, requireObject = false } = {}) { const contentLength = Number(req.headers['content-length']); if (Number.isFinite(contentLength) && contentLength > maxBodyBytes) throw clientError(413, 'payload_too_large'); const chunks = []; let bytes = 0; for await (const chunk of req) { bytes += chunk.length; if (bytes > maxBodyBytes) throw clientError(413, 'payload_too_large'); chunks.push(chunk); } if (!chunks.length) { if (requireObject) throw clientError(400, 'json_object_required'); return null; } const text = Buffer.concat(chunks).toString('utf8'); let value; try { value = JSON.parse(text); } catch { throw clientError(400, 'invalid_json'); } if (requireObject && (!value || typeof value !== 'object' || Array.isArray(value))) throw clientError(400, 'json_object_required'); return value; }
function normalizeCursor(value) { const cursor = Number(value); if (!Number.isSafeInteger(cursor) || cursor < 0) throw clientError(400, 'invalid_cursor'); return cursor; }
function requestIdFor(req) { const supplied = req.headers['x-request-id']; if (typeof supplied === 'string' && /^[A-Za-z0-9._:-]{1,128}$/.test(supplied)) return supplied; return crypto.randomUUID(); }
function clientError(statusCode, code) { const error = new Error(code); error.statusCode = statusCode; error.code = code; return error; }
function respond(res, status, body, context) { json(res, status, body); finishObserved(context.observe, context.requestId, context.req, res, context.startedAt); }
function finishObserved(observe, requestId, req, res, startedAt) { const durationMs = Number(process.hrtime.bigint() - startedAt) / 1_000_000; safeObserve(observe, { type: 'request_complete', requestId, method: req.method, path: req.url, status: res.statusCode, durationMs }); }
function safeObserve(observe, event) { try { observe(Object.freeze({ ...event })); } catch {} }
function json(res, status, body) { if (res.writableEnded) return; res.statusCode = status; res.setHeader('content-type', 'application/json; charset=utf-8'); res.end(JSON.stringify(body)); }
