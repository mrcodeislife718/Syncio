import http from 'node:http';
import crypto from 'node:crypto';
import { ChangeLog, OfflineQueue, createPolicyEngine, createReplicationPacket, verifyReplicationPacket, resolveConflict, queryRecords } from './advanced.js';

export function createSyncioServer({ db, policies = [], authenticate = async () => null, nodeId = crypto.randomUUID(), conflictStrategy = 'last-write-wins' } = {}) {
  if (!db) throw new Error('Syncio server requires a database');
  const policy = createPolicyEngine(policies);
  const changes = new ChangeLog(nodeId);
  const subscriptions = new Map();

  const server = http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url, 'http://syncio.local');
      const user = await authenticate(req);
      const body = await readJson(req);
      const parts = url.pathname.split('/').filter(Boolean);
      if (req.method === 'GET' && url.pathname === '/health') return json(res, 200, { ok: true, nodeId });
      if (req.method === 'POST' && url.pathname === '/replicate/pull') {
        if (!authorize(policy, { user, action: 'replicate', collection: '*' }, res)) return;
        return json(res, 200, createReplicationPacket({ from: nodeId, cursor: body?.cursor ?? 0, changes: changes.since(body?.cursor ?? 0) }));
      }
      if (req.method === 'POST' && url.pathname === '/replicate/push') {
        if (!authorize(policy, { user, action: 'replicate', collection: '*' }, res)) return;
        if (!verifyReplicationPacket(body)) return json(res, 400, { error: 'invalid_replication_packet' });
        let accepted = 0;
        for (const change of body.changes ?? []) {
          await applyReplicatedChange(db, change, { conflictStrategy });
          changes.merge([change]);
          publish(subscriptions, change.collection, change);
          accepted++;
        }
        return json(res, 200, { ok: true, accepted });
      }
      if (parts[0] !== 'collections' || !parts[1]) return json(res, 404, { error: 'not_found' });
      const collectionName = decodeURIComponent(parts[1]);
      const id = parts[2] ? decodeURIComponent(parts[2]) : null;
      const collection = db.collection(collectionName);
      const action = req.method === 'GET' ? 'read' : req.method === 'DELETE' ? 'delete' : 'write';
      if (!authorize(policy, { user, action, collection: collectionName, id, body }, res)) return;
      if (req.method === 'GET' && !id) {
        const where = url.searchParams.get('where');
        const query = { where: where ? JSON.parse(where) : undefined, limit: url.searchParams.has('limit') ? Number(url.searchParams.get('limit')) : undefined };
        return json(res, 200, { records: queryRecords(collection.all(), query) });
      }
      if (req.method === 'GET' && id) { const record = collection.get(id); return record ? json(res, 200, record) : json(res, 404, { error: 'not_found' }); }
      if (req.method === 'POST' && !id) { const record = await collection.insert(body); const change = changes.append({ collection: collectionName, type: 'insert', record }); publish(subscriptions, collectionName, change); return json(res, 201, record); }
      if (req.method === 'PUT' && id) { const record = await collection.upsert({ ...body, id }); const change = changes.append({ collection: collectionName, type: 'upsert', record }); publish(subscriptions, collectionName, change); return json(res, 200, record); }
      if (req.method === 'DELETE' && id) { const removed = await collection.remove(id); const change = changes.append({ collection: collectionName, type: 'remove', id }); publish(subscriptions, collectionName, change); return json(res, removed ? 200 : 404, { removed }); }
      return json(res, 405, { error: 'method_not_allowed' });
    } catch (error) { json(res, 500, { error: 'internal_error', message: error.message }); }
  });

  return {
    server,
    changes,
    async listen({ port = 0, host = '127.0.0.1' } = {}) { await new Promise((resolve, reject) => { server.once('error', reject); server.listen(port, host, resolve); }); const address = server.address(); return { host, port: address.port, url: `http://${host}:${address.port}` }; },
    async close() { if (server.listening) await new Promise((resolve) => server.close(resolve)); },
    subscribe(collection, listener) { const set = subscriptions.get(collection) ?? new Set(); set.add(listener); subscriptions.set(collection, set); return () => set.delete(listener); }
  };
}

export class ReplicationClient {
  constructor({ baseUrl, nodeId = crypto.randomUUID(), fetchImpl = globalThis.fetch } = {}) { this.baseUrl = baseUrl.replace(/\/$/, ''); this.nodeId = nodeId; this.fetch = fetchImpl; this.log = new ChangeLog(nodeId); this.offline = new OfflineQueue(); this.cursor = 0; }
  async pull(apply) {
    const response = await this.fetch(`${this.baseUrl}/replicate/pull`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ cursor: this.cursor }) });
    if (!response.ok) throw new Error(`replication pull failed: ${response.status}`);
    const packet = await response.json(); if (!verifyReplicationPacket(packet)) throw new Error('server returned invalid replication packet');
    for (const change of packet.changes ?? []) { await apply(change); this.cursor = Math.max(this.cursor, change.clock ?? this.cursor); }
    this.log.merge(packet.changes ?? []); return packet.changes?.length ?? 0;
  }
  queue(change) { return this.offline.enqueue(change); }
  async flush() { return this.offline.flush(async (item) => { const packet = createReplicationPacket({ from: this.nodeId, cursor: this.cursor, changes: [item] }); const response = await this.fetch(`${this.baseUrl}/replicate/push`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(packet) }); if (!response.ok) throw new Error(`replication push failed: ${response.status}`); }); }
}

export function applyReplicatedChange(db, change, { conflictStrategy = 'last-write-wins' } = {}) {
  const collection = db.collection(change.collection);
  if (change.type === 'remove') return collection.remove(change.id);
  const incoming = change.record;
  if (!incoming?.id) throw new Error('replicated record requires id');
  const current = collection.get(incoming.id);
  const resolved = resolveConflict(current, incoming, conflictStrategy);
  return collection.upsert(resolved);
}

function authorize(policy, context, res) { const allowed = policy.authorize(context); if (!allowed) json(res, 403, { error: 'forbidden' }); return allowed; }
function publish(subscriptions, collection, change) { for (const listener of subscriptions.get(collection) ?? []) queueMicrotask(() => listener(structuredClone(change))); }
async function readJson(req) { const chunks=[]; for await (const chunk of req) chunks.push(chunk); if (!chunks.length) return null; const text=Buffer.concat(chunks).toString('utf8'); return text ? JSON.parse(text) : null; }
function json(res,status,body){res.statusCode=status;res.setHeader('content-type','application/json');res.end(JSON.stringify(body));}
