import path from 'node:path';
import { IndexedSyncioDatabase } from './indexed.js';
import { createSyncioServer } from './server.js';
import { createTokenAuthority, MetricsRegistry, AuditLog } from './operations.js';
import { TokenBucketLimiter, rateLimitError } from './resource-control.js';

export async function startSelfHostedSyncio({
  file,
  secret,
  projectId = 'self-hosted',
  host = '127.0.0.1',
  port = 8787,
  auditFile = `${file}.audit.ndjson`,
  tokenTtlSeconds = 3600,
  rateLimit = { capacity: 240, refillPerSecond: 4, maxKeys: 10_000 }
} = {}) {
  if (!file) throw new TypeError('self-hosted Syncio requires file');
  if (!projectId || typeof projectId !== 'string') throw new TypeError('self-hosted Syncio requires projectId');
  const authority = createTokenAuthority(secret, { issuer:`syncio:${projectId}`, ttlSeconds:tokenTtlSeconds });
  const limiter = new TokenBucketLimiter(rateLimit);
  const db = await IndexedSyncioDatabase.open(path.resolve(file));
  const metrics = new MetricsRegistry();
  const audit = new AuditLog(path.resolve(auditFile));
  const metricObserver = metrics.observer();
  const observe = (event) => {
    metricObserver(event);
    if (event.type === 'request_error' || event.type === 'replication_cursor_expired' || event.type === 'subscription_backpressure') {
      void audit.append({ type:event.type, requestId:event.requestId, status:event.status, code:event.code, cursor:event.cursor, collection:event.collection }).catch(()=>undefined);
    }
  };
  const authenticate = async (req) => {
    const token = req?.headers?.authorization ?? '';
    const identity = `${req?.socket?.remoteAddress ?? 'unknown'}:${typeof token === 'string' ? token.slice(-24) : ''}`;
    const decision = limiter.consume(identity);
    if (!decision.allowed) throw rateLimitError(decision);
    const user = authority.authenticateRequest(req);
    return user?.projectId === projectId ? user : null;
  };
  const policies = [
    { effect:'allow', collection:'*', action:'read', when:({user})=>Boolean(user) && has(user,'database') },
    { effect:'allow', collection:'*', action:'write', when:({user})=>Boolean(user) && has(user,'database') },
    { effect:'allow', collection:'*', action:'delete', when:({user})=>Boolean(user) && has(user,'database') },
    { effect:'allow', collection:'*', action:'replicate', when:({user})=>Boolean(user) && (has(user,'realtime') || has(user,'realtime:basic')) }
  ];
  const service = createSyncioServer({ db, policies, authenticate, observe });
  const address = await service.listen({ host, port });
  return Object.freeze({
    address,
    db,
    metrics,
    audit,
    limiter,
    issueToken({ subject='operator', role='owner', entitlements=['database','realtime'] } = {}) {
      return authority.issue({ subject, projectId, role, entitlements });
    },
    async close() { await service.close(); await db.close(); }
  });
}

function has(user, entitlement) { const grants=new Set(user?.entitlements??[]); return grants.has('*')||grants.has(entitlement); }
