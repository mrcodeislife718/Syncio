import { AsyncLocalStorage } from 'node:async_hooks';

const storage = new AsyncLocalStorage();
const ID = /^[A-Za-z0-9_.:@/-]{1,256}$/;

export function normalizeWriteProvenance(value = null) {
  if (value == null) return null;
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new TypeError('write provenance must be an object');
  const normalized = {};
  for (const key of ['actorId', 'actorKind', 'executionId', 'requestId', 'authorityRef', 'causeRef']) {
    const candidate = value[key];
    if (candidate == null || candidate === '') continue;
    if (typeof candidate !== 'string' || !ID.test(candidate)) throw new TypeError(`invalid write provenance ${key}`);
    normalized[key] = candidate;
  }
  if (value.metadata !== undefined) {
    if (!value.metadata || typeof value.metadata !== 'object' || Array.isArray(value.metadata)) throw new TypeError('write provenance metadata must be an object');
    const entries = Object.entries(value.metadata);
    if (entries.length > 16) throw new TypeError('write provenance metadata exceeds 16 entries');
    normalized.metadata = Object.fromEntries(entries.map(([key, candidate]) => {
      if (!/^[A-Za-z0-9_.-]{1,64}$/.test(key)) throw new TypeError('invalid write provenance metadata key');
      if (!['string', 'number', 'boolean'].includes(typeof candidate) && candidate !== null) throw new TypeError('write provenance metadata values must be scalar');
      if (typeof candidate === 'string' && candidate.length > 512) throw new TypeError('write provenance metadata value too long');
      return [key, candidate];
    }));
  }
  return Object.keys(normalized).length ? Object.freeze(structuredClone(normalized)) : null;
}

export function runWithWriteProvenance(provenance, work) {
  if (typeof work !== 'function') throw new TypeError('write provenance requires work function');
  const normalized = normalizeWriteProvenance(provenance);
  return storage.run(normalized, work);
}

export function currentWriteProvenance() {
  const value = storage.getStore();
  return value ? structuredClone(value) : null;
}

export function provenanceFromAuthenticatedRequest(user, requestId, req = null) {
  if (!user || typeof user !== 'object') return normalizeWriteProvenance({ requestId });
  const actorId = firstString(user.sub, user.id, user.userId, user.principalId, user.subject);
  const actorKind = firstString(user.kind, user.type, user.role);
  const executionHeader = req?.headers?.['x-syncio-execution-id'];
  const authorityHeader = req?.headers?.['x-syncio-authority-ref'];
  return normalizeWriteProvenance({
    actorId,
    actorKind,
    executionId: typeof executionHeader === 'string' ? executionHeader : undefined,
    requestId,
    authorityRef: typeof authorityHeader === 'string' ? authorityHeader : undefined,
  });
}

function firstString(...values) {
  return values.find((value) => typeof value === 'string' && value.length > 0);
}
