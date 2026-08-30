import tls from 'node:tls';
import crypto from 'node:crypto';

const baseUrl = requiredUrl('SYNCIO_PUBLIC_BASE_URL');
const token = required('SYNCIO_PUBLIC_TOKEN');
const project = process.env.SYNCIO_EXTERNAL_PROJECT ?? 'external-qualification';
const collection = process.env.SYNCIO_EXTERNAL_COLLECTION ?? '__syncio_qualification';
const id = `q-${Date.now()}-${crypto.randomUUID()}`;
const evidence = { format: 'syncio-external-qualification/1', baseUrl: baseUrl.origin, startedAt: new Date().toISOString(), checks: {} };

try {
  if (baseUrl.protocol !== 'https:') throw new Error('SYNCIO_PUBLIC_BASE_URL must use https for production qualification');
  evidence.checks.tls = await inspectTls(baseUrl);

  const health = await jsonRequest(new URL('/health', baseUrl));
  assertStatus(health, 200, 'health');
  if (health.body?.ok !== true) throw new Error('health response did not report ok=true');
  evidence.checks.health = { ok: true, nodeId: health.body.nodeId, sequence: health.body.sequence, storage: health.body.storage };

  const protocols = await jsonRequest(new URL('/protocols', baseUrl));
  assertStatus(protocols, 200, 'protocols');
  if (!protocols.body?.protocols) throw new Error('protocol capability response is missing');
  evidence.checks.protocols = { ok: true, protocols: protocols.body.protocols };

  const headers = { authorization: `Bearer ${token}`, 'content-type': 'application/json', 'x-syncio-project': project };
  const value = { id, kind: 'external-qualification', nonce: crypto.randomUUID(), createdAt: new Date().toISOString() };
  const write = await jsonRequest(new URL(`/collections/${encodeURIComponent(collection)}/${encodeURIComponent(id)}`, baseUrl), { method: 'PUT', headers, body: JSON.stringify(value) });
  assertStatus(write, 200, 'authenticated write');

  const read = await jsonRequest(new URL(`/collections/${encodeURIComponent(collection)}/${encodeURIComponent(id)}`, baseUrl), { headers });
  assertStatus(read, 200, 'authenticated read');
  if (read.body?.id !== id || read.body?.nonce !== value.nonce) throw new Error('read-after-write value mismatch');
  evidence.checks.persistence = { ok: true, id, writeRequestId: write.requestId, readRequestId: read.requestId };

  const denied = await jsonRequest(new URL(`/collections/${encodeURIComponent(collection)}/${encodeURIComponent(id)}`, baseUrl));
  if (![401, 403, 404].includes(denied.status)) throw new Error(`anonymous access was not denied: HTTP ${denied.status}`);
  evidence.checks.anonymousDeny = { ok: true, status: denied.status };

  const remove = await jsonRequest(new URL(`/collections/${encodeURIComponent(collection)}/${encodeURIComponent(id)}`, baseUrl), { method: 'DELETE', headers });
  if (![200, 404].includes(remove.status)) throw new Error(`qualification cleanup failed: HTTP ${remove.status}`);
  evidence.checks.cleanup = { ok: true, status: remove.status };

  evidence.finishedAt = new Date().toISOString();
  evidence.pass = true;
  console.log(JSON.stringify(evidence, null, 2));
} catch (error) {
  evidence.finishedAt = new Date().toISOString();
  evidence.pass = false;
  evidence.error = { message: error.message, code: error.code ?? null };
  console.error(JSON.stringify(evidence, null, 2));
  process.exitCode = 1;
}

async function jsonRequest(url, options = {}) {
  const response = await fetch(url, { ...options, signal: AbortSignal.timeout(15_000) });
  const body = await response.json().catch(() => null);
  return { status: response.status, body, requestId: response.headers.get('x-syncio-request-id') };
}

function assertStatus(result, expected, label) {
  if (result.status !== expected) throw new Error(`${label} failed: expected HTTP ${expected}, received ${result.status}`);
}

function inspectTls(url) {
  return new Promise((resolve, reject) => {
    const port = Number(url.port || 443);
    const socket = tls.connect({ host: url.hostname, port, servername: url.hostname, rejectUnauthorized: true }, () => {
      const cert = socket.getPeerCertificate();
      const validTo = Date.parse(cert.valid_to);
      const remainingMs = validTo - Date.now();
      socket.end();
      if (!cert.subject || !Number.isFinite(validTo)) return reject(new Error('TLS peer certificate is missing validity metadata'));
      if (remainingMs < 7 * 24 * 60 * 60 * 1000) return reject(new Error('TLS certificate expires in less than 7 days'));
      resolve({ ok: true, authorized: socket.authorized, subject: cert.subject.CN ?? null, issuer: cert.issuer?.CN ?? null, validTo: cert.valid_to, daysRemaining: remainingMs / 86_400_000 });
    });
    socket.setTimeout(10_000, () => socket.destroy(new Error('TLS qualification timed out')));
    socket.once('error', reject);
  });
}

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`${name} is required`);
  return value;
}
function requiredUrl(name) {
  const value = required(name);
  let url;
  try { url = new URL(value); } catch { throw new Error(`${name} must be a valid URL`); }
  return url;
}
