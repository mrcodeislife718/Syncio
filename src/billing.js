import crypto from 'node:crypto';
import { BillingStateProcessor } from './control-plane.js';

export function createBillingWebhookProcessor({ controlPlane, secret, toleranceSeconds = 300, now = () => Date.now() } = {}) {
  if (!controlPlane) throw new TypeError('billing webhook processor requires controlPlane');
  const key = Buffer.isBuffer(secret) ? secret : Buffer.from(String(secret ?? ''));
  if (key.length < 32) throw new TypeError('billing webhook secret requires at least 32 bytes');
  if (!Number.isSafeInteger(toleranceSeconds) || toleranceSeconds < 1) throw new TypeError('toleranceSeconds must be a positive safe integer');
  const billing = new BillingStateProcessor(controlPlane);

  return Object.freeze({
    sign(rawBody, timestamp = Math.floor(now() / 1000)) {
      const body = normalizeRawBody(rawBody);
      const signature = crypto.createHmac('sha256', key).update(`${timestamp}.${body}`).digest('hex');
      return `t=${timestamp},v1=${signature}`;
    },
    async process({ rawBody, signature } = {}) {
      const body = normalizeRawBody(rawBody);
      const parsed = parseSignature(signature);
      const current = Math.floor(now() / 1000);
      if (Math.abs(current - parsed.timestamp) > toleranceSeconds) throw webhookError('billing_webhook_expired');
      const expected = crypto.createHmac('sha256', key).update(`${parsed.timestamp}.${body}`).digest();
      const supplied = decodeCanonicalHex(parsed.signature);
      if (supplied.length !== expected.length || !crypto.timingSafeEqual(supplied, expected)) throw webhookError('billing_webhook_invalid_signature');
      let event;
      try { event = JSON.parse(body); } catch { throw webhookError('billing_webhook_invalid_json'); }
      return billing.process(event);
    }
  });
}

function normalizeRawBody(rawBody) {
  if (typeof rawBody === 'string') return rawBody;
  if (Buffer.isBuffer(rawBody)) return rawBody.toString('utf8');
  throw new TypeError('billing webhook rawBody must be string or Buffer');
}

function parseSignature(header) {
  if (typeof header !== 'string' || header.length > 1024) throw webhookError('billing_webhook_invalid_signature');
  const fields = Object.fromEntries(header.split(',').map((part) => part.trim().split('=', 2)));
  const timestamp = Number(fields.t);
  if (!Number.isSafeInteger(timestamp) || !fields.v1) throw webhookError('billing_webhook_invalid_signature');
  return { timestamp, signature: fields.v1 };
}

function decodeCanonicalHex(value) {
  if (typeof value !== 'string' || !/^[0-9a-f]{64}$/.test(value)) throw webhookError('billing_webhook_invalid_signature');
  return Buffer.from(value, 'hex');
}

function webhookError(code) {
  const error = new Error(code);
  error.code = code;
  error.statusCode = 400;
  return error;
}
