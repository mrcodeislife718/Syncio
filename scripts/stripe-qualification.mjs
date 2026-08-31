const secretKey = required('STRIPE_SECRET_KEY');
const priceId = required('SYNCIO_STRIPE_PRICE_ID');
const apiBase = process.env.STRIPE_API_BASE ?? 'https://api.stripe.com/v1';
const evidence = { format: 'syncio-stripe-qualification/1', startedAt: new Date().toISOString(), checks: {} };

try {
  if (!secretKey.startsWith('sk_test_') && process.env.SYNCIO_ALLOW_LIVE_STRIPE !== 'true') {
    throw new Error('Refusing non-test Stripe key unless SYNCIO_ALLOW_LIVE_STRIPE=true');
  }
  const account = await stripeGet('/account');
  if (!account.id) throw new Error('Stripe account response missing id');
  evidence.checks.account = { ok: true, id: account.id, country: account.country ?? null, chargesEnabled: Boolean(account.charges_enabled), payoutsEnabled: Boolean(account.payouts_enabled) };

  const price = await stripeGet(`/prices/${encodeURIComponent(priceId)}`);
  if (price.id !== priceId || price.active !== true) throw new Error('configured Stripe price is missing or inactive');
  if (price.type !== 'recurring') throw new Error('configured Stripe price must be recurring');
  evidence.checks.price = { ok: true, id: price.id, currency: price.currency, unitAmount: price.unit_amount, interval: price.recurring?.interval ?? null };

  evidence.finishedAt = new Date().toISOString(); evidence.pass = true;
  console.log(JSON.stringify(evidence, null, 2));
} catch (error) {
  evidence.finishedAt = new Date().toISOString(); evidence.pass = false; evidence.error = { message: error.message, code: error.code ?? null };
  console.error(JSON.stringify(evidence, null, 2)); process.exitCode = 1;
}

async function stripeGet(path) {
  const response = await fetch(`${apiBase}${path}`, { headers: { authorization: `Bearer ${secretKey}` }, signal: AbortSignal.timeout(15_000) });
  const body = await response.json().catch(() => null);
  if (!response.ok) {
    const error = new Error(body?.error?.message ?? `Stripe HTTP ${response.status}`);
    error.code = body?.error?.code ?? 'stripe_qualification_error'; throw error;
  }
  return body;
}
function required(name) { const value = process.env[name]; if (!value) throw new Error(`${name} is required`); return value; }
