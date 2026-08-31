function required(value, name) {
  if (typeof value !== 'string' || !value.trim()) throw new TypeError(`${name} is required`);
  return value.trim();
}

export function createStripePaymentProvider({ secretKey, apiBase = 'https://api.stripe.com/v1', fetchImpl = globalThis.fetch } = {}) {
  const key = required(secretKey, 'Stripe secretKey');
  if (typeof fetchImpl !== 'function') throw new TypeError('Stripe provider requires fetch');

  async function request(path, fields, idempotencyKey) {
    const body = new URLSearchParams();
    for (const [name, value] of Object.entries(fields ?? {})) {
      if (value !== undefined && value !== null) body.set(name, String(value));
    }
    const response = await fetchImpl(`${apiBase}${path}`, {
      method: 'POST',
      headers: {
        authorization: `Bearer ${key}`,
        'content-type': 'application/x-www-form-urlencoded',
        ...(idempotencyKey ? { 'idempotency-key': idempotencyKey } : {})
      },
      body,
      signal: AbortSignal.timeout(15_000)
    });
    const payload = await response.json().catch(() => null);
    if (!response.ok) {
      const error = new Error(payload?.error?.message ?? `Stripe HTTP ${response.status}`);
      error.code = payload?.error?.code ?? 'stripe_provider_error';
      error.statusCode = response.status;
      throw error;
    }
    return payload;
  }

  return Object.freeze({
    name: 'stripe',
    async createCheckoutSession({ project, plan, successUrl, cancelUrl, idempotencyKey }) {
      const priceId = required(plan?.priceId ?? plan?.stripePriceId ?? plan, 'Stripe priceId');
      const result = await request('/checkout/sessions', {
        mode: 'subscription',
        success_url: required(successUrl, 'successUrl'),
        cancel_url: required(cancelUrl, 'cancelUrl'),
        'line_items[0][price]': priceId,
        'line_items[0][quantity]': 1,
        client_reference_id: project.id,
        'metadata[projectId]': project.id
      }, idempotencyKey);
      return { id: result.id, url: result.url, customerId: result.customer ?? null, subscriptionId: result.subscription ?? null };
    },
    async createPortalSession({ project, returnUrl, idempotencyKey }) {
      const customerId = required(project.billingCustomerId ?? project.customerId, 'project billing customer id');
      const result = await request('/billing_portal/sessions', { customer: customerId, return_url: required(returnUrl, 'returnUrl') }, idempotencyKey);
      return { id: result.id, url: result.url };
    },
    async cancelSubscription({ subscriptionId, atPeriodEnd = true, idempotencyKey }) {
      const id = required(subscriptionId, 'subscriptionId');
      const result = atPeriodEnd
        ? await request(`/subscriptions/${encodeURIComponent(id)}`, { cancel_at_period_end: true }, idempotencyKey)
        : await request(`/subscriptions/${encodeURIComponent(id)}`, {}, idempotencyKey);
      return { id: result.id, status: result.status, cancelAtPeriodEnd: Boolean(result.cancel_at_period_end) };
    },
    async refundPayment({ paymentId, amount, reason, idempotencyKey }) {
      const result = await request('/refunds', {
        payment_intent: required(paymentId, 'paymentId'),
        amount: amount == null ? undefined : Math.round(Number(amount)),
        reason: reason ?? undefined
      }, idempotencyKey);
      return { id: result.id, status: result.status, amount: result.amount ?? null, paymentId: result.payment_intent ?? paymentId };
    }
  });
}
