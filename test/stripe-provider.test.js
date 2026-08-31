import test from 'node:test';
import assert from 'node:assert/strict';
import { createStripePaymentProvider } from '../src/stripe-provider.js';

function fakeStripe() {
  const calls = [];
  const fetchImpl = async (url, options) => {
    calls.push({ url, options, body: Object.fromEntries(options.body ?? []) });
    const path = new URL(url).pathname;
    let payload = { id: 'obj_1', status: 'active' };
    if (path.endsWith('/checkout/sessions')) payload = { id: 'cs_1', url: 'https://checkout.test/session' };
    if (path.endsWith('/billing_portal/sessions')) payload = { id: 'bps_1', url: 'https://portal.test/session' };
    if (path.endsWith('/refunds')) payload = { id: 're_1', status: 'succeeded', amount: 500, payment_intent: 'pi_1' };
    return { ok: true, status: 200, json: async () => payload };
  };
  return { calls, fetchImpl };
}

test('Stripe adapter maps checkout, portal, cancellation and refunds with idempotency', async () => {
  const fake = fakeStripe();
  const provider = createStripePaymentProvider({ secretKey: 'sk_test_x', apiBase: 'https://stripe.test/v1', fetchImpl: fake.fetchImpl });
  const project = { id: 'p1', billingCustomerId: 'cus_1' };
  const checkout = await provider.createCheckoutSession({ project, plan: { priceId: 'price_1' }, successUrl: 'https://app.test/success', cancelUrl: 'https://app.test/cancel', idempotencyKey: 'i1' });
  assert.equal(checkout.id, 'cs_1');
  assert.equal(fake.calls[0].body.client_reference_id, 'p1');
  assert.equal(fake.calls[0].body['line_items[0][price]'], 'price_1');
  assert.equal(fake.calls[0].options.headers['idempotency-key'], 'i1');
  await provider.createPortalSession({ project, returnUrl: 'https://app.test', idempotencyKey: 'i2' });
  assert.equal(fake.calls[1].body.customer, 'cus_1');
  await provider.cancelSubscription({ project, subscriptionId: 'sub_1', atPeriodEnd: false, idempotencyKey: 'i3' });
  assert.equal(fake.calls[2].options.method, 'DELETE');
  const refund = await provider.refundPayment({ project, paymentId: 'pi_1', amount: 500, idempotencyKey: 'i4' });
  assert.equal(refund.amount, 500);
  assert.equal(fake.calls[3].body.payment_intent, 'pi_1');
});

test('Stripe adapter surfaces provider errors without leaking credentials', async () => {
  const provider = createStripePaymentProvider({ secretKey: 'sk_secret', fetchImpl: async () => ({ ok: false, status: 402, json: async () => ({ error: { code: 'card_declined', message: 'declined' } }) }) });
  await assert.rejects(() => provider.refundPayment({ paymentId: 'pi_bad', amount: 1 }), (error) => error.code === 'card_declined' && !error.message.includes('sk_secret'));
});
