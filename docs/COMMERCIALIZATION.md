# Syncio Commercialization

Syncio is designed to monetize managed infrastructure and operational value while keeping the database easy to adopt locally and self-host.

## Positioning

**One database for documents, realtime, offline synchronization, and fast application state.**

The commercial value proposition is not simply a lower database price. Syncio aims to reduce the number of independent systems a team must operate for document storage, realtime delivery, synchronization, caching-like hot access, recovery, and regional operation.

## Revenue ladder

| Plan | Default base price | Intended customer |
| --- | ---: | --- |
| Free | $0/month | Development, evaluation, small applications |
| Pro | $49/month + usage | Production applications and small teams |
| Scale | $499/month + usage | Growing products needing substantially higher limits and regional/replica capability |
| Enterprise | Contract | Dedicated infrastructure, private networking, SSO/compliance/SLA/data-residency/support requirements |

`business` remains accepted by the control plane as a legacy alias for Scale capability so existing projects are not broken.

The catalog is product configuration, not a permanent promise. Prices, allowances, and limits must be versioned and reviewed against real infrastructure cost and customer willingness to pay before public launch.

## Metered resources

Syncio's revenue engine records durable, idempotent usage events. Supported raw units are:

- reads;
- writes;
- storage byte-hours;
- realtime connection-seconds;
- outbound bytes;
- backup byte-hours;
- PITR byte-hours;
- replica-region hours;
- compute milliseconds.

Byte-hours are converted to GB-months using a 730-hour normalization. Realtime seconds are converted to connection-hours. This avoids charging storage from an arbitrary end-of-month snapshot.

A usage event has a project, metric, quantity, timestamp, period, metadata, and idempotency key. Duplicate delivery of the same usage event does not create a second charge.

## Invoice calculation

For each metered dimension:

```text
billable overage = max(0, measured usage - included allowance)
charge = billable overage / billing unit * unit price
```

The monthly estimate is:

```text
base subscription
+ read overage
+ write overage
+ storage overage
+ realtime connection overage
+ egress overage
+ backup storage overage
+ PITR storage overage
+ replica/region overage
```

Invoice finalization is idempotent by project and billing month. Enterprise invoices are contract-priced and Syncio does not invent a dollar total unless explicit enterprise pricing is configured.

## Quotas and upgrade pressure

Pricing and resource protection are separate concepts. Each plan also has hard operating limits such as monthly operations, stored bytes, concurrent realtime connections, regions, replicas, and PITR retention.

The quota engine returns an explicit `upgradeRequired` decision when a requested resource would exceed a plan limit. Paid overage therefore does not automatically mean unlimited resource authority.

## Customer lifecycle

```text
visitor
  -> signup
  -> free project
  -> first durable write / realtime subscription
  -> recurring application value
  -> usage growth
  -> checkout
  -> verified payment-provider event
  -> current entitlements change
  -> usage metering
  -> monthly invoice
  -> additional storage / PITR / replicas / regions
  -> Scale or Enterprise
```

A customer request to upgrade does not directly grant paid entitlements. Checkout is provider-neutral, while the authoritative plan transition remains a verified billing event.

## Syncio Cloud control APIs

The hosted control plane exposes:

- `GET /v1/plans` for the public product catalog;
- project-scoped usage summaries;
- project-scoped invoice estimates and invoice history;
- checkout session creation when a payment provider is configured;
- customer billing-portal session creation;
- subscription cancellation initiation;
- signed billing webhook ingestion.

Project ownership is checked before any usage, invoice, or payment operation is returned.

## Managed runtime metering

Managed database processes emit usage events to the parent control plane over the existing authenticated process boundary. The control plane records those events into the durable usage ledger. Database users do not submit their own billable operation counts.

The tested path is:

```text
managed project HTTP request
  -> database operation
  -> request observation
  -> tenant IPC usage event
  -> central usage meter
  -> durable monthly totals
  -> invoice estimate
```

Storage, backups, PITR, replica/region capacity, and compute must be sampled from authoritative runtime/infrastructure state before those dimensions are used for real customer billing.

## Secondary revenue products

### Dedicated Syncio

Dedicated runtime or cluster isolation with base-plus-usage pricing. The minimum intended tier is Scale.

### Enterprise self-hosting

Annual commercial contracts can include security updates, priority support, advanced administration, enterprise policy features, and commercial licensing/support terms while customer data remains in customer infrastructure.

### Migration services

Paid migration engagements can move applications from MongoDB, Firebase, Supabase, Redis combinations, and other supported systems. Migration tooling should ultimately become repeatable product capability, with professional services reserved for complex migrations.

### Marketplace and integrations

Future revenue can include hosted connectors, observability integrations, specialized storage tiers, and extensions through usage charges or revenue share.

## Economic telemetry

Commercial operation should track at minimum:

- visitor-to-signup conversion;
- signup-to-first-value activation;
- free-to-paid conversion;
- average revenue per account and project;
- infrastructure cost by project and metered dimension;
- gross margin and contribution margin;
- customer acquisition cost;
- retention and churn;
- net revenue retention;
- lifetime value;
- CAC payback period;
- upgrade and downgrade reasons;
- payment failure and recovery rates.

A technically correct invoice is not sufficient commercial proof. Public launch qualification requires a real payment-provider environment, real infrastructure cost measurements, and an exercised signup-to-paid-entitlement flow.
