# Syncio

Syncio is a lightweight realtime document database designed to combine rich document-database capability with a small operational footprint, durable offline synchronization, and resumable realtime data delivery.

The product target is:

- rich document capability comparable to the reasons developers choose MongoDB;
- operational lightness, fast hot-data access, and inexpensive writes associated with Redis-class systems;
- realtime and offline behavior built into the database rather than bolted on as a separate service;
- self-hosting and provider independence;
- a managed Syncio Cloud path that turns the same database into recurring infrastructure revenue.

## Current status

Syncio is under active production qualification. The repository contains the embedded database, authenticated self-host runtime, process-isolated managed runtime, control plane, usage/invoice engine, payment-provider boundary, TLS edge, deployment/rollback primitives, and production verification suites. Public managed-cloud launch still requires the external production environment and the final evidence gates documented in `docs/COMPLETION_LEDGER.md` and `docs/PRODUCTION_QUALIFICATION.md`.

## Core architecture

A successful mutation follows one ordered path:

```text
client write
  -> validation and authorization
  -> transaction/conflict control
  -> fsynced append-only durable log
  -> committed document state
  -> index maintenance
  -> realtime change stream
  -> replication/offline synchronization history
  -> checkpoint / compaction / recovery
```

The same committed change drives local watchers, network realtime, replication, reconnect replay, offline synchronization, TTL removal, recovery history, and operational observation.

## Document capability

Syncio supports nested JSON documents and arrays, dotted-path queries, logical/comparison/array operators, projections, sorting and pagination, persistent single/compound/unique indexes, atomic document updates, multi-document transactions, aggregation, schema validation, TTL expiration, text search, geospatial search, point-in-time recovery, partition routing/rebalancing, and resumable realtime streams.

Examples include:

```text
customer.address.city = "Bronx"
total >= 25
$and / $or / $nor
$in / $nin / $exists / $all / $elemMatch
$set / $unset / $inc / $mul / $min / $max
$push / $addToSet / $pull / $rename
$text
$near
```

## Realtime and offline

Collection streams resume from a durable sequence:

```text
GET /subscribe/orders?after=88201
```

Every delivered change carries a sequence ID. Clients can reconnect with `after=<sequence>` or `Last-Event-ID`; retained missed changes are replayed before live delivery resumes. Expired history produces an explicit reseed requirement rather than silent divergence.

Restart-persistent offline queues use stable idempotency identities so retries do not intentionally create duplicate remote changes.

## Storage direction

Normal commits use WAL-first persistence instead of rewriting the complete database file per mutation. Syncio also contains a segmented SSD-backed state plane with persisted offsets, bounded hot-record cache, streaming scans, tombstones and compaction.

The remaining large-dataset qualification work is to finish proving the default compatibility database path on the segmented/write-set architecture at the intended 1x/10x/100x workloads. The repository does not claim a 500 GB low-RAM production ceiling until that evidence exists.

## Syncio Cloud

Syncio Cloud is the primary recurring-revenue product. The default commercial catalog is:

| Plan | Default base price | Model |
| --- | ---: | --- |
| Free | $0/month | development and small hosted projects |
| Pro | $49/month | production applications + usage |
| Scale | $499/month | high limits, PITR, regional/replica capability + usage |
| Enterprise | contract | dedicated/enterprise requirements |

Usage is durably metered for reads, writes, storage byte-hours, realtime connection time, egress, backup storage, PITR storage, replica-region hours and compute time. The revenue engine calculates included allowances, overages, quota decisions, invoice estimates, and idempotent finalized invoices.

Managed project traffic reports usage from the database process to the central control plane. Customers can inspect their own usage and invoice estimates through the hosted control API. Checkout, billing portal and cancellation routes use a provider-neutral payment adapter; paid authority changes only after verified billing events.

Additional commercial products are Dedicated Syncio, enterprise self-hosting/support, migration services, and a future connector/integration marketplace. See `docs/COMMERCIALIZATION.md`.

## Requirements

Node.js 20 or newer. The production runtime currently has no third-party package dependencies.

```bash
npm install
npm run qualify
```

## Embedded example

```js
import { IndexedSyncioDatabase } from './src/indexed.js';
import { atomicUpdateDocument } from './src/document-api.js';

const db = await IndexedSyncioDatabase.open('./data/app.syncio.json');
await db.defineIndex('users', ['tenantId', 'email'], { name: 'tenant_email', unique: true });
await db.collection('users').insert({
  id: 'u1',
  tenantId: 't1',
  email: 'ada@example.com',
  profile: { address: { city: 'Bronx' } },
  score: 10
});
await atomicUpdateDocument(db, 'users', 'u1', { $inc: { score: 5 }, $set: { 'profile.level': 'gold' } });
const users = db.collection('users').query({ where: { 'profile.address.city': 'Bronx', score: { $gte: 15 } }, projection: { email: 1, score: 1 } });
console.log(users);
await db.close();
```

## Documentation

- `docs/ARCHITECTURE.md` — architecture, invariants, and scale boundaries.
- `docs/COMMERCIALIZATION.md` — plans, metering, invoices, funnel and commercial products.
- `docs/COMPLETION_LEDGER.md` — evidence-backed capability status.
- `docs/PRODUCTION_QUALIFICATION.md` — independent launch qualification.

Passing CI means the tested guarantees passed on that exact commit. It does not convert external payment, public TLS/DNS, geographic infrastructure, competitive benchmarking, or customer adoption into evidence that has not actually been produced.
