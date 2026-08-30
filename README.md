# Syncio

Syncio is a lightweight realtime document database designed to combine rich document-database capability with a small operational footprint, durable offline synchronization, resumable realtime delivery, bounded-memory storage, and optional distributed operation.

The product target is:

- rich document capability comparable to the reasons developers choose MongoDB;
- operational lightness, bounded hot-memory use, and inexpensive writes associated with Redis-class systems;
- realtime and offline behavior built into the database rather than bolted on as a separate service;
- self-hosting and provider independence;
- a managed Syncio Cloud path that turns the same database into recurring infrastructure revenue.

## Current status

The technical-superiority implementation is integrated into the production/self-host runtime and qualification suite. The repository contains the embedded database, storage-backed production engine, authenticated self-host runtime, managed runtime, control plane, usage/invoice engine, provider-neutral payment boundary, TLS edge, deployment/rollback primitives, bounded indexes, distributed partition groups, durable cross-shard transactions, regional failover, and production verification suites.

Repository-side qualification gaps have also been closed: deterministic disk-full/read-only WAL failure tests, package export/import verification, production-path repeated benchmarking, strict same-hardware/workload comparison gates, and an executable public-deployment qualification command.

Public managed-cloud launch still depends on external production evidence such as the real deployment environment, live payment provider, public DNS/TLS operations, protected repository governance, customer operations, and direct competitor benchmark runs. Those external gates are tracked separately from implementation completeness in `docs/COMPLETION_LEDGER.md` and `docs/PRODUCTION_QUALIFICATION.md`.

## One authoritative commit path

```text
client write
  -> validation and authorization
  -> write-set / OCC transaction control
  -> resource admission
  -> Commit Fabric identity + checksum
  -> fsynced WAL
  -> authoritative segmented SSD state
  -> bounded persistent index maintenance
  -> committed change stream
      -> realtime
      -> reactive queries
      -> replication
      -> selective/offline sync
      -> PITR / recovery
```

A specialized subsystem may optimize this path, but it cannot create a second source of truth. If an index becomes unavailable after a durable commit, the production boundary marks the index degraded and falls back to authoritative SSD state until repair completes.

## Document capability

Syncio supports nested JSON documents and arrays, dotted-path queries, logical/comparison/array operators, projections, sorting and pagination, persistent single/compound/unique/sparse indexes, atomic document updates, multi-document transactions, aggregation, schema validation, TTL expiration, persistent text search, persistent geospatial search, point-in-time recovery, partition routing/rebalancing, and resumable realtime streams.

Index keys use canonical typed encoding so missing values, nulls, numbers, strings, arrays, and structurally equivalent objects do not collapse incorrectly.

## Realtime, reactive queries, and offline sync

Collection streams resume from a durable sequence:

```text
GET /subscribe/orders?after=88201
```

Every delivered change carries a sequence ID. Clients reconnect with `after=<sequence>` or `Last-Event-ID`; retained missed changes are replayed before live delivery resumes. Expired history produces an explicit reseed requirement rather than silent divergence.

Reactive queries consume real Commit Fabric mutations. Safe query shapes are maintained incrementally; incomplete deltas or query shapes that cannot be updated safely fall back to recomputation rather than returning stale results.

Persistent Sync Views provide bounded selective synchronization. Restart-persistent transaction intents retain preconditions, conflict policy, retry state, expiry, and idempotent identity, and reconcile through the real transaction engine.

## Storage and bounded memory

The production state plane uses segmented SSD files as authoritative state. Normal commits do not rewrite or clone the full database. Transactions use read/write sets and optimistic concurrency control.

Primary offsets, ordinary secondary indexes, text postings, and geo cells are persisted in hashed buckets with bounded caches. Record-version conflict checks come from persistent offset metadata instead of one RAM entry per document. Hot record caches and index caches expose hit/miss/eviction telemetry.

Compaction uses a sibling replacement directory with rollback-safe replacement. WAL recovery verifies Commit Fabric identities and emits a recovery manifest. The WAL layer supports deterministic I/O fault injection for qualification while production defaults remain the real filesystem.

The architecture therefore does not require the complete logical dataset to remain resident in RAM. The exact maximum dataset size remains hardware/workload dependent and is established by benchmark evidence rather than a hard-coded marketing claim.

## Distributed operation

Syncio includes optional distributed primitives rather than forcing distributed complexity onto every deployment:

- deterministic consistent-hash partitioning and targeted shard-key queries;
- overlapping read/write quorum partition groups;
- read repair and minority-replica repair;
- rollback of successful minority writes when quorum cannot be reached;
- session monotonic/read-your-writes behavior;
- durable two-phase cross-partition transactions;
- participant key reservations that block conflicting routed writes while prepared;
- recovery that completes transactions after a durable commit decision;
- append-only tamper-evident global commit metadata;
- bounded dedicated subscription routing;
- regional health routing and failover.

Applications that only need a single local database do not pay the operational complexity of this layer.

## Policy, protocols, and scheduling

The policy plane remains deny-by-default. Declarative row constraints may be compiled into query planning only when doing so is provably safe; final record-level authorization remains authoritative. A collection-specific deny also prevents a broad wildcard operation from bypassing the narrower rule.

Query, Commit, and Sync protocol structures are versioned and integrity checked. The server exposes protocol capabilities and uses a digest-verified Query IR for planned HTTP queries.

A hierarchical scheduler accounts for CPU, memory, SSD I/O, network, egress, and coordination. Foreground commits, realtime, replication, TTL work, PITR snapshots, and other background work are assigned explicit priorities. Durability-critical history is never silently dropped merely because optional background capacity is constrained.

## Syncio Cloud

Syncio Cloud is the primary recurring-revenue product. The default commercial catalog is:

| Plan | Default base price | Model |
| --- | ---: | --- |
| Free | $0/month | development and small hosted projects |
| Pro | $49/month | production applications + usage |
| Scale | $499/month | high limits, PITR, regional/replica capability + usage |
| Enterprise | contract | dedicated/enterprise requirements |

Usage is durably metered for reads, writes, storage byte-hours, realtime connection time, egress, backup storage, PITR storage, replica-region hours and compute time. The revenue engine calculates included allowances, overages, quota decisions, invoice estimates, and idempotent finalized invoices.

## Requirements and qualification

Node.js 20 or newer. The runtime has no third-party package dependencies.

```bash
npm install
npm run qualify
npm run benchmark
```

`npm run qualify` checks syntax, imports every declared package export, audits runtime/test/CLI/qualification code for unfinished or disabled work, and runs the complete Node test suite. CI repeats qualification on Node 20 and Node 22, runs an independent proof gate, and boots the non-root production container.

To compare benchmark reports, Syncio refuses mismatched hardware or workloads:

```bash
npm run benchmark:compare -- syncio.json baseline.json
```

To qualify a real public deployment:

```bash
SYNCIO_PUBLIC_BASE_URL=https://db.example.com \
SYNCIO_PUBLIC_TOKEN='<real qualification token>' \
npm run qualify:external
```

The external gate validates HTTPS certificate health, `/health`, protocol compatibility, authenticated write/read persistence, anonymous denial, and cleanup.

## Production example

```js
import { openSuperiorProduction } from 'syncio-db/superior-production';

const db = await openSuperiorProduction('./data/app.syncio.json');
await db.defineIndex('users', ['tenantId', 'email'], {
  name: 'tenant_email',
  unique: true
});

await db.collection('users').upsert({
  id: 'u1',
  tenantId: 't1',
  email: 'ada@example.com',
  profile: { address: { city: 'Bronx' } },
  score: 10
});

const users = db.collection('users').query({
  where: { 'profile.address.city': 'Bronx', score: { $gte: 10 } }
});

console.log(users);
await db.close();
```

## Documentation

- `docs/ARCHITECTURE.md` — architecture, invariants, and scale boundaries.
- `docs/COMMERCIALIZATION.md` — plans, metering, invoices, funnel and commercial products.
- `docs/COMPLETION_LEDGER.md` — evidence-backed capability status.
- `docs/PRODUCTION_QUALIFICATION.md` — independent launch qualification.

Passing CI means the tested guarantees passed on that exact commit. It does not turn unperformed competitor benchmarks, external payment operations, public infrastructure, protected-branch governance, or customer adoption into evidence.
