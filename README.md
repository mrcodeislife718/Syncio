# Syncio

Syncio is a lightweight realtime document database designed to combine rich document-database capability with a small operational footprint, durable offline synchronization, and resumable realtime data delivery.

The product target is straightforward:

- document-oriented application capability comparable to the reasons developers choose MongoDB;
- operational lightness, fast hot-data access, and inexpensive writes associated with Redis-class systems;
- realtime and offline behavior built into the database rather than added as a separate notification service;
- self-hosting and provider independence without forcing applications into a proprietary data trap.

## Current status

Syncio is under active production qualification. The embedded core and authenticated self-host runtime have substantial verified functionality. The managed commercial platform is not yet launch-ready. `docs/COMPLETION_LEDGER.md` records what is proven, partial, unverified, or failed.

## Core architecture

A successful mutation follows one ordered path:

```text
client write
  -> validation and authorization
  -> transaction/conflict control
  -> fsynced append-only durable log
  -> committed in-memory document state
  -> index maintenance
  -> realtime change stream
  -> replication/offline synchronization history
  -> periodic checkpoint and compaction
```

The same committed change is the source of truth for local watchers, network realtime, replication, reconnect replay, and offline synchronization.

## Document capability

Syncio now supports flexible nested JSON documents and arrays, dotted-path queries, logical and comparison operators, projections, multi-field sorting, pagination, nested persistent indexes, compound indexes, unique and sparse-unique indexes, partial atomic document updates, multi-document transactions, and aggregation.

Examples of supported query/update concepts include:

```text
customer.address.city = "Bronx"
total >= 25
$and / $or / $nor
$in / $nin / $exists / $all / $elemMatch
$set / $unset / $inc / $mul / $min / $max
$push / $addToSet / $pull / $rename
```

Aggregation currently includes match, project, sort, skip, limit, count, unwind and group, with sum/average/min/max/first/last/push/set accumulators.

These capabilities work through the embedded API and the HTTP server. HTTP supports nested query parameters, `PATCH` for atomic document updates, and `POST /collections/:collection/aggregate` for aggregation pipelines.

Unique indexes are enforced before commit, including inside transactions, replicated writes, and simultaneous competing writes.

## Verified storage behavior

Normal mutations are persisted to an append-only write-ahead log instead of rewriting the complete database file for every record change. The log is fsynced before the mutation becomes committed in memory. Periodic checkpoints consolidate current state and compact old log entries.

Verified recovery behavior includes replay after restart, interrupted-final-write handling, corruption detection, stale-log handling after checkpoint, and latest-checkpoint backup recovery.

## Realtime

Collection streams are resumable by durable sequence number:

```text
GET /subscribe/orders?after=88201
```

Every delivered change includes its sequence as the Server-Sent Events `id`. Clients can reconnect using `after=<sequence>` or `Last-Event-ID`. Syncio replays retained missed changes first and then continues delivering live commits.

If a requested cursor has fallen outside retained history, Syncio returns an explicit conflict instead of silently skipping changes.

## Requirements

Node.js 20 or newer. The production runtime currently has no third-party package dependencies.

## Install from source

```bash
npm install
npm run qualify
```

## Embedded example

```js
import { IndexedSyncioDatabase } from './src/indexed.js';
import { atomicUpdateDocument } from './src/document-api.js';

const db = await IndexedSyncioDatabase.open('./data/app.syncio.json');
await db.defineIndex('users', ['tenantId', 'email'], {
  name: 'tenant_email',
  unique: true
});

await db.collection('users').insert({
  id: 'u1',
  tenantId: 't1',
  email: 'ada@example.com',
  profile: { address: { city: 'Bronx' } },
  score: 10
});

await atomicUpdateDocument(db, 'users', 'u1', {
  $inc: { score: 5 },
  $set: { 'profile.level': 'gold' }
});

const users = db.collection('users').query({
  where: { 'profile.address.city': 'Bronx', score: { $gte: 15 } },
  projection: { email: 1, score: 1 }
});

console.log(users);
await db.close();
```

## Architecture and qualification

- `docs/ARCHITECTURE.md` — architecture, invariants, and scaling boundaries.
- `docs/COMPLETION_LEDGER.md` — evidence-backed capability status.
- `docs/PRODUCTION_QUALIFICATION.md` — independent launch qualification and remaining gates.

Passing CI means the tested guarantees passed on that exact commit. It does not turn untested managed-cloud or commercial capabilities into proven claims.

## Remaining capability direction

The next MongoDB-class capability gaps include schema validation modes, TTL, broader aggregation/query operators, text/search and geospatial indexes, point-in-time recovery, partitioning/sharding, mature client drivers, and managed clusters. Large-dataset work also needs paged/segmented state so the database does not require its complete logical state to remain hot in memory.
