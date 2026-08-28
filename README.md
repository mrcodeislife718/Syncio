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

The same committed change is the source of truth for local watchers, network realtime, replication, reconnect replay, and offline synchronization. Syncio does not maintain an independent notification history that can drift away from the database.

## Verified storage behavior

Normal mutations are persisted to an append-only write-ahead log instead of rewriting the complete database file for every record change. The log is fsynced before the mutation becomes committed in memory. Periodic checkpoints consolidate current state and compact old log entries.

Verified recovery behavior includes:

- replaying committed log entries after process-style restart;
- ignoring an incomplete final log entry caused by an interrupted append;
- rejecting corruption in a complete log entry;
- ignoring stale log entries already represented by a durable checkpoint;
- retaining a latest-checkpoint backup that can recover a corrupted primary checkpoint;
- preserving ordered durable change history for replication and realtime resume.

## Realtime

Collection streams are resumable by durable sequence number.

```text
GET /subscribe/orders?after=88201
```

Every delivered change includes its sequence as the Server-Sent Events `id`. Clients can reconnect using `after=<sequence>` or the standard `Last-Event-ID` header. Syncio replays retained missed changes first and then continues delivering live commits.

If a requested cursor has fallen outside retained history, Syncio returns an explicit `409 stream_resume_expired` instead of silently skipping changes. Applications can refresh/reseed state and resume from a new current cursor.

## Requirements

Node.js 20 or newer.

## Install from source

```bash
npm install
npm run qualify
```

The runtime currently has no third-party production dependencies.

## Embedded usage

```js
import { open } from './src/index.js';

const db = await open('./data/app.syncio.json');
const users = db.collection('users');

await users.insert({ id: 'u1', name: 'Ada' });
console.log(users.get('u1'));

const stop = db.watchChanges({ collection: 'users', after: db.sequence }, (change) => {
  console.log(change.sequence, change.type, change.record);
});

await db.transaction(async (tx) => {
  tx.collection('accounts').put({ id: 'a', balance: 90 });
  tx.collection('accounts').put({ id: 'b', balance: 10 });
});

stop();
await db.close();
```

## Self-hosted service

The production-oriented self-host runtime provides authentication, entitlement checks, rate limiting, metrics, durable audit events, indexed queries, replication, resumable realtime, graceful shutdown, and non-root container packaging.

See the CLI entrypoint `syncio-server` and `docs/PRODUCTION_QUALIFICATION.md` for the exact verified deployment path.

## Architecture and qualification

- `docs/ARCHITECTURE.md` — architecture, invariants, and scaling boundaries.
- `docs/COMPLETION_LEDGER.md` — evidence-backed capability status.
- `docs/PRODUCTION_QUALIFICATION.md` — independent launch qualification and remaining gates.

Passing CI means the tested guarantees passed on that exact commit. It does not turn untested managed-cloud or commercial capabilities into proven claims.

## Product capability direction

Syncio's document-database roadmap includes rich nested queries and projections, secondary/compound/unique indexes, atomic document updates, aggregation, optional and enforced schemas, multi-document transactions, replication, partitioning, point-in-time recovery, TTL, geospatial and text/search indexes, client drivers, and managed clusters.

Those capabilities are promoted to complete only when they are implemented, integrated, secured where applicable, documented, and independently verified.
