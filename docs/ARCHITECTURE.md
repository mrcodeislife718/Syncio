# Syncio Production Architecture

## Product intent

Syncio is a lightweight realtime document database. Its target is MongoDB-class application capability, Redis-class operational lightness, and offline/realtime behavior that remains durable through disconnects and process restarts.

The defining architectural rule is that realtime, replication, offline synchronization, CDC, and recovery derive from the same ordered durable database changes. They must not maintain independent histories that can disagree.

## Plane boundaries

### State plane
Owns durable documents, transaction state, indexes, write-ahead log, checkpoints, retained change history, migration metadata, backup metadata, and replica/resume cursors.

### Data plane
Owns reads, writes, queries, transactions, realtime streams, replication transfer, flow control, and conflict application. It remains correct without the hosted control plane.

### Control plane
Owns accounts, projects, regions, configuration, credentials, quotas, billing linkage, deployment orchestration, backup policies, and lifecycle operations. A working single-node control plane exists; managed multi-instance orchestration remains a launch gate.

### Policy plane
Owns authentication, authorization, resource limits, tenancy boundaries, entitlements, and administrative authority. Policy enforcement remains outside application data logic.

### Observability plane
Owns structured events, metrics, traces, audit records, health, SLOs, and operator diagnostics. Data-plane operation must not fail merely because observability export is unavailable.

### Execution plane
Owns replication work, checkpoint/compaction, backups, migrations, index builds, repair, and future distributed scheduling. Background work must be idempotent, resumable, observable, and admission-controlled.

## Single committed-change spine

A successful write follows this logical sequence:

```text
INPUT
  -> validate request/document
  -> authorize
  -> serialize transaction/conflict decision
  -> create ordered change event(s)
  -> append commit to WAL
  -> fsync WAL
  -> publish committed in-memory state
  -> update derived index state
  -> emit database change stream
       -> local watchers
       -> network realtime
       -> replication
       -> offline synchronization
       -> future CDC/PITR consumers
  -> periodically checkpoint + compact
```

No observer receives a committed event before the durable WAL append succeeds.

## WAL and checkpoint invariant

Normal record commits do not rewrite the full database checkpoint. They append a digest-protected ordered WAL entry and fsync it before the new state becomes committed in memory.

A checkpoint periodically writes the consolidated state atomically and then compacts WAL entries already represented by that checkpoint. Recovery replays only WAL entries newer than the checkpoint sequence.

The ordering makes both important crash shapes safe:

1. **WAL durable, checkpoint absent/stale:** recovery replays the durable WAL.
2. **checkpoint durable, old WAL still present:** recovery ignores WAL entries whose sequence is already included in the checkpoint.

A truncated final WAL record is treated as an interrupted append and ignored. A complete record whose digest is wrong is treated as corruption and rejected.

The checkpoint path also writes a latest-state backup mirror after the primary checkpoint is durable. A corrupted primary checkpoint can therefore recover the latest successfully completed checkpoint rather than intentionally losing the most recent checkpointed mutation.

## Realtime resume invariant

Every committed database change has a monotonically increasing local sequence. Realtime change events use that same sequence as the resume cursor and SSE event ID.

A collection subscriber may request:

```text
/subscribe/orders?after=88201
```

The database first replays retained `orders` changes after `88201`, then remains attached to future committed changes. `Last-Event-ID` is also accepted as the resume cursor.

If the cursor is older than retained history, the server returns an explicit `stream_resume_expired` conflict before opening the stream. It never pretends a partial history is complete.

Replication uses the same retention boundary. If a replication cursor has expired, a verified snapshot/reseed is required before incremental synchronization resumes.

## Transaction invariant

A transaction executes against a private draft. No live state is changed until the full transaction has a durable WAL commit. Exceptions discard the draft. All record-level events from a successful transaction are emitted in durable sequence order from the same database change stream.

## Index invariant

Indexes are derived acceleration structures, never a second source of truth. Persistent index definitions survive restart. Index contents are rebuilt or maintained from committed document state, and query execution falls back to correct scanning whenever an index cannot satisfy a query.

## Lightweight storage direction

"Redis-like" means low operational and resource overhead, not a requirement that the full dataset always remain in RAM.

The target hierarchy is:

```text
HOT WORKING SET   -> RAM / compact indexes
WARM DATA         -> page or mapped cache
COLD DATA         -> local durable SSD segments
CHANGE HISTORY    -> compacted ordered segments
HOSTED ARCHIVE    -> object storage
```

The application-facing document model should not depend on which tier currently holds a document.

## 1x / 10x / 100x

### 1x — embedded / single-process
Current strengths: WAL-first durability, periodic atomic checkpoints, latest-checkpoint backup recovery, persistent indexes, transactions, resumable realtime, offline queues, and deterministic local write serialization.

Remaining optimization: mutations still clone substantial logical in-memory state before commit. This is acceptable for correctness and modest datasets but is not the final large-dataset memory architecture.

### 10x — serious local-first and self-hosted workloads
Required work includes paged/segmented document storage, incremental transaction write sets instead of whole-state drafts, compound and unique indexes, richer query planning, aggregation, TTL scheduling, point-in-time restore, query subscriptions, sustained workload qualification, and explicit hot/warm cache limits.

### 100x — managed foundational infrastructure
Required work includes partitioning/sharding, replicated logs, explicit consistency modes, tenant scheduling and isolation, distributed admission control, regional placement, rolling compatibility, online index/schema changes, remote backups, disaster recovery, workload-aware routing, and measured cost accounting.

## Success-too-well failures

Rapid adoption can exhaust retained history, overload realtime fan-out, create noisy-neighbor contention, inflate egress cost, saturate compaction/backup windows, or make rolling upgrades incompatible across many clients. The architecture therefore requires bounded history with explicit reseed, per-project quotas, backpressure, fan-out accounting, compatibility negotiation, and independently scalable state/data/control planes.

## Technical-superiority rule

Syncio only claims superiority where evidence proves it. The design is intentionally positioned to test advantages such as:

- one durable ordered change source for persistence, realtime, replication and offline synchronization;
- resumable realtime without a separate notification database;
- restart-persistent offline work;
- small dependency and idle-resource footprint;
- WAL-first inexpensive normal writes;
- explicit corruption/cursor-expiry behavior instead of silent divergence;
- self-hosted-to-managed portability without changing the application data model.

Each superiority claim requires a named competitor baseline, identical workload, reproducible hardware/environment, latency/throughput/memory/storage measurements, failure tests, and an acceptable-regression threshold before it becomes a product claim.
