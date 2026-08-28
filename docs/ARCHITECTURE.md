# Syncio Production Architecture

## Product intent

Syncio is a local-first realtime database platform intended to combine the responsiveness and offline ergonomics of Firebase-style systems, the openness and policy strength associated with Postgres/Supabase-style systems, and reactive data delivery while preserving self-hosting and provider independence.

## Plane boundaries

### State plane
Owns durable records, transaction state, indexes, durable change history, migration metadata, backup metadata, and replica cursors.

### Data plane
Owns reads, writes, queries, transactions, subscriptions, replication transfer, flow control, and conflict application. It must remain correct without depending on the hosted control plane.

### Control plane
Owns projects, tenants, regions, configuration, credentials, quotas, billing linkage, deployment orchestration, backup policies, and lifecycle operations. This is not yet implemented.

### Policy plane
Owns authentication integration, authorization policy evaluation, resource limits, tenancy boundaries, and administrative authority. Policy enforcement must remain outside application/model logic.

### Observability plane
Owns structured events, metrics, traces, audit records, health, SLOs, and operator diagnostics. Data-plane operation must not fail merely because observability export is unavailable.

### Execution plane
Owns background replication, compaction, backup jobs, migrations, index builds, repair, and future distributed scheduling. Background work must be idempotent, resumable, observable, and admission-controlled.

## Current durable commit path

INPUT write
→ validate collection/id/JSON semantics
→ serialize through writer queue
→ copy current committed state
→ mutate private draft
→ append durable change metadata in the same draft
→ write temporary file
→ fsync temporary file
→ preserve previous durable backup
→ atomic rename
→ fsync containing directory
→ publish new committed state in memory
→ emit realtime event

This order is intentional: listeners never observe a mutation before the durable commit succeeds, and a failed persistence operation cannot leave the process serving an uncommitted in-memory value.

## Replication invariant

A locally committed mutation and its replication history are one durable state transition. Replication uses stable change identifiers for duplicate suppression. A receiver applies a remote change and records its deduplication identity in the same durable commit.

Remaining protocol requirement: bounded history must expose an explicit expired-cursor/snapshot-required response. A replica must never silently advance past history that has already been compacted.

## Transaction invariant

A transaction executes against a private draft. No live state is mutated until the complete draft is durably persisted. Exceptions discard the draft. The resulting record-level changes are appended to the durable change feed in the same commit.

## 1x / 10x / 100x

### 1x — embedded / single-process
The current JSON snapshot engine is acceptable for correctness development, local tooling, tests, and modest datasets. Primary bottlenecks are whole-database cloning and whole-file rewrite cost.

### 10x — serious local-first workloads
Required changes: persistent query indexes, incremental storage pages/segments, write-ahead logging or append-only segments, snapshot compaction, persistent offline client queue, streaming subscriptions, cursor expiry/reseed, benchmark harness, resource limits, and richer policy semantics.

### 100x — hosted foundational infrastructure
The storage engine must no longer rewrite the full logical database per mutation. The platform needs partitioning, replicated logs, quorum/consensus decisions appropriate to consistency mode, tenant isolation, admission control, backpressure, regional placement, rolling compatibility, online schema/index changes, encrypted backups, disaster recovery, workload-aware scheduling, and explicit cost accounting.

## Success-too-well failures

Rapid adoption can exhaust storage through unbounded history, overload fan-out through realtime subscriptions, create noisy-neighbor failure across tenants, inflate egress cost, saturate backup windows, overwhelm compaction, or create upgrade incompatibility across many deployed clients. The design therefore requires bounded logs with safe reseed, per-tenant quotas, backpressure, fan-out accounting, compatibility negotiation, and independent evolution of storage/data/control planes.

## Technical-superiority targets

Syncio should only claim superiority where benchmarks prove it. Planned measurable targets include restart-persistent offline writes and transactions, deterministic replayable replication, lower idle/embedded resource cost, predictable self-hosting economics, explicit failure/recovery semantics, provider-independent auth and deployment adapters, policy tests as a first-class artifact, and a local-to-hosted architecture that does not require application rewrites.
