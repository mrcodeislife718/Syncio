# Syncio Technical Superiority Program

## Permanent invariant
A specialized execution path may optimize a projection of state, but no subsystem may create a second source of truth for the same state transition. Persistence, realtime, replication, offline sync, CDC, PITR and reactive queries must prove state against the canonical Commit Fabric.

## Architecture
- Commit Fabric: tamper-evident logical identity for every externally visible transition.
- Adaptive State Plane: bounded hot cache over segmented SSD state; cold/archive adapters remain replaceable.
- Transaction Plane: migrate whole-state drafts to MVCC read/write sets and optimistic validation.
- Reactive Plane: dependency-aware invalidation and incremental materialization with bounded metadata.
- Sync Plane: selective views plus restart-durable semantic transaction intents.
- Policy Plane: deterministic compiled predicates with reference-interpreter differential testing.
- Execution Plane: priority budgets and cost vectors protect durability/correctness from realtime and background amplification.
- Distribution Plane: explicit consistency contracts and partition-locality classification/advice.
- Recovery Plane: integrity-verifiable Recovery Manifests make recovery explainable and replayable.

## Required evidence before superiority claims
No comparative claim is accepted without an identical workload, reproducible environment, named baseline, raw measurements and failure behavior.

Database: point reads/writes, mixed load, indexed query, aggregation, transactions, RSS, storage amplification, startup and recovery against MongoDB, Redis where semantically applicable, PostgreSQL and SQLite/libSQL.

Realtime: setup latency, commit-to-client p50/p95/p99, CPU/RAM per subscriber, fan-out, bytes, reconnect/resume and authorization overhead against Firestore, Supabase Realtime and Convex.

Local-first: offline read/write, restart-durable intent, conflicts, selective sync, long disconnect, reconnect and reseed against Firebase/Firestore, Couchbase Lite, Turso and Electric.

Infrastructure: 1x/10x/100x with process crash, corruption, disk pressure, packet loss, partitions, slow/dead replicas, hot partitions, expired cursors, ancient clients, backup/control-plane failure and subscription storms.

## Hard gates
- Memory superiority requires materially lower RSS on identical document workloads.
- Hot-path claims require defined latency ratio to Redis, not adjectives.
- Offline superiority requires restart-durable transaction intent with exactly-once/idempotent reconciliation evidence.
- Realtime superiority requires lower resource cost per maintained query under identical fan-out.
- Operational simplicity requires a representative application to remove separate cache/realtime/sync infrastructure.
- Cost superiority requires equal SLO and workload at lower measured infrastructure cost.
- Recovery superiority requires deterministic state digests and manifests under fault injection.
- Security superiority requires differential policy tests, tenant isolation and adversarial access qualification.

## 1x / 10x / 100x
1x: single node, bounded RAM, SSD segments, local OCC, reactive graph, selective sync, PITR, billing/control plane.
10x: partitioned storage, stateless gateways, independent subscription/replication workers, replicated partitions and regional locality.
100x: regional partition groups, explicit global coordination only where semantics require it, hierarchical fan-out, regional DR and multi-region control plane.

## Success-too-well controls
Bound dependency cardinality, subscriptions/query, queries/client, indexes/project, write amplification, fan-out/commit, retained history, protocol age, background-work debt, tenant resource share and cross-partition transaction frequency. Durability and correctness always outrank freshness and background throughput.
