# Syncio Technical Superiority V2

## Permanent laws

1. Commit Fabric is the canonical source of truth for externally visible state transitions.
2. Derived structures are reconstructable accelerators, never authority.
3. Memory, CPU, SSD I/O, network, egress, coordination, materialization, and fan-out are bounded resources.
4. Distributed coordination is paid only when data semantics require it.
5. Durability and correctness outrank freshness and background throughput.

## Implemented architecture

### A. Canonical Commit Fabric 2.0
`AttestedCommitChain` adds hash chaining, periodic Merkle-style roots, root comparison for anti-entropy, and deterministic verification over Commit Fabric entries. Roots are acceleration metadata; sequential commit verification remains the fallback.

### B. Adaptive Segment Engine
`AdaptiveSegmentPolicy` classifies measured access patterns into general, append, dense-read, indexed-locality, and blob-separated physical representations. Rewrites require minimum evidence, minimum expected benefit, and cooldown hysteresis. The general representation is the safe fallback.

### C. Working-Set Governor
`WorkingSetGovernor` provides hierarchical byte budgets for database/tenant/workload/operation scopes. Optional memory may register ordered shedding callbacks. Work that cannot fit after shedding is rejected explicitly; critical work raises `SYNCIO_MEMORY_BUDGET` instead of silently exhausting the process.

### D. Shared Reactive Execution Graph
`SharedReactiveExecutionGraph` canonicalizes equivalent queries into shared upstream nodes, performs one evaluation per node refresh, and applies authorization-safe terminal projections per subscriber. Node and subscriber ceilings prevent success-driven fan-out from becoming unbounded state.

### E. Authorization Factoring
`AuthorizationFactor` caches only stable scope decisions using subject/action/collection/policy-version identity. Final record authorization is mandatory on every record. Policy changes invalidate the scope cache.

### F. Consistency Domains
`ConsistencyDomainPlanner` classifies work as local, partition, regional, or global and promotes only when partition, region, collection, or requested semantics require stronger coordination.

### G. Semantic Offline Transactions
`applySemanticOperation` implements deterministic semantic intents for increment, append, transition, and claim operations. Transition and claim preserve preconditions and raise `SYNCIO_INTENT_CONFLICT` when assumptions no longer hold.

### H. Protocol-Neutral Database Core
`ProtocolAdapterRegistry` provides versioned transport adapters, capability reporting, and canonical semantic digests so JSON, binary, WebSocket, embedded, and future protocol frontends can be different encodings of one database contract.

### I. Automatic Hot-Partition Fission
`HotPartitionFissionController` splits on measured request heat rather than data size alone, includes cooldown hysteresis, and pins rather than proliferating metadata after the partition ceiling is reached.

### J. Cost-Governed Execution
`BoundedCostLearner` learns bounded exponential moving averages from observed cost vectors and refuses to trust the learned model until sample count and prediction error gates pass. Static conservative estimates remain the fallback.

### K. Deterministic Recovery Simulation
`DeterministicRecoveryReplay` records indexed, digest-bound incident events, verifies the event log, produces an event root manifest, and replays through a deterministic reducer to a state digest.

### L. Progressive Operational Topology
`ProgressiveTopology` preserves the logical database contract from embedded through single-server, replicated, partitioned, and multi-region modes. Promotion reports required topology steps and explicitly states that application rewrites are not part of the migration contract.

## 1x / 10x / 100x contract

### 1x
- one authoritative segmented store
- local OCC
- local shared reactive graph
- direct durable intents
- strict process memory budget
- optional replication
- local recovery manifests
- direct routing

### 10x
- partitioned state
- partition-aware OCC and reservations
- independently scalable subscription/replication workers
- tenant and workload memory budgets
- replicated partition groups
- locality-aware routing
- partition recovery roots

### 100x
- regional partition groups
- explicit global coordination only for global semantics
- hierarchical regional fan-out
- fleet-level resource budgets
- geo-replicated consistency domains
- regional/global state attestations
- workload, region, consistency, and cost-aware placement

## Success-too-well controls

Hard ceilings or governed budgets must exist for subscription cardinality, dependency cardinality, materialization memory, retained history, reseed concurrency, cross-partition transactions, index count, partition count, split/migration frequency, backup bandwidth, repair bandwidth, tenant share, and egress. Resource pressure must degrade optional caches/materializations and background throughput before correctness or durability.

## Evidence contract

`npm run qualify:evidence -- <manifest.json>` validates benchmark evidence metadata. A benchmark family must name supported baselines and record hardware, OS/kernel identity, storage device, database versions, configuration, dataset, workload generator/specification, warmup, duration, raw results, failure behavior, and reproducibility seed.

Evidence families:

- document: MongoDB, Couchbase
- embedded: SQLite, libSQL
- realtime: Convex, Supabase Realtime, MongoDB change streams
- local-first: Couchbase Lite, Electric, Turso
- distributed: CockroachDB, FoundationDB, TiKV
- hot-path: Redis

No superiority claim becomes `proven` merely because code exists. Comparative claims require an identical workload and environment plus raw competitor measurements.

## Fallback doctrine

Every adaptive mechanism has a non-adaptive fallback:

- state roots -> sequential Commit Fabric verification
- adaptive layouts -> general segment layout
- shared reactive graph -> isolated recomputation
- authorization factoring -> full policy evaluation
- learned cost model -> static conservative costs
- consistency promotion -> strongest configured consistency
- semantic intent -> ordinary transaction/conflict path
- protocol adapters -> canonical JSON transport
- partition fission -> pinned partition/capacity increase
- deterministic replay -> ordinary WAL/PITR recovery
- topology promotion -> capability-negotiated current topology

The architectural objective is not to win every isolated benchmark. It is to reduce the number of separate systems required to deliver durable document storage, realtime, offline operation, bounded resource behavior, and an upgrade path from one process to distributed infrastructure while preserving one canonical history.
