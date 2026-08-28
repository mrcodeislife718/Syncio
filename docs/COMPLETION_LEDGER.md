# Syncio Production Completion Ledger

Status vocabulary: PROVEN, PARTIALLY_PROVEN, UNVERIFIED, FAILED.

A capability is PROVEN only when repository evidence demonstrates its implemented path. PROVEN does not imply the entire product is launch-ready.

| Capability | Status | Evidence / remaining gate |
| --- | --- | --- |
| Durable embedded records | PROVEN | Restart and concurrent-writer tests. |
| Crash-aware atomic file replacement | PROVEN | Temp fsync, backup, atomic rename, directory fsync, corruption recovery. |
| Durable change feed | PROVEN | Change history commits with data and survives restart. |
| Replication push idempotency | PROVEN | Stable change IDs and duplicate replay tests. |
| Replication cursor retention safety | PROVEN | Expired cursor returns snapshot-required; verified snapshot reseed resumes incremental replication. |
| Integrated transactions | PROVEN | Multi-record commit, rollback, restart durability. |
| Transaction isolation under contention | PROVEN | 100 contended increments plus queued-failure recovery tests. |
| JSON persistence semantic consistency | PROVEN | Values that change JSON meaning are rejected before commit. |
| Query engine | PARTIALLY_PROVEN | Filtering/order/limit and indexed equality queries work. HTTP list-query path still requires planner integration. |
| Persistent indexes | PROVEN | Persistent catalog, reopen, maintenance across writes/removes/transactions, execution-plan tests. |
| Realtime local watchers | PROVEN | Durable-write event tests. |
| Network realtime delivery | PROVEN | Auth-policy-compatible SSE delivery, heartbeat, bounded connection capacity and backpressure disconnect. Resume/replay semantics for SSE clients remain unverified. |
| Conflict resolution | PARTIALLY_PROVEN | LWW deterministic convergence is proven; causal/CRDT semantics are not. |
| Restart-persistent offline queue | PROVEN | Queue and retry state survive reopen; ReplicationClient can use it for later exactly-once server application via change IDs. |
| First-party token authentication | PROVEN | HMAC token issue/verify, expiry, canonical encoding, tamper rejection, project/account scoping. Key rotation/revocation lists remain unimplemented. |
| Authorization | PARTIALLY_PROVEN | Default deny, deny override, wildcard, request-body policy, project and entitlement boundaries are tested. Per-record read filtering/field-level policy tooling remains. |
| Request boundary security | PROVEN | Request IDs, limits, malformed input handling, error redaction and HTTP timeouts. |
| Rate limiting | PROVEN | Bounded token bucket, deterministic refill and self-host enforcement. Distributed/shared limiter remains unverified. |
| Admission control primitive | PROVEN | Concurrent admission primitive is tested; integration into every hosted request path remains. |
| Observability | PARTIALLY_PROVEN | Request/error metrics, percentile snapshots and durable audit events exist. External metrics/traces, alerting and SLOs remain. |
| Encrypted backup / restore | PROVEN | AES-256-GCM backup, digest/authentication, tamper rejection and restore tests. Scheduled/remote backup service remains. |
| Migration orchestration / rollback | PROVEN | Pre-migration encrypted backup, migration history, failed-migration safety and rollback tests. |
| Account lifecycle / privacy export / deletion | PROVEN | Durable signup state, export, redaction/deletion and token revocation tests. Legal retention policy remains external. |
| Project lifecycle and tenant authority | PROVEN | Project creation, ownership isolation, scoped token issuance and durable entitlement state. |
| Billing state / entitlements | PARTIALLY_PROVEN | Atomic idempotent subscription state and verified provider-neutral webhook ingestion. Live checkout/payment-provider adapter, refunds and failed-payment flows remain. |
| Hosted control plane | PARTIALLY_PROVEN | Separate HTTP control API and runnable `syncio-control` process exist with signup/projects/export/delete/billing ingress. HA, multi-instance consistency and production deployment remain. |
| Hosted data plane | PARTIALLY_PROVEN | Authenticated self-host runtime and container are verified. Managed multi-tenant routing/isolation and regional placement remain. |
| Self-host deployment | PROVEN | Non-root Docker image is built, booted, health-checked, authorization-tested and cleanly stopped in CI. |
| Deployment rollback | UNVERIFIED | No published immutable release artifact or verified previous-version rollback exercise yet. |
| CI qualification | PROVEN | Node 20/22 syntax + closure audit + full tests plus container qualification. |
| Repository closure audit | PROVEN | CI fails on TODO/FIXME/placeholders/not-implemented markers/skipped tests in production/test code. |
| Main branch protection | FAILED | GitHub reports `main` protection disabled; connector available in this session is read-only for protection settings. |
| Performance harness | PROVEN | Reproducible hardware/runtime/workload/throughput/memory benchmark command exists. |
| Performance qualification | UNVERIFIED | No accepted 1x/10x/100x thresholds or comparative benchmark evidence yet. |
| Storage scaling architecture | PARTIALLY_PROVEN | Current snapshot engine is correct; full-state clone/rewrite per commit remains a known 10x/100x ceiling requiring WAL/segments/compaction. |
| TLS / edge termination | UNVERIFIED | Runtime expects a trusted TLS-terminating edge; no deployment-specific TLS proof exists. |
| Key rotation / secret lifecycle | UNVERIFIED | Strong secret length and token expiry exist; rotation, revocation and KMS integration are absent. |
| Remote tracing / alerting / SLO response | UNVERIFIED | Local metrics/audit exist only. |
| Live payment provider / checkout | UNVERIFIED | No provider credentials or production provider contract have been exercised. |
| Customer-facing documentation/support | PARTIALLY_PROVEN | Technical README/architecture/ledger exist; operational runbook, API reference and support process require completion. |
| Commercialization | UNVERIFIED | Technical billing state exists, but discover → signup → checkout → paid entitlement → retention has not been exercised with real money/users. |

## Current release authority

**NOT READY** for public managed-cloud production.

### What is now strong enough to qualify separately

The embedded/local-first core and authenticated self-host container have moved substantially beyond prototype status. The repository now proves restart durability, transactions, replication recovery, durable offline work, SSE realtime, persistent indexes, first-party authentication, entitlement boundaries, encrypted backup/rollback, control-plane account/project lifecycle, rate limiting, and container operation.

### Blocking launch gaps

1. **BLOCKER — managed hosted data plane:** tenant routing, process/resource isolation, lifecycle orchestration and regional placement are not implemented or load-tested.
2. **BLOCKER — live monetization:** checkout/payment-provider adapter, signed provider contract verification against a real provider, failed-payment/cancellation/refund paths and real-money end-to-end verification are absent.
3. **CRITICAL — storage scale ceiling:** each commit still clones/serializes the logical database and atomically rewrites the snapshot. This is correct but cannot be claimed competitive at 10x/100x without WAL/segment/compaction work and benchmarks.
4. **CRITICAL — repository governance:** `main` is currently unprotected; required checks cannot be enforced until branch protection/rulesets are enabled in GitHub settings or through a connector with write support.
5. **HIGH — production secret lifecycle:** no KMS-backed rotation/revocation path.
6. **HIGH — production observability:** no remote metrics/traces, alerting, SLOs or incident response verification.
7. **HIGH — TLS/edge deployment:** no production edge configuration or certificate lifecycle has been tested.
8. **HIGH — performance evidence:** benchmark harness exists, but comparative baselines and 1x/10x/100x acceptance thresholds have not been established.

Anything above that has not been exercised remains UNVERIFIED rather than inferred.
