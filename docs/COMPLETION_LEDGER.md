# Syncio Production Completion Ledger

Status vocabulary: PROVEN, PARTIALLY_PROVEN, UNVERIFIED, FAILED.

| Capability | Status | Evidence / next gate |
| --- | --- | --- |
| Durable embedded records | PROVEN | Restart tests cover persisted records and concurrent writes. |
| Crash-aware atomic file replacement | PROVEN | Temp-file fsync, atomic rename, directory fsync, backup recovery tests. |
| Durable change feed | PROVEN | Changes persist with the database and survive server/database restart. |
| Replication push idempotency | PROVEN | Duplicate `changeId` test proves same packet is not applied twice within retention horizon. |
| Replication cursor retention safety | UNVERIFIED | Must reject expired cursors and force snapshot/reseed instead of silently skipping history. |
| Integrated transactions | PROVEN | Multi-record commit, durable restart, rollback-on-exception tests. |
| Transaction isolation under contention | PARTIALLY_PROVEN | Serialized writer queue exists; dedicated contention and starvation tests still required. |
| JSON persistence semantic consistency | PROVEN | Non-JSON/non-finite/non-plain values are rejected before commit. |
| Query engine | PARTIALLY_PROVEN | Filtering/order/limit work; indexes are not yet integrated into execution planning. |
| Persistent indexes | UNVERIFIED | QueryIndex exists as a utility only. |
| Realtime local watchers | PROVEN | Durable-write event test exists. |
| Multi-client realtime delivery | PARTIALLY_PROVEN | In-process service subscription exists; network streaming protocol is not implemented. |
| Conflict resolution | PARTIALLY_PROVEN | Deterministic LWW convergence test exists; stronger causality/CRDT strategies remain unverified. |
| Offline queue | PARTIALLY_PROVEN | Retry retention in memory is tested; restart-persistent client queue is not implemented. |
| Authentication integration | PARTIALLY_PROVEN | Auth hook exists; first-party token/session implementation is absent. |
| Authorization | PARTIALLY_PROVEN | Deny-by-default policy engine is tested; row/document-field policy semantics and policy test tooling remain. |
| Request boundary security | PROVEN | Request IDs, body limits, malformed JSON handling, error redaction, timeouts. |
| Observability | PARTIALLY_PROVEN | Structured request/error event hook exists; metrics/tracing/exporters/SLOs remain. |
| Backup/export/import | PARTIALLY_PROVEN | Local backup recovery and export/import utilities exist; scheduled, encrypted, remote backup verification is absent. |
| Migrations | PARTIALLY_PROVEN | Ordered migration utility tested; deploy-time migration orchestration/rollback absent. |
| Hosted control plane | UNVERIFIED | Not implemented. |
| Hosted data plane | UNVERIFIED | Self-host HTTP process exists; multi-tenant hosted service is absent. |
| Billing / entitlements | UNVERIFIED | Not implemented. |
| Account lifecycle / deletion / export | UNVERIFIED | Product-level user/project lifecycle absent. |
| Deployment / rollback | UNVERIFIED | CI exists; deployable image/package and rollback procedure absent. |
| Performance qualification | UNVERIFIED | No benchmark harness or 1x/10x/100x workload evidence yet. |
| Commercialization | UNVERIFIED | Revenue model is documented in roadmap; monetized product path not implemented. |

## Current release authority

**NOT READY** for public production hosting.

The embedded/local-first core is materially stronger than the previous repository state, but launch remains blocked by cursor-retention correctness, integrated indexing, persistent offline client state, network realtime subscriptions, hosted tenancy/auth, deployment packaging, production observability, backups, billing/entitlements, and qualification at expected load.
