# Syncio Roadmap

Syncio is the Cannon realtime and local-first database platform.

## Product contract

Syncio owns durable storage, documents/records, queries, indexes, transactions, realtime subscriptions, change streams, offline operation, replication, conflict handling, authentication integration, access policies, backups, migrations, export, and eventual self-hosting.

## Design sources

Syncio combines Firebase's realtime simplicity, Supabase's openness and relational power, and Convex's reactive query model while avoiding vendor lock-in, opaque pricing behavior, and proprietary data traps.

## Implementation order

1. Embedded durable local store.
2. Collections and indexed queries.
3. ACID transaction layer.
4. Change log and subscriptions.
5. Replication protocol.
6. Offline queue and conflict resolution.
7. Auth/access policy integration.
8. Backup/export/migration tooling.
9. Hosted Syncio service and self-hosting distribution.

## Proof gates

Persistence requires restart-survival tests. Transactions require atomicity/isolation tests. Realtime requires concurrent-client tests. Replication requires disconnect/reconnect and conflict tests. No cloud feature is supported until it survives failure injection.

## Commercial boundary

Syncio is a primary revenue product: hosted projects, storage, bandwidth, realtime connections, backups, compute, enterprise support, private regions, dedicated clusters, compliance, and managed migration.
