# Syncio

Syncio is a local-first realtime database core designed for durable embedded storage, self-hosted synchronization, offline operation, policy-controlled access, and an eventual managed service without forcing applications into a proprietary data trap.

## Current status

Syncio is under active production qualification. The embedded core is usable for development and controlled deployments, but the hosted commercial platform is **not yet launch-ready**. See `docs/COMPLETION_LEDGER.md` for evidence-backed status rather than feature claims.

## Core guarantees currently under test

- records survive process restart;
- writes are serialized and persisted through atomic replacement;
- committed mutations and replication history are persisted together;
- duplicate replication changes are suppressed within the retained change horizon;
- transactions execute against private drafts and publish only after durable commit;
- failed transactions leave committed state unchanged;
- local collection listeners fire after durable writes;
- request bodies are bounded and malformed JSON is rejected;
- authorization is deny-by-default and write policies may inspect proposed data;
- internal exceptions are observable without leaking exception messages to clients;
- a previous durable database image is retained for corruption recovery.

## Requirements

Node.js 20 or newer.

## Install from source

```bash
npm install
npm test
```

## Embedded usage

```js
import { open } from './src/index.js';

const db = await open('./data/app.syncio.json');
const users = db.collection('users');

await users.insert({ id: 'u1', name: 'Ada' });
console.log(users.get('u1'));

await db.transaction(async (tx) => {
  tx.collection('accounts').put({ id: 'a', balance: 90 });
  tx.collection('accounts').put({ id: 'b', balance: 10 });
});

await db.close();
```

## Self-hosted HTTP service

```js
import { open, createSyncioServer } from './src/index.js';

const db = await open('./data/app.syncio.json');
const service = createSyncioServer({
  db,
  policies: [
    { effect: 'allow', collection: 'users', action: 'read' },
    { effect: 'allow', collection: 'users', action: 'write' }
  ]
});

const address = await service.listen({ port: 8787 });
console.log(address.url);
```

Production systems should provide an authentication function and explicit least-privilege policies. An empty policy set denies access.

## Architecture

See `docs/ARCHITECTURE.md` for the state/data/control/policy/observability/execution plane boundaries and the 1x/10x/100x scaling plan.

## Production qualification

Repository CI runs the Node test suite on every push and pull request. Passing CI means the tested guarantees passed; it does **not** imply the entire product is production-ready. Untested capabilities remain UNVERIFIED in the completion ledger.

## Commercial direction

The planned managed product includes hosted projects, storage, bandwidth, realtime connections, backups, compute, enterprise support, private regions, dedicated clusters, compliance, and managed migrations. Billing, entitlements, hosted tenancy, deployment packaging, and production operations are still gated work and will not be represented as complete until verified.
