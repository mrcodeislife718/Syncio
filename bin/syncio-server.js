#!/usr/bin/env node
import crypto from 'node:crypto';
import { startSelfHostedSyncio } from '../src/self-host.js';
import { createTokenAuthority } from '../src/operations.js';

const command = process.argv[2] ?? 'serve';
const file = process.env.SYNCIO_DATA_FILE ?? './data/syncio.json';
const projectId = process.env.SYNCIO_PROJECT_ID ?? 'self-hosted';
const secret = process.env.SYNCIO_AUTH_SECRET;

if (!secret || Buffer.byteLength(secret) < 32) {
  console.error('SYNCIO_AUTH_SECRET must contain at least 32 bytes');
  process.exitCode = 64;
} else if (command === 'token') {
  const subject = process.argv[3] ?? 'operator';
  const authority = createTokenAuthority(secret, { issuer:`syncio:${projectId}` });
  process.stdout.write(`${authority.issue({subject,projectId,role:'owner',entitlements:['database','realtime']})}\n`);
} else if (command === 'serve') {
  const host = process.env.SYNCIO_HOST ?? '0.0.0.0';
  const port = Number(process.env.SYNCIO_PORT ?? 8787);
  if (!Number.isSafeInteger(port) || port < 1 || port > 65535) {
    console.error('SYNCIO_PORT must be an integer between 1 and 65535');
    process.exitCode = 64;
  } else {
    const runtime = await startSelfHostedSyncio({file,secret,projectId,host,port});
    console.log(JSON.stringify({event:'syncio.ready',projectId,url:runtime.address.url,pid:process.pid,instanceId:crypto.randomUUID()}));
    const shutdown = async (signal) => {
      try { await runtime.close(); console.log(JSON.stringify({event:'syncio.stopped',signal})); process.exit(0); }
      catch (error) { console.error(error); process.exit(1); }
    };
    process.once('SIGTERM',()=>void shutdown('SIGTERM'));
    process.once('SIGINT',()=>void shutdown('SIGINT'));
  }
} else {
  console.error(`Unknown command: ${command}`);
  process.exitCode = 64;
}
