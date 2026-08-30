import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { openSuperiorProduction } from '../src/superior-production.js';
import { pathBytes } from '../src/usage-sampler.js';

const records = positiveInt(process.env.SYNCIO_BENCH_RECORDS, 1000);
const reads = positiveInt(process.env.SYNCIO_BENCH_READS, 5000);
const indexedQueries = positiveInt(process.env.SYNCIO_BENCH_QUERIES, 1000);
const trials = positiveInt(process.env.SYNCIO_BENCH_TRIALS, 5);
const results = [];

for (let trial = 0; trial < trials; trial++) results.push(await runTrial(trial));

console.log(JSON.stringify({
  format: 'syncio-benchmark/2',
  engine: 'syncio-production',
  runtime: {
    node: process.version,
    platform: process.platform,
    arch: process.arch,
    cpus: os.cpus().length,
    cpuModel: os.cpus()[0]?.model ?? 'unknown',
    totalMemoryBytes: os.totalmem()
  },
  workload: { records, reads, indexedQueries, trials },
  results: summarize(results),
  trials: results
}, null, 2));

async function runTrial(trial) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), `syncio-benchmark-${trial}-`));
  const file = path.join(root, 'bench.syncio');
  let db;
  try {
    db = await openSuperiorProduction(file, {
      maxCacheBytes: positiveInt(process.env.SYNCIO_BENCH_CACHE_BYTES, 16 * 1024 * 1024),
      maxIndexCacheBuckets: positiveInt(process.env.SYNCIO_BENCH_INDEX_BUCKETS, 32)
    });
    await db.defineIndex('records', 'bucket');
    const collection = db.collection('records');

    const writeStart = performance.now();
    for (let i = 0; i < records; i++) await collection.upsert({ id: `r${i}`, bucket: `b${i % 100}`, value: i });
    const writeMs = performance.now() - writeStart;

    for (let i = 0; i < Math.min(records, 100); i++) collection.get(`r${i}`);
    const readStart = performance.now();
    let checksum = 0;
    for (let i = 0; i < reads; i++) checksum += collection.get(`r${i % records}`).value;
    const readMs = performance.now() - readStart;

    for (let i = 0; i < 100; i++) collection.query({ where: { bucket: `b${i % 100}` } });
    const queryStart = performance.now();
    for (let i = 0; i < indexedQueries; i++) checksum += collection.query({ where: { bucket: `b${i % 100}` } }).length;
    const indexedQueryMs = performance.now() - queryStart;

    const memory = process.memoryUsage();
    const status = db.storageStatus();
    await db.close();
    db = null;
    return {
      writeMs,
      readMs,
      indexedQueryMs,
      writesPerSecond: records / (writeMs / 1000),
      readsPerSecond: reads / (readMs / 1000),
      indexedQueriesPerSecond: indexedQueries / (indexedQueryMs / 1000),
      databaseBytes: await pathBytes(root),
      rssBytes: memory.rss,
      heapUsedBytes: memory.heapUsed,
      checksum,
      authoritativeState: status.authoritativeState,
      fullStateResidentInRam: status.fullStateResidentInRam
    };
  } finally {
    if (db) await db.close().catch(() => undefined);
    await fs.rm(root, { recursive: true, force: true });
  }
}

function summarize(rows) {
  const fields = ['writesPerSecond', 'readsPerSecond', 'indexedQueriesPerSecond', 'databaseBytes', 'rssBytes', 'heapUsedBytes'];
  return Object.fromEntries(fields.map((field) => {
    const values = rows.map((row) => row[field]).sort((a, b) => a - b);
    return [field, { min: values[0], p50: percentile(values, 0.5), p95: percentile(values, 0.95), max: values.at(-1), mean: values.reduce((a, b) => a + b, 0) / values.length }];
  }));
}

function percentile(values, p) {
  if (values.length === 1) return values[0];
  const index = (values.length - 1) * p;
  const lower = Math.floor(index);
  const upper = Math.ceil(index);
  if (lower === upper) return values[lower];
  const weight = index - lower;
  return values[lower] * (1 - weight) + values[upper] * weight;
}

function positiveInt(value, fallback) {
  const number = Number(value ?? fallback);
  if (!Number.isSafeInteger(number) || number < 1) throw new TypeError('benchmark counts must be positive integers');
  return number;
}
