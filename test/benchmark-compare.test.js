import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';

const runtime = { platform: process.platform, arch: process.arch, cpus: 8, cpuModel: 'test-cpu', totalMemoryBytes: 16_000_000_000 };
const workload = { records: 1000, reads: 5000, indexedQueries: 1000, trials: 5 };

function report(engine, values = {}) {
  const metric = (value) => ({ min: value, p50: value, p95: value, max: value, mean: value });
  return {
    format: 'syncio-benchmark/2',
    engine,
    runtime,
    workload,
    results: {
      writesPerSecond: metric(values.writes ?? 1000),
      readsPerSecond: metric(values.reads ?? 5000),
      indexedQueriesPerSecond: metric(values.queries ?? 2000),
      rssBytes: metric(values.rss ?? 100_000_000),
      databaseBytes: metric(values.storage ?? 10_000_000)
    }
  };
}

async function runCompare(syncio, baseline, env = {}) {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'syncio-bench-compare-'));
  const syncioFile = path.join(dir, 'syncio.json');
  const baselineFile = path.join(dir, 'baseline.json');
  await fs.writeFile(syncioFile, JSON.stringify(syncio));
  await fs.writeFile(baselineFile, JSON.stringify(baseline));
  const result = spawnSync(process.execPath, ['scripts/benchmark-compare.mjs', syncioFile, baselineFile], {
    cwd: process.cwd(),
    encoding: 'utf8',
    env: { ...process.env, ...env }
  });
  await fs.rm(dir, { recursive: true, force: true });
  return result;
}

test('comparative benchmark gate passes only when declared throughput/memory/storage thresholds pass', async () => {
  const result = await runCompare(
    report('syncio-production', { writes: 1200, reads: 6000, queries: 2400, rss: 80_000_000, storage: 8_000_000 }),
    report('baseline', { writes: 1000, reads: 5000, queries: 2000, rss: 100_000_000, storage: 10_000_000 }),
    { SYNCIO_BENCH_MIN_THROUGHPUT_RATIO: '1.1', SYNCIO_BENCH_MAX_MEMORY_RATIO: '0.9', SYNCIO_BENCH_MAX_STORAGE_RATIO: '0.9' }
  );
  assert.equal(result.status, 0, result.stderr);
  const parsed = JSON.parse(result.stdout);
  assert.equal(parsed.pass, true);
});

test('comparative benchmark gate rejects mismatched workloads instead of producing misleading ratios', async () => {
  const baseline = report('baseline');
  baseline.workload = { ...workload, records: 2000 };
  const result = await runCompare(report('syncio-production'), baseline);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /workload mismatch/i);
});

test('comparative benchmark gate rejects mismatched hardware', async () => {
  const baseline = report('baseline');
  baseline.runtime = { ...runtime, cpuModel: 'different-cpu' };
  const result = await runCompare(report('syncio-production'), baseline);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /hardware mismatch/i);
});
