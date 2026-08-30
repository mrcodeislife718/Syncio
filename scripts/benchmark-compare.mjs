import fs from 'node:fs/promises';
import path from 'node:path';

const files = process.argv.slice(2);
if (files.length < 2) {
  throw new Error('usage: node scripts/benchmark-compare.mjs <syncio.json> <baseline.json> [baseline2.json ...]');
}

const reports = await Promise.all(files.map(async (file) => {
  const raw = JSON.parse(await fs.readFile(file, 'utf8'));
  validateReport(raw, file);
  return { file: path.resolve(file), ...raw };
}));

const syncio = reports[0];
for (const baseline of reports.slice(1)) assertComparable(syncio, baseline);

const minThroughputRatio = finitePositive(process.env.SYNCIO_BENCH_MIN_THROUGHPUT_RATIO, 1);
const maxMemoryRatio = finitePositive(process.env.SYNCIO_BENCH_MAX_MEMORY_RATIO, 1);
const maxStorageRatio = finitePositive(process.env.SYNCIO_BENCH_MAX_STORAGE_RATIO, 1);

const comparisons = reports.slice(1).map((baseline) => {
  const metrics = {
    writes: ratio(syncio.results.writesPerSecond.p50, baseline.results.writesPerSecond.p50),
    reads: ratio(syncio.results.readsPerSecond.p50, baseline.results.readsPerSecond.p50),
    indexedQueries: ratio(syncio.results.indexedQueriesPerSecond.p50, baseline.results.indexedQueriesPerSecond.p50),
    rss: ratio(syncio.results.rssBytes.p50, baseline.results.rssBytes.p50),
    storage: ratio(syncio.results.databaseBytes.p50, baseline.results.databaseBytes.p50)
  };
  const pass = metrics.writes >= minThroughputRatio &&
    metrics.reads >= minThroughputRatio &&
    metrics.indexedQueries >= minThroughputRatio &&
    metrics.rss <= maxMemoryRatio &&
    metrics.storage <= maxStorageRatio;
  return { baseline: baseline.engine, file: baseline.file, metrics, pass };
});

const result = {
  format: 'syncio-benchmark-comparison/1',
  workload: syncio.workload,
  thresholds: { minThroughputRatio, maxMemoryRatio, maxStorageRatio },
  comparisons,
  pass: comparisons.every((comparison) => comparison.pass)
};
console.log(JSON.stringify(result, null, 2));
if (!result.pass) process.exitCode = 1;

function validateReport(report, file) {
  if (!report || typeof report !== 'object') throw new TypeError(`${file}: benchmark report must be an object`);
  if (typeof report.engine !== 'string' || !report.engine) throw new TypeError(`${file}: engine is required`);
  if (!report.workload || typeof report.workload !== 'object') throw new TypeError(`${file}: workload is required`);
  for (const metric of ['writesPerSecond', 'readsPerSecond', 'indexedQueriesPerSecond', 'rssBytes', 'databaseBytes']) {
    const value = report.results?.[metric]?.p50;
    if (!Number.isFinite(value) || value <= 0) throw new TypeError(`${file}: results.${metric}.p50 must be positive`);
  }
}

function assertComparable(a, b) {
  const keys = ['records', 'reads', 'indexedQueries', 'trials'];
  for (const key of keys) {
    if (a.workload[key] !== b.workload[key]) {
      throw new Error(`benchmark workload mismatch for ${key}: ${a.engine}=${a.workload[key]} ${b.engine}=${b.workload[key]}`);
    }
  }
  const hardwareKeys = ['platform', 'arch', 'cpus', 'cpuModel', 'totalMemoryBytes'];
  for (const key of hardwareKeys) {
    if (a.runtime?.[key] !== b.runtime?.[key]) {
      throw new Error(`benchmark hardware mismatch for ${key}: ${a.engine}=${a.runtime?.[key]} ${b.engine}=${b.runtime?.[key]}`);
    }
  }
}

function ratio(a, b) { return a / b; }
function finitePositive(value, fallback) {
  const number = Number(value ?? fallback);
  if (!Number.isFinite(number) || number <= 0) throw new TypeError('benchmark comparison thresholds must be positive numbers');
  return number;
}
