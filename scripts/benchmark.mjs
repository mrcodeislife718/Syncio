import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import { IndexedSyncioDatabase } from '../src/indexed.js';

const records=positiveInt(process.env.SYNCIO_BENCH_RECORDS,1000);
const reads=positiveInt(process.env.SYNCIO_BENCH_READS,5000);
const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-benchmark-'));
const file=path.join(root,'bench.json');
try {
  const db=await IndexedSyncioDatabase.open(file);
  await db.defineIndex('records','bucket');
  const collection=db.collection('records');
  const writeStart=performance.now();
  for(let i=0;i<records;i++)await collection.upsert({id:`r${i}`,bucket:`b${i%100}`,value:i});
  const writeMs=performance.now()-writeStart;
  const readStart=performance.now();
  let checksum=0;
  for(let i=0;i<reads;i++)checksum+=collection.get(`r${i%records}`).value;
  const readMs=performance.now()-readStart;
  const queryStart=performance.now();
  for(let i=0;i<1000;i++)checksum+=collection.query({where:{bucket:`b${i%100}`}}).length;
  const queryMs=performance.now()-queryStart;
  const stat=await fs.stat(file);
  const memory=process.memoryUsage();
  await db.close();
  console.log(JSON.stringify({
    runtime:{node:process.version,platform:process.platform,arch:process.arch,cpus:os.cpus().length,totalMemoryBytes:os.totalmem()},
    workload:{records,reads,indexedQueries:1000},
    results:{writeMs,readMs,indexedQueryMs:queryMs,writesPerSecond:records/(writeMs/1000),readsPerSecond:reads/(readMs/1000),indexedQueriesPerSecond:1000/(queryMs/1000),databaseBytes:stat.size,rssBytes:memory.rss,heapUsedBytes:memory.heapUsed,checksum}
  },null,2));
} finally { await fs.rm(root,{recursive:true,force:true}); }

function positiveInt(value,fallback){const number=Number(value??fallback);if(!Number.isSafeInteger(number)||number<1)throw new TypeError('benchmark counts must be positive integers');return number;}
