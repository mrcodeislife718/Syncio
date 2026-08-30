import fs from 'node:fs/promises';
import path from 'node:path';

const families=Object.freeze({
  document:['MongoDB','Couchbase'],
  embedded:['SQLite','libSQL'],
  realtime:['Convex','Supabase Realtime','MongoDB change streams'],
  localFirst:['Couchbase Lite','Electric','Turso'],
  distributed:['CockroachDB','FoundationDB','TiKV'],
  hotPath:['Redis']
});
const required=['hardware','os','storage','databaseVersions','configuration','dataset','workload','warmupSeconds','durationSeconds','rawResults','failureBehavior','seed'];

export function validateEvidenceManifest(manifest){
  const errors=[];
  if(!manifest||typeof manifest!=='object'||Array.isArray(manifest))return{ok:false,errors:['manifest must be object']};
  if(manifest.version!==1)errors.push('version must be 1');
  if(!families[manifest.family])errors.push(`family must be one of ${Object.keys(families).join(', ')}`);
  for(const field of required)if(manifest[field]===undefined||manifest[field]===null||manifest[field]==='')errors.push(`missing ${field}`);
  if(!Array.isArray(manifest.baselines)||!manifest.baselines.length)errors.push('baselines must be non-empty array');
  else if(families[manifest.family])for(const baseline of manifest.baselines)if(!families[manifest.family].includes(baseline))errors.push(`unsupported baseline for ${manifest.family}: ${baseline}`);
  if(!manifest.syncio||typeof manifest.syncio!=='object')errors.push('syncio measurements required');
  if(!Array.isArray(manifest.rawResults))errors.push('rawResults must be array');
  if(!Number.isFinite(manifest.warmupSeconds)||manifest.warmupSeconds<0)errors.push('warmupSeconds must be non-negative');
  if(!Number.isFinite(manifest.durationSeconds)||manifest.durationSeconds<=0)errors.push('durationSeconds must be positive');
  if(manifest.claim&&manifest.claim.status==='proven'){
    if(!manifest.claim.metric||!manifest.claim.direction||!Number.isFinite(manifest.claim.threshold))errors.push('proven claim requires metric, direction, and threshold');
    if(!['lower','higher'].includes(manifest.claim.direction))errors.push('claim direction must be lower or higher');
  }
  return{ok:errors.length===0,errors};
}

async function main(){
  const target=process.argv[2];
  if(!target){console.log(JSON.stringify({ok:true,mode:'schema-only',families,required},null,2));return;}
  const manifest=JSON.parse(await fs.readFile(path.resolve(target),'utf8'));
  const result=validateEvidenceManifest(manifest);
  console.log(JSON.stringify(result,null,2));
  if(!result.ok)process.exitCode=1;
}

if(import.meta.url===new URL(`file://${process.argv[1]}`).href)await main();
