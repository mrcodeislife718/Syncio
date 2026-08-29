import { queryRecords } from './advanced.js';

const clone=(value)=>structuredClone(value);

export class ReactiveQueryGraph {
  constructor({maxDependencies=10000,maxMaterializedRows=100000}={}){this.maxDependencies=maxDependencies;this.maxMaterializedRows=maxMaterializedRows;this.queries=new Map();this.byCollection=new Map();}
  register(id,{collection,query={},evaluate,initial=[]}){
    if(!id||!collection||typeof evaluate!=='function')throw new TypeError('invalid reactive query');
    const deps=compileDependencies(query);if(deps.length>this.maxDependencies)throw limit('reactive dependency limit exceeded');
    const normalized=normalizeQuery(query);const incremental=isIncrementalSafe(normalized);
    const materialized=Array.isArray(initial)?clone(initial):[];if(materialized.length>this.maxMaterializedRows)throw limit('reactive materialized row limit exceeded');
    const entry={id,collection,query:clone(query),normalized,deps,evaluate,incremental,last:materialized,serial:Promise.resolve()};
    this.queries.set(id,entry);if(!this.byCollection.has(collection))this.byCollection.set(collection,new Set());this.byCollection.get(collection).add(id);return()=>this.remove(id);
  }
  remove(id){const q=this.queries.get(id);if(!q)return false;this.queries.delete(id);this.byCollection.get(q.collection)?.delete(id);return true;}
  async applyCommit(commit){
    const tasks=[];const changed=new Set((commit.mutations??[]).map(m=>m.collection));
    for(const collection of changed)for(const id of this.byCollection.get(collection)??[]){const q=this.queries.get(id);const mutations=(commit.mutations??[]).filter(m=>m.collection===collection);if(!mutations.some(m=>mayAffect(q.deps,m)))continue;
      const task=q.serial.then(async()=>{const next=q.incremental?applyIncremental(q.last,q.normalized,mutations):await q.evaluate();if(next.length>this.maxMaterializedRows)throw limit('reactive materialized row limit exceeded');if(stable(next)===stable(q.last))return null;q.last=clone(next);return{queryId:id,commitId:commit.commitId,value:clone(next),mode:q.incremental?'incremental':'recompute'};});q.serial=task.catch(()=>undefined);tasks.push(task);
    }
    return (await Promise.all(tasks)).filter(Boolean);
  }
  stats(){return{queries:this.queries.size,dependencies:[...this.queries.values()].reduce((n,q)=>n+q.deps.length,0),incremental:[...this.queries.values()].filter(q=>q.incremental).length,recompute:[...this.queries.values()].filter(q=>!q.incremental).length,materializedRows:[...this.queries.values()].reduce((n,q)=>n+q.last.length,0)};}
}

function normalizeQuery(query){if(query&&Object.hasOwn(query,'where'))return clone(query);return{where:clone(query??{})};}
function isIncrementalSafe(spec){return !spec.orderBy&&!spec.limit&&!spec.offset&&!spec.projection;}
function applyIncremental(current,spec,mutations){const map=new Map(current.map(r=>[r.id,clone(r)]));for(const mutation of mutations){if(mutation.before?.id)map.delete(mutation.before.id);if(mutation.record&&matches(mutation.record,spec.where))map.set(mutation.record.id,clone(mutation.record));}return[...map.values()].sort((a,b)=>String(a.id).localeCompare(String(b.id)));}
function matches(record,where){return queryRecords([record],{where}).length===1;}
function compileDependencies(query){const out=[];walk(query?.where??query,'',out);return out.length?out:[{path:'*'}];}
function walk(v,p,out){if(!v||typeof v!=='object'||Array.isArray(v))return;for(const[k,x]of Object.entries(v)){if(k.startsWith('$')){walk(x,p,out);continue;}const path=p?`${p}.${k}`:k;out.push({path});walk(x,path,out);}}
function mayAffect(deps,mutation){const fields=new Set(mutation.fields??['*']);return deps.some(d=>d.path==='*'||fields.has('*')||[...fields].some(f=>f===d.path||f.startsWith(`${d.path}.`)||d.path.startsWith(`${f}.`)));}
function stable(value){return JSON.stringify(value);}
function limit(message){const e=new Error(message);e.code='SYNCIO_RESOURCE_LIMIT';return e;}
