export class ReactiveQueryGraph {
  constructor({ maxDependencies = 10000 } = {}) { this.maxDependencies=maxDependencies; this.queries=new Map(); this.byCollection=new Map(); }
  register(id,{collection,query={},evaluate}) { if(!id||!collection||typeof evaluate!=='function')throw new TypeError('invalid reactive query'); const deps=compileDependencies(query); if(deps.length>this.maxDependencies)throw limit('reactive dependency limit exceeded'); const entry={id,collection,query:structuredClone(query),deps,evaluate,last:undefined}; this.queries.set(id,entry); if(!this.byCollection.has(collection))this.byCollection.set(collection,new Set()); this.byCollection.get(collection).add(id); return ()=>this.remove(id); }
  remove(id){const q=this.queries.get(id);if(!q)return false;this.queries.delete(id);this.byCollection.get(q.collection)?.delete(id);return true;}
  async applyCommit(commit){const changed=new Set((commit.mutations??[]).map(m=>m.collection));const results=[];for(const collection of changed){for(const id of this.byCollection.get(collection)??[]){const q=this.queries.get(id);const mutations=commit.mutations.filter(m=>m.collection===collection);if(!mutations.some(m=>mayAffect(q.deps,m)))continue;const next=await q.evaluate();if(JSON.stringify(next)!==JSON.stringify(q.last)){results.push({queryId:id,commitId:commit.commitId,value:next});q.last=structuredClone(next);}}}return results;}
  stats(){return{queries:this.queries.size,dependencies:[...this.queries.values()].reduce((n,q)=>n+q.deps.length,0)};}
}
function compileDependencies(query){const out=[];walk(query,'',out);return out.length?out:[{path:'*'}];}
function walk(v,p,out){if(!v||typeof v!=='object'||Array.isArray(v))return;for(const [k,x]of Object.entries(v)){if(k.startsWith('$')){walk(x,p,out);continue;}const path=p?`${p}.${k}`:k;out.push({path});walk(x,path,out);}}
function mayAffect(deps,mutation){const fields=new Set(mutation.fields??['*']);return deps.some(d=>d.path==='*'||fields.has('*')||[...fields].some(f=>f===d.path||f.startsWith(`${d.path}.`)||d.path.startsWith(`${f}.`)));}
function limit(message){const e=new Error(message);e.code='SYNCIO_RESOURCE_LIMIT';return e;}
