import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { SyncioDatabase } from './index.js';
import { QueryIndex, queryRecords } from './advanced.js';

export class IndexedSyncioDatabase {
  constructor(base, catalogFile, definitions = {}) {
    this.base=base;
    this.file=base.file;
    this.catalogFile=catalogFile;
    this.definitions=definitions;
    this.indexes=new Map();
    this.catalogQueue=Promise.resolve();
    this.#rebuildAll();
  }

  static async open(file, options={}) {
    const base=await SyncioDatabase.open(file,options);
    const catalogFile=path.resolve(`${base.file}.indexes.json`);
    let definitions={};
    try { definitions=JSON.parse(await fs.readFile(catalogFile,'utf8')).indexes??{}; }
    catch(error){ if(error.code!=='ENOENT') throw error; }
    validateDefinitions(definitions);
    return new IndexedSyncioDatabase(base,catalogFile,definitions);
  }

  get databaseId(){return this.base.databaseId;}
  get sequence(){return this.base.sequence;}

  async defineIndex(collection, field) {
    validateName(collection,'collection'); validateName(field,'field');
    const fields=new Set(this.definitions[collection]??[]); fields.add(field);
    this.definitions={...this.definitions,[collection]:[...fields].sort()};
    this.#rebuildCollection(collection);
    await this.#persistCatalog();
    return {collection,field};
  }

  async dropIndex(collection, field) {
    const fields=(this.definitions[collection]??[]).filter((item)=>item!==field);
    const next={...this.definitions};
    if(fields.length) next[collection]=fields; else delete next[collection];
    this.definitions=next;
    this.indexes.delete(indexKey(collection,field));
    await this.#persistCatalog();
    return true;
  }

  listIndexes(){ return Object.entries(this.definitions).flatMap(([collection,fields])=>fields.map((field)=>({collection,field}))); }

  collection(name) {
    const baseCollection=this.base.collection(name);
    const db=this;
    return Object.freeze({
      async insert(value){const result=await baseCollection.insert(value);db.#updateIndexes(name,null,result);return result;},
      async upsert(value){const before=value?.id?baseCollection.get(value.id):null;const result=await baseCollection.upsert(value);db.#updateIndexes(name,before,result);return result;},
      get:(id)=>baseCollection.get(id),
      all:()=>baseCollection.all(),
      async remove(id){const before=baseCollection.get(id);const result=await baseCollection.remove(id);if(result)db.#updateIndexes(name,before,null);return result;},
      watch:(listener)=>baseCollection.watch(listener),
      query(spec={}){return db.query(name,spec);},
      explain(spec={}){return db.explainQuery(name,spec);}
    });
  }

  query(collection, spec={}) {
    const plan=this.explainQuery(collection,spec);
    if(plan.strategy==='index') {
      const index=this.indexes.get(indexKey(collection,plan.field));
      const records=index.find(plan.value).map((id)=>this.base.collection(collection).get(id)).filter(Boolean);
      return queryRecords(records,spec);
    }
    return queryRecords(this.base.collection(collection).all(),spec);
  }

  explainQuery(collection,spec={}) {
    const entries=spec.where&&typeof spec.where==='object'&&!Array.isArray(spec.where)?Object.entries(spec.where):[];
    if(entries.length===1) {
      const [field,value]=entries[0];
      const simpleEquality=!(value&&typeof value==='object'&&!Array.isArray(value));
      if(simpleEquality&&this.indexes.has(indexKey(collection,field))) return {strategy:'index',field,value};
    }
    return {strategy:'scan'};
  }

  async transaction(work){const result=await this.base.transaction(work);this.#rebuildAll();return result;}
  changesSince(...args){return this.base.changesSince(...args);}
  hasAppliedChange(...args){return this.base.hasAppliedChange(...args);}
  snapshot(){return this.base.snapshot();}
  async replaceState(state){const result=await this.base.replaceState(state);this.#rebuildAll();return result;}
  async applyReplicationChange(change,resolver){const result=await this.base.applyReplicationChange(change,resolver);if(result?.applied)this.#rebuildCollection(change.collection);return result;}
  async close(){await this.catalogQueue;await this.base.close();}

  #updateIndexes(collection,before,after){
    for(const field of this.definitions[collection]??[]){const index=this.indexes.get(indexKey(collection,field));if(before)index?.remove(before);if(after)index?.add(after);}
  }
  #rebuildCollection(collection){
    const records=this.base.collection(collection).all();
    for(const field of this.definitions[collection]??[]){this.indexes.set(indexKey(collection,field),new QueryIndex(field).rebuild(records));}
  }
  #rebuildAll(){this.indexes.clear();for(const collection of Object.keys(this.definitions))this.#rebuildCollection(collection);}
  #persistCatalog(){
    const operation=this.catalogQueue.then(()=>atomicWriteJson(this.catalogFile,{version:1,indexes:this.definitions}));
    this.catalogQueue=operation.catch(()=>undefined);
    return operation;
  }
}

function validateDefinitions(definitions){if(!definitions||typeof definitions!=='object'||Array.isArray(definitions))throw new Error('invalid Syncio index catalog');for(const [collection,fields]of Object.entries(definitions)){validateName(collection,'collection');if(!Array.isArray(fields))throw new Error('invalid Syncio index fields');for(const field of fields)validateName(field,'field');}}
function validateName(value,label){if(typeof value!=='string'||!/^[A-Za-z0-9_.-]+$/.test(value)||value.length>128)throw new TypeError(`invalid index ${label}`);}
function indexKey(collection,field){return `${collection}\u0000${field}`;}
async function atomicWriteJson(file,value){await fs.mkdir(path.dirname(file),{recursive:true});const temp=`${file}.${process.pid}.${crypto.randomUUID()}.tmp`;try{await fs.writeFile(temp,`${JSON.stringify(value,null,2)}\n`,{encoding:'utf8',mode:0o600});const handle=await fs.open(temp,'r');try{await handle.sync();}finally{await handle.close();}await fs.rename(temp,file);const dir=await fs.open(path.dirname(file),'r');try{await dir.sync();}finally{await dir.close();}}finally{await fs.rm(temp,{force:true}).catch(()=>undefined);}}
