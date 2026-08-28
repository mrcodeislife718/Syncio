import fsSync from 'node:fs';
import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';

const clone = (value) => structuredClone(value);

export class SegmentedDocumentStore {
  constructor(directory, state, { segmentBytes, cacheBytes, fsync }) {
    this.directory = path.resolve(directory);
    this.manifestFile = path.join(this.directory, 'manifest.json');
    this.indexFile = path.join(this.directory, 'offset-index.json');
    this.state = state;
    this.segmentBytes = segmentBytes;
    this.cache = new LruByteCache(cacheBytes);
    this.fsync = fsync;
    this.queue = Promise.resolve();
    this.index = new Map(Object.entries(state.index ?? {}));
  }

  static async open(directory, { segmentBytes = 16 * 1024 * 1024, cacheBytes = 32 * 1024 * 1024, fsync = true } = {}) {
    if (!Number.isSafeInteger(segmentBytes) || segmentBytes < 64 * 1024) throw new TypeError('segmentBytes must be at least 64KiB');
    if (!Number.isSafeInteger(cacheBytes) || cacheBytes < 0) throw new TypeError('cacheBytes must be non-negative');
    const dir = path.resolve(directory); await fs.mkdir(dir,{recursive:true});
    const manifestFile=path.join(dir,'manifest.json'); const indexFile=path.join(dir,'offset-index.json');
    let manifest={version:1,activeSegment:1,activeBytes:0,sequence:0,recordCount:0,tombstones:0};
    let index={};
    try{manifest=JSON.parse(await fs.readFile(manifestFile,'utf8'));}catch(error){if(error.code!=='ENOENT')throw error;}
    try{index=JSON.parse(await fs.readFile(indexFile,'utf8')).index??{};}catch(error){if(error.code!=='ENOENT')throw error;}
    validateManifest(manifest);
    const store=new SegmentedDocumentStore(dir,{...manifest,index},{segmentBytes,cacheBytes,fsync});
    await store.#verifyIndex();
    if(!(await exists(manifestFile)))await store.#persistMetadata();
    return store;
  }

  get size(){return this.state.recordCount;}
  stats(){return Object.freeze({records:this.state.recordCount,tombstones:this.state.tombstones,segments:this.state.activeSegment,activeSegmentBytes:this.state.activeBytes,indexEntries:this.index.size,cache:this.cache.stats()});}
  has(id){validateId(id);const entry=this.index.get(id);return Boolean(entry&&!entry.deleted);}
  ids(){return [...this.index.entries()].filter(([,entry])=>!entry.deleted).map(([id])=>id).sort();}

  getSync(id){
    validateId(id);
    const cached=this.cache.get(id); if(cached!==undefined)return clone(cached);
    const entry=this.index.get(id); if(!entry||entry.deleted)return null;
    const envelope=this.#readEntrySync(entry);
    if(envelope.id!==id||envelope.deleted)throw corruption('segment/index identity mismatch');
    this.cache.set(id,envelope.record);
    return clone(envelope.record);
  }

  async get(id){return this.getSync(id);}

  allSync(){const records=[];for(const id of this.ids()){const record=this.getSync(id);if(record)records.push(record);}return records;}

  *scanSync(){for(const id of this.ids()){const record=this.getSync(id);if(record)yield record;}}

  async put(record){
    validateRecord(record);
    const [result]=await this.applyBatch([{type:'put',record}]);
    return result;
  }

  async remove(id){
    validateId(id);
    const [result]=await this.applyBatch([{type:'remove',id}]);
    return result;
  }

  async applyBatch(operations=[]){
    if(!Array.isArray(operations))throw new TypeError('segment batch must be an array');
    for(const op of operations){if(!op||typeof op!=='object')throw new TypeError('invalid segment operation');if(op.type==='put')validateRecord(op.record);else if(op.type==='remove')validateId(op.id);else throw new TypeError('invalid segment operation type');}
    return this.#enqueue(async()=>{
      const results=[];
      for(const operation of operations){
        if(operation.type==='put'){
          const value=clone(operation.record);const previous=this.index.get(value.id);
          const envelope={version:1,sequence:++this.state.sequence,id:value.id,deleted:false,record:value,at:new Date().toISOString()};
          const entry=await this.#appendEnvelope(envelope);this.index.set(value.id,entry);
          if(!previous||previous.deleted)this.state.recordCount+=1;this.cache.set(value.id,value);results.push(clone(value));
        }else{
          const id=operation.id;const previous=this.index.get(id);
          if(!previous||previous.deleted){results.push(false);continue;}
          const envelope={version:1,sequence:++this.state.sequence,id,deleted:true,at:new Date().toISOString()};
          const entry=await this.#appendEnvelope(envelope);this.index.set(id,entry);this.state.recordCount-=1;this.state.tombstones+=1;this.cache.delete(id);results.push(true);
        }
      }
      if(operations.length)await this.#persistMetadata();
      return results;
    });
  }

  async *scan({ batchSize = 256 } = {}){
    if(!Number.isSafeInteger(batchSize)||batchSize<1)throw new TypeError('batchSize must be positive');
    const ids=this.ids();
    for(let i=0;i<ids.length;i+=batchSize){const batch=ids.slice(i,i+batchSize);for(const id of batch){const record=this.getSync(id);if(record)yield record;}}
  }

  async compact(){
    return this.#enqueue(async()=>{
      const tempDir=path.join(this.directory,`.compact-${crypto.randomUUID()}`);
      const replacement=await SegmentedDocumentStore.open(tempDir,{segmentBytes:this.segmentBytes,cacheBytes:0,fsync:this.fsync});
      for(const record of this.scanSync())await replacement.put(record);
      await replacement.close();
      const oldDir=path.join(this.directory,`.old-${crypto.randomUUID()}`);
      const entries=await fs.readdir(this.directory);
      await fs.mkdir(oldDir,{recursive:true});
      for(const name of entries){if(name.startsWith('.compact-')||name.startsWith('.old-'))continue;await fs.rename(path.join(this.directory,name),path.join(oldDir,name));}
      for(const name of await fs.readdir(tempDir))await fs.rename(path.join(tempDir,name),path.join(this.directory,name));
      await fs.rm(tempDir,{recursive:true,force:true});
      const reopened=await SegmentedDocumentStore.open(this.directory,{segmentBytes:this.segmentBytes,cacheBytes:this.cache.maxBytes,fsync:this.fsync});
      this.state=reopened.state;this.index=reopened.index;this.cache.clear();
      await reopened.close();await fs.rm(oldDir,{recursive:true,force:true});
      return this.stats();
    });
  }

  async close(){await this.queue;await this.#persistMetadata();}

  async #appendEnvelope(envelope){
    const payload=Buffer.from(`${JSON.stringify(envelope)}\n`,'utf8');
    if(this.state.activeBytes>0&&this.state.activeBytes+payload.length>this.segmentBytes){this.state.activeSegment+=1;this.state.activeBytes=0;}
    const file=this.#segmentFile(this.state.activeSegment);await fs.mkdir(path.dirname(file),{recursive:true});
    const offset=this.state.activeBytes;const handle=await fs.open(file,'a+',0o600);
    try{await handle.write(payload,0,payload.length,null);if(this.fsync)await handle.sync();}finally{await handle.close();}
    this.state.activeBytes+=payload.length;
    return {segment:this.state.activeSegment,offset,length:payload.length,sequence:envelope.sequence,deleted:Boolean(envelope.deleted)};
  }

  #readEntrySync(entry){
    const fd=fsSync.openSync(this.#segmentFile(entry.segment),'r');
    try{const buffer=Buffer.allocUnsafe(entry.length);const bytesRead=fsSync.readSync(fd,buffer,0,entry.length,entry.offset);if(bytesRead!==entry.length)throw corruption('short segment read');const text=buffer.toString('utf8');if(!text.endsWith('\n'))throw corruption('incomplete segment entry');return JSON.parse(text);}
    catch(error){if(error?.code==='SYNCIO_SEGMENT_CORRUPT')throw error;throw corruption(`segment read failed: ${error.message}`);}
    finally{fsSync.closeSync(fd);}
  }

  #segmentFile(number){return path.join(this.directory,`segment-${String(number).padStart(8,'0')}.ndjson`);}
  #enqueue(work){const operation=this.queue.then(work);this.queue=operation.catch(()=>undefined);return operation;}
  async #persistMetadata(){
    const manifest={version:1,activeSegment:this.state.activeSegment,activeBytes:this.state.activeBytes,sequence:this.state.sequence,recordCount:this.state.recordCount,tombstones:this.state.tombstones};
    const index=Object.fromEntries(this.index);
    await atomicWriteJson(this.indexFile,{version:1,index});await atomicWriteJson(this.manifestFile,manifest);
    this.state={...this.state,...manifest,index};
  }
  async #verifyIndex(){
    let live=0,tombstones=0,maxSequence=0;
    for(const [id,entry] of this.index){validateId(id);validateEntry(entry);maxSequence=Math.max(maxSequence,entry.sequence);if(entry.deleted)tombstones++;else live++;}
    if(live!==this.state.recordCount)throw corruption('manifest record count does not match offset index');
    if(maxSequence>this.state.sequence)throw corruption('offset index sequence is ahead of manifest');
    if(tombstones>this.state.tombstones)this.state.tombstones=tombstones;
  }
}

export class TieredStatePlane {
  constructor(root,options){this.root=path.resolve(root);this.options=options;this.collections=new Map();}
  static async open(root,options={}){await fs.mkdir(path.resolve(root),{recursive:true});return new TieredStatePlane(root,options);}
  async collection(name){validateCollection(name);if(!this.collections.has(name))this.collections.set(name,await SegmentedDocumentStore.open(path.join(this.root,encodeURIComponent(name)),this.options));return this.collections.get(name);}
  getOpened(name){validateCollection(name);return this.collections.get(name)??null;}
  async stats(){const result={};for(const [name,store]of this.collections)result[name]=store.stats();return result;}
  async compact(){const result={};for(const [name,store]of this.collections)result[name]=await store.compact();return result;}
  async close(){await Promise.all([...this.collections.values()].map((store)=>store.close()));this.collections.clear();}
}

class LruByteCache{
  constructor(maxBytes){this.maxBytes=maxBytes;this.bytes=0;this.map=new Map();}
  get(key){const entry=this.map.get(key);if(!entry)return undefined;this.map.delete(key);this.map.set(key,entry);return entry.value;}
  set(key,value){if(this.maxBytes===0)return;const copy=clone(value);const bytes=Buffer.byteLength(JSON.stringify(copy));if(bytes>this.maxBytes){this.delete(key);return;}this.delete(key);this.map.set(key,{value:copy,bytes});this.bytes+=bytes;while(this.bytes>this.maxBytes){const oldest=this.map.keys().next().value;this.delete(oldest);}}
  delete(key){const entry=this.map.get(key);if(entry){this.bytes-=entry.bytes;this.map.delete(key);}}
  clear(){this.map.clear();this.bytes=0;}
  stats(){return{entries:this.map.size,bytes:this.bytes,maxBytes:this.maxBytes};}
}
function validateManifest(value){if(!value||value.version!==1||!Number.isSafeInteger(value.activeSegment)||value.activeSegment<1||!Number.isSafeInteger(value.activeBytes)||value.activeBytes<0||!Number.isSafeInteger(value.sequence)||value.sequence<0||!Number.isSafeInteger(value.recordCount)||value.recordCount<0||!Number.isSafeInteger(value.tombstones)||value.tombstones<0)throw corruption('invalid segmented-store manifest');}
function validateEntry(entry){if(!entry||!Number.isSafeInteger(entry.segment)||entry.segment<1||!Number.isSafeInteger(entry.offset)||entry.offset<0||!Number.isSafeInteger(entry.length)||entry.length<2||!Number.isSafeInteger(entry.sequence)||entry.sequence<1)throw corruption('invalid offset-index entry');}
function validateId(id){if(typeof id!=='string'||!id||id.length>512)throw new TypeError('record id must be non-empty string up to 512 chars');}
function validateRecord(record){if(!record||typeof record!=='object'||Array.isArray(record))throw new TypeError('record must be object');validateId(record.id);const serialized=JSON.stringify(record);if(serialized===undefined)throw new TypeError('record must be JSON serializable');}
function validateCollection(name){if(typeof name!=='string'||!/^[A-Za-z0-9_-]+$/.test(name)||name.length>128)throw new TypeError('invalid collection name');}
function corruption(message){const error=new Error(message);error.code='SYNCIO_SEGMENT_CORRUPT';return error;}
async function exists(file){try{await fs.access(file);return true;}catch{return false;}}
async function atomicWriteJson(target,value){const temp=`${target}.${process.pid}.${crypto.randomUUID()}.tmp`;try{await fs.mkdir(path.dirname(target),{recursive:true});await fs.writeFile(temp,`${JSON.stringify(value)}\n`,{encoding:'utf8',mode:0o600});const handle=await fs.open(temp,'r');try{await handle.sync();}finally{await handle.close();}await fs.rename(temp,target);const dir=await fs.open(path.dirname(target),'r');try{await dir.sync();}finally{await dir.close();}}finally{await fs.rm(temp,{force:true}).catch(()=>undefined);}}
