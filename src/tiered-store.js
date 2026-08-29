import fsSync from 'node:fs';
import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';

const clone=(value)=>structuredClone(value);

export class SegmentedDocumentStore {
  constructor(directory,state,{segmentBytes,cacheBytes,fsync,indexCacheBuckets}){
    this.directory=path.resolve(directory);
    this.manifestFile=path.join(this.directory,'manifest.json');
    this.legacyIndexFile=path.join(this.directory,'offset-index.json');
    this.state=state;
    this.segmentBytes=segmentBytes;
    this.cache=new HotRecordCache(cacheBytes);
    this.fsync=fsync;
    this.queue=Promise.resolve();
    this.index=new BucketedOffsetIndex(path.join(this.directory,'index-buckets'),{maxCachedBuckets:indexCacheBuckets});
  }

  static async open(directory,{segmentBytes=16*1024*1024,cacheBytes=32*1024*1024,fsync=true,indexCacheBuckets=64}={}){
    if(!Number.isSafeInteger(segmentBytes)||segmentBytes<64*1024)throw new TypeError('segmentBytes must be at least 64KiB');
    if(!Number.isSafeInteger(cacheBytes)||cacheBytes<0)throw new TypeError('cacheBytes must be non-negative');
    if(!Number.isSafeInteger(indexCacheBuckets)||indexCacheBuckets<1||indexCacheBuckets>4096)throw new TypeError('indexCacheBuckets must be 1-4096');
    const dir=path.resolve(directory);await fs.mkdir(dir,{recursive:true});
    const manifestFile=path.join(dir,'manifest.json');
    let manifest={version:1,activeSegment:1,activeBytes:0,sequence:0,recordCount:0,tombstones:0};
    try{manifest=JSON.parse(await fs.readFile(manifestFile,'utf8'));}catch(error){if(error.code!=='ENOENT')throw error;}
    validateManifest(manifest);
    const store=new SegmentedDocumentStore(dir,manifest,{segmentBytes,cacheBytes,fsync,indexCacheBuckets});
    await store.index.open();
    await store.#migrateLegacyIndex();
    await store.#reconcileSegments();
    await store.#verifyIndex();
    if(!(await exists(manifestFile)))await store.#persistManifest();
    return store;
  }

  get size(){return this.state.recordCount;}
  stats(){return Object.freeze({records:this.state.recordCount,tombstones:this.state.tombstones,segments:this.state.activeSegment,activeSegmentBytes:this.state.activeBytes,indexEntries:this.index.liveEntries+this.index.deletedEntries,index:this.index.stats(),cache:this.cache.stats()});}
  has(id){validateId(id);const entry=this.index.get(id);return Boolean(entry&&!entry.deleted);}
  version(id){validateId(id);return this.index.get(id)?.sequence??0;}
  ids(){return this.index.ids();}

  getSync(id){
    validateId(id);
    const cached=this.cache.get(id);if(cached!==undefined)return clone(cached);
    const entry=this.index.get(id);if(!entry||entry.deleted)return null;
    const envelope=this.#readEntrySync(entry);
    if(envelope.id!==id||envelope.deleted)throw corruption('segment/index identity mismatch');
    this.cache.set(id,envelope.record);
    return clone(envelope.record);
  }
  async get(id){return this.getSync(id);}
  allSync(){return[...this.scanSync()];}
  *scanSync(){for(const[id,entry]of this.index.liveEntryIterator()){const envelope=this.#readEntrySync(entry);if(envelope.id!==id||envelope.deleted)throw corruption('segment/index identity mismatch');this.cache.set(id,envelope.record);yield clone(envelope.record);}}

  async put(record){validateRecord(record);const[result]=await this.applyBatch([{type:'put',record}]);return result;}
  async remove(id){validateId(id);const[result]=await this.applyBatch([{type:'remove',id}]);return result;}

  async applyBatch(operations=[]){
    if(!Array.isArray(operations))throw new TypeError('segment batch must be an array');
    for(const op of operations){if(!op||typeof op!=='object')throw new TypeError('invalid segment operation');if(op.type==='put')validateRecord(op.record);else if(op.type==='remove')validateId(op.id);else throw new TypeError('invalid segment operation type');}
    return this.#enqueue(async()=>{
      const results=[],dirty=new Map();
      for(const operation of operations){
        if(operation.type==='put'){
          const value=clone(operation.record),previous=this.index.get(value.id);
          const envelope={version:1,sequence:++this.state.sequence,id:value.id,deleted:false,record:value,at:new Date().toISOString()};
          const entry=await this.#appendEnvelope(envelope);const bucket=this.index.set(value.id,entry);dirty.set(bucket.name,bucket);
          if(!previous||previous.deleted)this.state.recordCount++;if(previous?.deleted)this.state.tombstones=Math.max(0,this.state.tombstones-1);
          this.cache.set(value.id,value);results.push(clone(value));
        }else{
          const id=operation.id,previous=this.index.get(id);if(!previous||previous.deleted){results.push(false);continue;}
          const envelope={version:1,sequence:++this.state.sequence,id,deleted:true,at:new Date().toISOString()};
          const entry=await this.#appendEnvelope(envelope);const bucket=this.index.set(id,entry);dirty.set(bucket.name,bucket);
          this.state.recordCount--;this.state.tombstones++;this.cache.delete(id);results.push(true);
        }
      }
      if(dirty.size){await this.index.persist([...dirty.values()],this.fsync);await this.#persistManifest();}
      return results;
    });
  }

  async *scan({batchSize=256}={}){if(!Number.isSafeInteger(batchSize)||batchSize<1)throw new TypeError('batchSize must be positive');let batch=[];for(const record of this.scanSync()){batch.push(record);if(batch.length>=batchSize){for(const item of batch)yield item;batch=[];}}for(const item of batch)yield item;}

  async compact(){
    return this.#enqueue(async()=>{
      const parent=path.dirname(this.directory),base=path.basename(this.directory);
      const tempDir=path.join(parent,`.compact-${base}-${crypto.randomUUID()}`);
      const oldDir=path.join(parent,`.old-${base}-${crypto.randomUUID()}`);
      const failedDir=path.join(parent,`.failed-${base}-${crypto.randomUUID()}`);
      const replacement=await SegmentedDocumentStore.open(tempDir,{segmentBytes:this.segmentBytes,cacheBytes:0,fsync:this.fsync,indexCacheBuckets:this.index.maxCachedBuckets});
      try{
        for(const record of this.scanSync())await replacement.put(record);
        await replacement.close();
        await fs.rename(this.directory,oldDir);
        try{
          await fs.rename(tempDir,this.directory);await fsyncDirectory(parent);
          const reopened=await SegmentedDocumentStore.open(this.directory,{segmentBytes:this.segmentBytes,cacheBytes:this.cache.maxBytes,fsync:this.fsync,indexCacheBuckets:this.index.maxCachedBuckets});
          this.state=reopened.state;this.index=reopened.index;this.cache.clear();
          await fs.rm(oldDir,{recursive:true,force:true});
          return this.stats();
        }catch(error){
          if(await exists(this.directory))await fs.rename(this.directory,failedDir).catch(()=>undefined);
          if(await exists(oldDir))await fs.rename(oldDir,this.directory).catch(()=>undefined);
          await fsyncDirectory(parent).catch(()=>undefined);
          await fs.rm(failedDir,{recursive:true,force:true}).catch(()=>undefined);
          throw error;
        }
      }finally{await fs.rm(tempDir,{recursive:true,force:true}).catch(()=>undefined);}
    });
  }

  async close(){await this.queue;await this.#persistManifest();this.index.clearCache();}

  async #appendEnvelope(envelope){
    const payload=Buffer.from(`${JSON.stringify(envelope)}\n`,'utf8');
    if(this.state.activeBytes>0&&this.state.activeBytes+payload.length>this.segmentBytes){this.state.activeSegment++;this.state.activeBytes=0;}
    const file=this.#segmentFile(this.state.activeSegment);await fs.mkdir(path.dirname(file),{recursive:true});
    const handle=await fs.open(file,'a+',0o600);let offset;
    try{const stat=await handle.stat();offset=stat.size;await handle.write(payload,0,payload.length,null);if(this.fsync)await handle.sync();}finally{await handle.close();}
    this.state.activeBytes=offset+payload.length;
    return{segment:this.state.activeSegment,offset,length:payload.length,sequence:envelope.sequence,deleted:Boolean(envelope.deleted)};
  }
  #readEntrySync(entry){const fd=fsSync.openSync(this.#segmentFile(entry.segment),'r');try{const buffer=Buffer.allocUnsafe(entry.length);const bytesRead=fsSync.readSync(fd,buffer,0,entry.length,entry.offset);if(bytesRead!==entry.length)throw corruption('short segment read');const text=buffer.toString('utf8');if(!text.endsWith('\n'))throw corruption('incomplete segment entry');return JSON.parse(text);}catch(error){if(error?.code==='SYNCIO_SEGMENT_CORRUPT')throw error;throw corruption(`segment read failed: ${error.message}`);}finally{fsSync.closeSync(fd);}}
  #segmentFile(number){return path.join(this.directory,`segment-${String(number).padStart(8,'0')}.ndjson`);}
  #enqueue(work){const operation=this.queue.then(work);this.queue=operation.catch(()=>undefined);return operation;}
  async #persistManifest(){const manifest={version:1,activeSegment:this.state.activeSegment,activeBytes:this.state.activeBytes,sequence:this.state.sequence,recordCount:this.state.recordCount,tombstones:this.state.tombstones};await atomicWriteJson(this.manifestFile,manifest);this.state={...this.state,...manifest};}
  async #migrateLegacyIndex(){if(this.index.bucketCount>0)return;let legacy=null;try{legacy=JSON.parse(await fs.readFile(this.legacyIndexFile,'utf8')).index??null;}catch(error){if(error.code!=='ENOENT')throw error;}if(!legacy)return;const dirty=new Map();for(const[id,entry]of Object.entries(legacy)){validateId(id);validateEntry(entry);const bucket=this.index.set(id,entry);dirty.set(bucket.name,bucket);}if(dirty.size)await this.index.persist([...dirty.values()],this.fsync);await fs.rm(this.legacyIndexFile,{force:true});}
  async #reconcileSegments(){const names=(await fs.readdir(this.directory)).filter(name=>/^segment-\d{8}\.ndjson$/.test(name)).sort();if(!names.length){this.state.activeSegment=1;this.state.activeBytes=0;return;}const last=names.at(-1),number=Number(last.slice(8,16)),stat=await fs.stat(path.join(this.directory,last));this.state.activeSegment=Math.max(this.state.activeSegment,number);if(this.state.activeSegment===number)this.state.activeBytes=stat.size;}
  async #verifyIndex(){let live=0,deleted=0,maxSequence=0;for(const[id,entry]of this.index.entryIterator()){validateId(id);validateEntry(entry);maxSequence=Math.max(maxSequence,entry.sequence);if(entry.deleted)deleted++;else live++;}this.index.liveEntries=live;this.index.deletedEntries=deleted;this.state.recordCount=live;this.state.tombstones=deleted;this.state.sequence=Math.max(this.state.sequence,maxSequence);}
}

export class TieredStatePlane {
  constructor(root,options){this.root=path.resolve(root);this.options=options;this.collections=new Map();}
  static async open(root,options={}){await fs.mkdir(path.resolve(root),{recursive:true});return new TieredStatePlane(root,options);}
  async collection(name){validateCollection(name);if(!this.collections.has(name))this.collections.set(name,await SegmentedDocumentStore.open(path.join(this.root,encodeURIComponent(name)),this.options));return this.collections.get(name);}
  getOpened(name){validateCollection(name);return this.collections.get(name)??null;}
  async stats(){const result={};for(const[name,store]of this.collections)result[name]=store.stats();return result;}
  async compact(){const result={};for(const[name,store]of this.collections)result[name]=await store.compact();return result;}
  async close(){await Promise.all([...this.collections.values()].map(store=>store.close()));this.collections.clear();}
}

class BucketedOffsetIndex {
  constructor(directory,{maxCachedBuckets}){this.directory=directory;this.maxCachedBuckets=maxCachedBuckets;this.cache=new Map();this.bucketNames=[];this.bucketCount=0;this.liveEntries=0;this.deletedEntries=0;this.hits=0;this.misses=0;this.evictions=0;}
  async open(){await fs.mkdir(this.directory,{recursive:true});this.bucketNames=(await fs.readdir(this.directory)).filter(name=>/^bucket-[a-f0-9]{4}\.json$/.test(name)).sort();this.bucketCount=this.bucketNames.length;return this;}
  get(id){const bucket=this.load(this.bucketName(id));return bucket.map.get(id)??null;}
  set(id,entry){
    validateEntry(entry);const name=this.bucketName(id),bucket=this.load(name),previous=bucket.map.get(id);
    if(previous){if(previous.deleted)this.deletedEntries--;else this.liveEntries--;}
    if(entry.deleted)this.deletedEntries++;else this.liveEntries++;
    bucket.map.set(id,{...entry});if(!this.bucketNames.includes(name)){this.bucketNames.push(name);this.bucketNames.sort();this.bucketCount=this.bucketNames.length;}return bucket;
  }
  ids(){const ids=[];for(const[id,entry]of this.entryIterator())if(!entry.deleted)ids.push(id);return ids.sort();}
  *entryIterator(){for(const name of this.bucketNames){const bucket=this.load(name);for(const[id,entry]of bucket.map)yield[id,entry];}}
  *liveEntryIterator(){for(const[id,entry]of this.entryIterator())if(!entry.deleted)yield[id,entry];}
  async persist(buckets,doFsync){for(const bucket of buckets)await atomicWriteJson(path.join(this.directory,bucket.name),{version:1,entries:Object.fromEntries(bucket.map)},doFsync);this.bucketCount=this.bucketNames.length;}
  load(name){
    const cached=this.cache.get(name);if(cached){this.hits++;this.cache.delete(name);this.cache.set(name,cached);return cached;}
    this.misses++;let value={version:1,entries:{}};try{value=JSON.parse(fsSync.readFileSync(path.join(this.directory,name),'utf8'));}catch(error){if(error.code!=='ENOENT')throw corruption(`offset bucket read failed: ${error.message}`);}
    if(value.version!==1||!value.entries||typeof value.entries!=='object'||Array.isArray(value.entries))throw corruption('invalid offset bucket');
    for(const[id,entry]of Object.entries(value.entries)){validateId(id);validateEntry(entry);}
    const bucket={name,map:new Map(Object.entries(value.entries))};this.cache.set(name,bucket);while(this.cache.size>this.maxCachedBuckets){const oldest=this.cache.keys().next().value;this.cache.delete(oldest);this.evictions++;}return bucket;
  }
  bucketName(id){return`bucket-${crypto.createHash('sha256').update(id).digest('hex').slice(0,4)}.json`;}
  clearCache(){this.cache.clear();}
  stats(){return{format:'bucketed-offset-index/1',buckets:this.bucketCount,cachedBuckets:this.cache.size,maxCachedBuckets:this.maxCachedBuckets,hits:this.hits,misses:this.misses,evictions:this.evictions};}
}

class HotRecordCache {
  constructor(maxBytes){this.maxBytes=maxBytes;this.bytes=0;this.map=new Map();this.hits=0;this.misses=0;this.evictions=0;this.admissionRejects=0;this.frequency=new Map();this.maxFrequencyEntries=8192;}
  get(key){const entry=this.map.get(key);if(!entry){this.misses++;this.bump(key);return undefined;}this.hits++;this.bump(key);this.map.delete(key);this.map.set(key,entry);return entry.value;}
  set(key,value){if(this.maxBytes===0)return;const copy=clone(value),bytes=Buffer.byteLength(JSON.stringify(copy));this.bump(key);if(bytes>this.maxBytes){this.delete(key);this.admissionRejects++;return;}this.delete(key);this.map.set(key,{value:copy,bytes});this.bytes+=bytes;while(this.bytes>this.maxBytes)this.evictCandidate();}
  bump(key){this.frequency.set(key,(this.frequency.get(key)??0)+1);if(this.frequency.size>this.maxFrequencyEntries)this.frequency.delete(this.frequency.keys().next().value);}
  evictCandidate(){const candidates=[...this.map.entries()].slice(0,8);if(!candidates.length)return;let loser=candidates[0];for(const candidate of candidates){const score=(this.frequency.get(candidate[0])??0)/Math.max(1,candidate[1].bytes);const loserScore=(this.frequency.get(loser[0])??0)/Math.max(1,loser[1].bytes);if(score<loserScore)loser=candidate;}this.delete(loser[0]);this.evictions++;}
  delete(key){const entry=this.map.get(key);if(entry){this.bytes-=entry.bytes;this.map.delete(key);}}
  clear(){this.map.clear();this.frequency.clear();this.bytes=0;}
  stats(){const accesses=this.hits+this.misses;return{policy:'adaptive-lru',entries:this.map.size,bytes:this.bytes,maxBytes:this.maxBytes,hits:this.hits,misses:this.misses,hitRate:accesses?this.hits/accesses:0,evictions:this.evictions,admissionRejects:this.admissionRejects};}
}

function validateManifest(value){if(!value||value.version!==1||!Number.isSafeInteger(value.activeSegment)||value.activeSegment<1||!Number.isSafeInteger(value.activeBytes)||value.activeBytes<0||!Number.isSafeInteger(value.sequence)||value.sequence<0||!Number.isSafeInteger(value.recordCount)||value.recordCount<0||!Number.isSafeInteger(value.tombstones)||value.tombstones<0)throw corruption('invalid segmented-store manifest');}
function validateEntry(entry){if(!entry||!Number.isSafeInteger(entry.segment)||entry.segment<1||!Number.isSafeInteger(entry.offset)||entry.offset<0||!Number.isSafeInteger(entry.length)||entry.length<2||!Number.isSafeInteger(entry.sequence)||entry.sequence<1)throw corruption('invalid offset-index entry');}
function validateId(id){if(typeof id!=='string'||!id||id.length>512)throw new TypeError('record id must be non-empty string up to 512 chars');}
function validateRecord(record){if(!record||typeof record!=='object'||Array.isArray(record))throw new TypeError('record must be object');validateId(record.id);if(JSON.stringify(record)===undefined)throw new TypeError('record must be JSON serializable');}
function validateCollection(name){if(typeof name!=='string'||!/^[A-Za-z0-9_-]+$/.test(name)||name.length>128)throw new TypeError('invalid collection name');}
function corruption(message){const error=new Error(message);error.code='SYNCIO_SEGMENT_CORRUPT';return error;}
async function exists(file){try{await fs.access(file);return true;}catch{return false;}}
async function atomicWriteJson(target,value,doFsync=true){const temp=`${target}.${process.pid}.${crypto.randomUUID()}.tmp`;try{await fs.mkdir(path.dirname(target),{recursive:true});await fs.writeFile(temp,`${JSON.stringify(value)}\n`,{encoding:'utf8',mode:0o600});if(doFsync){const handle=await fs.open(temp,'r');try{await handle.sync();}finally{await handle.close();}}await fs.rename(temp,target);if(doFsync)await fsyncDirectory(path.dirname(target));}finally{await fs.rm(temp,{force:true}).catch(()=>undefined);}}
async function fsyncDirectory(directory){const handle=await fs.open(directory,'r');try{await handle.sync();}finally{await handle.close();}}
