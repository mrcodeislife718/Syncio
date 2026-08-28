import crypto from 'node:crypto';
import { queryRecords } from './advanced.js';

export class ConsistentHashRing {
  constructor(shardIds, { virtualNodes = 128 } = {}) {
    if (!Array.isArray(shardIds) || shardIds.length < 1 || new Set(shardIds).size !== shardIds.length) throw new TypeError('unique shardIds required');
    if (!Number.isSafeInteger(virtualNodes) || virtualNodes < 1 || virtualNodes > 4096) throw new TypeError('virtualNodes must be between 1 and 4096');
    this.shardIds = [...shardIds].sort();
    this.virtualNodes = virtualNodes;
    this.points = [];
    for (const shardId of this.shardIds) {
      if (typeof shardId !== 'string' || !shardId) throw new TypeError('shard id must be non-empty string');
      for (let i = 0; i < virtualNodes; i++) this.points.push({ hash: hash32(`${shardId}:${i}`), shardId });
    }
    this.points.sort((a,b)=>a.hash-b.hash || a.shardId.localeCompare(b.shardId));
  }
  locate(key) {
    if (typeof key !== 'string' || !key) throw new TypeError('shard key must be non-empty string');
    const hash = hash32(key);
    let low=0, high=this.points.length;
    while(low<high){const mid=(low+high)>>1;if(this.points[mid].hash<hash)low=mid+1;else high=mid;}
    return this.points[low===this.points.length?0:low].shardId;
  }
  distribution(keys) { const counts=Object.fromEntries(this.shardIds.map((id)=>[id,0])); for(const key of keys)counts[this.locate(String(key))]++; return counts; }
}

export class ShardedSyncioDatabase {
  constructor(shards, { virtualNodes = 128, shardKey = 'id' } = {}) {
    if (!shards || typeof shards !== 'object' || Array.isArray(shards)) throw new TypeError('shards object required');
    const entries=Object.entries(shards);
    if (!entries.length) throw new TypeError('at least one shard required');
    for(const [id,db] of entries) if(!db||typeof db.collection!=='function'||typeof db.transaction!=='function') throw new TypeError(`invalid database for shard ${id}`);
    this.shards=new Map(entries);
    this.ring=new ConsistentHashRing([...this.shards.keys()],{virtualNodes});
    this.shardKey=shardKey;
  }
  get shardIds(){return [...this.shards.keys()].sort();}
  locate(value){return this.ring.locate(extractShardKey(value,this.shardKey));}
  shard(id){const db=this.shards.get(id);if(!db)throw new Error(`unknown shard ${id}`);return db;}
  collection(name){
    const router=this;
    return Object.freeze({
      async insert(value){const record={...value,id:value?.id??crypto.randomUUID()};return router.shard(router.locate(record)).collection(name).insert(record);},
      async upsert(value){if(!value?.id)throw new TypeError('sharded upsert requires id');return router.shard(router.locate(value)).collection(name).upsert(value);},
      get(id, shardValue=id){return router.shard(router.locate(typeof shardValue==='object'?shardValue:{id:String(shardValue)})).collection(name).get(id);},
      async remove(id, shardValue=id){return router.shard(router.locate(typeof shardValue==='object'?shardValue:{id:String(shardValue)})).collection(name).remove(id);},
      all(){return router.shardIds.flatMap((id)=>router.shard(id).collection(name).all());},
      query(spec={}){return queryRecords(router.shardIds.flatMap((id)=>router.shard(id).collection(name).all()),spec);}
    });
  }
  async transaction(shardValue, work){
    if(typeof work!=='function')throw new TypeError('sharded transaction requires work callback');
    const shardId=this.locate(typeof shardValue==='object'?shardValue:{id:String(shardValue)});
    return this.shard(shardId).transaction(work);
  }
  async moveRecord(collection, id, { fromShard, toShard, verify = true } = {}) {
    const source=this.shard(fromShard);const target=this.shard(toShard);
    if(fromShard===toShard)return {moved:false,reason:'same_shard'};
    const record=source.collection(collection).get(id);
    if(!record)return {moved:false,reason:'not_found'};
    await target.collection(collection).upsert(record);
    if(verify){const copied=target.collection(collection).get(id);if(JSON.stringify(copied)!==JSON.stringify(record)){await target.collection(collection).remove(id).catch(()=>undefined);throw shardError('SYNCIO_SHARD_VERIFY_FAILED','target verification failed');}}
    await source.collection(collection).remove(id);
    return {moved:true,id,fromShard,toShard};
  }
  async rebalanceCollection(collection, { dryRun = false, limit = Infinity } = {}) {
    if(!(Number.isFinite(limit)||limit===Infinity)||limit<1)throw new TypeError('rebalance limit must be positive');
    const moves=[];
    for(const fromShard of this.shardIds){
      for(const record of this.shard(fromShard).collection(collection).all()){
        const toShard=this.locate(record);
        if(toShard!==fromShard){moves.push({id:record.id,fromShard,toShard});if(moves.length>=limit)break;}
      }
      if(moves.length>=limit)break;
    }
    if(!dryRun)for(const move of moves)await this.moveRecord(collection,move.id,move);
    return {planned:moves.length,moved:dryRun?0:moves.length,moves};
  }
  async close(){await Promise.all(this.shardIds.map((id)=>this.shard(id).close()));}
}

export function planShardMigration(records, currentShard, nextShardIds, { virtualNodes=128, shardKey='id' }={}){
  const ring=new ConsistentHashRing(nextShardIds,{virtualNodes});
  return records.map((record)=>({id:record.id,fromShard:currentShard,toShard:ring.locate(extractShardKey(record,shardKey))})).filter((item)=>item.fromShard!==item.toShard);
}
function extractShardKey(value, field){if(typeof value==='string')return value;if(!value||typeof value!=='object')throw new TypeError('record or shard key required');let current=value;for(const part of String(field).split('.'))current=current?.[part];if(current===undefined||current===null||current==='')throw new TypeError(`missing shard key ${field}`);return String(current);}
function hash32(value){const digest=crypto.createHash('sha256').update(value).digest();return digest.readUInt32BE(0);}
function shardError(code,message){const error=new Error(message);error.code=code;return error;}
