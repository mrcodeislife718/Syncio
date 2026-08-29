import { queryRecords } from './advanced.js';

export function createNetworkDatabase(db){
  if(!db||typeof db.collection!=='function'||typeof db.search!=='function'||typeof db.near!=='function')throw new TypeError('network database requires production Syncio database');
  return new Proxy(db,{get(target,property,receiver){if(property==='collection')return(name)=>networkCollection(target,name);const value=Reflect.get(target,property,receiver);return typeof value==='function'?value.bind(target):value;}});
}

function networkCollection(db,name){
  const base=db.collection(name);
  return Object.freeze({
    insert:(value)=>base.insert(value),upsert:(value)=>base.upsert(value),get:(id)=>base.get(id),remove:(id)=>base.remove(id),watch:(listener)=>base.watch(listener),query:(spec={})=>advancedQuery(db,name,base,spec),explain:(spec={})=>base.explain(spec),update:(id,update)=>base.update(id,update),
    all(){const records=base.all().map((record)=>structuredClone(record));Object.defineProperty(records,'__syncioQuery',{value:(spec)=>advancedQuery(db,name,base,spec),enumerable:false,configurable:false,writable:false});return records;}
  });
}

function advancedQuery(db,name,base,spec={}){
  const where=spec.where&&typeof spec.where==='object'&&!Array.isArray(spec.where)?{...spec.where}:spec.where;
  let records=null;
  if(where&&'$text'in where){const clause=where.$text;delete where.$text;if(!clause||typeof clause!=='object'||typeof clause.query!=='string')throw badQuery('$text requires {query,index?}');records=db.search(name,clause.query,{index:clause.index,limit:normalizeCandidateLimit(spec.limit)}).map((row)=>row.record);}
  if(where&&'$near'in where){const clause=where.$near;delete where.$near;if(!clause||typeof clause!=='object'||!clause.point)throw badQuery('$near requires {point,index?,maxDistanceMeters?}');const nearby=db.near(name,clause.point,{index:clause.index,maxDistanceMeters:clause.maxDistanceMeters??Infinity,limit:normalizeCandidateLimit(spec.limit)}).map((row)=>row.record);if(records){const ids=new Set(nearby.map((record)=>record.id));records=records.filter((record)=>ids.has(record.id));}else records=nearby;}
  return queryRecords(records??base.all(),{...spec,where});
}
function normalizeCandidateLimit(limit){if(limit===undefined)return 1000;if(!Number.isSafeInteger(limit)||limit<1||limit>10000)throw badQuery('limit must be between 1 and 10000');return Math.min(1000,limit);}
function badQuery(message){const error=new TypeError(message);error.code='invalid_query';error.statusCode=400;return error;}
