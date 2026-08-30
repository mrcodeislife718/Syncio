import crypto from 'node:crypto';

const clone=(value)=>structuredClone(value);
const stable=(value)=>{if(value===null||typeof value!=='object')return JSON.stringify(value);if(Array.isArray(value))return`[${value.map(stable).join(',')}]`;return`{${Object.keys(value).sort().map((key)=>`${JSON.stringify(key)}:${stable(value[key])}`).join(',')}}`;};
const digest=(value)=>crypto.createHash('sha256').update(typeof value==='string'?value:stable(value)).digest('hex');

export class AttestedCommitChain {
  constructor({partitionId='local',rootInterval=1024}={}){if(!Number.isSafeInteger(rootInterval)||rootInterval<1)throw new TypeError('rootInterval must be positive');this.partitionId=partitionId;this.rootInterval=rootInterval;this.entries=[];this.roots=[];}
  append(commit){if(!commit?.commitId||!commit?.checksum||!Number.isSafeInteger(commit.sequence))throw new TypeError('valid commit required');const previous=this.entries.at(-1);if(previous&&commit.sequence<=previous.sequence)throw new Error('commit sequence must increase');const chained=digest({previous:previous?.chainHash??null,commitId:commit.commitId,checksum:commit.checksum,sequence:commit.sequence,partitionId:this.partitionId});const entry=Object.freeze({sequence:commit.sequence,commitId:commit.commitId,checksum:commit.checksum,chainHash:chained});this.entries.push(entry);if(this.entries.length%this.rootInterval===0)this.#seal();return entry;}
  #seal(){const start=Math.max(0,this.entries.length-this.rootInterval);const slice=this.entries.slice(start);const root=merkleRoot(slice.map((entry)=>entry.chainHash));const record=Object.freeze({partitionId:this.partitionId,startSequence:slice[0]?.sequence??0,endSequence:slice.at(-1)?.sequence??0,count:slice.length,root});this.roots.push(record);return record;}
  seal(){const last=this.roots.at(-1);if(last?.endSequence===this.entries.at(-1)?.sequence)return last;return this.#seal();}
  verify(){let previous=null;for(const entry of this.entries){const expected=digest({previous,commitId:entry.commitId,checksum:entry.checksum,sequence:entry.sequence,partitionId:this.partitionId});if(expected!==entry.chainHash)return false;previous=entry.chainHash;}for(const root of this.roots){const slice=this.entries.filter((entry)=>entry.sequence>=root.startSequence&&entry.sequence<=root.endSequence);if(slice.length!==root.count||merkleRoot(slice.map((entry)=>entry.chainHash))!==root.root)return false;}return true;}
  compareRoots(remoteRoots=[]){const remote=new Map(remoteRoots.map((root)=>[`${root.startSequence}:${root.endSequence}`,root.root]));return this.roots.filter((root)=>remote.get(`${root.startSequence}:${root.endSequence}`)!==root.root).map(clone);}
  snapshot(){return{partitionId:this.partitionId,entries:this.entries.map(clone),roots:this.roots.map(clone)};}
}

export class AdaptiveSegmentPolicy {
  constructor({minSamples=32,minBenefit=0.15,cooldownMs=300000,now=()=>Date.now()}={}){this.minSamples=minSamples;this.minBenefit=minBenefit;this.cooldownMs=cooldownMs;this.now=now;this.state=new Map();}
  observe(segmentId,{reads=0,writes=0,bytesRead=0,bytesWritten=0,cacheHits=0,cacheMisses=0,blobBytes=0,totalBytes=1}={}){const current=this.state.get(segmentId)??{samples:0,reads:0,writes:0,bytesRead:0,bytesWritten:0,cacheHits:0,cacheMisses:0,blobBytes:0,totalBytes:0,format:'general',lastRewriteAt:0};current.samples++;for(const key of ['reads','writes','bytesRead','bytesWritten','cacheHits','cacheMisses','blobBytes','totalBytes'])current[key]+=Number(arguments[1]?.[key]??0);this.state.set(segmentId,current);return this.recommend(segmentId);}
  recommend(segmentId){const s=this.state.get(segmentId);if(!s||s.samples<this.minSamples)return{format:s?.format??'general',reason:'insufficient_evidence',rewrite:false};const io=s.reads+s.writes||1;const writeRatio=s.writes/io;const hitRatio=s.cacheHits/(s.cacheHits+s.cacheMisses||1);const blobRatio=s.blobBytes/(s.totalBytes||1);let target='general';let benefit=0;if(blobRatio>.5){target='blob-separated';benefit=blobRatio;}else if(writeRatio>.65){target='append';benefit=writeRatio-.5;}else if(writeRatio<.1&&hitRatio<.4){target='dense-read';benefit=.5-writeRatio;}else if(s.reads>s.writes*8&&hitRatio>.7){target='indexed-locality';benefit=hitRatio-.5;}const cooldown=this.now()-s.lastRewriteAt>=this.cooldownMs;const rewrite=target!==s.format&&benefit>=this.minBenefit&&cooldown;return{format:target,current:s.format,benefit,rewrite,reason:rewrite?'measured_benefit':'no_safe_gain'};}
  markRewritten(segmentId,format){const s=this.state.get(segmentId);if(!s)throw new Error('unknown segment');s.format=format;s.lastRewriteAt=this.now();return this.recommend(segmentId);}
}

export class WorkingSetGovernor {
  constructor({limitBytes=Infinity,parent=null,name='database'}={}){if(!(limitBytes>0))throw new TypeError('limitBytes must be positive');this.limitBytes=limitBytes;this.parent=parent;this.name=name;this.usedBytes=0;this.children=new Map();this.shedders=[];this.metrics={reserved:0,rejected:0,shedBytes:0};}
  child(name,{limitBytes=Infinity}={}){if(this.children.has(name))return this.children.get(name);const child=new WorkingSetGovernor({limitBytes,parent:this,name:`${this.name}/${name}`});this.children.set(name,child);return child;}
  registerShedder(priority,fn){if(typeof fn!=='function')throw new TypeError('shedder function required');this.shedders.push({priority,fn});this.shedders.sort((a,b)=>a.priority-b.priority);}
  reserve(bytes,{critical=false}={}){if(!Number.isSafeInteger(bytes)||bytes<0)throw new TypeError('bytes must be non-negative integer');if(this.usedBytes+bytes>this.limitBytes)this.#shed(this.usedBytes+bytes-this.limitBytes);if(this.usedBytes+bytes>this.limitBytes){this.metrics.rejected++;if(!critical)return null;const e=new Error('working set budget exceeded');e.code='SYNCIO_MEMORY_BUDGET';throw e;}const parentReservation=this.parent?.reserve(bytes,{critical});if(this.parent&&!parentReservation)return null;this.usedBytes+=bytes;this.metrics.reserved+=bytes;let released=false;return{release:()=>{if(released)return;released=true;this.usedBytes=Math.max(0,this.usedBytes-bytes);parentReservation?.release();}};}
  #shed(required){for(const item of this.shedders){if(required<=0)break;const freed=Math.max(0,Number(item.fn(required)??0));this.usedBytes=Math.max(0,this.usedBytes-freed);this.metrics.shedBytes+=freed;required-=freed;}}
  snapshot(){return{name:this.name,limitBytes:this.limitBytes,usedBytes:this.usedBytes,metrics:{...this.metrics},children:Object.fromEntries([...this.children].map(([k,v])=>[k,v.snapshot()]))};}
}

export class SharedReactiveExecutionGraph {
  constructor({maxNodes=10000,maxSubscribersPerNode=100000}={}){this.maxNodes=maxNodes;this.maxSubscribersPerNode=maxSubscribersPerNode;this.nodes=new Map();}
  subscribe(spec,{authorize=(value)=>value,evaluate}={}){if(typeof evaluate!=='function')throw new TypeError('evaluate required');const fingerprint=digest(canonicalQuery(spec));let node=this.nodes.get(fingerprint);if(!node){if(this.nodes.size>=this.maxNodes)throw resource('reactive node limit');node={fingerprint,spec:clone(spec),evaluate,last:undefined,subscribers:new Map(),serial:Promise.resolve()};this.nodes.set(fingerprint,node);}if(node.subscribers.size>=this.maxSubscribersPerNode)throw resource('subscriber limit');const id=crypto.randomUUID();node.subscribers.set(id,{authorize});return{fingerprint,id,unsubscribe:()=>{node.subscribers.delete(id);if(!node.subscribers.size)this.nodes.delete(fingerprint);}};}
  async refresh(fingerprint,context={}){const node=this.nodes.get(fingerprint);if(!node)return[];const task=node.serial.then(async()=>{const next=await node.evaluate(node.spec,context);node.last=clone(next);const deliveries=[];for(const[id,subscriber]of node.subscribers){const projected=await subscriber.authorize(clone(next),context);if(projected!==undefined&&projected!==null)deliveries.push({subscriberId:id,value:clone(projected)});}return deliveries;});node.serial=task.catch(()=>undefined);return task;}
  stats(){return{nodes:this.nodes.size,subscribers:[...this.nodes.values()].reduce((n,x)=>n+x.subscribers.size,0),sharedSubscribers:[...this.nodes.values()].filter(x=>x.subscribers.size>1).reduce((n,x)=>n+x.subscribers.size,0)};}
}

export class AuthorizationFactor {
  constructor({maxEntries=10000}={}){this.maxEntries=maxEntries;this.cache=new Map();}
  scopeKey({subject,action,collection,policyVersion=1,claimsDigest=''}){return digest({subject,action,collection,policyVersion,claimsDigest});}
  decideScope(context,evaluate){const key=this.scopeKey(context);if(this.cache.has(key))return this.cache.get(key);const decision=Boolean(evaluate(context));if(this.cache.size>=this.maxEntries)this.cache.delete(this.cache.keys().next().value);this.cache.set(key,decision);return decision;}
  authorizeRecord(context,record,{scope,final}){if(!this.decideScope(context,scope))return false;return Boolean(final(context,record));}
  invalidatePolicy(policyVersion){for(const[key]of this.cache){void key;}this.cache.clear();return policyVersion;}
}

export const CONSISTENCY_DOMAIN=Object.freeze({local:0,partition:1,regional:2,global:3});
export class ConsistencyDomainPlanner {
  constructor({defaultDomain='local'}={}){if(!(defaultDomain in CONSISTENCY_DOMAIN))throw new TypeError('invalid default domain');this.defaultDomain=defaultDomain;}
  plan({collections=[],partitions=[],regions=[],requiredDomain=this.defaultDomain}={}){let level=CONSISTENCY_DOMAIN[requiredDomain];if(new Set(partitions).size>1)level=Math.max(level,CONSISTENCY_DOMAIN.partition);if(new Set(regions).size>1)level=Math.max(level,CONSISTENCY_DOMAIN.regional);if(collections.some((item)=>item?.consistency==='global'))level=CONSISTENCY_DOMAIN.global;const domain=Object.entries(CONSISTENCY_DOMAIN).find(([,value])=>value===level)[0];return{domain,promoted:level>CONSISTENCY_DOMAIN[requiredDomain],coordination:domain==='local'?0:domain==='partition'?1:domain==='regional'?2:3};}
}

export function applySemanticOperation(record,operation){const current=clone(record??{});if(!operation||typeof operation!=='object')throw new TypeError('semantic operation required');switch(operation.type){case'increment':{const value=Number(current[operation.field]??0);if(!Number.isFinite(value)||!Number.isFinite(operation.by))throw new TypeError('increment requires numeric values');current[operation.field]=value+operation.by;break;}case'append':{const list=Array.isArray(current[operation.field])?[...current[operation.field]]:[];if(operation.unique&&!list.some((v)=>stable(v)===stable(operation.value)))list.push(clone(operation.value));else if(!operation.unique)list.push(clone(operation.value));current[operation.field]=list;break;}case'transition':if(current[operation.field]!==operation.from)throw semanticConflict('transition precondition failed');current[operation.field]=operation.to;break;case'claim':if(current[operation.field]!==undefined&&current[operation.field]!==null)throw semanticConflict('claim precondition failed');current[operation.field]=clone(operation.value);break;default:throw new TypeError('unsupported semantic operation');}return current;}

export class ProtocolAdapterRegistry {
  constructor({version=1}={}){this.version=version;this.adapters=new Map();}
  register(name,{encode,decode,capabilities=[]}){if(!name||typeof encode!=='function'||typeof decode!=='function')throw new TypeError('invalid adapter');this.adapters.set(name,{encode,decode,capabilities:[...capabilities]});return this;}
  encode(name,envelope){const adapter=this.adapters.get(name);if(!adapter)throw new Error('unknown protocol adapter');return adapter.encode({version:this.version,...clone(envelope)});}
  decode(name,payload){const adapter=this.adapters.get(name);if(!adapter)throw new Error('unknown protocol adapter');const envelope=adapter.decode(payload);if(envelope.version!==this.version)throw new Error('protocol version mismatch');return envelope;}
  semanticDigest(name,payload){return digest(this.decode(name,payload));}
  capabilities(){return Object.fromEntries([...this.adapters].map(([name,a])=>[name,[...a.capabilities]]));}
}

export class HotPartitionFissionController {
  constructor({splitThreshold=.35,minRequests=1000,cooldownMs=300000,maxPartitions=4096,now=()=>Date.now()}={}){this.splitThreshold=splitThreshold;this.minRequests=minRequests;this.cooldownMs=cooldownMs;this.maxPartitions=maxPartitions;this.now=now;this.stats=new Map();this.total=0;}
  observe(partition,{requests=1,bytes=0,p99Ms=0}={}){const s=this.stats.get(partition)??{requests:0,bytes:0,p99Ms:0,lastSplitAt:0};s.requests+=requests;s.bytes+=bytes;s.p99Ms=Math.max(s.p99Ms,p99Ms);this.total+=requests;this.stats.set(partition,s);return this.recommend(partition);}
  recommend(partition){const s=this.stats.get(partition);if(!s)return{action:'none'};const share=s.requests/(this.total||1);if(this.stats.size>=this.maxPartitions)return{action:'pin',reason:'partition_cap',share};if(this.total>=this.minRequests&&share>=this.splitThreshold&&this.now()-s.lastSplitAt>=this.cooldownMs)return{action:'split',partition,share,reason:'workload_heat'};return{action:'none',share};}
  markSplit(partition){const s=this.stats.get(partition);if(s)s.lastSplitAt=this.now();}
}

export class BoundedCostLearner {
  constructor({alpha=.2,maxRelativeError=.5,minSamples=8,fallback={cpu:1,memory:65536,ssdIo:4096,network:1024,egress:1024,coordination:0}}={}){this.alpha=alpha;this.maxRelativeError=maxRelativeError;this.minSamples=minSamples;this.fallback=clone(fallback);this.models=new Map();}
  observe(kind,actual){const vector=normalizeCost(actual);let model=this.models.get(kind);if(!model)model={count:0,ema:clone(vector),error:0,healthy:false};const predicted=clone(model.ema);model.count++;for(const key of Object.keys(vector))model.ema[key]=model.count===1?vector[key]:this.alpha*vector[key]+(1-this.alpha)*model.ema[key];const denom=Math.max(1,Object.values(vector).reduce((a,b)=>a+b,0));model.error=this.alpha*(Object.keys(vector).reduce((n,key)=>n+Math.abs(vector[key]-predicted[key]),0)/denom)+(1-this.alpha)*model.error;model.healthy=model.count>=this.minSamples&&model.error<=this.maxRelativeError;this.models.set(kind,model);return this.estimate(kind);}
  estimate(kind){const model=this.models.get(kind);return model?.healthy?{source:'learned',cost:clone(model.ema),error:model.error,count:model.count}:{source:'fallback',cost:clone(this.fallback),error:model?.error??null,count:model?.count??0};}
}

export class DeterministicRecoveryReplay {
  constructor({seed='syncio-recovery'}={}){this.seed=seed;this.events=[];}
  record(type,payload={}){const event={index:this.events.length,type,payload:clone(payload)};event.digest=digest({seed:this.seed,...event});this.events.push(Object.freeze(event));return clone(event);}
  verify(){return this.events.every((event,index)=>event.index===index&&event.digest===digest({seed:this.seed,index:event.index,type:event.type,payload:event.payload}));}
  async replay(initialState,reducer){let state=clone(initialState);for(const event of this.events)state=await reducer(state,clone(event));return{state,digest:digest(state),events:this.events.length};}
  manifest(){return{version:1,seed:this.seed,eventCount:this.events.length,eventRoot:merkleRoot(this.events.map((event)=>event.digest))};}
}

export const TOPOLOGY_ORDER=Object.freeze(['embedded','single-server','replicated','partitioned','multi-region']);
export class ProgressiveTopology {
  constructor({current='embedded'}={}){if(!TOPOLOGY_ORDER.includes(current))throw new TypeError('invalid topology');this.current=current;}
  capabilities(topology=this.current){const index=TOPOLOGY_ORDER.indexOf(topology);return{documentApi:true,commitFabric:true,realtime:true,offlineSync:true,replication:index>=2,partitioning:index>=3,multiRegion:index>=4};}
  promote(target){const from=TOPOLOGY_ORDER.indexOf(this.current),to=TOPOLOGY_ORDER.indexOf(target);if(to<0||to<from)throw new Error('promotion target must be same or higher topology');const steps=TOPOLOGY_ORDER.slice(from+1,to+1);return{from:this.current,to:target,steps,applicationRewriteRequired:false,compatibilityGate:true};}
  commit(target){this.promote(target);this.current=target;return this.capabilities();}
  rollback(target){const to=TOPOLOGY_ORDER.indexOf(target),from=TOPOLOGY_ORDER.indexOf(this.current);if(to<0||to>from)throw new Error('rollback target must be lower or equal topology');this.current=target;return this.capabilities();}
}

export function architectureGuardrails(){return Object.freeze({canonicalTruth:'commit-fabric',derivedStructuresAuthoritative:false,boundedResources:true,distributedComplexityOptional:true,priorityOrder:['durability','correctness','foreground','replication','realtime','background']});}

function canonicalQuery(spec){return JSON.parse(stable(spec??{}));}
function merkleRoot(values){if(!values.length)return digest('');let level=values.map((value)=>digest(value));while(level.length>1){const next=[];for(let i=0;i<level.length;i+=2)next.push(digest(`${level[i]}:${level[i+1]??level[i]}`));level=next;}return level[0];}
function normalizeCost(value={}){return{cpu:0,memory:0,ssdIo:0,network:0,egress:0,coordination:0,...Object.fromEntries(Object.entries(value).map(([k,v])=>[k,Math.max(0,Number(v)||0)]))};}
function resource(message){const e=new Error(message);e.code='SYNCIO_RESOURCE_LIMIT';return e;}
function semanticConflict(message){const e=new Error(message);e.code='SYNCIO_INTENT_CONFLICT';return e;}
