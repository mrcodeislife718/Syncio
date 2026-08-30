import test from 'node:test';
import assert from 'node:assert/strict';
import { createCommitFabric } from '../src/commit-fabric.js';
import {
  AttestedCommitChain, AdaptiveSegmentPolicy, WorkingSetGovernor, SharedReactiveExecutionGraph,
  AuthorizationFactor, ConsistencyDomainPlanner, applySemanticOperation, ProtocolAdapterRegistry,
  HotPartitionFissionController, BoundedCostLearner, DeterministicRecoveryReplay, ProgressiveTopology,
  architectureGuardrails
} from '../src/superiority-v2.js';

test('attested commit chain detects divergence and localizes mismatched roots',()=>{
  const chain=new AttestedCommitChain({partitionId:'p1',rootInterval:2});
  for(let sequence=1;sequence<=4;sequence++)chain.append(createCommitFabric({databaseId:'db',partitionId:'p1',sequence,mutations:[{collection:'items',id:String(sequence)}]}));
  chain.seal();assert.equal(chain.verify(),true);const remote=chain.snapshot().roots.map((root)=>({...root}));remote[0].root='bad';const mismatches=chain.compareRoots(remote);assert.equal(mismatches.length,1);assert.equal(mismatches[0].startSequence,1);
});

test('adaptive segment policy requires evidence and hysteresis before rewrite',()=>{
  let now=1_000_000;const policy=new AdaptiveSegmentPolicy({minSamples:4,minBenefit:.1,cooldownMs:1000,now:()=>now});
  for(let i=0;i<4;i++)policy.observe('s1',{writes:10,reads:1,totalBytes:100});
  const first=policy.recommend('s1');assert.equal(first.format,'append');assert.equal(first.rewrite,true);policy.markRewritten('s1','append');assert.equal(policy.recommend('s1').rewrite,false);now+=1001;assert.equal(policy.recommend('s1').format,'append');
});

test('working set governor sheds optional memory before rejecting',()=>{
  const root=new WorkingSetGovernor({limitBytes:100});const tenant=root.child('tenant',{limitBytes:80});let cached=30;tenant.usedBytes=70;tenant.registerShedder(1,(need)=>{const freed=Math.min(cached,need);cached-=freed;return freed;});const reservation=tenant.reserve(20);assert.ok(reservation);assert.ok(tenant.usedBytes<=80);reservation.release();assert.equal(tenant.reserve(200),null);
});

test('shared reactive graph evaluates equivalent query once and projects per subscriber',async()=>{
  const graph=new SharedReactiveExecutionGraph();let evaluations=0;const spec={collection:'orders',where:{status:'open'}};const a=graph.subscribe(spec,{evaluate:async()=>{evaluations++;return[{id:'1',owner:'a'}];},authorize:(rows)=>rows});const b=graph.subscribe({where:{status:'open'},collection:'orders'},{evaluate:async()=>{evaluations++;return[];},authorize:(rows)=>rows.filter(r=>r.owner==='a')});assert.equal(a.fingerprint,b.fingerprint);const delivered=await graph.refresh(a.fingerprint);assert.equal(evaluations,1);assert.equal(delivered.length,2);assert.equal(graph.stats().nodes,1);a.unsubscribe();b.unsubscribe();assert.equal(graph.stats().nodes,0);
});

test('authorization factoring caches only scope and always performs final record decision',()=>{
  const factor=new AuthorizationFactor();let scopeCalls=0,finalCalls=0;const context={subject:'u1',action:'read',collection:'docs',policyVersion:1};const policy={scope:()=>{scopeCalls++;return true;},final:(_ctx,record)=>{finalCalls++;return record.owner==='u1';}};assert.equal(factor.authorizeRecord(context,{owner:'u1'},policy),true);assert.equal(factor.authorizeRecord(context,{owner:'u2'},policy),false);assert.equal(scopeCalls,1);assert.equal(finalCalls,2);factor.invalidatePolicy(2);factor.authorizeRecord({...context,policyVersion:2},{owner:'u1'},policy);assert.equal(scopeCalls,2);
});

test('consistency planner promotes only when topology semantics require it',()=>{
  const planner=new ConsistencyDomainPlanner();assert.equal(planner.plan({partitions:['p1']}).domain,'local');assert.equal(planner.plan({partitions:['p1','p2']}).domain,'partition');assert.equal(planner.plan({partitions:['p1','p2'],regions:['us','eu']}).domain,'regional');assert.equal(planner.plan({collections:[{consistency:'global'}]}).domain,'global');
});

test('semantic offline operations preserve intent and reject violated preconditions',()=>{
  assert.equal(applySemanticOperation({count:2},{type:'increment',field:'count',by:3}).count,5);assert.deepEqual(applySemanticOperation({items:[1]},{type:'append',field:'items',value:2,unique:true}).items,[1,2]);assert.equal(applySemanticOperation({state:'pending'},{type:'transition',field:'state',from:'pending',to:'shipped'}).state,'shipped');assert.throws(()=>applySemanticOperation({state:'cancelled'},{type:'transition',field:'state',from:'pending',to:'shipped'}),{code:'SYNCIO_INTENT_CONFLICT'});
});

test('protocol adapters must decode to identical canonical semantics',()=>{
  const registry=new ProtocolAdapterRegistry();registry.register('json',{encode:(x)=>JSON.stringify(x),decode:(x)=>JSON.parse(x),capabilities:['query','commit']});registry.register('binary',{encode:(x)=>Buffer.from(JSON.stringify(x)),decode:(x)=>JSON.parse(Buffer.from(x).toString('utf8')),capabilities:['query','commit']});const envelope={kind:'query',payload:{collection:'items',where:{x:1}}};assert.equal(registry.semanticDigest('json',registry.encode('json',envelope)),registry.semanticDigest('binary',registry.encode('binary',envelope)));assert.deepEqual(registry.capabilities().json,['query','commit']);
});

test('hot partition fission reacts to workload heat and respects partition cap',()=>{
  let now=10_000;const controller=new HotPartitionFissionController({splitThreshold:.6,minRequests:100,cooldownMs:1000,maxPartitions:2,now:()=>now});controller.observe('a',{requests:80});controller.observe('b',{requests:20});assert.equal(controller.recommend('a').action,'pin');const roomy=new HotPartitionFissionController({splitThreshold:.6,minRequests:100,cooldownMs:1000,maxPartitions:10,now:()=>now});roomy.observe('a',{requests:80});roomy.observe('b',{requests:20});assert.equal(roomy.recommend('a').action,'split');roomy.markSplit('a');assert.equal(roomy.recommend('a').action,'none');now+=1001;assert.equal(roomy.recommend('a').action,'split');
});

test('bounded cost learner uses conservative fallback until evidence is stable',()=>{
  const learner=new BoundedCostLearner({minSamples:3,maxRelativeError:.5,fallback:{cpu:10,memory:10,ssdIo:10,network:10,egress:10,coordination:10}});assert.equal(learner.estimate('query').source,'fallback');for(let i=0;i<4;i++)learner.observe('query',{cpu:2,memory:4,ssdIo:1,network:1,egress:1,coordination:0});const estimate=learner.estimate('query');assert.equal(estimate.source,'learned');assert.ok(estimate.cost.memory>0);
});

test('deterministic recovery replay reproduces identical state digest',async()=>{
  const replay=new DeterministicRecoveryReplay({seed:'case-1'});replay.record('put',{id:'a',value:1});replay.record('put',{id:'b',value:2});assert.equal(replay.verify(),true);const reducer=(state,event)=>({...state,[event.payload.id]:event.payload.value});const a=await replay.replay({},reducer);const b=await replay.replay({},reducer);assert.equal(a.digest,b.digest);assert.equal(replay.manifest().eventCount,2);
});

test('progressive topology preserves application contract across promotion and rollback',()=>{
  const topology=new ProgressiveTopology();const plan=topology.promote('multi-region');assert.equal(plan.applicationRewriteRequired,false);assert.deepEqual(plan.steps,['single-server','replicated','partitioned','multi-region']);topology.commit('multi-region');assert.equal(topology.capabilities().multiRegion,true);topology.rollback('embedded');assert.equal(topology.capabilities().replication,false);
});

test('architecture guardrails keep Commit Fabric authoritative and distributed complexity optional',()=>{
  const guardrails=architectureGuardrails();assert.equal(guardrails.canonicalTruth,'commit-fabric');assert.equal(guardrails.derivedStructuresAuthoritative,false);assert.equal(guardrails.boundedResources,true);assert.equal(guardrails.distributedComplexityOptional,true);
});
