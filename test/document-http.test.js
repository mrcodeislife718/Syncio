import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { IndexedSyncioDatabase } from '../src/indexed.js';
import { createSyncioServer } from '../src/server.js';

const policies=[
  {effect:'allow',collection:'orders',action:'read'},
  {effect:'allow',collection:'orders',action:'write'},
  {effect:'allow',collection:'orders',action:'delete'}
];

async function setup(){
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-document-http-'));
  const db=await IndexedSyncioDatabase.open(path.join(root,'db.json'));
  await db.defineIndex('orders','customer.address.city');
  const service=createSyncioServer({db,policies});
  const address=await service.listen();
  return {root,db,service,address};
}

async function cleanup(state){await state.service.close();await state.db.close();await fs.rm(state.root,{recursive:true,force:true});}

async function put(base,id,body){
  const response=await fetch(`${base}/collections/orders/${id}`,{method:'PUT',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
  assert.equal(response.status,200);
  return response.json();
}

test('HTTP supports nested filters projections ordering and indexed execution',async(t)=>{
  const state=await setup();t.after(()=>cleanup(state));
  await put(state.address.url,'a',{customer:{name:'Ada',address:{city:'Bronx'}},total:25});
  await put(state.address.url,'b',{customer:{name:'Grace',address:{city:'Brooklyn'}},total:80});
  await put(state.address.url,'c',{customer:{name:'Linus',address:{city:'Bronx'}},total:50});

  const params=new URLSearchParams({
    where:JSON.stringify({'customer.address.city':'Bronx',total:{$gte:20}}),
    orderBy:JSON.stringify([{field:'total',direction:'desc'}]),
    projection:JSON.stringify({'customer.name':1,total:1,id:0}),
    limit:'2'
  });
  const response=await fetch(`${state.address.url}/collections/orders?${params}`);
  assert.equal(response.status,200);
  assert.deepEqual((await response.json()).records,[
    {customer:{name:'Linus'},total:50},
    {customer:{name:'Ada'},total:25}
  ]);
});

test('HTTP PATCH performs atomic document update and emits one durable realtime change',async(t)=>{
  const state=await setup();t.after(()=>cleanup(state));
  await put(state.address.url,'a',{customer:{name:'Ada'},total:25,tags:['new'],lines:[{sku:'x',qty:2},{sku:'y',qty:1}]});
  const after=state.db.sequence;
  const seen=[];
  const stop=state.db.watchChanges({collection:'orders',after},(change)=>seen.push(change));t.after(stop);

  const response=await fetch(`${state.address.url}/collections/orders/a`,{
    method:'PATCH',headers:{'content-type':'application/json'},
    body:JSON.stringify({$inc:{total:5},$set:{'customer.level':'gold'},$addToSet:{tags:{$each:['new','vip']}},$pull:{lines:{qty:{$lt:2}}}})
  });
  assert.equal(response.status,200);
  const updated=await response.json();
  assert.equal(updated.total,30);
  assert.equal(updated.customer.level,'gold');
  assert.deepEqual(updated.tags,['new','vip']);
  assert.deepEqual(updated.lines,[{sku:'x',qty:2}]);
  await new Promise((resolve)=>setImmediate(resolve));
  assert.equal(seen.length,1);
  assert.equal(seen[0].record.total,30);
});

test('HTTP aggregation runs against persisted documents',async(t)=>{
  const state=await setup();t.after(()=>cleanup(state));
  await put(state.address.url,'a',{customer:{address:{city:'Bronx'}},total:25,lines:[{sku:'x',qty:2},{sku:'y',qty:1}]});
  await put(state.address.url,'b',{customer:{address:{city:'Brooklyn'}},total:80,lines:[{sku:'x',qty:4}]});
  await put(state.address.url,'c',{customer:{address:{city:'Bronx'}},total:50,lines:[{sku:'x',qty:1}]});

  const response=await fetch(`${state.address.url}/collections/orders/aggregate`,{
    method:'POST',headers:{'content-type':'application/json'},
    body:JSON.stringify({pipeline:[{$unwind:'$lines'},{$group:{_id:'$lines.sku',units:{$sum:'$lines.qty'}}},{$sort:{units:-1}}]})
  });
  assert.equal(response.status,200);
  assert.deepEqual((await response.json()).records,[{_id:'x',units:7},{_id:'y',units:1}]);
});

test('HTTP aggregation and PATCH still obey collection policies',async(t)=>{
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-document-policy-'));
  const db=await IndexedSyncioDatabase.open(path.join(root,'db.json'));
  const service=createSyncioServer({db,policies:[]});
  const address=await service.listen();
  t.after(async()=>{await service.close();await db.close();await fs.rm(root,{recursive:true,force:true});});

  const aggregate=await fetch(`${address.url}/collections/orders/aggregate`,{method:'POST',headers:{'content-type':'application/json'},body:'{"pipeline":[]}'});
  assert.equal(aggregate.status,403);
  const patch=await fetch(`${address.url}/collections/orders/a`,{method:'PATCH',headers:{'content-type':'application/json'},body:'{"$set":{"x":1}}'});
  assert.equal(patch.status,403);
});
