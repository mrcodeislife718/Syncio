import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { SyncioDatabase } from '../src/index.js';
import { IndexedSyncioDatabase } from '../src/indexed.js';
import { queryRecords } from '../src/advanced.js';
import { applyDocumentUpdate, aggregateDocuments, matchesDocument, projectDocument } from '../src/document.js';
import { atomicUpdateDocument, aggregateCollection } from '../src/document-api.js';

async function setup(indexed=false) {
  const root=await fs.mkdtemp(path.join(os.tmpdir(),'syncio-doc-'));
  const file=path.join(root,'db.json');
  const db=indexed?await IndexedSyncioDatabase.open(file):await SyncioDatabase.open(file);
  return {root,file,db};
}

async function cleanup(state){await state.db.close();await fs.rm(state.root,{recursive:true,force:true});}

const records=[
  {id:'a',customer:{name:'Ada',address:{city:'Bronx'}},total:25,tags:['new','priority'],lines:[{sku:'x',qty:2},{sku:'y',qty:1}]},
  {id:'b',customer:{name:'Grace',address:{city:'Brooklyn'}},total:80,tags:['priority'],lines:[{sku:'z',qty:5}]},
  {id:'c',customer:{name:'Linus',address:{city:'Bronx'}},total:50,tags:['repeat'],lines:[{sku:'x',qty:1}]}
];

test('nested document queries support logical range array and element operators',()=>{
  assert.equal(matchesDocument(records[0],{'customer.address.city':'Bronx',total:{$gte:20,$lt:30}}),true);
  assert.equal(matchesDocument(records[1],{$or:[{'customer.address.city':'Bronx'},{total:{$gt:70}}]}),true);
  assert.equal(matchesDocument(records[0],{tags:{$all:['new','priority']}}),true);
  assert.equal(matchesDocument(records[1],{lines:{$elemMatch:{sku:'z',qty:{$gte:5}}}}),true);
  assert.equal(matchesDocument(records[2],{$nor:[{total:{$gt:60}},{tags:'priority'}]}),true);
});

test('query engine combines nested filters multi-field sort pagination and projection',()=>{
  const result=queryRecords(records,{where:{'customer.address.city':'Bronx'},orderBy:[{field:'total',direction:'desc'},{field:'customer.name',direction:'asc'}],offset:0,limit:1,projection:{'customer.name':1,total:1,id:0}});
  assert.deepEqual(result,[{customer:{name:'Linus'},total:50}]);
});

test('projection supports nested inclusion and exclusion without mutating source',()=>{
  const included=projectDocument(records[0],{'customer.name':1,total:1});
  assert.deepEqual(included,{id:'a',customer:{name:'Ada'},total:25});
  const excluded=projectDocument(records[0],{'customer.address':0,lines:0});
  assert.equal(excluded.customer.address,undefined);
  assert.equal(excluded.lines,undefined);
  assert.equal(records[0].customer.address.city,'Bronx');
});

test('document update operators mutate nested data deterministically',()=>{
  const updated=applyDocumentUpdate(records[0],{
    $set:{'customer.status':'active'},
    $inc:{total:5,'metrics.visits':1},
    $push:{tags:'vip'},
    $addToSet:{tags:{$each:['priority','returning']}},
    $pull:{lines:{qty:{$lt:2}}},
    $rename:{'customer.status':'customer.state'}
  });
  assert.equal(updated.total,30);
  assert.equal(updated.metrics.visits,1);
  assert.equal(updated.customer.state,'active');
  assert.deepEqual(updated.tags,['new','priority','vip','returning']);
  assert.deepEqual(updated.lines,[{sku:'x',qty:2}]);
  assert.equal(records[0].total,25);
});

test('atomic document update commits through WAL and realtime change spine',async(t)=>{
  const state=await setup();t.after(()=>cleanup(state));
  await state.db.collection('orders').upsert(records[0]);
  const after=state.db.sequence;
  const seen=[];
  const stop=state.db.watchChanges({collection:'orders',after},(change)=>seen.push(change));t.after(stop);
  const result=await atomicUpdateDocument(state.db,'orders','a',{$inc:{total:10},$set:{'customer.level':'gold'}});
  assert.equal(result.total,35);
  assert.equal(result.customer.level,'gold');
  await new Promise((resolve)=>setImmediate(resolve));
  assert.equal(seen.length,1);
  assert.equal(seen[0].type,'upsert');
  assert.equal(seen[0].record.total,35);
  await state.db.close();
  state.db=await SyncioDatabase.open(state.file);
  assert.equal(state.db.collection('orders').get('a').total,35);
});

test('nested persistent index accelerates eligible equality query and remains correct',async(t)=>{
  const state=await setup(true);t.after(()=>cleanup(state));
  for(const record of records) await state.db.collection('orders').upsert(record);
  await state.db.defineIndex('orders','customer.address.city');
  assert.deepEqual(state.db.collection('orders').explain({where:{'customer.address.city':'Bronx'}}),{strategy:'index',field:'customer.address.city',value:'Bronx'});
  const result=state.db.collection('orders').query({where:{'customer.address.city':'Bronx'},orderBy:{field:'total',direction:'desc'},projection:{'customer.name':1,total:1}});
  assert.deepEqual(result.map((row)=>row.customer.name),['Linus','Ada']);
});

test('aggregation performs match unwind group projection and count operations',()=>{
  const grouped=aggregateDocuments(records,[
    {$match:{total:{$gte:25}}},
    {$unwind:'$lines'},
    {$group:{_id:'$lines.sku',units:{$sum:'$lines.qty'},orders:{$addToSet:'$id'},maxTotal:{$max:'$total'}}},
    {$sort:{units:-1}}
  ]);
  const x=grouped.find((row)=>row._id==='x');
  assert.deepEqual(x,{_id:'x',units:3,orders:['a','c'],maxTotal:50});
  assert.deepEqual(aggregateDocuments(records,[{$match:{'customer.address.city':'Bronx'}},{$count:'count'}]),[{count:2}]);
});

test('aggregation runs against real persisted collection state',async(t)=>{
  const state=await setup();t.after(()=>cleanup(state));
  for(const record of records) await state.db.collection('orders').upsert(record);
  const result=aggregateCollection(state.db,'orders',[{$group:{_id:'$customer.address.city',revenue:{$sum:'$total'}}},{$sort:{revenue:-1}}]);
  assert.deepEqual(result,[{_id:'Bronx',revenue:75},{_id:'Brooklyn',revenue:80}].sort((a,b)=>b.revenue-a.revenue));
});
