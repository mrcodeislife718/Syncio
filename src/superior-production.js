import fs from 'node:fs/promises';
import path from 'node:path';
import { ProductionSyncioDatabase } from './production-db.js';
import { SuperiorIndexedSyncioDatabase } from './superior-indexed.js';

export async function openSuperiorProduction(file,options={}){
  const base=await SuperiorIndexedSyncioDatabase.open(file,options);
  const metadataFile=path.resolve(`${base.file}.capabilities.json`);
  let metadata={version:1,schemas:{},ttl:{},text:{},geo:{}};
  try{metadata=JSON.parse(await fs.readFile(metadataFile,'utf8'));}catch(error){if(error.code!=='ENOENT'){await base.close();throw error;}}
  try{return new ProductionSyncioDatabase(base,metadataFile,metadata);}catch(error){await base.close();throw error;}
}
