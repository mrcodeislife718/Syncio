import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import { SyncioDatabase } from '../src/index.js';
import { MaintenanceManager } from '../src/maintenance.js';

async function setup(name) {
  const root=await fs.mkdtemp(path.join(os.tmpdir(),`syncio-maint-${name}-`));
  const db=await SyncioDatabase.open(path.join(root,'db.json'));
  const manager=new MaintenanceManager(db,{backupKey:crypto.randomBytes(32),backupDirectory:path.join(root,'backups')});
  return {root,db,manager};
}

test('migration creates verified encrypted backup and rollback restores prior state', async (t) => {
  const state=await setup('rollback');
  t.after(async()=>{await state.db.close();await fs.rm(state.root,{recursive:true,force:true});});
  await state.db.collection('users').upsert({id:'u1',name:'Ada'});
  const result=await state.manager.runMigrations([{version:2,up:(snapshot)=>{snapshot.collections.users.u1.name='ADA';return snapshot;}}],{label:'uppercase-name'});
  assert.equal(result.fromVersion,1);
  assert.equal(result.toVersion,2);
  assert.equal(state.db.collection('users').get('u1').name,'ADA');
  assert.equal((await state.manager.verifyBackup(result.backup.file)).ok,true);
  const rolled=await state.manager.rollbackMigration(result.migrationId);
  assert.equal(rolled.rolledBack,true);
  assert.equal(rolled.restoredVersion,1);
  assert.equal(state.db.collection('users').get('u1').name,'Ada');
});

test('failed migration leaves live database unchanged and retains pre-migration backup', async (t) => {
  const state=await setup('failure');
  t.after(async()=>{await state.db.close();await fs.rm(state.root,{recursive:true,force:true});});
  await state.db.collection('items').upsert({id:'a',value:1});
  let failure;
  try {
    await state.manager.runMigrations([{version:2,up:()=>{throw new Error('migration exploded');}}]);
  } catch (error) { failure=error; }
  assert.match(failure.message,/migration exploded/);
  assert.ok(failure.backup?.file);
  assert.equal(state.db.snapshot().version,1);
  assert.equal(state.db.collection('items').get('a').value,1);
  assert.equal((await state.manager.verifyBackup(failure.backup.file)).ok,true);
});
