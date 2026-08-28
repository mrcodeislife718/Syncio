import path from 'node:path';
import crypto from 'node:crypto';
import { migrate } from './advanced.js';
import { createEncryptedBackupManager } from './operations.js';

export class MaintenanceManager {
  constructor(db, { backupKey, backupDirectory } = {}) {
    if (!db || typeof db.snapshot !== 'function' || typeof db.replaceState !== 'function') throw new TypeError('MaintenanceManager requires Syncio-compatible database');
    if (!backupDirectory) throw new TypeError('backupDirectory required');
    this.db = db;
    this.backups = createEncryptedBackupManager({key:backupKey});
    this.backupDirectory = path.resolve(backupDirectory);
    this.lastBackup = null;
  }

  async createBackup({ reason='manual', metadata={} } = {}) {
    const snapshot=this.db.snapshot();
    const id=`${Date.now()}-${crypto.randomUUID()}`;
    const file=path.join(this.backupDirectory,`${id}.syncio-backup.json`);
    const result=await this.backups.backup({state:snapshot,file,metadata:{reason,databaseId:this.db.databaseId,sequence:this.db.sequence,...metadata}});
    this.lastBackup={id,...result,reason,createdAt:new Date().toISOString()};
    return structuredClone(this.lastBackup);
  }

  async verifyBackup(file) {
    const payload=await this.backups.restore(file);
    if (payload.metadata?.databaseId && payload.metadata.databaseId !== this.db.databaseId) {
      const error=new Error('backup belongs to a different database');
      error.code='SYNCIO_BACKUP_DATABASE_MISMATCH';
      throw error;
    }
    return {ok:true,version:payload.state.version,sequence:payload.state._syncio?.sequence ?? 0,metadata:payload.metadata};
  }

  async restoreBackup(file) {
    const payload=await this.backups.restore(file);
    if (payload.metadata?.databaseId && payload.metadata.databaseId !== this.db.databaseId) {
      const error=new Error('backup belongs to a different database');
      error.code='SYNCIO_BACKUP_DATABASE_MISMATCH';
      throw error;
    }
    await this.db.replaceState(payload.state);
    return {restored:true,version:this.db.snapshot().version,sequence:this.db.sequence};
  }

  async runMigrations(migrations, { label='migration' } = {}) {
    if (!Array.isArray(migrations) || !migrations.length) throw new TypeError('non-empty migrations array required');
    const before=this.db.snapshot();
    const targetVersion=Math.max(...migrations.map((item)=>item.version));
    if (!migrations.every((item)=>Number.isSafeInteger(item.version)&&item.version>0&&typeof item.up==='function')) throw new TypeError('each migration requires positive integer version and up function');
    const backup=await this.createBackup({reason:'pre-migration',metadata:{label,targetVersion}});
    let next;
    try { next=migrate(before,migrations); }
    catch (error) { error.backup=backup; throw error; }
    next.collections ??= {};
    next.collections._syncio_migration_history ??= {};
    const migrationId=crypto.randomUUID();
    next.collections._syncio_migration_history[migrationId]={id:migrationId,label,fromVersion:before.version,toVersion:next.version,backupFile:backup.file,appliedAt:new Date().toISOString()};
    try { await this.db.replaceState(next); }
    catch (error) { error.backup=backup; throw error; }
    return {applied:true,fromVersion:before.version,toVersion:next.version,backup,migrationId};
  }

  async rollbackMigration(migrationId) {
    const record=this.db.collection('_syncio_migration_history').get(migrationId);
    if (!record) {
      const error=new Error('migration history not found');
      error.code='SYNCIO_MIGRATION_NOT_FOUND';
      throw error;
    }
    const verified=await this.verifyBackup(record.backupFile);
    await this.restoreBackup(record.backupFile);
    return {rolledBack:true,migrationId,restoredVersion:verified.version};
  }
}
