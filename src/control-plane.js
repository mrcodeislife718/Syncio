import crypto from 'node:crypto';
import path from 'node:path';
import { SyncioDatabase } from './index.js';
import { createTokenAuthority } from './operations.js';

const PLAN_ENTITLEMENTS = Object.freeze({
  free: ['database','realtime:basic'],
  pro: ['database','realtime:basic','realtime:extended','backups','higher-limits'],
  business: ['database','realtime:basic','realtime:extended','backups','higher-limits','audit','priority-support'],
  enterprise: ['*']
});

export class SyncioControlPlane {
  constructor(db, { tokenSecret, storageRoot = './syncio-projects' } = {}) {
    this.db = db;
    this.tokens = createTokenAuthority(tokenSecret);
    this.storageRoot = path.resolve(storageRoot);
  }

  static async open(file, options) {
    if (!options?.tokenSecret) throw new TypeError('control plane requires tokenSecret');
    return new SyncioControlPlane(await SyncioDatabase.open(file), options);
  }

  async createAccount({ email, name = null } = {}) {
    if (typeof email !== 'string' || !/^\S+@\S+\.\S+$/.test(email)) throw new TypeError('valid email required');
    const normalized = email.trim().toLowerCase();
    const existing = this.db.collection('_accounts').all().find((account) => account.email === normalized && account.status !== 'deleted');
    if (existing) throw conflict('account_email_exists');
    return this.db.collection('_accounts').insert({ id: crypto.randomUUID(), email: normalized, name, status:'active', createdAt:new Date().toISOString() });
  }

  async createProject({ accountId, name, plan = 'free' } = {}) {
    const account = this.db.collection('_accounts').get(accountId);
    if (!account || account.status !== 'active') throw notFound('account_not_found');
    if (typeof name !== 'string' || !name.trim() || name.length > 120) throw new TypeError('project name required');
    validatePlan(plan);
    const now = new Date().toISOString();
    const project = await this.db.collection('_projects').insert({ id: crypto.randomUUID(), accountId, name:name.trim(), plan, status:'active', createdAt:now, updatedAt:now });
    await this.db.collection('_entitlements').upsert({ id:project.id, projectId:project.id, grants:[...PLAN_ENTITLEMENTS[plan]], source:'plan', updatedAt:now });
    return project;
  }

  project(projectId) {
    const project = this.db.collection('_projects').get(projectId);
    return project?.status === 'active' ? project : null;
  }

  entitlements(projectId) {
    const record = this.db.collection('_entitlements').get(projectId);
    return record ? [...record.grants] : [];
  }

  async changePlan(projectId, plan, { source = 'operator', externalReference = null } = {}) {
    validatePlan(plan);
    const project = this.project(projectId);
    if (!project) throw notFound('project_not_found');
    const now = new Date().toISOString();
    await this.db.transaction(async (tx) => {
      tx.collection('_projects').put({ ...project, plan, updatedAt:now });
      tx.collection('_entitlements').put({ id:projectId, projectId, grants:[...PLAN_ENTITLEMENTS[plan]], source, externalReference, updatedAt:now });
    });
    return this.project(projectId);
  }

  issueProjectToken({ accountId, projectId, role = 'owner', ttlSeconds } = {}) {
    const project = this.project(projectId);
    if (!project || project.accountId !== accountId) throw notFound('project_not_found');
    return this.tokens.issue({ subject:accountId, projectId, role, entitlements:this.entitlements(projectId), expiresInSeconds:ttlSeconds });
  }

  authenticateProjectRequest(projectId, req) {
    const user = this.tokens.authenticateRequest(req);
    if (!user || user.projectId !== projectId || !this.project(projectId)) return null;
    return user;
  }

  projectStorageFile(projectId) {
    const project = this.project(projectId);
    if (!project) throw notFound('project_not_found');
    return path.join(this.storageRoot, projectId, 'data.syncio.json');
  }

  async exportAccount(accountId) {
    const account = this.db.collection('_accounts').get(accountId);
    if (!account) throw notFound('account_not_found');
    const projects = this.db.collection('_projects').all().filter((project)=>project.accountId===accountId);
    return {
      exportedAt:new Date().toISOString(),
      account,
      projects,
      entitlements:Object.fromEntries(projects.map((project)=>[project.id,this.entitlements(project.id)])),
      billingEvents:this.db.collection('_billing_events').all().filter((event)=>projects.some((project)=>project.id===event.projectId))
    };
  }

  async deleteAccount(accountId) {
    const account = this.db.collection('_accounts').get(accountId);
    if (!account) return false;
    const projects = this.db.collection('_projects').all().filter((project)=>project.accountId===accountId);
    const deletedAt = new Date().toISOString();
    await this.db.transaction(async (tx) => {
      tx.collection('_accounts').put({ ...account, status:'deleted', email:`deleted-${account.id}@invalid.local`, name:null, deletedAt });
      for (const project of projects) {
        tx.collection('_projects').put({ ...project, status:'deleted', deletedAt, updatedAt:deletedAt });
        tx.collection('_entitlements').remove(project.id);
      }
    });
    return true;
  }

  async close() { await this.db.close(); }
}

export class BillingStateProcessor {
  constructor(controlPlane) { this.controlPlane = controlPlane; }

  async process(event) {
    if (!event || typeof event !== 'object' || Array.isArray(event)) throw new TypeError('billing event object required');
    const { id, projectId, type, plan, status = 'active', provider = 'external', occurredAt = new Date().toISOString() } = event;
    if (!id || !projectId || !type) throw new TypeError('billing event id, projectId and type required');
    const events = this.controlPlane.db.collection('_billing_events');
    if (events.get(id)) return { duplicate:true, applied:false };
    const project = this.controlPlane.project(projectId);
    if (!project) throw notFound('project_not_found');

    if (type === 'subscription.updated' || type === 'subscription.created') {
      validatePlan(plan);
      await this.controlPlane.changePlan(projectId, plan, { source:`billing:${provider}`, externalReference:id });
    } else if (type === 'subscription.cancelled' || status === 'cancelled') {
      await this.controlPlane.changePlan(projectId, 'free', { source:`billing:${provider}:cancelled`, externalReference:id });
    } else {
      throw new Error(`unsupported billing event type: ${type}`);
    }
    await events.insert({ id, projectId, type, plan:plan ?? null, status, provider, occurredAt, processedAt:new Date().toISOString() });
    return { duplicate:false, applied:true, plan:this.controlPlane.project(projectId).plan };
  }
}

export function planEntitlements(plan) { validatePlan(plan); return [...PLAN_ENTITLEMENTS[plan]]; }

function validatePlan(plan) { if (!Object.hasOwn(PLAN_ENTITLEMENTS, plan)) throw new TypeError(`unknown Syncio plan: ${plan}`); }
function conflict(code) { const error=new Error(code); error.code=code; error.statusCode=409; return error; }
function notFound(code) { const error=new Error(code); error.code=code; error.statusCode=404; return error; }
