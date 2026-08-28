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
    const emailKey = hashEmail(normalized);
    const id = crypto.randomUUID();
    let account;
    await this.db.transaction(async (tx) => {
      const keys=tx.collection('_account_email_keys');
      if (keys.get(emailKey)) throw conflict('account_email_exists');
      const accounts=tx.collection('_accounts');
      if (accounts.all().some((item)=>item.email===normalized&&item.status!=='deleted')) throw conflict('account_email_exists');
      account={ id, email:normalized, name, status:'active', createdAt:new Date().toISOString() };
      accounts.put(account);
      keys.put({id:emailKey,accountId:id});
    });
    return structuredClone(account);
  }

  async createProject({ accountId, name, plan = 'free' } = {}) {
    const account = this.db.collection('_accounts').get(accountId);
    if (!account || account.status !== 'active') throw notFound('account_not_found');
    if (typeof name !== 'string' || !name.trim() || name.length > 120) throw new TypeError('project name required');
    validatePlan(plan);
    const now = new Date().toISOString();
    const project={ id:crypto.randomUUID(), accountId, name:name.trim(), plan, status:'active', createdAt:now, updatedAt:now };
    await this.db.transaction(async(tx)=>{
      const currentAccount=tx.collection('_accounts').get(accountId);
      if(!currentAccount||currentAccount.status!=='active')throw notFound('account_not_found');
      tx.collection('_projects').put(project);
      tx.collection('_entitlements').put({ id:project.id, projectId:project.id, grants:[...PLAN_ENTITLEMENTS[plan]], source:'plan', updatedAt:now });
    });
    return structuredClone(project);
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
      const current=tx.collection('_projects').get(projectId);
      if(!current||current.status!=='active')throw notFound('project_not_found');
      tx.collection('_projects').put({ ...current, plan, updatedAt:now });
      tx.collection('_entitlements').put({ id:projectId, projectId, grants:[...PLAN_ENTITLEMENTS[plan]], source, externalReference, updatedAt:now });
    });
    return this.project(projectId);
  }

  issueProjectToken({ accountId, projectId, role = 'owner', ttlSeconds } = {}) {
    const project = this.project(projectId);
    const account=this.db.collection('_accounts').get(accountId);
    if (!project || project.accountId !== accountId || !account || account.status!=='active') throw notFound('project_not_found');
    return this.tokens.issue({ subject:accountId, projectId, role, entitlements:this.entitlements(projectId), expiresInSeconds:ttlSeconds });
  }

  authenticateProjectRequest(projectId, req) {
    const tokenUser = this.tokens.authenticateRequest(req);
    if (!tokenUser || tokenUser.projectId !== projectId) return null;
    const project=this.project(projectId);
    const account=this.db.collection('_accounts').get(tokenUser.sub);
    if(!project||project.accountId!==tokenUser.sub||!account||account.status!=='active')return null;
    return { ...tokenUser, entitlements:this.entitlements(projectId), plan:project.plan };
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
    const emailKey=account.email&&!account.email.endsWith('@invalid.local')?hashEmail(account.email):null;
    await this.db.transaction(async (tx) => {
      tx.collection('_accounts').put({ ...account, status:'deleted', email:`deleted-${account.id}@invalid.local`, name:null, deletedAt });
      if(emailKey)tx.collection('_account_email_keys').remove(emailKey);
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
    let targetPlan;
    if(type==='subscription.updated'||type==='subscription.created'){validatePlan(plan);targetPlan=plan;}
    else if(type==='subscription.cancelled'||status==='cancelled')targetPlan='free';
    else throw new Error(`unsupported billing event type: ${type}`);
    let duplicate=false;
    await this.controlPlane.db.transaction(async(tx)=>{
      const events=tx.collection('_billing_events');
      if(events.get(id)){duplicate=true;return;}
      const projects=tx.collection('_projects');
      const project=projects.get(projectId);
      if(!project||project.status!=='active')throw notFound('project_not_found');
      const now=new Date().toISOString();
      projects.put({...project,plan:targetPlan,updatedAt:now});
      tx.collection('_entitlements').put({id:projectId,projectId,grants:[...PLAN_ENTITLEMENTS[targetPlan]],source:`billing:${provider}${targetPlan==='free'?':cancelled':''}`,externalReference:id,updatedAt:now});
      events.put({id,projectId,type,plan:plan??null,status,provider,occurredAt,processedAt:now});
    });
    if(duplicate)return{duplicate:true,applied:false};
    return { duplicate:false, applied:true, plan:targetPlan };
  }
}

export function planEntitlements(plan) { validatePlan(plan); return [...PLAN_ENTITLEMENTS[plan]]; }

function validatePlan(plan) { if (!Object.hasOwn(PLAN_ENTITLEMENTS, plan)) throw new TypeError(`unknown Syncio plan: ${plan}`); }
function hashEmail(email){return crypto.createHash('sha256').update(email).digest('hex');}
function conflict(code) { const error=new Error(code); error.code=code; error.statusCode=409; return error; }
function notFound(code) { const error=new Error(code); error.code=code; error.statusCode=404; return error; }
