import crypto from 'node:crypto';

const GB = 1024 ** 3;
const HOURS_PER_MONTH = 730;
const clone = (value) => structuredClone(value);

export const PLAN_CATALOG = Object.freeze({
  free: Object.freeze({
    name: 'Free', monthlyBaseCents: 0,
    included: Object.freeze({ reads: 100_000, writes: 25_000, storageGbMonths: 1, realtimeHours: 100, egressGb: 1, backupGbMonths: 0, pitrGbMonths: 0, replicaRegionHours: 0 }),
    limits: Object.freeze({ storageBytes: 1 * GB, monthlyReads: 100_000, monthlyWrites: 25_000, concurrentRealtime: 20, regions: 1, replicas: 0, pitrDays: 0 }),
    overage: Object.freeze({ readsPerMillionCents: 60, writesPerMillionCents: 180, storageGbMonthCents: 30, realtimeHourCents: 8, egressGbCents: 10, backupGbMonthCents: 10, pitrGbMonthCents: 15, replicaRegionHourCents: 4 })
  }),
  pro: Object.freeze({
    name: 'Pro', monthlyBaseCents: 4900,
    included: Object.freeze({ reads: 1_000_000, writes: 250_000, storageGbMonths: 5, realtimeHours: 500, egressGb: 10, backupGbMonths: 5, pitrGbMonths: 5, replicaRegionHours: 0 }),
    limits: Object.freeze({ storageBytes: 50 * GB, monthlyReads: 10_000_000, monthlyWrites: 2_500_000, concurrentRealtime: 500, regions: 1, replicas: 0, pitrDays: 7 }),
    overage: Object.freeze({ readsPerMillionCents: 50, writesPerMillionCents: 150, storageGbMonthCents: 25, realtimeHourCents: 6, egressGbCents: 9, backupGbMonthCents: 8, pitrGbMonthCents: 12, replicaRegionHourCents: 4 })
  }),
  scale: Object.freeze({
    name: 'Scale', monthlyBaseCents: 49_900,
    included: Object.freeze({ reads: 10_000_000, writes: 2_000_000, storageGbMonths: 50, realtimeHours: 5_000, egressGb: 100, backupGbMonths: 50, pitrGbMonths: 50, replicaRegionHours: HOURS_PER_MONTH }),
    limits: Object.freeze({ storageBytes: 2 * 1024 * GB, monthlyReads: 1_000_000_000, monthlyWrites: 200_000_000, concurrentRealtime: 20_000, regions: 4, replicas: 3, pitrDays: 30 }),
    overage: Object.freeze({ readsPerMillionCents: 35, writesPerMillionCents: 110, storageGbMonthCents: 20, realtimeHourCents: 4, egressGbCents: 7, backupGbMonthCents: 6, pitrGbMonthCents: 9, replicaRegionHourCents: 3 })
  }),
  enterprise: Object.freeze({
    name: 'Enterprise', monthlyBaseCents: null,
    included: Object.freeze({ reads: Infinity, writes: Infinity, storageGbMonths: Infinity, realtimeHours: Infinity, egressGb: Infinity, backupGbMonths: Infinity, pitrGbMonths: Infinity, replicaRegionHours: Infinity }),
    limits: Object.freeze({ storageBytes: Infinity, monthlyReads: Infinity, monthlyWrites: Infinity, concurrentRealtime: Infinity, regions: Infinity, replicas: Infinity, pitrDays: Infinity }),
    overage: Object.freeze({ readsPerMillionCents: 0, writesPerMillionCents: 0, storageGbMonthCents: 0, realtimeHourCents: 0, egressGbCents: 0, backupGbMonthCents: 0, pitrGbMonthCents: 0, replicaRegionHourCents: 0 })
  })
});

const PLAN_ALIASES = Object.freeze({ business: 'scale' });
const METRICS = new Set(['reads','writes','storage_byte_hours','realtime_seconds','egress_bytes','backup_byte_hours','pitr_byte_hours','replica_region_hours','compute_milliseconds']);

export function canonicalPlan(plan) {
  const normalized = PLAN_ALIASES[plan] ?? plan;
  if (!Object.hasOwn(PLAN_CATALOG, normalized)) throw new TypeError(`unknown Syncio commercial plan: ${plan}`);
  return normalized;
}

export function planCatalog() {
  return Object.fromEntries(Object.entries(PLAN_CATALOG).map(([id, plan]) => [id, clonePlan(plan)]));
}

export class UsageMeter {
  constructor(controlPlane) {
    if (!controlPlane?.db || typeof controlPlane.project !== 'function') throw new TypeError('usage meter requires controlPlane');
    this.controlPlane = controlPlane;
  }

  async record({ projectId, metric, quantity = 1, at = new Date().toISOString(), idempotencyKey = crypto.randomUUID(), metadata = {} } = {}) {
    const project = this.controlPlane.project(projectId);
    if (!project) throw usageError('project_not_found', 404);
    if (!METRICS.has(metric)) throw new TypeError(`unsupported usage metric: ${metric}`);
    if (!Number.isFinite(quantity) || quantity < 0) throw new TypeError('usage quantity must be a non-negative finite number');
    if (typeof idempotencyKey !== 'string' || !idempotencyKey || idempotencyKey.length > 256) throw new TypeError('usage idempotencyKey required');
    const timestamp = new Date(at);
    if (Number.isNaN(timestamp.valueOf())) throw new TypeError('usage at must be a valid date');
    const period = monthKey(timestamp);
    const id = digest(`${projectId}:${metric}:${idempotencyKey}`);
    let duplicate = false;
    await this.controlPlane.db.transaction(async (tx) => {
      const events = tx.collection('_usage_events');
      if (events.get(id)) { duplicate = true; return; }
      events.put({ id, projectId, metric, quantity, at: timestamp.toISOString(), period, idempotencyKey, metadata: clone(metadata), createdAt: new Date().toISOString() });
      const totals = tx.collection('_usage_totals');
      const totalId = `${projectId}:${period}:${metric}`;
      const current = totals.get(totalId);
      totals.put({ id: totalId, projectId, period, metric, quantity: (current?.quantity ?? 0) + quantity, updatedAt: new Date().toISOString() });
    });
    return { duplicate, recorded: !duplicate, projectId, metric, quantity, period };
  }

  summary(projectId, period = monthKey(new Date())) {
    if (!this.controlPlane.project(projectId)) throw usageError('project_not_found', 404);
    validatePeriod(period);
    const usage = emptyUsage();
    for (const row of this.controlPlane.db.collection('_usage_totals').all()) {
      if (row.projectId === projectId && row.period === period && METRICS.has(row.metric)) usage[row.metric] += row.quantity;
    }
    return Object.freeze({ projectId, period, raw: clone(usage), billable: normalizeUsage(usage) });
  }

  events(projectId, period = monthKey(new Date())) {
    validatePeriod(period);
    return this.controlPlane.db.collection('_usage_events').all().filter((row) => row.projectId === projectId && row.period === period).sort((a,b) => a.at.localeCompare(b.at));
  }
}

export class RevenueEngine {
  constructor({ controlPlane, enterprisePricing = null } = {}) {
    if (!controlPlane?.db || typeof controlPlane.project !== 'function') throw new TypeError('revenue engine requires controlPlane');
    this.controlPlane = controlPlane;
    this.usage = new UsageMeter(controlPlane);
    this.enterprisePricing = enterprisePricing;
  }

  catalog() { return planCatalog(); }

  usageSummary(projectId, period) { return this.usage.summary(projectId, period); }

  estimateInvoice(projectId, period = monthKey(new Date())) {
    const project = this.controlPlane.project(projectId);
    if (!project) throw usageError('project_not_found', 404);
    validatePeriod(period);
    const planId = canonicalPlan(project.plan);
    const plan = PLAN_CATALOG[planId];
    const usage = this.usage.summary(projectId, period);
    if (planId === 'enterprise' && !this.enterprisePricing) return { projectId, period, plan: planId, currency: 'usd', contractPriced: true, usage, totalCents: null, lines: [] };
    const pricedPlan = planId === 'enterprise' ? normalizeEnterprisePlan(plan, this.enterprisePricing) : plan;
    const lines = [];
    if (pricedPlan.monthlyBaseCents > 0) lines.push(line('base', 1, pricedPlan.monthlyBaseCents, pricedPlan.monthlyBaseCents));
    addOverage(lines, 'reads', usage.billable.reads, pricedPlan.included.reads, pricedPlan.overage.readsPerMillionCents, 1_000_000);
    addOverage(lines, 'writes', usage.billable.writes, pricedPlan.included.writes, pricedPlan.overage.writesPerMillionCents, 1_000_000);
    addOverage(lines, 'storage_gb_month', usage.billable.storageGbMonths, pricedPlan.included.storageGbMonths, pricedPlan.overage.storageGbMonthCents, 1);
    addOverage(lines, 'realtime_connection_hour', usage.billable.realtimeHours, pricedPlan.included.realtimeHours, pricedPlan.overage.realtimeHourCents, 1);
    addOverage(lines, 'egress_gb', usage.billable.egressGb, pricedPlan.included.egressGb, pricedPlan.overage.egressGbCents, 1);
    addOverage(lines, 'backup_gb_month', usage.billable.backupGbMonths, pricedPlan.included.backupGbMonths, pricedPlan.overage.backupGbMonthCents, 1);
    addOverage(lines, 'pitr_gb_month', usage.billable.pitrGbMonths, pricedPlan.included.pitrGbMonths, pricedPlan.overage.pitrGbMonthCents, 1);
    addOverage(lines, 'replica_region_hour', usage.billable.replicaRegionHours, pricedPlan.included.replicaRegionHours, pricedPlan.overage.replicaRegionHourCents, 1);
    const totalCents = lines.reduce((sum, item) => sum + item.amountCents, 0);
    return Object.freeze({ projectId, period, plan: planId, currency: 'usd', contractPriced: false, usage, lines: lines.map(clone), totalCents });
  }

  quotaDecision(projectId, resource, requested = 1, period = monthKey(new Date())) {
    const project = this.controlPlane.project(projectId);
    if (!project) throw usageError('project_not_found', 404);
    const plan = PLAN_CATALOG[canonicalPlan(project.plan)];
    const usage = this.usage.summary(projectId, period).billable;
    const map = {
      storageBytes: { current: usage.storageBytesAverage, limit: plan.limits.storageBytes },
      monthlyReads: { current: usage.reads, limit: plan.limits.monthlyReads },
      monthlyWrites: { current: usage.writes, limit: plan.limits.monthlyWrites },
      concurrentRealtime: { current: 0, limit: plan.limits.concurrentRealtime },
      regions: { current: 0, limit: plan.limits.regions },
      replicas: { current: 0, limit: plan.limits.replicas },
      pitrDays: { current: 0, limit: plan.limits.pitrDays }
    };
    const item = map[resource];
    if (!item) throw new TypeError(`unsupported quota resource: ${resource}`);
    if (!Number.isFinite(requested) || requested < 0) throw new TypeError('requested quota amount must be non-negative');
    const allowed = item.limit === Infinity || item.current + requested <= item.limit;
    return Object.freeze({ allowed, resource, current: item.current, requested, limit: item.limit, plan: canonicalPlan(project.plan), upgradeRequired: !allowed });
  }

  async finalizeInvoice(projectId, period = previousMonthKey(new Date()), { externalInvoiceId = null } = {}) {
    const estimate = this.estimateInvoice(projectId, period);
    const id = `${projectId}:${period}`;
    let invoice;
    await this.controlPlane.db.transaction(async (tx) => {
      const invoices = tx.collection('_invoices');
      const existing = invoices.get(id);
      if (existing) { invoice = existing; return; }
      invoice = { id, projectId, period, status: 'open', currency: 'usd', plan: estimate.plan, contractPriced: estimate.contractPriced, totalCents: estimate.totalCents, lines: clone(estimate.lines), usage: clone(estimate.usage), externalInvoiceId, createdAt: new Date().toISOString() };
      invoices.put(invoice);
    });
    return clone(invoice);
  }

  invoices(projectId) {
    return this.controlPlane.db.collection('_invoices').all().filter((item) => item.projectId === projectId).sort((a,b) => a.period.localeCompare(b.period));
  }
}

export function createUsageObserver({ meter, projectId } = {}) {
  if (!meter || typeof meter.record !== 'function') throw new TypeError('usage observer requires meter');
  if (!projectId) throw new TypeError('usage observer requires projectId');
  return (event) => {
    const at = new Date().toISOString();
    if (event?.type === 'request_complete') {
      const path = String(event.path ?? '');
      if (path.startsWith('/collections/')) {
        const metric = event.method === 'GET' ? 'reads' : ['POST','PUT','PATCH','DELETE'].includes(event.method) ? 'writes' : null;
        if (metric) void meter.record({ projectId, metric, quantity: 1, at, idempotencyKey: `request:${event.requestId}:${metric}` }).catch(() => undefined);
      }
      if (Number.isFinite(event.responseBytes) && event.responseBytes > 0) void meter.record({ projectId, metric:'egress_bytes', quantity:event.responseBytes, at, idempotencyKey:`request:${event.requestId}:egress` }).catch(() => undefined);
    }
    if (event?.type === 'subscription_closed' && Number.isFinite(event.durationMs) && event.durationMs > 0) void meter.record({ projectId, metric:'realtime_seconds', quantity:event.durationMs / 1000, at, idempotencyKey:`subscription:${event.requestId}` }).catch(() => undefined);
  };
}

export function serviceCatalog() {
  return Object.freeze({
    cloud: ['free','pro','scale','enterprise'],
    dedicated: { minimumPlan:'scale', billing:'base-plus-usage', isolation:'dedicated-runtime' },
    enterpriseSelfHosted: { billing:'annual-contract', includes:['security-updates','support','advanced-admin','commercial-license'] },
    migrationServices: { billing:'fixed-or-scoped-services', sources:['mongodb','firebase','supabase','redis'] },
    marketplace: { billing:'revenue-share-or-usage', categories:['connectors','observability','specialized-storage','extensions'] }
  });
}

function emptyUsage(){return{reads:0,writes:0,storage_byte_hours:0,realtime_seconds:0,egress_bytes:0,backup_byte_hours:0,pitr_byte_hours:0,replica_region_hours:0,compute_milliseconds:0};}
function normalizeUsage(raw){return Object.freeze({reads:raw.reads,writes:raw.writes,storageBytesAverage:raw.storage_byte_hours/HOURS_PER_MONTH,storageGbMonths:raw.storage_byte_hours/(GB*HOURS_PER_MONTH),realtimeHours:raw.realtime_seconds/3600,egressGb:raw.egress_bytes/GB,backupGbMonths:raw.backup_byte_hours/(GB*HOURS_PER_MONTH),pitrGbMonths:raw.pitr_byte_hours/(GB*HOURS_PER_MONTH),replicaRegionHours:raw.replica_region_hours,computeHours:raw.compute_milliseconds/3_600_000});}
function addOverage(lines,metric,used,included,unitCents,unitSize){if(!Number.isFinite(used)||included===Infinity)return;const over=Math.max(0,used-included);if(over<=0||unitCents<=0)return;const units=over/unitSize;lines.push(line(metric,units,unitCents,Math.round(units*unitCents)));}
function line(metric,quantity,unitCents,amountCents){return{metric,quantity,unitCents,amountCents};}
function clonePlan(plan){return{name:plan.name,monthlyBaseCents:plan.monthlyBaseCents,included:clone(plan.included),limits:clone(plan.limits),overage:clone(plan.overage)};}
function normalizeEnterprisePlan(base,pricing){const candidate={...clonePlan(base),...clone(pricing),included:{...base.included,...pricing?.included},limits:{...base.limits,...pricing?.limits},overage:{...base.overage,...pricing?.overage}};if(!Number.isSafeInteger(candidate.monthlyBaseCents)||candidate.monthlyBaseCents<0)throw new TypeError('enterprise monthlyBaseCents must be non-negative integer');return candidate;}
function monthKey(date){return `${date.getUTCFullYear()}-${String(date.getUTCMonth()+1).padStart(2,'0')}`;}
function previousMonthKey(date){const d=new Date(Date.UTC(date.getUTCFullYear(),date.getUTCMonth()-1,1));return monthKey(d);}
function validatePeriod(period){if(typeof period!=='string'||!/^[0-9]{4}-(0[1-9]|1[0-2])$/.test(period))throw new TypeError('period must be YYYY-MM');}
function digest(value){return crypto.createHash('sha256').update(value).digest('hex');}
function usageError(code,statusCode){const error=new Error(code);error.code=code;error.statusCode=statusCode;return error;}
