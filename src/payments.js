import crypto from 'node:crypto';
import { BillingStateProcessor, planEntitlements } from './control-plane.js';

const clone=(value)=>structuredClone(value);

export class PaymentOrchestrator {
  constructor({controlPlane,provider,failedPaymentPolicy='restrict'}={}){
    if(!controlPlane?.db||typeof controlPlane.project!=='function')throw new TypeError('payment orchestrator requires controlPlane');
    validateProvider(provider);if(!['restrict','grace'].includes(failedPaymentPolicy))throw new TypeError('failedPaymentPolicy must be restrict or grace');
    this.controlPlane=controlPlane;this.provider=provider;this.billing=new BillingStateProcessor(controlPlane);this.failedPaymentPolicy=failedPaymentPolicy;
  }
  async createCheckout({projectId,plan,successUrl,cancelUrl,idempotencyKey=crypto.randomUUID()}={}){return this.#action('checkout',projectId,idempotencyKey,async()=>this.provider.createCheckoutSession({project:this.#project(projectId),plan,successUrl,cancelUrl,idempotencyKey}));}
  async createPortal({projectId,returnUrl,idempotencyKey=crypto.randomUUID()}={}){return this.#action('portal',projectId,idempotencyKey,async()=>this.provider.createPortalSession({project:this.#project(projectId),returnUrl,idempotencyKey}));}
  async cancelSubscription({projectId,subscriptionId,atPeriodEnd=true,idempotencyKey=crypto.randomUUID()}={}){return this.#action('cancel',projectId,idempotencyKey,async()=>this.provider.cancelSubscription({project:this.#project(projectId),subscriptionId,atPeriodEnd,idempotencyKey}));}
  async refund({projectId,paymentId,amount,reason,idempotencyKey=crypto.randomUUID()}={}){return this.#action('refund',projectId,idempotencyKey,async()=>this.provider.refundPayment({project:this.#project(projectId),paymentId,amount,reason,idempotencyKey}));}
  async processEvent(event){
    if(!event||typeof event!=='object'||Array.isArray(event)||!event.id||!event.projectId||!event.type)throw new TypeError('verified payment event requires id projectId type');
    if(['subscription.created','subscription.updated','subscription.cancelled'].includes(event.type))return this.billing.process({...event,provider:event.provider??this.provider.name});
    let result;
    await this.controlPlane.db.transaction(async(tx)=>{
      const events=tx.collection('_billing_events');if(events.get(event.id)){result={duplicate:true,applied:false};return;}
      const projects=tx.collection('_projects');const project=projects.get(event.projectId);if(!project||project.status!=='active')throw billingError('project_not_found',404);
      const now=new Date().toISOString();
      if(event.type==='payment.failed'){
        const next={...project,billingStatus:'past_due',billingFailureAt:event.occurredAt??now,updatedAt:now};projects.put(next);
        if(this.failedPaymentPolicy==='restrict')tx.collection('_entitlements').put({id:project.id,projectId:project.id,grants:planEntitlements('free'),source:'billing:payment_failed',externalReference:event.id,updatedAt:now});
        result={duplicate:false,applied:true,status:'past_due',restricted:this.failedPaymentPolicy==='restrict'};
      }else if(event.type==='payment.recovered'){
        const plan=event.plan??project.plan;projects.put({...project,billingStatus:'active',billingRecoveredAt:event.occurredAt??now,updatedAt:now});tx.collection('_entitlements').put({id:project.id,projectId:project.id,grants:planEntitlements(plan),source:'billing:payment_recovered',externalReference:event.id,updatedAt:now});result={duplicate:false,applied:true,status:'active',plan};
      }else if(event.type==='refund.completed'){
        result={duplicate:false,applied:true,status:'refunded'};
      }else throw billingError('unsupported_payment_event',400);
      events.put({id:event.id,projectId:event.projectId,type:event.type,provider:event.provider??this.provider.name,plan:event.plan??null,status:result.status,occurredAt:event.occurredAt??now,processedAt:now,metadata:clone(event.metadata??{})});
    });
    return result;
  }
  async actionHistory(projectId){return this.controlPlane.db.collection('_billing_actions').all().filter((item)=>item.projectId===projectId).sort((a,b)=>a.createdAt.localeCompare(b.createdAt));}
  #project(projectId){const project=this.controlPlane.project(projectId);if(!project)throw billingError('project_not_found',404);return project;}
  async #action(type,projectId,idempotencyKey,run){
    if(typeof idempotencyKey!=='string'||!idempotencyKey||idempotencyKey.length>256)throw new TypeError('idempotencyKey required');this.#project(projectId);const actionId=digest(`${projectId}:${type}:${idempotencyKey}`);const existing=this.controlPlane.db.collection('_billing_actions').get(actionId);if(existing?.status==='succeeded')return clone(existing.result);
    let result;try{result=await run();}catch(error){await this.#recordAction({id:actionId,projectId,type,idempotencyKey,status:'failed',errorCode:error.code??'provider_error'});throw error;}
    if(!result||typeof result!=='object'||Array.isArray(result))throw new Error('payment provider returned invalid result');await this.#recordAction({id:actionId,projectId,type,idempotencyKey,status:'succeeded',result:clone(result)});return clone(result);
  }
  async #recordAction(action){const now=new Date().toISOString();await this.controlPlane.db.transaction(async(tx)=>{const actions=tx.collection('_billing_actions');const existing=actions.get(action.id);actions.put({...existing,...action,createdAt:existing?.createdAt??now,updatedAt:now});});}
}

export function createPaymentProviderAdapter({name,createCheckoutSession,createPortalSession,cancelSubscription,refundPayment}={}){const provider={name,createCheckoutSession,createPortalSession,cancelSubscription,refundPayment};validateProvider(provider);return Object.freeze(provider);}
function validateProvider(provider){if(!provider||typeof provider.name!=='string'||!provider.name)throw new TypeError('payment provider name required');for(const method of ['createCheckoutSession','createPortalSession','cancelSubscription','refundPayment'])if(typeof provider[method]!=='function')throw new TypeError(`payment provider requires ${method}`);}
function digest(value){return crypto.createHash('sha256').update(value).digest('hex');}
function billingError(code,statusCode){const error=new Error(code);error.code=code;error.statusCode=statusCode;return error;}
