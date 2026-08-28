import crypto from 'node:crypto';

export function compilePolicy(policy){
  const source=normalizeSource(policy),ir=Object.freeze({...source,version:2,policyVersion:hash(source)});
  return Object.freeze({ir,evaluate:(context)=>evaluateIr(ir,context),queryConstraint:(context)=>queryConstraint(ir,context)});
}

export function interpretPolicy(policy,context){
  const source=normalizeSource(policy);
  if(!source.actions.includes('*')&&!source.actions.includes(context.action))return false;
  if(source.tenant!==null&&context.tenant!==source.tenant)return false;
  for(const entitlement of source.requiredEntitlements)if(!Array.isArray(context.entitlements)||!context.entitlements.includes(entitlement))return false;
  const document=context.after??context.before??context.resource??{};
  return interpretWhere(source.where,document);
}

export function differentialPolicyCheck(policy,contexts){
  if(!Array.isArray(contexts))throw new TypeError('contexts must be an array');const compiled=compilePolicy(policy);
  for(const context of contexts){const optimized=compiled.evaluate(context),reference=interpretPolicy(policy,context);if(optimized!==reference)return{ok:false,context:structuredClone(context),compiled:optimized,reference};}
  return{ok:true,cases:contexts.length,policyVersion:compiled.ir.policyVersion};
}

function evaluateIr(ir,context){
  if(!ir.actions.includes('*')&&!ir.actions.includes(context.action))return false;
  if(ir.tenant!==null&&context.tenant!==ir.tenant)return false;
  for(const entitlement of ir.requiredEntitlements)if(!context.entitlements?.includes(entitlement))return false;
  return evaluateWhere(ir.where,context.after??context.before??context.resource??{});
}

function queryConstraint(ir,context){
  if(context.action!=='read'&&context.action!=='*')return null;
  if(!ir.actions.includes('*')&&!ir.actions.includes('read'))return null;
  if(ir.tenant!==null&&context.tenant!==ir.tenant)return null;
  for(const entitlement of ir.requiredEntitlements)if(!context.entitlements?.includes(entitlement))return null;
  return structuredClone(ir.where);
}

function normalizeSource(policy){
  if(!policy||typeof policy!=='object'||Array.isArray(policy))throw new TypeError('policy required');
  const actions=[...new Set(policy.actions??['*'])];if(!actions.length||actions.some((value)=>typeof value!=='string'||!value))throw new TypeError('policy actions must be non-empty strings');
  const requiredEntitlements=[...new Set(policy.requiredEntitlements??[])];if(requiredEntitlements.some((value)=>typeof value!=='string'||!value))throw new TypeError('requiredEntitlements must be strings');
  const where=structuredClone(policy.where??{});validateWhere(where);
  return Object.freeze({actions:Object.freeze(actions.sort()),tenant:policy.tenant??null,requiredEntitlements:Object.freeze(requiredEntitlements.sort()),where:Object.freeze(where)});
}

function validateWhere(where){if(!where||typeof where!=='object'||Array.isArray(where))throw new TypeError('policy where must be object');for(const[key,value]of Object.entries(where)){if(typeof key!=='string'||!key||key.length>256)throw new TypeError('invalid policy field');if(value&&typeof value==='object'&&!Array.isArray(value)){const operators=Object.keys(value);if(operators.some((operator)=>!['$eq','$ne','$in','$nin','$exists','$gt','$gte','$lt','$lte'].includes(operator)))throw new TypeError('unsupported policy operator');if('$in'in value&&!Array.isArray(value.$in))throw new TypeError('$in requires array');if('$nin'in value&&!Array.isArray(value.$nin))throw new TypeError('$nin requires array');}}}
function evaluateWhere(where,document){for(const[key,condition]of Object.entries(where)){const actual=readPath(document,key);if(!compiledCondition(condition,actual))return false;}return true;}
function compiledCondition(condition,actual){if(!condition||typeof condition!=='object'||Array.isArray(condition))return Object.is(actual,condition);if('$eq'in condition&&!Object.is(actual,condition.$eq))return false;if('$ne'in condition&&Object.is(actual,condition.$ne))return false;if('$in'in condition&&!condition.$in.some((value)=>Object.is(value,actual)))return false;if('$nin'in condition&&condition.$nin.some((value)=>Object.is(value,actual)))return false;if('$exists'in condition&&(actual!==undefined)!==Boolean(condition.$exists))return false;if('$gt'in condition&&!(actual>condition.$gt))return false;if('$gte'in condition&&!(actual>=condition.$gte))return false;if('$lt'in condition&&!(actual<condition.$lt))return false;if('$lte'in condition&&!(actual<=condition.$lte))return false;return true;}

function interpretWhere(where,document){
  for(const key of Object.keys(where)){
    const condition=where[key],actual=readPath(document,key);
    if(condition===null||typeof condition!=='object'||Array.isArray(condition)){if(!Object.is(actual,condition))return false;continue;}
    for(const operator of Object.keys(condition)){
      const expected=condition[operator];
      if(operator==='$eq'&&!Object.is(actual,expected))return false;
      if(operator==='$ne'&&Object.is(actual,expected))return false;
      if(operator==='$in'&&!expected.some((candidate)=>Object.is(candidate,actual)))return false;
      if(operator==='$nin'&&expected.some((candidate)=>Object.is(candidate,actual)))return false;
      if(operator==='$exists'&&(actual!==undefined)!==Boolean(expected))return false;
      if(operator==='$gt'&&!(actual>expected))return false;
      if(operator==='$gte'&&!(actual>=expected))return false;
      if(operator==='$lt'&&!(actual<expected))return false;
      if(operator==='$lte'&&!(actual<=expected))return false;
    }
  }
  return true;
}
function readPath(value,path){return path.split('.').reduce((current,part)=>current?.[part],value);}
function hash(value){return crypto.createHash('sha256').update(stable(value)).digest('hex');}
function stable(value){if(value===null||typeof value!=='object')return JSON.stringify(value);if(Array.isArray(value))return`[${value.map(stable).join(',')}]`;return`{${Object.keys(value).sort().map((key)=>`${JSON.stringify(key)}:${stable(value[key])}`).join(',')}}`;}
