import { compilePolicy } from './policy-compiler.js';

const clone=(value)=>structuredClone(value);
const UPDATE_OPERATORS=new Set(['$set','$unset','$inc','$mul','$min','$max','$push','$addToSet','$pull','$rename']);

export function createResourcePolicy(rules=[]){
  const normalized=rules.map((rule,index)=>normalizeRule(rule,index));
  return Object.freeze({
    hasScope(context){return decision(normalized,context).allowed;},
    authorize(context){return decision(normalized,context).allowed;},
    queryConstraint(context){return safeQueryConstraint(normalized,context);},
    read(context,record){
      if(!record)return null;const result=decision(normalized,{...context,record});if(!result.allowed)return null;
      const fieldRules=result.matches.filter((rule)=>rule.effect==='allow'&&rule.readFields);
      const denied=new Set(result.matches.flatMap((rule)=>rule.denyReadFields??[]));
      if(!fieldRules.length&&!denied.size)return clone(record);
      const allowed=fieldRules.length?new Set(fieldRules.flatMap((rule)=>rule.readFields)):null;
      const output={};
      for(const [key,value] of Object.entries(record)){
        if(key==='id'){output.id=clone(value);continue;}
        if(denied.has(key))continue;
        if(allowed&&!allowed.has(key))continue;
        output[key]=clone(value);
      }
      return output;
    },
    write(context,body){
      const result=decision(normalized,{...context,body});if(!result.allowed)return{allowed:false,code:'forbidden'};
      const allowRules=result.matches.filter((rule)=>rule.effect==='allow'&&rule.writeFields);if(!allowRules.length)return{allowed:true};
      const allowed=new Set(allowRules.flatMap((rule)=>rule.writeFields));
      for(const field of mutationFields(body))if(field!=='id'&&!allowed.has(field))return{allowed:false,code:'field_forbidden',field};
      return{allowed:true};
    }
  });
}

function decision(rules,context){let allowed=false;const matches=[];for(const rule of rules){if(!matchesScope(rule,context))continue;if(rule.when){let yes=false;try{yes=Boolean(rule.when(context));}catch{yes=false;}if(!yes)continue;}if(rule.compiledWhere){const resource=context.record??context.body??context.resource??{};if(!rule.compiledWhere.evaluate({action:context.action,resource,user:context.user,entitlements:context.user?.entitlements??[]}))continue;}matches.push(rule);if(rule.effect==='deny')return{allowed:false,matches};allowed=true;}return{allowed,matches};}
function safeQueryConstraint(rules,context){const candidates=rules.filter(rule=>rule.effect==='allow'&&matchesScope(rule,context));if(!candidates.length)return null;if(candidates.some(rule=>typeof rule.when==='function'))return null;const constraints=[];for(const rule of candidates){if(!rule.where||!Object.keys(rule.where).length)return{};constraints.push(clone(rule.where));}if(!constraints.length)return null;if(constraints.length===1)return constraints[0];return{$or:constraints};}
function matchesScope(rule,context){if(rule.collection&&rule.collection!=='*'&&rule.collection!==context.collection){if(!(context.collection==='*'&&rule.effect==='deny'))return false;}if(rule.action&&rule.action!=='*'&&rule.action!==context.action)return false;return true;}
function normalizeRule(rule,index){if(!rule||typeof rule!=='object'||Array.isArray(rule))throw new TypeError(`policy rule ${index} must be object`);const effect=rule.effect??'deny';if(!['allow','deny'].includes(effect))throw new TypeError(`policy rule ${index} has invalid effect`);const where=rule.where===undefined?null:clone(rule.where);const compiledWhere=where?compilePolicy({actions:[rule.action??'*'],where}):null;return{...rule,effect,where,compiledWhere,readFields:normalizeFields(rule.readFields),denyReadFields:normalizeFields(rule.denyReadFields),writeFields:normalizeFields(rule.writeFields)};}
function normalizeFields(value){if(value===undefined)return null;if(!Array.isArray(value)||value.some((item)=>typeof item!=='string'||!/^[A-Za-z0-9_-]+$/.test(item)))throw new TypeError('policy fields must be top-level field names');return[...new Set(value)];}
function mutationFields(body){if(!body||typeof body!=='object'||Array.isArray(body))return[];const keys=Object.keys(body);if(keys.some((key)=>UPDATE_OPERATORS.has(key))){const result=new Set();for(const [operator,payload]of Object.entries(body)){if(!UPDATE_OPERATORS.has(operator)||!payload||typeof payload!=='object'||Array.isArray(payload))continue;for(const key of Object.keys(payload)){result.add(topField(key));if(operator==='$rename')result.add(topField(payload[key]));}}return[...result];}return keys.map(topField);}
function topField(value){return String(value).split('.')[0];}
