const clone=(value)=>structuredClone(value);

export function createResourcePolicy(rules=[]){
  const normalized=rules.map((rule,index)=>normalizeRule(rule,index));
  return Object.freeze({
    authorize(context){return decision(normalized,context).allowed;},
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
      const allowed=new Set(allowRules.flatMap((rule)=>rule.writeFields));for(const key of Object.keys(body??{}))if(key!=='id'&&!allowed.has(topField(key)))return{allowed:false,code:'field_forbidden',field:topField(key)};
      return{allowed:true};
    }
  });
}

function decision(rules,context){let allowed=false;const matches=[];for(const rule of rules){if(!matchesScope(rule,context))continue;if(typeof rule.when==='function'&&!rule.when(context))continue;matches.push(rule);if(rule.effect==='deny')return{allowed:false,matches};allowed=true;}return{allowed,matches};}
function matchesScope(rule,context){if(rule.collection&&rule.collection!=='*'&&rule.collection!==context.collection)return false;if(rule.action&&rule.action!=='*'&&rule.action!==context.action)return false;return true;}
function normalizeRule(rule,index){if(!rule||typeof rule!=='object'||Array.isArray(rule))throw new TypeError(`policy rule ${index} must be object`);const effect=rule.effect??'deny';if(!['allow','deny'].includes(effect))throw new TypeError(`policy rule ${index} has invalid effect`);return{...rule,effect,readFields:normalizeFields(rule.readFields),denyReadFields:normalizeFields(rule.denyReadFields),writeFields:normalizeFields(rule.writeFields)};}
function normalizeFields(value){if(value===undefined)return null;if(!Array.isArray(value)||value.some((item)=>typeof item!=='string'||!/^[A-Za-z0-9_-]+$/.test(item)))throw new TypeError('policy fields must be top-level field names');return[...new Set(value)];}
function topField(value){return String(value).split('.')[0];}
