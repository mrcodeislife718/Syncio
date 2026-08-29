import crypto from 'node:crypto';

const clone=(value)=>structuredClone(value);

export class RotatingTokenAuthority {
  constructor({ keys, activeKeyId, issuer='syncio', ttlSeconds=3600, now=()=>Date.now() }={}) {
    if(!keys||typeof keys!=='object'||Array.isArray(keys)||!Object.keys(keys).length)throw new TypeError('token keyring requires keys');
    this.keys=new Map();for(const [id,value] of Object.entries(keys))this.addKey(id,value);
    if(!this.keys.has(activeKeyId))throw new TypeError('activeKeyId must reference configured key');
    if(!Number.isSafeInteger(ttlSeconds)||ttlSeconds<1)throw new TypeError('ttlSeconds must be positive');
    this.activeKeyId=activeKeyId;this.issuer=issuer;this.ttlSeconds=ttlSeconds;this.now=now;
    this.revokedJtis=new Map();this.revokedSubjects=new Map();this.revokedBefore=0;
  }
  addKey(id,secret){validateKeyId(id);const key=Buffer.isBuffer(secret)?Buffer.from(secret):Buffer.from(String(secret??''));if(key.length<32)throw new TypeError('token key must contain at least 32 bytes');this.keys.set(id,key);return this;}
  rotate(id,secret){this.addKey(id,secret);this.activeKeyId=id;return id;}
  retire(id){if(id===this.activeKeyId)throw new Error('cannot retire active token key');return this.keys.delete(id);}
  revokeToken(jti,{expiresAt=Infinity}={}){if(typeof jti!=='string'||!jti)throw new TypeError('jti required');this.revokedJtis.set(jti,expiresAt);return true;}
  revokeSubject(subject,{before=Math.floor(this.now()/1000)}={}){if(typeof subject!=='string'||!subject)throw new TypeError('subject required');if(!Number.isSafeInteger(before)||before<0)throw new TypeError('before must be unix seconds');this.revokedSubjects.set(subject,before);return true;}
  revokeAll({before=Math.floor(this.now()/1000)}={}){if(!Number.isSafeInteger(before)||before<0)throw new TypeError('before must be unix seconds');this.revokedBefore=before;return before;}
  issue({subject,projectId,role='member',entitlements=[],expiresInSeconds=this.ttlSeconds,claims={}}={}){
    if(!subject||!projectId)throw new TypeError('subject and projectId required');if(!Number.isSafeInteger(expiresInSeconds)||expiresInSeconds<1)throw new TypeError('expiresInSeconds must be positive');
    const now=Math.floor(this.now()/1000);const payload={...clone(claims),iss:this.issuer,sub:subject,projectId,role,entitlements:[...new Set(entitlements)],iat:now,exp:now+expiresInSeconds,jti:crypto.randomUUID()};
    const header={alg:'HS256',typ:'SYNCIO',kid:this.activeKeyId};const encodedHeader=base64url(JSON.stringify(header));const encodedPayload=base64url(JSON.stringify(payload));const signingInput=`${encodedHeader}.${encodedPayload}`;const signature=crypto.createHmac('sha256',this.keys.get(this.activeKeyId)).update(signingInput).digest('base64url');return `${signingInput}.${signature}`;
  }
  verify(token){
    if(typeof token!=='string'||token.length>16384)return null;const parts=token.split('.');if(parts.length!==3)return null;const [encodedHeader,encodedPayload,signature]=parts;
    let header,payload;try{header=parseCanonical(encodedHeader);payload=parseCanonical(encodedPayload);}catch{return null;}
    if(header.alg!=='HS256'||header.typ!=='SYNCIO'||!this.keys.has(header.kid))return null;
    const expected=crypto.createHmac('sha256',this.keys.get(header.kid)).update(`${encodedHeader}.${encodedPayload}`).digest();let supplied;try{supplied=Buffer.from(signature,'base64url');}catch{return null;}if(supplied.toString('base64url')!==signature||supplied.length!==expected.length||!crypto.timingSafeEqual(supplied,expected))return null;
    const now=Math.floor(this.now()/1000);this.#prune(now);if(payload.iss!==this.issuer||!payload.sub||!payload.projectId||!payload.jti||!Number.isSafeInteger(payload.iat)||!Number.isSafeInteger(payload.exp)||payload.exp<=now)return null;
    if(payload.iat<=this.revokedBefore)return null;const subjectBefore=this.revokedSubjects.get(payload.sub);if(subjectBefore!==undefined&&payload.iat<=subjectBefore)return null;if(this.revokedJtis.has(payload.jti))return null;
    return clone({...payload,kid:header.kid});
  }
  authenticateRequest(req){const header=req?.headers?.authorization;if(typeof header!=='string'||!header.startsWith('Bearer '))return null;return this.verify(header.slice(7));}
  snapshot(){return{activeKeyId:this.activeKeyId,keyIds:[...this.keys.keys()].sort(),revokedTokens:this.revokedJtis.size,revokedSubjects:this.revokedSubjects.size,revokedBefore:this.revokedBefore};}
  #prune(now){for(const [jti,expires] of this.revokedJtis)if(Number.isFinite(expires)&&expires<=now)this.revokedJtis.delete(jti);}
}

export function createSecretProvider({load,refreshIntervalMs=300000}={}){
  if(typeof load!=='function')throw new TypeError('secret provider requires load function');if(!Number.isFinite(refreshIntervalMs)||refreshIntervalMs<1000)throw new TypeError('refreshIntervalMs must be at least 1000');let cached=null,loadedAt=0,inFlight=null;
  return Object.freeze({async get({force=false}={}){const now=Date.now();if(!force&&cached&&now-loadedAt<refreshIntervalMs)return cloneSecret(cached);if(!inFlight)inFlight=Promise.resolve(load()).then((value)=>{validateSecretBundle(value);cached=value;loadedAt=Date.now();return value;}).finally(()=>{inFlight=null;});return cloneSecret(await inFlight);},invalidate(){loadedAt=0;}});
}
function validateKeyId(id){if(typeof id!=='string'||!/^[A-Za-z0-9_.-]{1,128}$/.test(id))throw new TypeError('invalid key id');}
function base64url(value){return Buffer.from(value).toString('base64url');}
function parseCanonical(value){const decoded=Buffer.from(value,'base64url');if(decoded.toString('base64url')!==value)throw new Error('noncanonical');return JSON.parse(decoded.toString('utf8'));}
function validateSecretBundle(value){if(!value||typeof value!=='object'||Array.isArray(value))throw new TypeError('secret provider returned invalid bundle');}
function cloneSecret(value){if(Buffer.isBuffer(value))return Buffer.from(value);if(value&&typeof value==='object'){const output=Array.isArray(value)?[]:{};for(const [k,v] of Object.entries(value))output[k]=cloneSecret(v);return output;}return value;}
