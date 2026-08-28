import fs from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';

const clone=(value)=>structuredClone(value);

export class ReleaseRegistry {
  constructor(file,state){this.file=path.resolve(file);this.state=state;this.queue=Promise.resolve();}
  static async open(file){const target=path.resolve(file);let state={version:1,releases:{},current:null,history:[]};try{state=JSON.parse(await fs.readFile(target,'utf8'));}catch(error){if(error.code!=='ENOENT')throw error;}validateState(state);return new ReleaseRegistry(target,state);}
  list(){return Object.values(this.state.releases).map(clone).sort((a,b)=>a.createdAt.localeCompare(b.createdAt));}
  current(){return this.state.current?clone(this.state.releases[this.state.current]):null;}
  async register({version,artifactDigest,sourceCommit,metadata={}}={}){validateVersion(version);validateDigest(artifactDigest);if(typeof sourceCommit!=='string'||!/^[0-9a-f]{7,64}$/i.test(sourceCommit))throw new TypeError('sourceCommit must be commit hash');return this.#mutate((draft)=>{const existing=draft.releases[version];const record={version,artifactDigest:artifactDigest.toLowerCase(),sourceCommit,metadata:clone(metadata),createdAt:existing?.createdAt??new Date().toISOString()};if(existing&&(existing.artifactDigest!==record.artifactDigest||existing.sourceCommit!==record.sourceCommit))throw releaseError('SYNCIO_RELEASE_IMMUTABLE','release version already registered with different artifact');draft.releases[version]=record;return clone(record);});}
  async promote(version,{healthProof}={}){if(!this.state.releases[version])throw releaseError('SYNCIO_RELEASE_NOT_FOUND','release not found');if(!healthProof||healthProof.ok!==true)throw releaseError('SYNCIO_RELEASE_UNQUALIFIED','health proof required before promotion');return this.#mutate((draft)=>{const previous=draft.current;draft.current=version;draft.history.push({id:crypto.randomUUID(),type:'promote',from:previous,to:version,at:new Date().toISOString(),proof:clone(healthProof)});return{from:previous,to:version};});}
  async rollback({toVersion,reason='operator'}={}){const target=toVersion??this.#previousVersion();if(!target||!this.state.releases[target])throw releaseError('SYNCIO_ROLLBACK_TARGET_NOT_FOUND','rollback target not found');return this.#mutate((draft)=>{const from=draft.current;draft.current=target;draft.history.push({id:crypto.randomUUID(),type:'rollback',from,to:target,reason,at:new Date().toISOString()});return{from,to:target};});}
  verifyArtifact(version,bytes){const release=this.state.releases[version];if(!release)throw releaseError('SYNCIO_RELEASE_NOT_FOUND','release not found');const buffer=Buffer.isBuffer(bytes)?bytes:Buffer.from(bytes);const digest=crypto.createHash('sha256').update(buffer).digest('hex');return{ok:digest===release.artifactDigest,expected:release.artifactDigest,actual:digest};}
  history(){return clone(this.state.history);}
  #previousVersion(){for(let i=this.state.history.length-1;i>=0;i--){const entry=this.state.history[i];if(entry.from&&entry.from!==this.state.current)return entry.from;}return null;}
  async #mutate(work){const operation=this.queue.then(async()=>{const draft=clone(this.state);const result=work(draft);await atomicWriteJson(this.file,draft);this.state=draft;return result;});this.queue=operation.catch(()=>undefined);return operation;}
}

export class RollingDeploymentController{
  constructor({registry,start,stop,health}={}){if(!registry||typeof registry.promote!=='function'||typeof start!=='function'||typeof stop!=='function'||typeof health!=='function')throw new TypeError('deployment controller requires registry start stop health');this.registry=registry;this.start=start;this.stop=stop;this.health=health;this.active=null;}
  async deploy(version){const previous=this.active;const candidate=await this.start(version);let proof;try{proof=await this.health(candidate,version);if(!proof?.ok)throw releaseError('SYNCIO_DEPLOY_HEALTH_FAILED','candidate health check failed');await this.registry.promote(version,{healthProof:proof});this.active=candidate;if(previous)await this.stop(previous);return{version,proof};}catch(error){await this.stop(candidate).catch(()=>undefined);throw error;}}
  async rollback(toVersion){const target=toVersion??this.registry.history().filter((e)=>e.type==='promote').at(-2)?.to;if(!target)throw releaseError('SYNCIO_ROLLBACK_TARGET_NOT_FOUND','rollback target not found');const previous=this.active;const candidate=await this.start(target);const proof=await this.health(candidate,target);if(!proof?.ok){await this.stop(candidate).catch(()=>undefined);throw releaseError('SYNCIO_DEPLOY_HEALTH_FAILED','rollback candidate health check failed');}await this.registry.rollback({toVersion:target,reason:'deployment_rollback'});this.active=candidate;if(previous)await this.stop(previous);return{version:target,proof};}
}

function validateState(state){if(!state||state.version!==1||!state.releases||typeof state.releases!=='object'||!Array.isArray(state.history))throw new Error('invalid release registry');}
function validateVersion(value){if(typeof value!=='string'||!/^[A-Za-z0-9_.+-]{1,128}$/.test(value))throw new TypeError('invalid release version');}
function validateDigest(value){if(typeof value!=='string'||!/^[0-9a-f]{64}$/i.test(value))throw new TypeError('artifactDigest must be sha256 hex');}
function releaseError(code,message){const error=new Error(message);error.code=code;return error;}
async function atomicWriteJson(target,value){await fs.mkdir(path.dirname(target),{recursive:true});const temp=`${target}.${process.pid}.${crypto.randomUUID()}.tmp`;try{await fs.writeFile(temp,`${JSON.stringify(value,null,2)}\n`,{encoding:'utf8',mode:0o600});const handle=await fs.open(temp,'r');try{await handle.sync();}finally{await handle.close();}await fs.rename(temp,target);const dir=await fs.open(path.dirname(target),'r');try{await dir.sync();}finally{await dir.close();}}finally{await fs.rm(temp,{force:true}).catch(()=>undefined);}}
