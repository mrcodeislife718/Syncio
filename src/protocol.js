import crypto from 'node:crypto';

const SUPPORTED=Object.freeze({query:[1],commit:[1],sync:[1]});
const clone=(v)=>structuredClone(v);

export function negotiateProtocols(remote){if(!remote||typeof remote!=='object')throw new TypeError('remote protocol capabilities required');const selected={};for(const [name,versions]of Object.entries(SUPPORTED)){const offered=Array.isArray(remote[name])?remote[name]:[];const mutual=versions.filter(v=>offered.includes(v)).sort((a,b)=>b-a);if(!mutual.length)throw protocolError(`no compatible ${name} protocol`);selected[name]=mutual[0];}return Object.freeze(selected);}
export function localProtocolCapabilities(){return clone(SUPPORTED);}

export function compileQueryIR({collection,where={},projection=null,orderBy=null,limit=null,offset=0,policyConstraints={}}={}){if(typeof collection!=='string'||!collection)throw new TypeError('query collection required');const merged=mergeWhere(where,policyConstraints);const ir={protocol:'syncio-query',version:1,collection,where:clone(merged),projection:projection?clone(projection):null,orderBy:orderBy?clone(orderBy):null,limit:limit??null,offset};return Object.freeze({...ir,digest:digest(ir)});}
export function verifyQueryIR(ir){if(ir?.protocol!=='syncio-query'||ir.version!==1)return false;const {digest:given,...body}=ir;return typeof given==='string'&&timingEqual(given,digest(body));}
export function encodeCommitProtocol(commit){if(!commit?.commitId||!Number.isSafeInteger(commit.sequence)||!Array.isArray(commit.mutations))throw new TypeError('invalid commit');const packet={protocol:'syncio-commit',version:1,commit:clone(commit)};return Object.freeze({...packet,digest:digest(packet)});}
export function decodeCommitProtocol(packet){if(packet?.protocol!=='syncio-commit'||packet.version!==1)throw protocolError('unsupported commit protocol');const {digest:given,...body}=packet;if(!timingEqual(given,digest(body)))throw protocolError('commit packet integrity failure');return clone(packet.commit);}
export function encodeSyncProtocol({view,cursor,sequence,changes=[],snapshot=null}={}){if(typeof view!=='string'||!Number.isSafeInteger(cursor)||!Number.isSafeInteger(sequence))throw new TypeError('invalid sync packet');const packet={protocol:'syncio-sync',version:1,view,cursor,sequence,changes:clone(changes),snapshot:snapshot?clone(snapshot):null};return Object.freeze({...packet,digest:digest(packet)});}
export function decodeSyncProtocol(packet){if(packet?.protocol!=='syncio-sync'||packet.version!==1)throw protocolError('unsupported sync protocol');const {digest:given,...body}=packet;if(!timingEqual(given,digest(body)))throw protocolError('sync packet integrity failure');return clone(body);}
export function migrateProtocolPacket(packet,targetVersion=1){if(targetVersion!==1)throw protocolError('unsupported target protocol version');if(!packet||typeof packet!=='object')throw new TypeError('packet required');return clone(packet);}

function mergeWhere(a,b){const left=a&&Object.keys(a).length?a:null,right=b&&Object.keys(b).length?b:null;if(left&&right)return{$and:[clone(left),clone(right)]};return clone(left??right??{});}
function canonical(value){if(Array.isArray(value))return value.map(canonical);if(value&&typeof value==='object'){const out={};for(const key of Object.keys(value).sort())out[key]=canonical(value[key]);return out;}return value;}
function digest(value){return crypto.createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');}
function timingEqual(a,b){if(typeof a!=='string'||typeof b!=='string')return false;const x=Buffer.from(a),y=Buffer.from(b);return x.length===y.length&&crypto.timingSafeEqual(x,y);}
function protocolError(message){const e=new Error(message);e.code='SYNCIO_PROTOCOL_ERROR';return e;}
