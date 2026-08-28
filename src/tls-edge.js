import https from 'node:https';
import http from 'node:http';
import tls from 'node:tls';

export function createTlsEdge({ targetUrl, key, cert, ca, minVersion='TLSv1.2', requestTimeoutMs=30000, headersTimeoutMs=10000, observe=()=>undefined }={}) {
  if(!targetUrl)throw new TypeError('TLS edge targetUrl required');const target=new URL(targetUrl);if(!['http:','https:'].includes(target.protocol))throw new TypeError('TLS edge target must be http or https');if(!key||!cert)throw new TypeError('TLS edge key and cert required');
  let secureContext=tls.createSecureContext({key,cert,ca,minVersion});
  const server=https.createServer({SNICallback:(_servername,callback)=>callback(null,secureContext),key,cert,ca,minVersion},(req,res)=>{
    const started=process.hrtime.bigint();const transport=target.protocol==='https:'?https:http;const outgoing=transport.request({protocol:target.protocol,hostname:target.hostname,port:target.port||undefined,path:req.url,method:req.method,headers:{...req.headers,host:target.host,'x-forwarded-proto':'https','x-forwarded-host':req.headers.host??'', 'x-forwarded-for':appendForwarded(req.headers['x-forwarded-for'],req.socket.remoteAddress)},timeout:requestTimeoutMs},(upstream)=>{
      res.writeHead(upstream.statusCode??502,upstream.headers);upstream.pipe(res);upstream.on('end',()=>safeObserve(observe,{type:'tls_proxy_complete',status:upstream.statusCode,durationMs:Number(process.hrtime.bigint()-started)/1e6}));
    });
    outgoing.on('timeout',()=>outgoing.destroy(Object.assign(new Error('upstream_timeout'),{code:'upstream_timeout'})));
    outgoing.on('error',(error)=>{safeObserve(observe,{type:'tls_proxy_error',code:error.code??'proxy_error'});if(!res.headersSent)res.writeHead(error.code==='upstream_timeout'?504:502,{'content-type':'application/json'});if(!res.writableEnded)res.end(JSON.stringify({error:error.code==='upstream_timeout'?'upstream_timeout':'bad_gateway'}));});
    req.pipe(outgoing);
  });
  server.requestTimeout=requestTimeoutMs;server.headersTimeout=headersTimeoutMs;server.keepAliveTimeout=5000;
  return Object.freeze({
    server,
    async listen({host='0.0.0.0',port=443}={}){if(!Number.isSafeInteger(port)||port<0||port>65535)throw new TypeError('TLS port must be 0..65535');await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(port,host,resolve);});const address=server.address();return{host,port:address.port,url:`https://${host}:${address.port}`};},
    reload({key:newKey,cert:newCert,ca:newCa=ca,minVersion:newMinVersion=minVersion}={}){if(!newKey||!newCert)throw new TypeError('TLS reload key and cert required');const next=tls.createSecureContext({key:newKey,cert:newCert,ca:newCa,minVersion:newMinVersion});server.setSecureContext({key:newKey,cert:newCert,ca:newCa,minVersion:newMinVersion});secureContext=next;return true;},
    async close(){if(server.listening)await new Promise((resolve,reject)=>server.close((error)=>error?reject(error):resolve()));}
  });
}

function appendForwarded(existing,address){const value=address??'unknown';return existing?`${existing}, ${value}`:value;}
function safeObserve(observe,event){try{observe(event);}catch{}}
