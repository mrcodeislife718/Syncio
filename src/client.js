import { setTimeout as sleep } from 'node:timers/promises';

export class SyncioClient {
  constructor({ baseUrl, token, tokenProvider, fetchImpl = globalThis.fetch, retry = { attempts: 4, baseDelayMs: 50, maxDelayMs: 1000 } } = {}) {
    if (!baseUrl) throw new TypeError('SyncioClient requires baseUrl');
    if (typeof fetchImpl !== 'function') throw new TypeError('SyncioClient requires fetch');
    this.baseUrl = String(baseUrl).replace(/\/$/, '');
    this.token = token;
    this.tokenProvider = tokenProvider;
    this.fetch = fetchImpl;
    this.retry = normalizeRetry(retry);
  }

  collection(name) {
    const client = this; const encoded = encodeURIComponent(name);
    return Object.freeze({
      async get(id) { const response = await client.request(`/collections/${encoded}/${encodeURIComponent(id)}`); if (response.status === 404) return null; return response.json(); },
      async query(spec = {}) { const params = encodeQuery(spec); const response = await client.request(`/collections/${encoded}${params ? `?${params}` : ''}`); return (await response.json()).records; },
      async insert(record) { const response = await client.request(`/collections/${encoded}`, { method:'POST', json:record }); return response.json(); },
      async upsert(id, record) { const response = await client.request(`/collections/${encoded}/${encodeURIComponent(id)}`, { method:'PUT', json:record }); return response.json(); },
      async update(id, update) { const response = await client.request(`/collections/${encoded}/${encodeURIComponent(id)}`, { method:'PATCH', json:update }); if(response.status===404)return null; return response.json(); },
      async remove(id) { const response = await client.request(`/collections/${encoded}/${encodeURIComponent(id)}`, { method:'DELETE' }); return response.status !== 404; },
      async aggregate(pipeline) { const response = await client.request(`/collections/${encoded}/aggregate`, { method:'POST', json:{pipeline} }); return (await response.json()).records; },
      watch(options = {}) { return client.watch(name, options); }
    });
  }

  async health() { const response = await this.request('/health', { auth:false }); return response.json(); }

  async request(pathname, { method='GET', json, headers={}, auth=true, retry=true } = {}) {
    const body = json === undefined ? undefined : JSON.stringify(json);
    const attempts = retry ? this.retry.attempts : 1;
    let lastError;
    for (let attempt=1; attempt<=attempts; attempt++) {
      try {
        const token = auth ? await this.#token() : null;
        const response = await this.fetch(`${this.baseUrl}${pathname}`, { method, headers:{ ...(body?{'content-type':'application/json'}:{}), ...(token?{authorization:`Bearer ${token}`}:{ }), ...headers }, body });
        if (response.ok || response.status === 404) return response;
        const problem = await safeJson(response);
        const error = new SyncioHttpError(response.status, problem?.error ?? 'request_failed', problem?.requestId, problem);
        if (!shouldRetry(response.status) || attempt === attempts) throw error;
        lastError = error;
      } catch (error) {
        lastError = error;
        if (error instanceof SyncioHttpError && !shouldRetry(error.status)) throw error;
        if (attempt === attempts) throw error;
      }
      await sleep(backoff(this.retry, attempt));
    }
    throw lastError;
  }

  watch(collection, { after = 0, signal, reconnect = true, onError, maxReconnects = Infinity } = {}) {
    if (!Number.isSafeInteger(after) || after < 0) throw new TypeError('watch after must be non-negative safe integer');
    const controller = new AbortController();
    if (signal) { if(signal.aborted)controller.abort(signal.reason); else signal.addEventListener('abort',()=>controller.abort(signal.reason),{once:true}); }
    const client=this;
    let cursor=after; let reconnects=0;
    async function* stream() {
      while(!controller.signal.aborted) {
        try {
          const token=await client.#token();
          const response=await client.fetch(`${client.baseUrl}/subscribe/${encodeURIComponent(collection)}?after=${cursor}`,{headers:{accept:'text/event-stream',...(token?{authorization:`Bearer ${token}`}:{})},signal:controller.signal});
          if(response.status===409){const problem=await safeJson(response);throw new SyncioResumeError(problem?.error??'stream_resume_failed',problem);}
          if(!response.ok)throw new SyncioHttpError(response.status,'stream_open_failed',response.headers.get('x-syncio-request-id'));
          for await(const event of parseSse(response.body)){
            if(event.id!==null){const next=Number(event.id);if(Number.isSafeInteger(next)&&next>=cursor)cursor=next;}
            if(event.event==='change'&&event.data)yield JSON.parse(event.data);
          }
          if(!reconnect)break;
        } catch(error) {
          if(controller.signal.aborted)break;
          onError?.(error);
          if(error instanceof SyncioResumeError || !reconnect || reconnects>=maxReconnects)throw error;
        }
        reconnects+=1;await sleep(backoff(client.retry,Math.min(reconnects,10)),undefined,{signal:controller.signal}).catch(()=>undefined);
      }
    }
    const iterator=stream();
    return Object.freeze({
      [Symbol.asyncIterator](){return iterator;},
      get cursor(){return cursor;},
      close(){controller.abort();return iterator.return?.();}
    });
  }

  async #token() { const value = this.tokenProvider ? await this.tokenProvider() : this.token; if(value!==undefined&&value!==null&&typeof value!=='string')throw new TypeError('token provider must return string'); return value ?? null; }
}

export class SyncioHttpError extends Error { constructor(status,code,requestId,details){super(`Syncio request failed (${status} ${code})`);this.name='SyncioHttpError';this.status=status;this.code=code;this.requestId=requestId;this.details=details;} }
export class SyncioResumeError extends Error { constructor(code,details){super(`Syncio stream cannot resume: ${code}`);this.name='SyncioResumeError';this.code=code;this.details=details;} }

function encodeQuery(spec){const params=new URLSearchParams();if(spec.where)params.set('where',JSON.stringify(spec.where));if(spec.projection)params.set('projection',JSON.stringify(spec.projection));if(spec.orderBy)params.set('orderBy',JSON.stringify(spec.orderBy));if(spec.limit!==undefined)params.set('limit',String(spec.limit));if(spec.offset!==undefined)params.set('offset',String(spec.offset));return params.toString();}
function normalizeRetry(value){const attempts=value?.attempts??4,baseDelayMs=value?.baseDelayMs??50,maxDelayMs=value?.maxDelayMs??1000;if(!Number.isSafeInteger(attempts)||attempts<1||!Number.isFinite(baseDelayMs)||baseDelayMs<0||!Number.isFinite(maxDelayMs)||maxDelayMs<baseDelayMs)throw new TypeError('invalid retry configuration');return{attempts,baseDelayMs,maxDelayMs};}
function backoff(config,attempt){return Math.min(config.maxDelayMs,config.baseDelayMs*2**Math.max(0,attempt-1));}
function shouldRetry(status){return status===408||status===425||status===429||status>=500;}
async function safeJson(response){try{return await response.json();}catch{return null;}}
async function* parseSse(body){if(!body)throw new Error('SSE response has no body');const decoder=new TextDecoder();let buffer='';for await(const chunk of body){buffer+=decoder.decode(chunk,{stream:true});while(true){const index=buffer.indexOf('\n\n');if(index<0)break;const block=buffer.slice(0,index).replace(/\r/g,'');buffer=buffer.slice(index+2);const event=parseSseBlock(block);if(event)yield event;}}buffer+=decoder.decode();if(buffer.trim()){const event=parseSseBlock(buffer.replace(/\r/g,''));if(event)yield event;}}
function parseSseBlock(block){let id=null,event='message';const data=[];for(const line of block.split('\n')){if(!line||line.startsWith(':'))continue;const colon=line.indexOf(':');const field=colon<0?line:line.slice(0,colon);let value=colon<0?'':line.slice(colon+1);if(value.startsWith(' '))value=value.slice(1);if(field==='id')id=value;else if(field==='event')event=value;else if(field==='data')data.push(value);}if(id===null&&!data.length&&event==='message')return null;return{id,event,data:data.join('\n')};}
