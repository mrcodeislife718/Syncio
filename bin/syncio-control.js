#!/usr/bin/env node
import { SyncioControlPlane } from '../src/control-plane.js';
import { createHostedControlServer } from '../src/hosted-control.js';

const controlFile=process.env.SYNCIO_CONTROL_FILE??'./data/control.syncio.json';
const storageRoot=process.env.SYNCIO_STORAGE_ROOT??'./data/projects';
const tokenSecret=process.env.SYNCIO_TOKEN_SECRET;
const sessionSecret=process.env.SYNCIO_SESSION_SECRET;
const billingWebhookSecret=process.env.SYNCIO_BILLING_WEBHOOK_SECRET;
const host=process.env.SYNCIO_CONTROL_HOST??'0.0.0.0';
const port=Number(process.env.SYNCIO_CONTROL_PORT??8788);

const secrets=[['SYNCIO_TOKEN_SECRET',tokenSecret],['SYNCIO_SESSION_SECRET',sessionSecret],['SYNCIO_BILLING_WEBHOOK_SECRET',billingWebhookSecret]];
const invalid=secrets.find(([,value])=>!value||Buffer.byteLength(value)<32);
if(invalid){console.error(`${invalid[0]} must contain at least 32 bytes`);process.exitCode=64;}
else if(!Number.isSafeInteger(port)||port<1||port>65535){console.error('SYNCIO_CONTROL_PORT must be an integer between 1 and 65535');process.exitCode=64;}
else{
  const control=await SyncioControlPlane.open(controlFile,{tokenSecret,storageRoot});
  const service=createHostedControlServer({controlPlane:control,sessionSecret,billingWebhookSecret});
  const address=await service.listen({host,port});
  console.log(JSON.stringify({event:'syncio.control.ready',url:address.url,pid:process.pid}));
  const shutdown=async(signal)=>{
    try{await service.close();await control.close();console.log(JSON.stringify({event:'syncio.control.stopped',signal}));process.exit(0);}
    catch(error){console.error(error);process.exit(1);}
  };
  process.once('SIGTERM',()=>void shutdown('SIGTERM'));
  process.once('SIGINT',()=>void shutdown('SIGINT'));
}
