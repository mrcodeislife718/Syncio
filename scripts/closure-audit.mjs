import fs from 'node:fs/promises';
import path from 'node:path';

const roots=['src','test','bin','scripts'];
const self=path.normalize('scripts/closure-audit.mjs');
const forbidden=[
  {name:'TODO',pattern:/\bTODO\b/},
  {name:'FIXME',pattern:/\bFIXME\b/},
  {name:'placeholder',pattern:/\bplaceholder\b/i},
  {name:'not implemented',pattern:/\bnot implemented\b/i},
  {name:'skipped node:test',pattern:/\btest\.skip\s*\(/},
  {name:'skipped test option',pattern:/\bskip\s*:\s*true\b/},
  {name:'focused test',pattern:/\b(?:test|describe|it)\.only\s*\(/},
  {name:'disabled test option',pattern:/\b(?:todo|only)\s*:\s*true\b/}
];
const findings=[];
for(const root of roots) {
  for(const file of await walk(root)) {
    if(path.normalize(file)===self||!/\.(?:js|mjs|cjs)$/.test(file))continue;
    const text=await fs.readFile(file,'utf8');
    const lines=text.split('\n');
    lines.forEach((line,index)=>{
      for(const rule of forbidden)if(rule.pattern.test(line))findings.push({file,line:index+1,rule:rule.name,text:line.trim()});
    });
  }
}
if(findings.length){console.error(JSON.stringify({ok:false,findings},null,2));process.exitCode=1;}
else console.log(JSON.stringify({ok:true,roots,checks:forbidden.map(item=>item.name)}));

async function walk(root){
  const output=[];
  let entries;
  try{entries=await fs.readdir(root,{withFileTypes:true});}catch(error){if(error.code==='ENOENT')return output;throw error;}
  for(const entry of entries){const target=path.join(root,entry.name);if(entry.isDirectory())output.push(...await walk(target));else output.push(target);}
  return output;
}
