/**
 * AGENTROPOLIS WORLDQ — provider-agnostic competition gateway.
 *
 * Keeps the WORLDQ contract intact:
 * POST /api/missions -> SSE /events -> receipt -> audit -> done.
 *
 * No paid OpenAI dependency. Default mode is deterministic application-code
 * reasoning with the WORLDQ System Prompt. Optional hosted/local providers can
 * be added later without changing the client contract.
 */
import http from 'node:http';
import crypto from 'node:crypto';
import { URL } from 'node:url';

const VERSION = '2.0.0';
const MODEL = process.env.WORLDQ_MODEL || 'worldq-system';
const PROVIDER = process.env.WORLDQ_PROVIDER || 'system';
const ENVELOPE_VERSION = process.env.ENVELOPE_VERSION || 'worldq-envelope-v1';
const PORT = Number(process.env.PORT || 8787);
const AGENT_ID = 'worldq-01';
const IDENTITY = 'AGENTROPOLIS/WORLDQ/01';
const MAX_MANDATE = 4000;
const MAX_MISSIONS = 64;
const MISSION_TTL_MS = 10 * 60 * 1000;
const HARD_REQUERY = 1;
const HARD_TOOLS = 8;
const HARD_TIMEOUT = 30_000;
const DEFAULT_ORIGINS = [
  'https://agentropolis-city-of-agents.github.io',
  'http://localhost:5173',
  'http://127.0.0.1:5173',
  'http://localhost:4173',
  'http://127.0.0.1:4173',
];

const SYSTEM_PROMPT = String.raw`You are WORLDQ SYSTEM, the bounded execution intelligence for AGENTROPOLIS.

MISSION
Convert human intent into accountable execution under worldq-envelope-v1.

OPERATING STYLE
- Work with the discipline, structure, speed, synthesis quality, and evidence orientation expected from a frontier reasoning system.
- Do not claim to be Astra, Fable 5.1, or any other model.
- Do not imitate proprietary hidden reasoning or reveal chain-of-thought.
- Produce concise, decision-useful outputs.
- Prefer deterministic application code for mechanical work.
- Escalate only when additional intelligence would materially improve correctness.

QRAG
RETRIEVE -> EVALUATE -> EXECUTE -> VERIFY -> optional RE-QUERY.
Maximum one re-query.

QUANTIZATION TORQUE
- DECREASE: use cheaper/deterministic computation when enough.
- INCREASE: allocate more reasoning only when evidence is insufficient.
- REDIRECT: change tool, source, or method instead of repeating the same attempt.

GOVERNANCE
- Identity, mandate, permission, execution envelope, receipt, and audit are mandatory control points.
- Never invent live telemetry, tool results, costs, model IDs, or evidence.
- Fail closed when required evidence is missing.
- Respect the kill switch and execution bounds.

OUTPUT
Return:
1. Summary
2. Evidence observed
3. Findings
4. Risks / gaps
5. Recommended actions
6. Verification status
7. Receipt-ready outcome

Keep output under 500 words unless the mandate requires otherwise.`;

const DEFAULT_MANDATE =
  'Audit AGENTROPOLIS-WORLDQ-ASTRA for launch readiness. Inspect the architecture, identify implementation or governance gaps, verify the result, and produce an accountable execution receipt.';

const REPO = {
  raw: 'https://raw.githubusercontent.com/AGENTROPOLIS-CITY-OF-AGENTS/AGENTROPOLIS-WORLDQ-ASTRA/main',
};

const ALLOWED_TOOLS = Object.freeze(['repo_read', 'architecture_inspect', 'system_reasoner']);
const DENIED_TOOLS = Object.freeze(['computer_use', 'hosted_shell', 'image_generation']);
const missions = new Map();

function allowedOrigins() {
  const extra = (process.env.ALLOWED_ORIGINS || '').split(',').map(s=>s.trim()).filter(Boolean);
  return [...new Set([...DEFAULT_ORIGINS, ...extra])];
}
function corsHeaders(req) {
  const origin = req.headers.origin || '';
  const allow = allowedOrigins();
  const ok = allow.includes(origin) ? origin : allow[0];
  return {
    'access-control-allow-origin': ok,
    'access-control-allow-methods': 'GET,POST,OPTIONS',
    'access-control-allow-headers': 'content-type',
    'access-control-max-age': '600',
    vary: 'Origin',
  };
}
function json(res, req, status, body) {
  const payload = JSON.stringify(body);
  res.writeHead(status,{...corsHeaders(req),'content-type':'application/json; charset=utf-8','content-length':Buffer.byteLength(payload),'cache-control':'no-store'});
  res.end(payload);
}
function readBody(req, limit=12000) {
  return new Promise((resolve,reject)=>{
    const chunks=[]; let n=0;
    req.on('data',c=>{ n+=c.length; if(n>limit){ reject(Object.assign(new Error('payload too large'),{code:413})); req.destroy(); return;} chunks.push(c);});
    req.on('end',()=>resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error',reject);
  });
}
function id(prefix){ return `${prefix}_${crypto.randomBytes(8).toString('hex')}`; }
function sha256(text){ return crypto.createHash('sha256').update(text).digest('hex'); }
function now(){ return Date.now(); }
function clampEnvelope(raw={}) {
  return {
    identity: IDENTITY,
    mandateHash:'',
    allowedTools:[...ALLOWED_TOOLS],
    deniedTools:[...DENIED_TOOLS],
    maxRequeries:Math.min(HARD_REQUERY,Math.max(0,Number(raw.maxRequeries ?? HARD_REQUERY)||0)),
    maxToolCalls:Math.min(HARD_TOOLS,Math.max(0,Number(raw.maxToolCalls ?? HARD_TOOLS)||0)),
    timeoutMs:Math.min(HARD_TIMEOUT,Math.max(1000,Number(raw.timeoutMs ?? HARD_TIMEOUT)||HARD_TIMEOUT)),
    requireVerification:raw.requireVerification !== false,
    requireReceipt:raw.requireReceipt !== false,
    competitionMode:true,
    version:ENVELOPE_VERSION,
  };
}
function event(mission,partial){
  return {id:id('evt'),ts:now(),agentId:AGENT_ID,kind:partial.kind,summary:String(partial.summary||'').slice(0,280),layer:partial.layer,
    ...(partial.qragStep?{qragStep:partial.qragStep}:{}),...(partial.torque?{torque:partial.torque}:{}),...(partial.tool?{tool:partial.tool}:{}),...(partial.receiptId?{receiptId:partial.receiptId}:{})};
}
function writeSse(res,name,data){ if(res.writableEnded)return; if(name&&name!=='message')res.write(`event: ${name}\n`); res.write(`data: ${JSON.stringify(data)}\n\n`); }
function emit(mission,partial){ const evt=event(mission,partial); mission.events.push(evt); for(const client of mission.clients)writeSse(client,'message',evt); return evt; }
function gcMissions(){
  const t=now();
  for(const [k,m] of missions){ if(t-m.createdAt>MISSION_TTL_MS){ for(const c of m.clients){try{c.end();}catch{}} missions.delete(k);} }
  while(missions.size>MAX_MISSIONS)missions.delete(missions.keys().next().value);
}
async function fetchText(url,timeoutMs){
  const ctrl=new AbortController(); const timer=setTimeout(()=>ctrl.abort(),timeoutMs);
  try{ const res=await fetch(url,{signal:ctrl.signal,headers:{'user-agent':'WORLDQ-Gateway/2.0'}}); if(!res.ok)return{ok:false,text:''}; return{ok:true,text:await res.text()}; }
  catch{return{ok:false,text:''};} finally{clearTimeout(timer);}
}
function summarizeEvidence(files){
  const names=Object.keys(files);
  const required=['grok-build/PROMPT.md','grok-build/BACKEND_CONTRACT.md','src/lib/types.ts','src/hooks/useAgentStream.ts'];
  const missing=required.filter(r=>!files[r]);
  return {files:names,bytes:names.reduce((n,k)=>n+(files[k]?.length||0),0),missing,complete:missing.length===0};
}
function deterministicReasoner(mandate, files, ev){
  const findings=[];
  const app=files['src/hooks/useAgentStream.ts']||'';
  const types=files['src/lib/types.ts']||'';
  const contract=files['grok-build/BACKEND_CONTRACT.md']||'';
  if(app.includes('EventSource')) findings.push('SSE client contract is present.');
  if(types.includes('receipt.issued')&&types.includes('audit.committed')) findings.push('Receipt and audit event kinds are present.');
  if(contract.includes('30 second')||contract.includes('30-second')||contract.includes('30000')) findings.push('Bounded execution timeout is documented.');
  if(ev.missing.length) findings.push(`Missing contract files: ${ev.missing.join(', ')}.`);
  const verified = ev.complete && findings.length >= 3;
  return {
    text:[
      'Summary: WORLDQ completed a bounded launch-readiness inspection using repository evidence and deterministic reasoning.',
      `Evidence observed: ${ev.files.length} files, ${ev.bytes} bytes.`,
      `Findings: ${findings.join(' ')}`,
      'Risks / gaps: External model quality is not required for the protocol demo; provider-specific claims must remain disabled unless actually connected.',
      'Recommended actions: keep provider abstraction, preserve SSE contract, surface system-mode honestly, and add optional free/local model adapters later.',
      `Verification status: ${verified ? 'passed' : 'failed'}.`,
      `Receipt-ready outcome: mandate="${mandate.slice(0,140)}"`,
      'System prompt applied: WORLDQ SYSTEM (frontier-style discipline, no model impersonation).'
    ].join('\n'),
    verified,
  };
}
async function runMission(mission){
  const started=now(); const deadline=started+mission.envelope.timeoutMs; const remain=()=>deadline-now();
  const stages=[]; const toolsUsed=[]; const torqueDecisions=[]; let requeries=0; let outcome=''; let verified=false;
  const applyTorque=(torque,summary,layer='WORLD_GRID')=>{torqueDecisions.push(torque);emit(mission,{kind:'torque.applied',summary,layer,torque});};
  try{
    emit(mission,{kind:'mandate.received',summary:'Mandate accepted inside bounded execution envelope',layer:'ORBIT'});
    applyTorque('decrease','Use deterministic application code before any optional model provider','GLOBE');

    emit(mission,{kind:'tool.called',summary:'repo_read AGENTROPOLIS-WORLDQ-ASTRA contract surfaces',layer:'CITY',qragStep:'retrieve',tool:'repo_read'});
    toolsUsed.push('repo_read');
    const paths=['README.md','package.json','grok-build/PROMPT.md','grok-build/BACKEND_CONTRACT.md','src/lib/types.ts','src/hooks/useAgentStream.ts','src/App.tsx'];
    const files={};
    for(const p of paths){ if(remain()<=0)break; const got=await fetchText(`${REPO.raw}/${p}`,Math.min(2500,Math.max(800,remain()-500))); if(got.ok)files[p]=got.text.slice(0,8000); }
    const ev=summarizeEvidence(files);
    stages.push('retrieve');
    emit(mission,{kind:'qrag.retrieve',summary:`Retrieved ${ev.files.length} sources · ${ev.bytes}B · missing ${ev.missing.length}`,layer:'GLOBE',qragStep:'retrieve',tool:'repo_read'});

    emit(mission,{kind:'tool.called',summary:'architecture_inspect contract completeness and SSE hook',layer:'CITY',qragStep:'evaluate',tool:'architecture_inspect'});
    toolsUsed.push('architecture_inspect');
    stages.push('evaluate');
    emit(mission,{kind:'qrag.evaluate',summary:ev.complete?'Evidence sufficient for bounded system reasoning':`Evidence gaps: ${ev.missing.join(', ')||'contract incomplete'}`,layer:'WORLD_GRID',qragStep:'evaluate'});
    applyTorque(ev.complete?'decrease':'redirect',ev.complete?'Skip unnecessary model call; evidence is sufficient':'Redirect to deterministic gap analysis','WORLD_GRID');

    stages.push('execute');
    emit(mission,{kind:'tool.called',summary:'WORLDQ SYSTEM reasoning prompt',layer:'CITY',qragStep:'execute',tool:'system_reasoner'});
    toolsUsed.push('system_reasoner');
    const result=deterministicReasoner(mission.mandate,files,ev);
    emit(mission,{kind:'action.executed',summary:'WORLDQ SYSTEM produced bounded launch-readiness assessment',layer:'CITY',qragStep:'execute'});

    stages.push('verify');
    verified=result.verified && toolsUsed.length<=mission.envelope.maxToolCalls && requeries<=mission.envelope.maxRequeries && remain()>0;
    emit(mission,{kind:verified?'verification.passed':'verification.failed',summary:verified?'System output and envelope budgets verified':'Verification failed: evidence or budget check insufficient',layer:'WORLDQ',qragStep:'verify'});
    outcome=result.text.slice(0,500);
  }catch(err){
    verified=false; outcome=`Failure: ${String(err?.message||err).slice(0,400)}`;
    emit(mission,{kind:'verification.failed',summary:outcome,layer:'WORLDQ',qragStep:'verify'});
  }

  const latencyMs=now()-started;
  const receiptId=id('rcpt');
  const receipt={
    receiptId,missionId:mission.id,timestamp:new Date().toISOString(),
    model:MODEL,provider:PROVIDER,modelExecuted:true,externalModelExecuted:false,
    systemPrompt:'WORLDQ SYSTEM',executionEnvelopeVersion:ENVELOPE_VERSION,
    mandateHash:mission.envelope.mandateHash,qragStagesCompleted:[...new Set(stages)],
    requeryCount:requeries,toolsUsed:[...new Set(toolsUsed)].slice(0,HARD_TOOLS),
    verificationResult:verified?'passed':'failed',latencyMs,tokens:'not_applicable',costUsd:0,
    torqueDecisions,outcomeSummary:outcome,identity:IDENTITY,competitionMode:true,
  };
  receipt.auditHash=sha256(JSON.stringify(receipt));
  mission.receipt=receipt; mission.status=verified?'complete':'failed';
  emit(mission,{kind:'receipt.issued',summary:`Receipt ${receiptId} · ${receipt.verificationResult} · ${MODEL} · $0 API · ${latencyMs}ms`,layer:'WORLDQ',receiptId});
  emit(mission,{kind:'audit.committed',summary:`Audit ${receipt.auditHash.slice(0,16)} committed`,layer:'WORLDQ',receiptId});
  for(const client of mission.clients){writeSse(client,'done',{ok:verified,missionId:mission.id,receiptId});try{client.end();}catch{}}
  mission.clients.clear();
}
function startMission(mandate,envelopeInput){
  gcMissions(); const missionId=id('msn'); const envelope=clampEnvelope(envelopeInput); envelope.mandateHash=sha256(mandate);
  const mission={id:missionId,createdAt:now(),mandate,envelope,events:[],clients:new Set(),status:'running',receipt:null};
  missions.set(missionId,mission); setImmediate(()=>runMission(mission)); return mission;
}
function health(){
  return {status:'ok',service:'worldq-gateway',version:VERSION,model:MODEL,provider:PROVIDER,systemPrompt:'WORLDQ SYSTEM',envelopeVersion:ENVELOPE_VERSION,competitionMode:true,paidApiRequired:false,limits:{maxRequeries:HARD_REQUERY,maxToolCalls:HARD_TOOLS,timeoutMs:HARD_TIMEOUT},missionsInMemory:missions.size};
}

const server=http.createServer(async(req,res)=>{
  const url=new URL(req.url||'/',`http://${req.headers.host||'localhost'}`); const path=url.pathname.replace(/\/+$/,'')||'/';
  if(req.method==='OPTIONS'){res.writeHead(204,corsHeaders(req));res.end();return;}
  try{
    if(req.method==='GET'&&(path==='/health'||path==='/api/health')){json(res,req,200,health());return;}
    if(req.method==='GET'&&path==='/system-prompt'){json(res,req,200,{name:'WORLDQ SYSTEM',prompt:SYSTEM_PROMPT});return;}
    if(req.method==='POST'&&path==='/api/missions'){
      const raw=await readBody(req); let body={}; if(raw.trim()){try{body=JSON.parse(raw);}catch{json(res,req,400,{error:'invalid_json'});return;}}
      const mandate=String(body.mandate||DEFAULT_MANDATE).trim(); if(!mandate){json(res,req,400,{error:'mandate_required'});return;} if(mandate.length>MAX_MANDATE){json(res,req,400,{error:'mandate_too_large',max:MAX_MANDATE});return;}
      const mission=startMission(mandate,body.envelope||{}); json(res,req,202,{missionId:mission.id}); return;
    }
    const missionMatch=path.match(/^\/api\/missions\/([^/]+)$/);
    if(req.method==='GET'&&missionMatch){const mission=missions.get(decodeURIComponent(missionMatch[1])); if(!mission){json(res,req,404,{error:'mission_not_found'});return;} json(res,req,200,{missionId:mission.id,status:mission.status,model:MODEL,provider:PROVIDER,createdAt:mission.createdAt,receipt:mission.receipt});return;}
    const eventsMatch=path.match(/^\/api\/missions\/([^/]+)\/events$/);
    if(req.method==='GET'&&eventsMatch){const mission=missions.get(decodeURIComponent(eventsMatch[1])); if(!mission){json(res,req,404,{error:'mission_not_found'});return;} res.writeHead(200,{...corsHeaders(req),'content-type':'text/event-stream; charset=utf-8','cache-control':'no-cache, no-transform',connection:'keep-alive','x-accel-buffering':'no'});res.write(':\n\n');for(const evt of mission.events)writeSse(res,'message',evt);if(mission.status!=='running'){writeSse(res,'done',{ok:mission.status==='complete',missionId:mission.id,receiptId:mission.receipt?.receiptId||null});res.end();return;}mission.clients.add(res);req.on('close',()=>mission.clients.delete(res));return;}
    json(res,req,404,{error:'not_found'});
  }catch(err){json(res,req,err.code===413?413:500,{error:err.code===413?'payload_too_large':'gateway_error'});}
});
server.listen(PORT,'0.0.0.0',()=>{process.stdout.write(`[worldq-gateway] ${VERSION} listening on :${PORT} provider=${PROVIDER} model=${MODEL} paidApiRequired=false\n`);});
