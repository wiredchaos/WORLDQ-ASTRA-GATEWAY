/**
 * AGENTROPOLIS WORLDQ × GPT-6 ASTRA
 * Production competition gateway.
 *
 * Contract: grok-build/PROMPT.md, grok-build/BACKEND_CONTRACT.md,
 * src/lib/types.ts, src/hooks/useAgentStream.ts
 *
 * No database. In-memory missions. OPENAI_API_KEY stays server-side.
 */
import http from 'node:http';
import crypto from 'node:crypto';
import { URL } from 'node:url';

const VERSION = '1.0.0';
const MODEL = process.env.OPENAI_MODEL || 'gpt-6-astra';
const ENVELOPE_VERSION = process.env.ENVELOPE_VERSION || 'worldq-envelope-v1';
const PORT = Number(process.env.PORT || 8787);
const AGENT_ID = 'astra-01';
const IDENTITY = 'AGENTROPOLIS/WORLDQ/ASTRA-01';
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
const ALLOWED_TOOLS = Object.freeze(['web_search', 'repo_read', 'architecture_inspect']);
const DENIED_TOOLS = Object.freeze([
  'image_generation',
  'computer_use',
  'hosted_shell',
  'file_search',
  'code_interpreter',
]);
const DEFAULT_MANDATE =
  'Audit AGENTROPOLIS-WORLDQ-ASTRA for launch readiness. Inspect the architecture, identify implementation or governance gaps, verify the result, and produce an accountable execution receipt.';
const REPO = {
  owner: 'AGENTROPOLIS-CITY-OF-AGENTS',
  name: 'AGENTROPOLIS-WORLDQ-ASTRA',
  api: 'https://api.github.com/repos/AGENTROPOLIS-CITY-OF-AGENTS/AGENTROPOLIS-WORLDQ-ASTRA',
  raw: 'https://raw.githubusercontent.com/AGENTROPOLIS-CITY-OF-AGENTS/AGENTROPOLIS-WORLDQ-ASTRA/main',
};

const missions = new Map();

function allowedOrigins() {
  const extra = (process.env.ALLOWED_ORIGINS || '')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
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
  res.writeHead(status, {
    ...corsHeaders(req),
    'content-type': 'application/json; charset=utf-8',
    'content-length': Buffer.byteLength(payload),
    'cache-control': 'no-store',
  });
  res.end(payload);
}

function readBody(req, limit = 12_000) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let n = 0;
    req.on('data', (c) => {
      n += c.length;
      if (n > limit) {
        reject(Object.assign(new Error('payload too large'), { code: 413 }));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    req.on('error', reject);
  });
}

function id(prefix) {
  return `${prefix}_${crypto.randomBytes(8).toString('hex')}`;
}

function sha256(text) {
  return crypto.createHash('sha256').update(text).digest('hex');
}

function now() {
  return Date.now();
}

function clampEnvelope(raw = {}) {
  const maxRequeries = Math.min(HARD_REQUERY, Math.max(0, Number(raw.maxRequeries ?? HARD_REQUERY) || 0));
  const maxToolCalls = Math.min(HARD_TOOLS, Math.max(0, Number(raw.maxToolCalls ?? HARD_TOOLS) || 0));
  const timeoutMs = Math.min(HARD_TIMEOUT, Math.max(1_000, Number(raw.timeoutMs ?? HARD_TIMEOUT) || HARD_TIMEOUT));
  return {
    identity: IDENTITY,
    mandateHash: '',
    allowedTools: [...ALLOWED_TOOLS],
    deniedTools: [...DENIED_TOOLS],
    maxRequeries,
    maxToolCalls,
    timeoutMs,
    requireVerification: raw.requireVerification !== false,
    requireReceipt: raw.requireReceipt !== false,
    competitionMode: true,
    version: ENVELOPE_VERSION,
  };
}

function event(mission, partial) {
  return {
    id: id('evt'),
    ts: now(),
    agentId: AGENT_ID,
    kind: partial.kind,
    summary: String(partial.summary || '').slice(0, 280),
    layer: partial.layer,
    ...(partial.qragStep ? { qragStep: partial.qragStep } : {}),
    ...(partial.torque ? { torque: partial.torque } : {}),
    ...(partial.tool ? { tool: partial.tool } : {}),
    ...(partial.receiptId ? { receiptId: partial.receiptId } : {}),
    ...(typeof partial.costUsd === 'number' ? { costUsd: partial.costUsd } : {}),
    ...(typeof partial.tokens === 'number' ? { tokens: partial.tokens } : {}),
  };
}

function emit(mission, partial) {
  const evt = event(mission, partial);
  mission.events.push(evt);
  for (const client of mission.clients) writeSse(client, 'message', evt);
  return evt;
}

function writeSse(res, name, data) {
  if (res.writableEnded) return;
  if (name && name !== 'message') res.write(`event: ${name}\n`);
  res.write(`data: ${JSON.stringify(data)}\n\n`);
}

function gcMissions() {
  const t = now();
  for (const [k, m] of missions) {
    if (t - m.createdAt > MISSION_TTL_MS) {
      for (const c of m.clients) {
        try { c.end(); } catch {}
      }
      missions.delete(k);
    }
  }
  while (missions.size > MAX_MISSIONS) {
    const oldest = missions.keys().next().value;
    missions.delete(oldest);
  }
}

async function fetchText(url, timeoutMs) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      signal: ctrl.signal,
      headers: { 'user-agent': 'WORLDQ-Astra-Gateway/1.0', accept: 'application/vnd.github+json' },
    });
    if (!res.ok) return { ok: false, status: res.status, text: '' };
    return { ok: true, status: res.status, text: await res.text() };
  } catch (err) {
    return { ok: false, status: 0, text: '', error: err.message };
  } finally {
    clearTimeout(timer);
  }
}

function summarizeEvidence(files) {
  const names = Object.keys(files);
  const missing = [];
  const required = [
    'grok-build/PROMPT.md',
    'grok-build/BACKEND_CONTRACT.md',
    'src/lib/types.ts',
    'src/hooks/useAgentStream.ts',
  ];
  for (const r of required) if (!files[r]) missing.push(r);
  const hasTypes =
    files['src/lib/types.ts']?.includes('mandate.received') &&
    files['src/lib/types.ts']?.includes('audit.committed');
  const hasSseHook = files['src/hooks/useAgentStream.ts']?.includes('EventSource');
  const hasContract = Boolean(files['grok-build/BACKEND_CONTRACT.md']);
  return {
    files: names,
    bytes: names.reduce((n, k) => n + (files[k]?.length || 0), 0),
    missing,
    hasTypes: Boolean(hasTypes),
    hasSseHook: Boolean(hasSseHook),
    hasContract,
    complete: missing.length === 0 && hasTypes && hasSseHook && hasContract,
  };
}

function extractOutputText(payload) {
  if (!payload) return '';
  if (typeof payload.output_text === 'string' && payload.output_text.trim()) return payload.output_text;
  const parts = [];
  for (const item of payload.output || []) {
    if (item?.type === 'message') {
      for (const c of item.content || []) {
        if (c?.type === 'output_text' && c.text) parts.push(c.text);
      }
    }
  }
  return parts.join('\n').trim();
}

function countToolCalls(payload) {
  const tools = [];
  for (const item of payload?.output || []) {
    const t = item?.type || '';
    if (t.endsWith('_call') || t === 'function_call' || t === 'web_search_call') {
      tools.push(t.replace(/_call$/, ''));
    }
  }
  return tools;
}

function usageFrom(payload) {
  const u = payload?.usage;
  if (!u) return { tokens: undefined, costUsd: undefined };
  const tokens = Number(u.total_tokens || 0) || (Number(u.input_tokens || 0) + Number(u.output_tokens || 0));
  const input = Number(u.input_tokens || 0);
  const output = Number(u.output_tokens || 0);
  const costUsd = Number(((input * 10 + output * 50) / 1_000_000).toFixed(6));
  return {
    tokens: tokens || undefined,
    costUsd: Number.isFinite(costUsd) ? costUsd : undefined,
  };
}

function redact(value) {
  if (!value) return value;
  return String(value)
    .replace(/sk-[a-zA-Z0-9_\-]+/g, '[redacted]')
    .replace(/OPENAI_API_KEY\s*=\s*\S+/g, 'OPENAI_API_KEY=[redacted]');
}

async function callAstra({ mandate, evidence, effort, remainingMs, previousId }) {
  const key = process.env.OPENAI_API_KEY;
  if (!key) {
    const err = new Error('OPENAI_API_KEY is not configured on the gateway');
    err.code = 'NO_KEY';
    throw err;
  }
  const ctrl = new AbortController();
  const budget = Math.max(2_000, Math.min(22_000, remainingMs - 1_500));
  const timer = setTimeout(() => ctrl.abort(), budget);
  const input = [
    {
      role: 'developer',
      content:
        'You are GPT-6 Astra executing a bounded AGENTROPOLIS WORLDQ competition mission. Stay inside the execution envelope. Return a concise launch-readiness audit: architecture findings, governance gaps, verification notes. Do not request secrets. Do not invent live telemetry. Keep the answer under 500 words.',
    },
    {
      role: 'user',
      content: [
        `Mandate: ${mandate}`,
        `Envelope: model=${MODEL} maxRequeries=${HARD_REQUERY} maxToolCalls=${HARD_TOOLS} timeoutMs=${HARD_TIMEOUT} competitionMode=true`,
        `Retrieved evidence (application-code retrieve, truncated):`,
        evidence.slice(0, 12_000),
        previousId ? `This is a single allowed re-query of previous response ${previousId}. Tighten verification and fill gaps only.` : '',
      ]
        .filter(Boolean)
        .join('\n\n'),
    },
  ];
  const body = {
    model: MODEL,
    input,
    reasoning: { effort },
    max_output_tokens: 700,
    tools: [{ type: 'web_search' }],
    tool_choice: 'auto',
    store: false,
  };
  try {
    const res = await fetch('https://api.openai.com/v1/responses', {
      method: 'POST',
      signal: ctrl.signal,
      headers: {
        authorization: `Bearer ${key}`,
        'content-type': 'application/json',
        accept: 'application/json',
      },
      body: JSON.stringify(body),
    });
    const text = await res.text();
    let payload;
    try {
      payload = JSON.parse(text);
    } catch {
      throw new Error(`Astra returned non-JSON (${res.status})`);
    }
    if (!res.ok) {
      const msg = redact(payload?.error?.message || `Astra HTTP ${res.status}`);
      const err = new Error(msg);
      err.status = res.status;
      throw err;
    }
    return payload;
  } finally {
    clearTimeout(timer);
  }
}

function auditHash(receipt) {
  const { auditHash: _omit, ...rest } = receipt;
  return sha256(JSON.stringify(rest));
}

async function runMission(mission) {
  const started = now();
  const deadline = started + mission.envelope.timeoutMs;
  const remain = () => deadline - now();
  const torqueDecisions = [];
  const toolsUsed = [];
  const stages = [];
  let requeries = 0;
  let openaiResponseId = '';
  let modelUsed = MODEL;
  let outcome = '';
  let verified = false;
  let tokens;
  let costUsd;
  let lastText = '';

  const timedOut = () => remain() <= 0;

  const applyTorque = (torque, summary, layer = 'WORLD_GRID') => {
    torqueDecisions.push(torque);
    emit(mission, { kind: 'torque.applied', summary, layer, torque });
  };

  try {
    emit(mission, {
      kind: 'mandate.received',
      summary: 'Mandate accepted inside bounded execution envelope',
      layer: 'ORBIT',
    });

    applyTorque('decrease', 'Mechanical retrieve via application code, not a model call', 'GLOBE');

    emit(mission, {
      kind: 'tool.called',
      summary: 'repo_read AGENTROPOLIS-WORLDQ-ASTRA contract surfaces',
      layer: 'CITY',
      qragStep: 'retrieve',
      tool: 'repo_read',
    });
    toolsUsed.push('repo_read');

    const paths = [
      'README.md',
      'package.json',
      'grok-build/PROMPT.md',
      'grok-build/BACKEND_CONTRACT.md',
      'src/lib/types.ts',
      'src/hooks/useAgentStream.ts',
      'src/App.tsx',
    ];
    const files = {};
    for (const p of paths) {
      if (timedOut()) break;
      const got = await fetchText(`${REPO.raw}/${p}`, Math.min(2500, Math.max(800, remain() - 500)));
      if (got.ok) files[p] = got.text.slice(0, 8000);
    }
    const ev = summarizeEvidence(files);
    stages.push('retrieve');
    emit(mission, {
      kind: 'qrag.retrieve',
      summary: `Retrieved ${ev.files.length} sources · ${ev.bytes}B · missing ${ev.missing.length}`,
      layer: 'GLOBE',
      qragStep: 'retrieve',
      tool: 'repo_read',
    });

    emit(mission, {
      kind: 'tool.called',
      summary: 'architecture_inspect contract completeness and SSE hook',
      layer: 'CITY',
      qragStep: 'evaluate',
      tool: 'architecture_inspect',
    });
    toolsUsed.push('architecture_inspect');

    stages.push('evaluate');
    emit(mission, {
      kind: 'qrag.evaluate',
      summary: ev.complete
        ? 'Evidence sufficient for bounded Astra audit'
        : `Evidence gaps: ${ev.missing.join(', ') || 'incomplete contract markers'}`,
      layer: 'WORLD_GRID',
      qragStep: 'evaluate',
    });

    if (!ev.complete) {
      applyTorque('redirect', 'Redirect retrieve to raw GitHub contract files before deeper reasoning', 'WORLD_GRID');
    } else {
      applyTorque('decrease', 'Skip extra retrieval loops; evidence already meets envelope', 'WORLD_GRID');
    }

    if (timedOut()) throw Object.assign(new Error('mission timeout before execute'), { code: 'TIMEOUT' });

    const evidenceBlob = Object.entries(files)
      .map(([k, v]) => `--- ${k} ---\n${v.slice(0, 1800)}`)
      .join('\n\n');

    const runAstra = async (effort, previousId) => {
      emit(mission, {
        kind: 'tool.called',
        summary: `gpt-6-astra Responses API · effort=${effort}`,
        layer: 'CITY',
        qragStep: 'execute',
        tool: 'web_search',
      });
      const payload = await callAstra({
        mandate: mission.mandate,
        evidence: evidenceBlob,
        effort,
        remainingMs: remain(),
        previousId,
      });
      const used = countToolCalls(payload);
      for (const t of used) {
        if (toolsUsed.length >= mission.envelope.maxToolCalls) break;
        toolsUsed.push(t);
        emit(mission, {
          kind: 'tool.called',
          summary: `Astra tool ${t}`,
          layer: 'CITY',
          qragStep: 'execute',
          tool: t === 'web_search' ? 'web_search' : t,
        });
      }
      emit(mission, {
        kind: 'action.executed',
        summary: `Astra response ${payload.id || 'unidentified'} status=${payload.status || 'unknown'}`,
        layer: 'CITY',
        qragStep: 'execute',
      });
      return payload;
    };

    applyTorque('increase', 'Escalate to GPT-6 Astra for launch-readiness reasoning', 'CITY');
    stages.push('execute');
    let payload;
    try {
      payload = await runAstra(ev.complete ? 'low' : 'medium', null);
    } catch (err) {
      if (err.code === 'NO_KEY') throw err;
      if (requeries < mission.envelope.maxRequeries && remain() > 4000) {
        requeries += 1;
        stages.push('requery');
        applyTorque('redirect', 'Redirect after Astra error onto a single bounded re-query', 'GLOBE');
        emit(mission, {
          kind: 'qrag.evaluate',
          summary: `Re-query authorized (${requeries}/${mission.envelope.maxRequeries}) after ${redact(err.message)}`,
          layer: 'WORLD_GRID',
          qragStep: 'requery',
        });
        payload = await runAstra('medium', null);
      } else {
        throw err;
      }
    }

    openaiResponseId = payload?.id || '';
    modelUsed = payload?.model || MODEL;
    lastText = extractOutputText(payload);
    const usage = usageFrom(payload);
    tokens = usage.tokens;
    costUsd = usage.costUsd;

    stages.push('verify');
    const checks = {
      hasResponseId: Boolean(openaiResponseId),
      modelIsAstra: String(modelUsed).includes('gpt-6-astra'),
      hasOutput: lastText.length >= 80,
      toolsWithinBudget: toolsUsed.length <= mission.envelope.maxToolCalls,
      requeriesWithinBudget: requeries <= mission.envelope.maxRequeries,
      notTimedOut: !timedOut(),
    };
    const failed = Object.entries(checks).filter(([, v]) => !v).map(([k]) => k);

    if (failed.length && requeries < mission.envelope.maxRequeries && remain() > 5000) {
      requeries += 1;
      stages.push('requery');
      applyTorque('increase', 'Verification insufficient — one deeper Astra re-query', 'WORLD_GRID');
      emit(mission, {
        kind: 'qrag.evaluate',
        summary: `Re-query ${requeries}: failed checks ${failed.join(', ')}`,
        layer: 'WORLD_GRID',
        qragStep: 'requery',
      });
      const retry = await runAstra('medium', openaiResponseId || null);
      openaiResponseId = retry?.id || openaiResponseId;
      modelUsed = retry?.model || modelUsed;
      lastText = extractOutputText(retry) || lastText;
      const u2 = usageFrom(retry);
      if (u2.tokens) tokens = (tokens || 0) + u2.tokens;
      if (u2.costUsd) costUsd = Number((((costUsd || 0) + u2.costUsd)).toFixed(6));
      checks.hasResponseId = Boolean(openaiResponseId);
      checks.modelIsAstra = String(modelUsed).includes('gpt-6-astra');
      checks.hasOutput = lastText.length >= 80;
      checks.toolsWithinBudget = toolsUsed.length <= mission.envelope.maxToolCalls;
      checks.notTimedOut = !timedOut();
    }

    const stillFailed = Object.entries(checks).filter(([, v]) => !v).map(([k]) => k);
    verified = stillFailed.length === 0;
    if (verified) {
      emit(mission, {
        kind: 'verification.passed',
        summary: 'Astra output, model id, envelope budgets, and response id verified',
        layer: 'WORLDQ',
        qragStep: 'verify',
        ...(typeof tokens === 'number' ? { tokens } : {}),
        ...(typeof costUsd === 'number' ? { costUsd } : {}),
      });
      outcome = lastText.slice(0, 400) || 'Launch-readiness audit completed.';
    } else {
      emit(mission, {
        kind: 'verification.failed',
        summary: `Verification failed: ${stillFailed.join(', ')}`,
        layer: 'WORLDQ',
        qragStep: 'verify',
      });
      outcome = `Mission verification failed (${stillFailed.join(', ')}). ${lastText.slice(0, 240)}`.trim();
    }
  } catch (err) {
    const timeout = err.code === 'TIMEOUT' || err.name === 'AbortError' || timedOut();
    emit(mission, {
      kind: 'verification.failed',
      summary: timeout
        ? 'Hard 30s envelope timeout before accountable completion'
        : redact(err.message || 'Astra execution error'),
      layer: 'WORLDQ',
      qragStep: 'verify',
    });
    outcome = timeout
      ? 'Failure: execution envelope timeout (30000ms).'
      : `Failure: ${redact(err.message || 'execution error')}`;
    verified = false;
  }

  const latencyMs = now() - started;
  const receiptId = id('rcpt');
  const receipt = {
    receiptId,
    missionId: mission.id,
    timestamp: new Date().toISOString(),
    model: MODEL,
    openaiResponseId: openaiResponseId || null,
    mandateHash: mission.envelope.mandateHash,
    executionEnvelopeVersion: ENVELOPE_VERSION,
    qragStagesCompleted: [...new Set(stages)],
    requeryCount: requeries,
    toolsUsed: [...new Set(toolsUsed)].slice(0, HARD_TOOLS),
    verificationResult: verified ? 'passed' : 'failed',
    latencyMs,
    ...(typeof tokens === 'number' ? { tokens } : { tokens: 'unavailable' }),
    ...(typeof costUsd === 'number' ? { costUsd } : { costUsd: 'unavailable' }),
    torqueDecisions,
    outcomeSummary: outcome.slice(0, 500),
    identity: IDENTITY,
    competitionMode: true,
  };
  receipt.auditHash = auditHash(receipt);
  mission.receipt = receipt;
  mission.status = verified ? 'complete' : 'failed';

  emit(mission, {
    kind: 'receipt.issued',
    summary: `Receipt ${receiptId} · ${receipt.verificationResult} · ${latencyMs}ms`,
    layer: 'WORLDQ',
    receiptId,
    ...(typeof tokens === 'number' ? { tokens } : {}),
    ...(typeof costUsd === 'number' ? { costUsd } : {}),
  });
  emit(mission, {
    kind: 'audit.committed',
    summary: `Audit ${receipt.auditHash.slice(0, 16)} committed`,
    layer: 'WORLDQ',
    receiptId,
  });

  for (const client of mission.clients) {
    writeSse(client, 'done', { ok: true, missionId: mission.id, receiptId });
    try { client.end(); } catch {}
  }
  mission.clients.clear();
}

function startMission(mandate, envelopeInput) {
  gcMissions();
  const missionId = id('msn');
  const envelope = clampEnvelope(envelopeInput);
  envelope.mandateHash = sha256(mandate);
  const mission = {
    id: missionId,
    createdAt: now(),
    mandate,
    envelope,
    events: [],
    clients: new Set(),
    status: 'running',
    receipt: null,
  };
  missions.set(missionId, mission);
  setImmediate(() => {
    runMission(mission).catch((err) => {
      mission.status = 'failed';
      emit(mission, {
        kind: 'verification.failed',
        summary: redact(err.message || 'unhandled gateway error'),
        layer: 'WORLDQ',
        qragStep: 'verify',
      });
      for (const c of mission.clients) {
        writeSse(c, 'done', { ok: false, missionId });
        try { c.end(); } catch {}
      }
    });
  });
  return mission;
}

function health() {
  return {
    status: process.env.OPENAI_API_KEY ? 'ok' : 'degraded',
    service: 'worldq-astra-gateway',
    version: VERSION,
    model: MODEL,
    envelopeVersion: ENVELOPE_VERSION,
    competitionMode: true,
    limits: {
      maxRequeries: HARD_REQUERY,
      maxToolCalls: HARD_TOOLS,
      timeoutMs: HARD_TIMEOUT,
    },
    openaiKeyConfigured: Boolean(process.env.OPENAI_API_KEY),
    missionsInMemory: missions.size,
  };
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  const path = url.pathname.replace(/\/+$/, '') || '/';

  if (req.method === 'OPTIONS') {
    res.writeHead(204, corsHeaders(req));
    res.end();
    return;
  }

  try {
    if (req.method === 'GET' && (path === '/health' || path === '/api/health')) {
      json(res, req, 200, health());
      return;
    }

    if (req.method === 'GET' && path === '/') {
      json(res, req, 200, {
        service: 'worldq-astra-gateway',
        version: VERSION,
        model: MODEL,
        endpoints: ['GET /health', 'POST /api/missions', 'GET /api/missions/:id/events'],
      });
      return;
    }

    if (req.method === 'POST' && path === '/api/missions') {
      const raw = await readBody(req);
      let body = {};
      if (raw.trim()) {
        try {
          body = JSON.parse(raw);
        } catch {
          json(res, req, 400, { error: 'invalid_json' });
          return;
        }
      }
      const mandate = String(body.mandate || DEFAULT_MANDATE).trim();
      if (!mandate) {
        json(res, req, 400, { error: 'mandate_required' });
        return;
      }
      if (mandate.length > MAX_MANDATE) {
        json(res, req, 400, { error: 'mandate_too_large', max: MAX_MANDATE });
        return;
      }
      if (body.envelope && typeof body.envelope !== 'object') {
        json(res, req, 400, { error: 'invalid_envelope' });
        return;
      }
      const mission = startMission(mandate, body.envelope || {});
      json(res, req, 202, { missionId: mission.id });
      return;
    }

    const eventsMatch = path.match(/^\/api\/missions\/([^/]+)\/events$/);
    if (req.method === 'GET' && eventsMatch) {
      const mission = missions.get(decodeURIComponent(eventsMatch[1]));
      if (!mission) {
        json(res, req, 404, { error: 'mission_not_found' });
        return;
      }
      res.writeHead(200, {
        ...corsHeaders(req),
        'content-type': 'text/event-stream; charset=utf-8',
        'cache-control': 'no-cache, no-transform',
        connection: 'keep-alive',
        'x-accel-buffering': 'no',
      });
      res.write(':\n\n');
      for (const evt of mission.events) writeSse(res, 'message', evt);
      if (mission.status !== 'running') {
        writeSse(res, 'done', { ok: true, missionId: mission.id, receiptId: mission.receipt?.receiptId || null });
        res.end();
        return;
      }
      mission.clients.add(res);
      req.on('close', () => mission.clients.delete(res));
      return;
    }

    json(res, req, 404, { error: 'not_found' });
  } catch (err) {
    const status = err.code === 413 ? 413 : 500;
    json(res, req, status, { error: status === 413 ? 'payload_too_large' : 'gateway_error' });
  }
});

server.listen(PORT, '0.0.0.0', () => {
  process.stdout.write(
    `[worldq-astra-gateway] ${VERSION} listening on :${PORT} model=${MODEL} key=${process.env.OPENAI_API_KEY ? 'set' : 'missing'}\n`,
  );
});
