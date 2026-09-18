# WORLDQ Astra Gateway

Production GPT-6 Astra backend for [AGENTROPOLIS-WORLDQ-ASTRA](https://github.com/AGENTROPOLIS-CITY-OF-AGENTS/AGENTROPOLIS-WORLDQ-ASTRA).

Does not replace the React + Three.js WORLDQ client.

## Endpoints

- `GET /health`
- `POST /api/missions` — acknowledges immediately with `{ missionId }`
- `GET /api/missions/:missionId/events` — SSE ExecutionEvent stream + `done`

## Contract

QRAG: RETRIEVE → EVALUATE → EXECUTE → VERIFY → optional RE-QUERY (max 1)
Envelope: max 8 tool calls, 30s hard timeout, competition mode, no database.
Torque: INCREASE / DECREASE / REDIRECT.
OPENAI_API_KEY remains server-side. Model: `gpt-6-astra` via Responses API.

## Run

```bash
export OPENAI_API_KEY=...
export PORT=8787
node server.mjs
```

Set `VITE_GATEWAY_URL` on the WORLDQ Pages build to this origin.
