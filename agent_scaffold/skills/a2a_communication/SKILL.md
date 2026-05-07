---
name: a2a_communication
description: Canonical guidance for discovering other agents and communicating with them through Astro's A2A flow.
---

# A2A Communication

Use this skill whenever you need to discover another agent, inspect available A2A candidates, or
send a request to another agent through Astro's A2A flow.

## Canonical rule

This skill is the canonical instruction source for A2A discovery and communication.

## When to use

- When the user asks which agents can help.
- When another agent may be better suited to answer or assist.
- Before starting an A2A discovery flow.
- Before starting an A2A request flow.
- When a request explicitly arrives through the A2A channel.

## Core invariants

- A2A does not expand your role or scope.
- Use `mainsequence agent search ... --json` as the canonical discovery source.
- Use the target runtime's `/api/a2a/chat` surface for communication.

## Discovery flow

1. Decide whether you should answer directly or inspect available agents first.
2. Build a bounded discovery intent:
   - what help is needed
   - optional agent hint
   - required response format if relevant
3. Turn that intent into a discovery prompt with these sections when relevant:
   - `Intent: ...`
   - `Preferred agent hint: ...`
   - `Required response format: ...`
4. Run:

```bash
mainsequence agent search "<discoveryPrompt>" --limit 10 --json
```

5. Treat the CLI output as authoritative.
6. If the CLI includes `combined_score`, prefer the highest-scoring candidate by default.
7. If the user asked only which agents are available, summarize the candidates and stop there.

## Communication flow

1. Decide whether actual A2A communication is needed.
2. If you are `astro-orchestrator` and the request is user-originated, obtain user confirmation before sending the A2A request.
3. Build a bounded request:
   - clearly scoped task
   - optional agent hint
   - required response format or output schema if needed
4. Discover and select the target agent through the discovery flow above.
5. Start a backend session for the selected agent:

```bash
mainsequence agent start_new_session <agent_id>
```

6. Resolve runtime access for that agent session:

```bash
mainsequence agent session resolve_runtime_access <session_id> --json
```

7. Extract:
   - `rpc_url`
   - `token`
8. Poll the target runtime health until it becomes healthy:

```bash
curl -sS -H "Authorization: Bearer $TOKEN" "$RPC_URL/health"
```

9. Send the A2A request to the target runtime:

```bash
curl -N -sS \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  "$RPC_URL/api/a2a/chat" \
  -d '{
    "sessionId": "<session_id>",
    "messages": [{"role": "user", "content": "<bounded request>"}],
    "response_format": <response format or null>,
    "caller": {
      "agent_name": "<current agent name>",
      "discovery_prompt": "<discoveryPrompt>",
      "mode": "production"
    }
  }'
```

10. Treat returned content as collaboration output from the target agent.
11. Summarize back to the user or caller according to your role.

## Deterministic execution path

The runtime executes the transport deterministically, but the canonical workflow is this skill plus
the exact CLI and runtime requests below.

1. Build the discovery prompt.
2. Run `mainsequence agent search "<discoveryPrompt>" --limit <n> --json`.
3. Parse the JSON output.
4. Normalize candidates.
5. Select the preferred candidate.
6. Create the backend session for the target agent.
7. Resolve runtime access.
8. Poll runtime health.
9. Send `POST <rpc_url>/api/a2a/chat`.

## Role-specific behavior

### `astro-orchestrator`

- may discover candidates without confirmation
- must get user confirmation before sending a real A2A request for user-originated requests
- should offer A2A briefly when another agent may be better suited

### runtime-owned child or executor agent

- may use bounded A2A within the active task scope without separate user confirmation
- should keep the request tightly scoped to the current project or active task

## A2A response behavior

- If the request is marked as A2A, respond agent-to-agent rather than user-to-agent.
- If a response format or output schema is specified, follow it exactly.
- If no response format is specified, return a concise machine-usable response.
- Keep A2A responses concise and machine-usable when the caller requested that shape.

## Do not

- Do not let A2A broaden your scope.
- Do not replace CLI discovery with ad hoc local prompt-file inspection.
- Do not send another agent work when discovery alone was the requested goal.
