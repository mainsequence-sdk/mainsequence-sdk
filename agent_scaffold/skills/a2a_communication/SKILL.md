---
name: a2a_communication
description: Canonical guidance for discovering other agents and communicating with them through Main Sequence's session-scoped A2A flow.
---

# A2A Communication

Use this skill whenever you need to discover another agent, inspect available A2A
candidates, or send a request to another agent through Main Sequence's A2A flow.

## Canonical Rule

A2A transport is session-scoped.

If you already have the target `AgentSession` UID, send to that session directly:

```bash
mainsequence agent session a2a_chat <target_agent_session_uid> --message "..." --json
```

Do not pass the target agent UID when sending to an existing session. The session is
already bound to its agent.

Only these backend A2A transport endpoints are part of this skill:

- `POST /orm/api/agents/v1/sessions/{session_uid}/runtime_ready/`
- `POST /orm/api/agents/v1/sessions/{session_uid}/a2a_chat/`

Do not invent wrapper endpoints or wrapper commands. Do not manually extract runtime RPC
URLs, runtime bearer tokens, or call target runtime health/chat endpoints with `curl`.

## When To Use

- When the user asks which agents can help.
- When another agent may be better suited to answer or assist.
- Before starting an A2A discovery flow.
- Before sending an A2A request to an existing target session.
- When a request explicitly arrives through the A2A channel.

## Core Invariants

- A2A does not expand your role or scope.
- Use `mainsequence agent search ... --json` as the canonical discovery source.
- Use `mainsequence agent session a2a_chat ...` as the canonical CLI send command.
- Use `AgentSession.send_a2a_chat(...)` as the canonical Python send helper.
- Use `mainsequence agent session wait_runtime_ready ...` only when you need an explicit
  readiness check separate from sending.
- Do not expose or copy runtime bearer tokens into prompts, logs, or user-facing answers.

## Discovery Flow

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
7. If the user asked only which agents are available, summarize the candidates and stop.
   Include agent name, stable unique id, and a short summary of relevant skills or
   capabilities.

## Getting A Target Session

If you already have the target session UID, skip this section.

If no target session exists yet, allocate or reuse one explicitly:

```bash
mainsequence agent allocate_a2a_target_session \
  <target_agent_uid> \
  <caller_agent_session_uid> \
  --json
```

For retries or reconnects in the same delegated conversation, reuse the returned
`handle_unique_id`:

```bash
mainsequence agent allocate_a2a_target_session \
  <target_agent_uid> \
  <caller_agent_session_uid> \
  --handle-unique-id <handle_unique_id> \
  --json
```

The allocation response includes:

- `handle_unique_id`: persist this for retries in the same delegated conversation
- `agent_session_uid`: use this as `<target_agent_session_uid>` for A2A chat
- `session`: the target backend `AgentSession` payload

Allocation is separate from sending. Do not combine these into a fake one-step command.

## Communication Flow With CLI

1. Decide whether actual A2A communication is needed.
2. If your agent type is `orchestrator` and the request is user-originated, obtain user
   confirmation before sending the A2A request.
3. Build a bounded request:
   - clearly scoped task
   - required response format or output schema if needed
4. Ensure you have the target session UID.
5. Send the request through the backend-managed session command:

```bash
mainsequence agent session a2a_chat \
  <target_agent_session_uid> \
  --message "Review the current portfolio drift." \
  --timeout 120 \
  --json
```

For raw A2A JSON-RPC:

```bash
mainsequence agent session a2a_chat \
  <target_agent_session_uid> \
  --a2a-payload-file request.json \
  --timeout 120 \
  --json
```

The command handles:

- optional runtime readiness waiting
- backend transport to the target runtime
- task polling until stable when enabled
- normalized response extraction

Do not manually call:

- `mainsequence agent session resolve_runtime_access ...` for normal A2A
- `curl "$RPC_URL/health"`
- `curl "$RPC_URL/api/a2a/chat"`

## Communication Flow With Python

Use the session-scoped SDK helper when running inside an agent runtime or project code:

```python
from mainsequence.client.agent_runtime_models import AgentSession

result = AgentSession.send_a2a_chat(
    target_agent_session_uid,
    message="Review the current portfolio drift.",
    runtime_ready_timeout_seconds=60,
    runtime_ready_poll_interval_seconds=2,
    timeout=120,
)

text = result.normalized.text if result.normalized is not None else ""
```

For raw A2A JSON-RPC:

```python
result = AgentSession.send_a2a_chat(
    target_agent_session_uid,
    a2a_payload={
        "jsonrpc": "2.0",
        "id": "request-1",
        "method": "message/send",
        "params": {
            "message": {
                "kind": "message",
                "messageId": "message-1",
                "role": "user",
                "parts": [
                    {
                        "kind": "text",
                        "text": "Review the current portfolio drift.",
                    }
                ],
            }
        },
    },
    timeout=120,
)
```

If no target session exists yet in Python, allocation is still a separate step:

```python
from mainsequence.client.agent_runtime_models import Agent, AgentSession

target_agent = Agent.get_by_uid(target_agent_uid)
allocation = target_agent.allocate_a2a_target_session(
    caller_agent_session_uid=caller_agent_session_uid,
    handle_unique_id=existing_handle_unique_id,
    timeout=120,
)

target_session_uid = allocation["agent_session_uid"]
result = AgentSession.send_a2a_chat(
    target_session_uid,
    message="Review the current portfolio drift.",
    timeout=120,
)
```

## Runtime Readiness

Normally, `a2a_chat` asks the backend to wait for runtime readiness before sending.

Use an explicit readiness check only when debugging or when you need to separate readiness
from message sending:

```bash
mainsequence agent session wait_runtime_ready \
  <target_agent_session_uid> \
  --timeout-seconds 60 \
  --poll-interval-seconds 2 \
  --json
```

Python equivalent:

```python
from mainsequence.client.agent_runtime_models import AgentSession

ready = AgentSession.wait_until_runtime_ready(
    target_agent_session_uid,
    timeout_seconds=60,
    poll_interval_seconds=2,
    timeout=120,
)
```

## Low-Level Runtime Debugging

Use runtime access resolution only for debugging:

```bash
mainsequence agent session resolve_runtime_access <target_agent_session_uid> --json
```

This returns runtime access metadata and is not the normal communication path.

## Handle Reuse

`handle_unique_id` is the stable delegated-conversation reuse key returned by allocation.

- If the delegated conversation is new, omit `--handle-unique-id` during allocation.
- The backend generates and returns a new `handle_unique_id`; persist it immediately.
- If you retry because of timeout, disconnect, readiness delay, stream restart, or runtime
  error recovery, allocate again with the same `handle_unique_id`.
- Reusing the same handle tells the backend to return the same delegated `AgentSession`.
- Allocate a fresh handle only when you intentionally want a new delegated conversation.

## Deterministic Execution Path

1. Build the discovery prompt.
2. Run `mainsequence agent search "<discoveryPrompt>" --limit <n> --json`.
3. Parse the JSON output.
4. Normalize candidates.
5. Select the preferred candidate.
6. Get or allocate the target session UID.
7. Send with `mainsequence agent session a2a_chat <target_agent_session_uid> ... --json`
   or `AgentSession.send_a2a_chat(...)`.
8. Persist `handle_unique_id` from allocation for retries.
9. Treat `normalized.text` as the primary concise target-agent answer when present.

## Role-Specific Behavior

### Orchestrator Agent

- May discover candidates without confirmation.
- Must get user confirmation before sending a real A2A request for user-originated requests.
- Should offer A2A briefly when another agent may be better suited.

### Runtime-Owned Child Or Executor Agent

- May use bounded A2A within the active task scope without separate user confirmation.
- Should keep the request tightly scoped to the current project or active task.

## A2A Response Behavior

- If the request is marked as A2A, respond agent-to-agent rather than user-to-agent.
- If a response format or output schema is specified, follow it exactly.
- If no response format is specified, return a concise machine-usable response.
- Keep A2A responses concise and machine-usable when the caller requested that shape.

## Do Not

- Do not let A2A broaden your scope.
- Do not replace CLI discovery with ad hoc local prompt-file inspection.
- Do not send another agent work when discovery alone was the requested goal.
- Do not manually extract `rpc_url` or `token` for normal A2A communication.
- Do not call target runtime `/health` or `/api/a2a/chat` directly for normal A2A communication.
- Do not invent one-step commands that require both target agent UID and target session behavior.
