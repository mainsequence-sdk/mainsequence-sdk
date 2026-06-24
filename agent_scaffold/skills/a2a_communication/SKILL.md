---
name: a2a_communication
description: Canonical guidance for discovering agents and sending session-scoped A2A requests through Main Sequence.
---

# A2A Communication

Use this skill when you need to discover another agent, create or resolve a
target `AgentSession`, and send a bounded A2A request.

## Canonical CLI Path

Use the standard A2A CLI send command. The CLI resolves the target runtime
internally and sends a standard A2A message.

## When To Use

- When the user asks which agents can help.
- When another agent may be better suited to answer or assist.
- Before sending a request to a target agent session.
- When a request explicitly arrives through the A2A channel.

## Core Rules

- A2A communication is scoped to an `AgentSession` UID.
- Use the target `AgentSession` UID for chat.
- Do not expose runtime credentials in prompts, logs, or user-facing answers.
- Do not use commands that require both target agent UID and target session UID for chat.
- Use `--strict-dictionary` when the target answer must be machine-parseable JSON.
- Do not send or request reasoning/thinking traces; standard A2A responses expose only message parts.

## Discovery Flow

1. Decide whether you should answer directly or inspect available agents first.
2. Build a bounded discovery prompt with the requested capability and expected output shape.
3. Run:

```bash
mainsequence agent search "<discoveryPrompt>" --limit 10 --json
```

4. Treat the CLI output as authoritative.
5. Prefer the highest `combined_score` when present.
6. If the user asked only which agents are available, summarize the candidates and stop.
   Include agent name, agent UID, and a short capability summary.

## Target Session

After selecting the target agent, create or resolve a target session:

```bash
mainsequence agent session get_or_create \
  <target_agent_uid> \
  --handle-unique-id <stable_handle_unique_id> \
  --name "<human_readable_session_name>" \
  --json
```

Use the returned session `uid` as the target `AgentSession` UID.

If this A2A request originates from an existing caller session, include the
parent session UID:

```bash
mainsequence agent session get_or_create \
  <target_agent_uid> \
  --handle-unique-id <stable_handle_unique_id> \
  --parent-session-uid <caller_agent_session_uid> \
  --name "<human_readable_session_name>" \
  --json
```

Handle rules:

- Repetitive workflow: use a stable semantic handle, for example `portfolio-review-q2-2026`.
- One-off delegation: use a fresh task-specific handle, for example `a2a-risk-summary-<uuid>`.
- Retry of the same session creation step: reuse the same handle.
- New user turn in the same target session: reuse the returned session UID, not a new handle.

## Send A2A Chat

For JSON-shaped answers, use:

```bash
mainsequence agent session a2a send \
  <target_agent_session_uid> \
  --message "Return a JSON object with keys: summary, risk_level, next_action." \
  --strict-dictionary
```

For plain text answers, use:

```bash
mainsequence agent session a2a send \
  <target_agent_session_uid> \
  --message "Summarize the current portfolio drift."
```

For a second message to the same target session, send another chat command
with the same target `AgentSession` UID:

```bash
mainsequence agent session a2a send \
  <target_agent_session_uid> \
  --message "Now identify the highest priority follow-up."
```

## Handle Shortcut Helper

For repeated communication with the same target agent, use a stable handle. On
first use, include the target agent UID so the CLI can create or resolve the
target session and cache the returned session UID:

```bash
mainsequence agent session a2a send \
  <stable_handle_unique_id> \
  --target-agent-uid <target_agent_uid> \
  --message "Return a JSON object with keys: summary, risk_level, next_action." \
  --strict-dictionary
```

`--name` is optional and only affects the session if the backend creates it:

```bash
mainsequence agent session a2a send \
  portfolios \
  --target-agent-uid <target_agent_uid> \
  --name "Portfolio analysis" \
  --message "Return a JSON object with keys: summary, risk_level, next_action." \
  --strict-dictionary
```

After the handle is cached, reuse it without the target agent UID:

```bash
mainsequence agent session a2a send \
  portfolios \
  --message "Now identify the highest priority follow-up." \
  --strict-dictionary
```

## Python Usage

In Python, use the backend handle to get or create the target session, then
reuse the returned session UID for every message in the same conversation. The
binding is:

```text
handle_unique_id -> AgentSession.uid -> A2A message.contextId
```

Direct SDK flow:

```python
from mainsequence.client.agent_runtime_models import Agent, AgentSession


agent = Agent.get_by_uid(target_agent_uid)
session = agent.get_or_create_session(
    handle_unique_id="portfolios",
    name="Portfolio analysis",
)

response = AgentSession.send_a2a_message(
    session.uid,
    message="Return a JSON object with keys: summary, risk_level, next_action.",
    strict_dictionary=True,
)
```

For another message in the same conversation, reuse the same `session.uid`:

```python
response = AgentSession.send_a2a_message(
    session.uid,
    message="Now identify the highest priority follow-up.",
    strict_dictionary=True,
)
```

Reusable helper:

```python
from mainsequence.client.agent_runtime_models import Agent, AgentSession


def send_to_agent_handle(
    target_agent_uid: str,
    handle_unique_id: str,
    message: str,
    *,
    name: str | None = None,
    strict_dictionary: bool = False,
):
    agent = Agent.get_by_uid(target_agent_uid)
    session = agent.get_or_create_session(
        handle_unique_id=handle_unique_id,
        name=name,
    )

    return AgentSession.send_a2a_message(
        session.uid,  # The SDK sends this as A2A message.contextId.
        message=message,
        strict_dictionary=strict_dictionary,
    )
```

Use the same `handle_unique_id` for the same target conversation. Use a new
handle for a different task or conversation.

## Response Handling

- Parse the CLI output as the standard A2A JSON response.
- Consume only `message.parts` from the A2A response.
- Do not depend on reasoning, tool traces, runtime paths, or transport metadata.
- If a send times out or disconnects and must be retried, reuse the same
  `--message-id` value so the target can treat it as the same logical message.
- If the CLI generated the message id and the send failed, reuse the
  `A2A message id for exact retry` value printed with the error.

## Deterministic Execution Path

1. Build the discovery prompt.
2. Run `mainsequence agent search "<discoveryPrompt>" --limit <n> --json`.
3. Select the target agent.
4. Create or resolve the target session with `mainsequence agent session get_or_create`.
5. Use the returned session `uid`.
6. Run `mainsequence agent session a2a send <target_agent_session_uid> ...`.
7. Parse `message.parts` from the CLI output.

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

## Do Not

- Do not let A2A broaden your scope.
- Do not replace CLI discovery with ad hoc local prompt-file inspection.
- Do not send another agent work when discovery alone was requested.
- Do not call lower-level transports or extract runtime credentials.
