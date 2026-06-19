---
name: a2a_communication
description: Canonical guidance for discovering agents and sending session-scoped A2A requests through Main Sequence.
---

# A2A Communication

Use this skill when you need to discover another agent or send a bounded A2A
request to an existing target `AgentSession`.

## Canonical CLI Path

Use the standard A2A CLI send command. The CLI resolves the target runtime
internally and sends a standard A2A message.

## When To Use

- When the user asks which agents can help.
- When another agent may be better suited to answer or assist.
- Before sending a request to an existing target agent session.
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
   Include agent name, stable unique id, and a short capability summary.

## Target Session

Use an existing target `AgentSession` UID. If the target session UID is not
available, get it from the active task context or ask for it before sending A2A
messages.

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
4. Get the target `AgentSession` UID.
5. Run `mainsequence agent session a2a send <target_agent_session_uid> ...`.
6. Parse `message.parts` from the CLI output.

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
