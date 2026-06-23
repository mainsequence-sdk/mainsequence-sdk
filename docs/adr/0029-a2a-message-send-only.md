# ADR 0029: A2A Message Send Only

## Status

Accepted

## Context

Agent-to-agent communication must present one user-facing operation: send a
message to an existing backend `AgentSession.uid`.

The backend `AgentSession.uid` is the durable conversation identity. The SDK
uses it as the A2A `message.contextId` on every turn. Runtime access resolution
is an SDK implementation detail and must not become a separate CLI workflow.

## Decision

The canonical A2A send flow is:

1. Build the complete A2A request body, including `message.messageId`.
2. Resolve runtime access through Django if no valid cached access exists.
3. Send the exact body to the standard runtime A2A message endpoint.
4. If runtime credentials are rejected with `401` or `403`, clear cached access,
   resolve fresh access, and retry once with the exact same body and
   `message.messageId`.

The SDK calls Django only for runtime access:

```http
POST /orm/api/agents/v1/sessions/{agent_session_uid}/resolve_runtime_access/
```

The SDK sends the message only to:

```http
POST {rpc_url}/api/a2a/v1/message:send
Content-Type: application/a2a+json
Accept: application/a2a+json
```

The SDK must not call legacy runtime attach/status endpoints or poll runtime
readiness for standard A2A sends.

## Request Shape

Plain text:

```json
{
  "message": {
    "messageId": "msg-client-uuid-1",
    "role": "ROLE_USER",
    "contextId": "0b2701a1-e777-4cfe-8437-b94025f00069",
    "parts": [
      {
        "text": "What can you do?"
      }
    ]
  },
  "configuration": {
    "acceptedOutputModes": ["text/plain"],
    "returnImmediately": false
  }
}
```

Strict dictionary:

```json
{
  "message": {
    "messageId": "msg-client-json-1",
    "role": "ROLE_USER",
    "contextId": "0b2701a1-e777-4cfe-8437-b94025f00069",
    "parts": [
      {
        "text": "Return a JSON dictionary with keys ok and answer."
      }
    ]
  },
  "configuration": {
    "acceptedOutputModes": ["application/json"],
    "returnImmediately": false
  },
  "metadata": {
    "https://mainsequence.ai/a2a/extensions/output-contract/v1": {
      "response_format": {
        "type": "dictionary",
        "strict": true
      },
      "jsonRepairAttempts": 3
    }
  }
}
```

## CLI Contract

The normal CLI command is:

```bash
mainsequence agent session a2a send \
  <agent_session_uid> \
  --message "What can you do?"
```

For a strict dictionary response:

```bash
mainsequence agent session a2a send \
  <agent_session_uid> \
  --message "Return a JSON dictionary with keys ok and answer." \
  --strict-dictionary
```

For retrying an exact logical message after a timeout or disconnect:

```bash
mainsequence agent session a2a send \
  <agent_session_uid> \
  --message-id msg-client-uuid-1 \
  --message "What can you do?"
```

The CLI must not require a prior runtime resolve, attach, prewarm, status, or
readiness command before `a2a send`.

If the CLI generates the `message.messageId` and the send fails, it must print
the generated message id so the caller can retry the exact same logical message.

## Caching

The SDK and CLI may cache resolved runtime access as an optimization.

Cache requirements:

- Scope persistent CLI cache by backend, authenticated user, and
  `agent_session_uid`.
- Prefer backend `expires_at` when returned.
- Apply an expiry safety skew before using cached credentials.
- If no backend expiry exists, use a short fallback TTL.
- Never print cached runtime tokens.
- Clear cached access on runtime `401` or `403`.

Cache behavior must not change the A2A message body. Retry after credential
refresh must reuse the original `message.messageId`.

## Consequences

- Users and agents only need the target backend `AgentSession.uid`.
- Cold start is handled behind `message:send`.
- Repeated CLI sends can reuse cached runtime access without exposing runtime
  credentials.
- Standard A2A send no longer depends on runtime session status endpoints.
