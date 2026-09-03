# ADR 0029: A2A Message Send Only

> Amended by Platform Agents ADR-027. Runtime access now carries the
> authoritative `runtime_interaction` admission decision and diagnostic
> `runtime_presence`. An active SDK send re-resolves access only while the
> backend reports `checking`, `starting`, `waking`, or `updating`, using the
> backend `retry_after_ms`; ready and terminal outcomes never create a polling
> loop.

## Status

Superseded by Astro ADR 47, Explicit A2A Response Kind

## Context

Agent-to-agent communication must present one user-facing operation: send a
message to an existing backend `AgentSession.uid`.

The backend `AgentSession.uid` is the durable conversation identity. The SDK
uses it as the A2A `message.contextId` on every turn. Runtime access resolution
is an SDK implementation detail and must not become a separate CLI workflow.

## Decision

The canonical A2A send flow is:

1. Build the complete A2A request body, including `message.messageId`.
2. Resolve runtime access through Django if no valid cached access exists and
   enforce `runtime_interaction.can_submit` as the sole admission decision.
3. If the decision is transient, re-resolve at `retry_after_ms` only for the
   lifetime of this active call; stop on `can_submit=true` or a terminal state.
4. Send the exact body to the standard runtime A2A message endpoint.
5. If runtime credentials are rejected with `401` or `403`, clear cached access,
   resolve fresh access, and retry once with the exact same body and
   `message.messageId`.

For the default direct Message path, the SDK calls Django only for runtime
access:

```http
POST /api/v1/agent-sessions/{agent_session_uid}/resolve-runtime-access/
```

The successful token response identifies the target with
`coding_agent_service_uid` and returns its canonical `rpc_url`. The SDK treats
that URL as opaque: it must not derive a host from tenancy, a service name, a
numeric identifier, or a remembered subdomain. A blocked runtime returns
`mode: "unavailable"` with null `rpc_url` and `token` values. The SDK never
opens an A2A connection while `can_submit=false`; terminal outcomes are
reported without automatic retry.

The SDK sends the message only to:

```http
POST {rpc_url}/api/a2a/v1/message:send
Content-Type: application/a2a+json
Accept: application/a2a+json
A2A-Extensions: https://mainsequence.ai/a2a/extensions/response-kind/v1
```

The SDK must not call legacy runtime attach/status endpoints or perform idle,
fixed-cadence, or provider readiness polling. Its only wait is the bounded,
server-directed re-resolution of the existing runtime-access route while an
active send is blocked by a backend-declared transient interaction.

Under the superseding ADR 47 contract, explicit Task selection additionally
uses backend Agent discovery (and, when necessary, the session's `agent_uid`)
to verify that the target advertises Task support before the runtime request.
That discovery call is not part of direct Message execution.

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
    "responseKind": "message"
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
    "responseKind": "message"
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

ADR 47 replaces the timing Boolean with an explicit result contract. The SDK
uses `response_kind="message"` for direct execution and
`response_kind="task"` only when Agent discovery advertises Task support. Task
mode returns a typed Task and is followed through the SDK get, wait, and cancel
helpers; direct Message mode does not create an A2A Task.

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
the generated message id so the caller can preserve request identity if a retry
is explicitly chosen. Under the superseding ADR 47 contract, direct `message`
execution has no durable replay receipt and must not be retried automatically;
the first turn may already have executed. Use `task` when durable recovery is
required.

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
- Never let a cached token bypass its accompanying
  `runtime_interaction.can_submit` decision.

Cache behavior must not change the A2A message body. Retry after credential
refresh must reuse the original `message.messageId`.

## Consequences

- Users and agents only need the target backend `AgentSession.uid`.
- Cold start is handled by Django's backend-owned wake before `message:send`.
- Repeated CLI sends can reuse cached runtime access without exposing runtime
  credentials.
- Standard A2A send no longer depends on runtime session status endpoints.
