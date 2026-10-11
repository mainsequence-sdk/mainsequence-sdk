# ADR 0029: A2A Message Send Only

> Historical decision: the SDK-owned A2A workflow described here was superseded by [ADR 0034](0034-extract-metatables-python-package.md). Follow the current [removal migration guide](../migrations/metatables-sdk-removal.md) for supported imports and commands.

> Amended by Platform Agents ADR-027. Runtime access now carries the
> authoritative `runtime_interaction` admission decision and diagnostic
> `runtime_presence`. An active SDK send re-resolves access only while the
> backend reports `checking`, `starting`, `waking`, or `updating`, using the
> backend `retry_after_ms`; ready and terminal outcomes never create a polling
> loop.

Amended 2026-10-11 for [SDK issue #214](https://github.com/mainsequence-sdk/mainsequence-sdk/issues/214):
a numeric `timeout` on `send_a2a_message`, `get_a2a_task` and `cancel_a2a_task`
is one total budget for the whole call, response body included, as for every
other SDK call. A `(connect, read)` tuple keeps its phase limits with a total
equal to their sum, and the default stays `(5, 900)`. The budget can only
shorten a wait; it cannot extend a platform limit. See
[Timeouts](#timeouts).

Amended 2026-10-11 for [SDK issue #216](https://github.com/mainsequence-sdk/mainsequence-sdk/issues/216):
the re-resolution while runtime access is transient now ends with the call's
total budget. When the platform's next `retry_after_ms` no longer fits in it,
the SDK raises `AgentRuntimeNotReadyError` and sends nothing; before, the wait
had no deadline. See step 3 of the [Decision](#decision) and
[Timeouts](#timeouts).

Amended 2026-10-11 for the platform Message limit: a direct Message is
answered within the platform's Message limit, 90 seconds, or the target Agent
stops the turn and answers `504` with the A2A `google.rpc` error whose
`ErrorInfo` has reason `MESSAGE_RESPONSE_LIMIT_EXCEEDED` and domain
`mainsequence.ai`. The SDK raises `A2AMessageLimitExceededError`, never retries
it and never turns it into a Task; longer work uses Task mode. The sentence
that called the re-resolution the SDK's only wait now also names the wait for
the Agent's answer. See [Message limit](#message-limit).

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
   lifetime of this active call and within its total budget; stop on
   `can_submit=true` or a terminal state. When the next re-resolution no longer
   fits in the budget, raise `AgentRuntimeNotReadyError` without sending.
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
fixed-cadence, or provider readiness polling. Its only wait before sending is
the server-directed re-resolution of the existing runtime-access route while an
active send is blocked by a backend-declared transient interaction, and that
wait ends with the call's total budget. After sending, a direct Message waits
for the Agent's answer, which the platform gives within its
[Message limit](#message-limit); a Task is followed only through the get, wait
and cancel helpers.

Under the superseding ADR 47 contract, explicit Task selection additionally
uses backend Agent discovery (and, when necessary, the session's `agent_uid`)
to verify that the target advertises Task support before the runtime request.
That discovery call is not part of direct Message execution.

### Timeouts

The whole call shares one total budget: the caller's `timeout`, or `(5, 900)`
when it is omitted. A number is a total in seconds; a `(connect, read)` tuple
keeps those phase limits and has a total equal to their sum. The budget covers
Agent discovery for Task mode, runtime access resolution and the wait for a
transient runtime, the runtime request and its whole response, so a runtime
that sends its answer slowly cannot extend it. If the runtime is still
transient when the platform's next retry no longer fits, the call raises
`AgentRuntimeNotReadyError` and nothing is sent. When the budget runs out later
the call raises `requests.Timeout`; the message may already have reached the
runtime and is not sent again. `get_a2a_task` and `cancel_a2a_task` follow the
same rule. A budget can only shorten a wait; it cannot extend a platform limit.

### Message limit

A direct Message holds one HTTP request open until the Agent answers. The
platform answers it within its Message limit, 90 seconds. This is a platform
characteristic: no client setting changes it, and anything longer uses Task
mode. When the limit expires, the target Agent stops the turn, does not undo
work the turn already did, and answers HTTP `504` with media type
`application/a2a+json`:

```json
{
  "error": {
    "code": 504,
    "status": "DEADLINE_EXCEEDED",
    "message": "The Agent did not answer the Message within 90 seconds. Use Task mode for longer work.",
    "details": [
      {
        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
        "reason": "MESSAGE_RESPONSE_LIMIT_EXCEEDED",
        "domain": "mainsequence.ai",
        "metadata": {"limitSeconds": "90"}
      }
    ]
  }
}
```

For a direct Message the SDK raises `A2AMessageLimitExceededError`, a
`ServerError` whose `limit_seconds` comes from `metadata.limitSeconds`. Its
message says that the Agent did not answer within the platform's limit and that
longer work uses `response_kind="task"` (CLI `--response-kind task`). The SDK
never retries it automatically and never converts it into a Task. Any other
`502` or `504`, for example from a gateway, keeps the ordinary `ServerError`.

A `timeout` can only shorten the SDK's own wait. A value above the limit does
not extend it, and a shorter one ends the SDK's wait while the Agent may go on
working until the limit.

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

The public A2A v1 request role is always `ROLE_USER`. A successful direct
Message must return `ROLE_AGENT`, a non-blank string `messageId`, a non-empty
`parts` array, and the same target `AgentSession.uid` as its `contextId`. The
SDK rejects an invalid Message envelope or a different session context. Text,
structured data, file Parts, metadata, extension URIs, and additional Message
fields are preserved; validation does not require a text-only answer.

`ROLE_REQUESTER` and `ROLE_RESPONDER` are internal Main Sequence binding
directions, not public A2A v1 roles. The runtime explicitly translates
`ROLE_USER` to `ROLE_REQUESTER` on entry and `ROLE_RESPONDER` to `ROLE_AGENT`
on exit. The SDK neither emits nor accepts internal direction values at this
public HTTP boundary. This amends the older role contract from issue #110;
issue #211 records the mismatch after the runtime introduced that translation.
See the [A2A v1 Message and Role definitions](https://a2a-protocol.org/v1.0.1/specification/#414-message).

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
