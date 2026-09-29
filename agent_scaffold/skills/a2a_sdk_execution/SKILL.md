---
name: mainsequence-a2a-sdk-execution
description: Use Main Sequence SDK Agent and AgentSession adapters for discovery, session resolution, runtime access, insights, logs, and archival; hand message transport to the owning runtime protocol package.
---

# Main Sequence A2A SDK Execution

## SDK-Owned Operations

Use `mainsequence.client.agent_runtime_models` to:

- discover Agents with declared `Agent.filter()` fields or
  `Agent.semantic_search()`
- resolve an existing session or create a reusable handle through
  `Agent.get_or_create_session()`
- obtain backend-authorized runtime connection data through
  `AgentSession.resolve_runtime_access()`
- inspect session insights, logs, resource usage, and archive state through
  declared model methods

Branch-derived Environment context is resolved internally. Do not accept an
Environment UID from the user and do not make an arbitrary Agent or session the
source of branch routing authority.

## Message Protocol Boundary

Message submission, task polling, cancellation, and runtime file transfer use
the runtime or harness package that owns the advertised protocol.

After resolving `AgentSessionRuntimeAccess`, use the runtime or harness package
that owns the advertised protocol and capabilities. Treat
`runtime_capabilities` as backend-extensible; do not infer unsupported actions
from an older SDK enum or skill.

## Session Rules

- Use exactly one lookup key with `get_or_create_session()`:
  `session_uid` or `handle_unique_id`.
- Creation options apply only to `handle_unique_id`.
- Do not supply credential-owner identity. The backend binds the human or parent
  session according to the authenticated request.
- Treat runtime access tokens as sensitive and short-lived.
- Keep the process's SDK credentials separate from the human represented by a
  request-bound FastAPI identity.

## Verification

Confirm the Agent identity, session UID, backend-returned runtime capability,
and terminal operation state. Preserve raw validation errors when the SDK model
and backend response disagree, without logging credentials.
