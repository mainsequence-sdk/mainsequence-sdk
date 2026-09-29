---
name: mainsequence-sdk-code-repository-execution
description: Use the thin Main Sequence Python SDK for authentication, Git-native CodeRepository context, and direct platform resource adapters. Route domain workflows to their owning package or platform skill.
---

# Main Sequence SDK CodeRepository Execution

## Scope

Use this skill for code that directly uses the installed `mainsequence` package:

- authentication and endpoint configuration
- Git-native CodeRepository and branch context
- direct platform resource requests
- response parsing, pagination, and error diagnosis
- selecting the package or platform skill that owns adjacent workflow logic

The SDK is a thin adapter. Route table, deployment, and runtime-protocol work
to the installed package or platform skill that owns that contract.

## Authority And Routing

1. Read the installed SDK code and version-aligned documentation for client
   behavior.
2. Use backend-advertised contracts or platform-owned skills for platform
   workflow policy.
3. Use the installed `metatables` package for tables, table updates, DataSources,
   SQLAlchemy contracts, and Alembic.
4. Use the runtime or harness package for an AgentSession message protocol after
   the SDK has resolved the Agent and session runtime access.

## Source Context

The containing Git worktree is the source of repository URL, attached branch,
and commit identity. `get_git_source_context()` reads those facts without a
network request. `get_code_repository_context()` maps them to registered
platform resources when an operation requires that mapping.

Never accept or ask the user for a branch UID or Organization Environment UID.
An unregistered local branch is valid for local work. Only an operation that
requires registered branch or Environment context should fail.

Context is resolved once per process. A branch change affects a subsequent
process, not an already running process with frozen context.

## Client Rules

- Prefer typed models exported by `mainsequence.client`.
- Use declared `filter()`, `get()`, action, and patch methods rather than
  constructing endpoint URLs in application code.
- Let SDK mixins add branch-derived Environment context. Do not add a caller
  parameter that selects the Environment.
- Treat backend response projections as read-only unless the model explicitly
  exposes them in a write contract.
- Preserve backend errors and request evidence when reporting a contract gap.
- Do not recreate removed domain helpers inside application code merely to keep
  an old SDK import working.

## Local Checks

Use the retained CLI only for its actual surface:

```bash
mainsequence version
mainsequence doctor
mainsequence login
mainsequence settings show
```

Inspect `mainsequence --help` before assuming any other command exists.

## Completion Evidence

Report the installed SDK version, Git source context used, relevant typed model
or operation, and the local or live verification result. State explicitly when
live platform verification was unavailable.
