---
name: mainsequence-code-repository-maintenance
description: Maintain Main Sequence SDK authentication, endpoint configuration, diagnostics, and dependency version only when the user explicitly requests the corresponding change.
---

# Main Sequence CodeRepository Maintenance

## Scope

This skill covers the retained core CLI:

```bash
mainsequence login
mainsequence logout
mainsequence settings show
mainsequence settings set-backend <url>
mainsequence settings reset
mainsequence version
mainsequence doctor
```

Inspect `mainsequence --help` for the installed version. Do not use historical
CodeRepository deployment, synchronization, agent-message, or skill-update
commands that are absent from the thin SDK.

## Authentication

- Use `mainsequence login` for an interactive user session.
- Use `mainsequence login --mcp` only when an authenticated Main Sequence MCP
  session is already available.
- Use `--access-token` and `--refresh-token` together when importing a JWT pair.
- Use `mainsequence doctor` to report endpoint and persistence state.
- Never print or commit credential values.

Persistent credentials use the operating-system credential store. When Linux
has no available Secret Service or KWallet backend, the CLI must not fall back
to plaintext token persistence.

## Updates Are Explicit

Do not update the SDK, lockfile, scaffold, or any independent skill namespace
unless the user requested that exact update. Use the repository's package
manager for dependency changes and preserve its lockfile policy.

`mainsequence.scaffold_skills.copy_scaffold_skills()` is a reusable library
utility for an owning library or tool. Its presence does not authorize an agent
to overwrite `.agents/skills/` during unrelated work.

## Verification

After an authentication or endpoint change, verify `mainsequence doctor`
without exposing secrets. After an SDK dependency update, report the resolved
version and run the repository checks appropriate to that change.
