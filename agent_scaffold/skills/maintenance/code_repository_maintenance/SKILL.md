---
name: mainsequence-code-repository-maintenance
description: Maintain Main Sequence authentication, local CodeRepository checkouts, Python environments, Git synchronization, and SDK-owned scaffold content when explicitly requested.
---

# Main Sequence CodeRepository Maintenance

## Scope

This skill covers the core CLI and retained CodeRepository development operations:

```bash
mainsequence login
mainsequence logout
mainsequence settings show
mainsequence settings set-backend <url>
mainsequence settings reset
mainsequence version
mainsequence doctor
mainsequence code-repository set-up-locally <code_repository_uid> [--branch <branch>]
mainsequence code-repository refresh-token --path .
mainsequence code-repository build-local-venv --path .
mainsequence code-repository freeze-env --path .
mainsequence code-repository update-sdk --path .
mainsequence code-repository update AGENTS.md --path .
mainsequence code-repository update-agent-skills --path .
mainsequence code-repository open-signed-terminal --path .
mainsequence code-repository sync --path . -m "<message>"
```

Inspect `mainsequence --help` and `mainsequence code-repository --help` for the
installed version. Docker/deployment and agent-message commands remain retired.

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

## Git-Native Context

Commands operating on an existing checkout derive repository and branch identity
from that checkout and the process-frozen SDK context. Never ask for or inject an
Organization Environment UID or a runtime branch override. `set-up-locally
--branch` selects what Git branch to clone before the checkout exists; subsequent
commands use the actual checked-out branch.

`sync --dry-run` performs read-only release and collision preflight. It must not
create an SSH key, register a deploy key, modify files, create a commit or tag, or
push. Run mutating synchronization only when the user explicitly requests it.

## Scaffold Maintenance

`update AGENTS.md` changes only the Main Sequence managed block when the markers
are present. `update-agent-skills` reads SDK skills from the target checkout's
installed `mainsequence` package, reads platform-owned skills from the
authenticated MCP catalog, validates provenance and collisions, and replaces only
`.agents/skills/mainsequence`. The command writes `PINNED_FROM.txt` with both SDK
and platform provenance and must preserve independent skill namespaces.

## Verification

After an authentication or endpoint change, verify `mainsequence doctor` without
exposing secrets. After environment, SDK, scaffold, or synchronization work,
report the exact command, resolved SDK version, and relevant repository checks.
