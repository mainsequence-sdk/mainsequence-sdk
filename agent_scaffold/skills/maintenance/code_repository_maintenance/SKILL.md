---
name: code_repository_maintenance
description: Maintain an existing Main Sequence CodeRepository checkout using the SDK-version-matched CLI. Use for inspecting repository state, building or repairing .venv, refreshing local authentication, updating the SDK, refreshing SDK and platform skills plus AGENTS.md, publishing changes with CodeRepository sync, or diagnosing a partially completed maintenance workflow.
---

# Main Sequence CodeRepository Maintenance

Maintain the local CodeRepository through the installed Main Sequence CLI. Treat the
CLI as the canonical implementation of SDK-version-specific filesystem, `uv`,
authentication, scaffold, and Git behavior. Select and sequence commands here;
do not reproduce their Python implementation or replace them with ad hoc shell
workflows.

## Preserve The Boundary

Own:

- local CodeRepository inspection and maintenance sequencing;
- `.venv`, SDK, managed skill, `AGENTS.md`, and Git-release workflows;
- precondition checks, explicit approval gates, and partial-failure diagnosis.

Do not own:

- platform ontology or CodeRepository Blueprint design;
- domain implementation for MetaTables, TimeIndexTableUpdaters, APIs, jobs, or releases;
- backend repository reconciliation;
- MCP authorization policy, OAuth token storage, or access-token extraction;
- repository-owned skills outside `.agents/skills/mainsequence/`.

Route architecture changes to `code_repository_design`, implementation routing to
`sdk_code_repository_execution`, and failure classification to
`maintenance/bug_auditor` when diagnosis extends beyond the maintenance
workflow itself.

The local identity contract is Git-native. The containing repository remote,
attached branch, and exact HEAD commit select the platform CodeRepository and
CodeRepositoryBranch. Never ask the user for a CodeRepositoryBranch UID, persist CodeRepository or
branch identity in `.env`, or use an environment variable as a branch selector.
The CLI resolves platform UIDs internally when a branch-owned API requires them.
The same rule applies inside deployed CodeRepository runtimes: runtime credentials
authorize the backend target but never replace Git source discovery. A missing,
detached, or mismatched deployed checkout is a hard runtime-image error; do not
fall back to credential claims or injected CodeRepositoryBranch values.

## Inspect Before Changing State

1. Read `AGENTS.md` and the relevant repository skills.
2. Confirm the repository root and the user's requested maintenance outcome.
3. Inspect code repository and Git state:

   ```bash
   mainsequence code-repository current --debug --json
   mainsequence code-repository sdk-status --path . --json
   git status --short
   ```

   The `code-repository current` result must report the logical `code_repository_uid`, current
   `git_branch`, `code_repository_branch_uid`, and `code_repository_branch_status=resolved`.
   Treat a detached checkout, unresolved repository, or unresolved/unregistered
   Git branch as a maintenance preflight failure before any
   mutating workflow.

4. Separate existing user changes from changes created by the maintenance
   task. Never assume every uncommitted file belongs to the current task.
5. Read command help when the installed SDK differs from the instructions in
   this skill. The installed CLI is authoritative for its version.

If `mainsequence` is unavailable, stop and report that the SDK CLI must be
installed or provided by the local development integration. Do not use an MCP
bearer token or an unpinned downloaded CLI as a substitute.

## Build Or Repair The Local Environment

Build the CodeRepository environment with:

```bash
mainsequence code-repository build-local-venv --path .
```

The command reads the package Python requirement, resolves `uv`, creates
`.venv`, and synchronizes dependencies. Do not manually parse
`pyproject.toml` or reconstruct those steps.

When an existing `.venv` is incompatible, show the detected mismatch and ask
before replacing it. Only then run:

```bash
mainsequence code-repository build-local-venv --path . --recreate
```

Treat `.venv` as generated state, never as source code or durable repository
documentation.

## Refresh CodeRepository Authentication

Establish CLI authentication before refreshing a CodeRepository. Select exactly one
existing authentication lane:

- When `MAINSEQUENCE_AUTH_MODE=runtime_credential`, run `mainsequence login`.
  The CLI exchanges the injected runtime credential without a browser or a
  refresh-backed user handoff.
- When the coding agent has an authenticated Main Sequence MCP connection but
  the CLI has no session, run `mainsequence login --mcp` in a terminal that can
  remain active. The command asks the configured backend to create the
  handoff, prints a JSON object naming `auth.cli_authorize` and its exact
  `handoff_uid`, and waits. Call that MCP tool with only the printed arguments.
  The backend-issued callback completes the waiting command and the CLI stores
  the normal tracked JWT pair locally.
- When neither lane is available, use the existing interactive browser login.

Never use `--export` with MCP handoff login, pass access or refresh tokens to
the model, invent a callback URI, or substitute a handoff UID from another
terminal. If `auth.cli_authorize` returns an error, preserve the pending local
workflow output and report that exact error rather than starting multiple
handoffs blindly.

Refresh only the CLI-managed runtime authentication entries in the CodeRepository
`.env`:

```bash
mainsequence code-repository refresh-token --path .
```

Require one supported Main Sequence CLI login lane first. Never print,
inspect, summarize, copy, or return access and refresh token values. Do not
attempt to extract the calling MCP host's protected bearer token: the handoff
authorizes a new PKCE grant and the backend returns credentials directly to
the waiting CLI process.

The command preserves unrelated repository configuration and renders only the
current supported authentication shape. It removes legacy token aliases and
all retired repository, branch, and environment identity entries. The Git
checkout supplies source identity; switching branches changes
context on the next process run without rewriting `.env`.

## Update The CodeRepository SDK

Inspect the current status, preview the update, and then update when requested
or required by repository instructions:

```bash
mainsequence code-repository sdk-status --path . --json
mainsequence code-repository update-sdk --path . --dry-run
mainsequence code-repository update-sdk --path .
```

`update-sdk` updates the lock and local environment. It does not publish the
working tree. After the SDK changes, refresh the managed agent context and run
the repository-specific validation required by `AGENTS.md`.

Do not automatically commit or push an SDK update unless the user also asked
to publish the CodeRepository changes.

## Refresh Managed Skills And Instructions

Refresh the SDK-owned and platform-owned skills first, then update the managed
Main Sequence block in `AGENTS.md`:

```bash
mainsequence code-repository update-agent-skills --path .
mainsequence code-repository update AGENTS.md --path .
```

Verify `.agents/skills/mainsequence/PINNED_FROM.txt` after success:

- `sdk_version` must match the installed CodeRepository SDK;
- platform manifest, ontology, and resource hashes must be present;
- repository-owned skills outside `.agents/skills/mainsequence/` must remain
  untouched.

The skill update is staged and atomic. If it fails, report the failing lane and
preserve the previous valid managed tree. Because this operation can update
this skill, reload the refreshed `code_repository_maintenance/SKILL.md` before starting
another maintenance routine.

## Publish With Canonical CodeRepository Sync

Use CodeRepository sync only when the user intends to commit, tag, and push all
reviewed repository changes.

Before execution:

```bash
git status --short
git diff --stat
mainsequence code-repository sync --path . -m "<specific commit message>" --dry-run
```

Review every pending file because the command stages with `git add -A`. Do not
continue when unrelated or unexplained changes would be included.

The command performs the same branch preflight even for `--dry-run`: it rejects
a detached checkout and rejects a Git branch that is not registered under the
logical CodeRepository. Do not bypass that validation or supply a CodeRepositoryBranch UID
manually. Register or select the correct Git branch first.

After that branch preflight, `--dry-run` resolves the existing `uv` executable,
previews its patch version without mutation, requests the backend-owned tag for
that future version, rejects an invalid or existing local tag, prints the
complete plan, and returns without generating an SSH key, querying private
remote refs, changing dependencies or repository files, or mutating Git state.

After review:

```bash
mainsequence code-repository sync --path . -m "<specific commit message>"
```

The canonical command always:

1. selects `mainsequence-<repository-slug>-<first-16-sha256>` from the normalized
   `host[:non-default-port]/repository/path`, never from the repository basename alone and never
   from a legacy basename-only key;
2. previews the `uv` patch version, requests the backend-owned CodeRepositoryBranch tag, and rejects an
   invalid or existing local tag;
3. registers a new or inaccessible key through the owning CodeRepository and verifies a dry-run push with
   that forced identity;
4. queries the exact tag ref on `origin` and stops if it exists or cannot be checked;
5. applies the patch version bump and verifies it matches the preview;
6. runs `uv lock` and `uv sync`;
7. exports locked production requirements;
8. stages and commits the changes;
9. creates the returned annotated tag unchanged; and
10. atomically pushes the explicit branch and tag refs with `--follow-tags`.

Do not offer alternate bump modes, a no-push mode, or a hand-written sequence
of equivalent commands. Do not call `sync-after-commit` or any backend repair
endpoint. The GitHub branch-push webhook owns backend repository
reconciliation.

## Diagnose Partial Completion Before Retrying

Never rerun an entire mutating workflow blindly.

- After an environment failure, inspect `.venv`, the declared Python
  requirement, and the exact failing `uv` output.
- After an SDK update failure, inspect `pyproject.toml`, `uv.lock`, the active
  environment, and the failed command before retrying.
- After a skill refresh failure, keep the previous managed tree and identify
  whether the SDK or platform lane failed.
- After a sync failure, inspect `git status`, `git log -1`, the current package
  version, tags pointing at `HEAD`, the upstream branch, and remote tags before
  deciding which step remains.

A blind sync retry may create another patch bump or collide with an existing
commit or tag.

## Validate And Report

Run only repository-relevant validation discovered from `AGENTS.md`, repository
documentation, and the changed files. Report:

- the maintenance routine performed;
- the code repository path and installed SDK version;
- files or generated state changed;
- validation run and its result;
- whether changes remain local or were committed, tagged, and pushed;
- any remaining authentication, environment, Git, or webhook verification
  gap.

Never claim backend reconciliation succeeded solely because the local push
succeeded. Verify backend state separately when the task requires that claim.
