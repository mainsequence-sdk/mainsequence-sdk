---
name: mainsequence-code-repository-maintenance
description: Maintain an existing Main Sequence CodeRepository checkout using the SDK-version-matched CLI. Use for inspecting repository state, building or repairing .venv, refreshing local authentication, performing explicitly requested SDK, skill, or AGENTS.md updates, publishing changes with CodeRepository sync, or diagnosing a partially completed maintenance workflow.
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
- domain implementation for data packages, APIs, jobs, or releases;
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

## Refresh Authentication

Authentication belongs to the machine, not to a CodeRepository. Establish the
CLI session through exactly one existing authentication lane:

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

The session is one record per backend in the operating system credential
store. A login made from any CodeRepository serves every other one on the
machine, and the SDK reads the saved session when it is imported. No
CodeRepository holds a copy: `.env` has the backend endpoint and no credential.

Renew the saved session and confirm that it works:

```bash
mainsequence refresh-token
```

The command takes no path, because the session does not belong to a checkout.
It renews the access token, saves it, and reports the session without printing
a token value. When the directory it runs in has a `.env` with an access token,
a refresh token, or a runtime credential left by an earlier version or another
tool, it removes those entries and reports them by name only; it changes
nothing else in that file.

Require one supported Main Sequence CLI login lane first. Never print,
inspect, summarize, copy, or return access and refresh token values. Do not
attempt to extract the calling MCP host's protected bearer token: the handoff
authorizes a new PKCE grant and the backend returns credentials directly to
the waiting CLI process.

Never write a token into `.env` to make a tool work. `mainsequence auth status`
reports whether a session exists, when it expires, and whether the process
takes its credentials from its environment or from the saved session; it
prints no token value. `mainsequence auth token` exists for local tools that
consume a short-lived access token programmatically. Do not run it to read a
token into the conversation.

The Git checkout supplies source identity; switching branches changes context
on the next process run without rewriting `.env`.

## Update The CodeRepository SDK

Run an SDK update only when the user explicitly requests it. A newer available
version, documentation mismatch, or non-trivial task is not permission to
mutate the environment. For an explicit update, inspect the current status,
preview it, and then update:

```bash
mainsequence code-repository sdk-status --path . --json
mainsequence code-repository update-sdk --path . --dry-run
mainsequence code-repository update-sdk --path .
```

`update-sdk` updates the lock and local environment. It does not publish the
working tree or authorize a managed-skill or `AGENTS.md` refresh. Report any
resulting pin mismatch and run those updates only when the user explicitly
requests them.

Do not automatically commit or push an SDK update unless the user also asked
to publish the CodeRepository changes.

## Refresh Managed Skills And Instructions

Run either managed-scaffold command only when the user explicitly requests that
specific update. When both are requested, refresh the SDK-owned skills first,
then update the managed Main Sequence block in
`AGENTS.md`:

```bash
uv run --offline mainsequence code-repository update-agent-skills --path .
uv run --offline mainsequence code-repository update AGENTS.md --path .
```

Verify `.agents/skills/mainsequence/PINNED_FROM.txt` after success:

- `pinned_version` must match the installed CodeRepository SDK;
- the namespace must contain exactly the installed SDK's skill folders;
- repository-owned skills outside `.agents/skills/mainsequence/` must remain
  untouched.

The SDK owns the whole `mainsequence` namespace. Copying it requires no login
or platform access and deletes obsolete folders, including retired table
workflow skills. Do not preserve old or platform-owned content in that folder.
The installed `metatables` package owns table workflows in its own namespace.

When the user requests platform skills, use the separate authenticated command:

```bash
mainsequence code-repository update-platform-skills --path .
```

It owns only `.agents/skills/mainsequence_platform/` and records the platform
manifest, ontology, and resource hashes there. A backend or authentication
failure in this command must not block a local SDK skill copy.

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
