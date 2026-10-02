---
name: mainsequence-code-repository-maintenance
description: Maintain an existing Main Sequence CodeRepository checkout using the SDK-version-matched CLI. Use for inspecting repository state, building or repairing .venv, refreshing local authentication, performing explicitly requested SDK, skill, or AGENTS.md updates, refreshing dependency files with CodeRepository sync, publishing changes with Git, or diagnosing a partially completed maintenance workflow.
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
- `.venv`, SDK, managed skill, `AGENTS.md`, dependency sync, and Git publishing
  workflows;
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

## Sync Dependencies After A Dependency Change

Run CodeRepository sync only after the dependencies changed, for example after
editing `[project.dependencies]` in `pyproject.toml`:

```bash
mainsequence code-repository sync --path .
```

In the project root it runs `uv lock`, `uv sync`, and the locked runtime export
`uv export --locked --no-dev --no-hashes` into `requirements.txt`. It accepts
only `--path`. It sends no request to the platform, creates no SSH or deploy
key, changes no version, and runs no `git add`, `commit`, `tag`, or `push`.
Review the changed `uv.lock` and `requirements.txt` and publish them with the
dependency change.

## Publish With Git

Publish only when the user intends to commit and push the reviewed changes.
Commit and push with Git as usual:

```bash
git status --short
git diff --stat
git add <reviewed paths>
git commit -m "<specific commit message>"
git push
```

Review every pending file and stage only the files that belong to the change.
Do not continue when unrelated or unexplained changes would be included. Push
to the attached branch that `mainsequence code-repository current` resolved; a
detached checkout or an unregistered branch is a preflight failure.

The platform deploys from the push according to the repository's
`.mainsequence/workflows/*.yaml`. A declaration's `tag_regex` decides which
pushes deploy:

- omitted or `null`: every push to the branch deploys;
- a regular expression: a push deploys only when a matching tag points at the
  branch's latest commit.

Automatic deployment and `tag_regex` are set in the workflow file only; a
`ResourceRelease` or `Job` update does not accept them. The Main Sequence
platform does not create tag names and the CLI does not tag. Versions and
release tags belong to the repository: the version is raised in
`pyproject.toml` (for example `uv version --bump patch`) in the commit to
release, and the repository's own CI creates the tag. Do not create or push a
release tag by hand unless the user asks for it. When the user wants tag-based
releases and the repository has no such CI yet, propose a workflow like this
example and wait for approval before adding it:

```yaml
# Example: .github/workflows/release.yml in the CodeRepository
name: release
on:
  push:
    branches: [main]
concurrency:
  group: release-main
  cancel-in-progress: false
permissions:
  contents: write
jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - uses: astral-sh/setup-uv@v6
      - run: uv sync --locked
      - run: uv run pytest
      - name: Tag the version declared in pyproject.toml
        run: |
          TAG="v$(uv version --short)"
          if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
            echo "$TAG already exists; nothing to release"
            exit 0
          fi
          git tag "$TAG" "$GITHUB_SHA"
          git push origin "refs/tags/$TAG"
```

With that example, a `tag_regex` such as `^v[0-9]+\.[0-9]+\.[0-9]+$` deploys
only the commits CI tagged. Details:
<https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/infrastructure/scheduling_jobs/>

Do not call `sync-after-commit` or any backend repair endpoint. The GitHub
branch-push webhook owns backend repository reconciliation.

## Diagnose Partial Completion Before Retrying

Never rerun an entire mutating workflow blindly.

- After an environment failure, inspect `.venv`, the declared Python
  requirement, and the exact failing `uv` output.
- After an SDK update failure, inspect `pyproject.toml`, `uv.lock`, the active
  environment, and the failed command before retrying.
- After a skill refresh failure, keep the previous managed tree and identify
  whether the SDK or platform lane failed.
- After a dependency sync failure, inspect `pyproject.toml`, `uv.lock`, and the
  exact failing `uv` output; the command stops at the first failing step.
- After a push failure, inspect `git status`, `git log -1`, and the upstream
  branch before retrying.

## Validate And Report

Run only repository-relevant validation discovered from `AGENTS.md`, repository
documentation, and the changed files. Report:

- the maintenance routine performed;
- the code repository path and installed SDK version;
- files or generated state changed;
- validation run and its result;
- whether changes remain local or were committed and pushed;
- any remaining authentication, environment, Git, or webhook verification
  gap.

Never claim backend reconciliation succeeded solely because the local push
succeeded. Verify backend state separately when the task requires that claim.
