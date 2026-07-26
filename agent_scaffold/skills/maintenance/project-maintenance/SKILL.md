---
name: project-maintenance
description: Maintain an existing Main Sequence project checkout using the SDK-version-matched CLI. Use for inspecting project state, building or repairing .venv, refreshing local project authentication, updating the SDK, refreshing SDK and platform skills plus AGENTS.md, publishing changes with project sync, or diagnosing a partially completed maintenance workflow.
---

# Main Sequence Project Maintenance

Maintain the local project through the installed Main Sequence CLI. Treat the
CLI as the canonical implementation of SDK-version-specific filesystem, `uv`,
authentication, scaffold, and Git behavior. Select and sequence commands here;
do not reproduce their Python implementation or replace them with ad hoc shell
workflows.

## Preserve The Boundary

Own:

- local project inspection and maintenance sequencing;
- `.venv`, SDK, managed skill, `AGENTS.md`, and Git-release workflows;
- precondition checks, explicit approval gates, and partial-failure diagnosis.

Do not own:

- platform ontology or Project Blueprint design;
- domain implementation for MetaTables, DataNodes, APIs, jobs, or releases;
- backend repository reconciliation;
- MCP authentication or access-token extraction;
- project-owned skills outside `.agents/skills/mainsequence/`.

Route architecture changes to `project_design`, implementation routing to
`sdk_project_execution`, and failure classification to
`maintenance/bug_auditor` when diagnosis extends beyond the maintenance
workflow itself.

## Inspect Before Changing State

1. Read `AGENTS.md` and the relevant repository skills.
2. Confirm the repository root and the user's requested maintenance outcome.
3. Inspect project and Git state:

   ```bash
   mainsequence project current --debug --json
   mainsequence project sdk-status --path . --json
   git status --short
   ```

4. Separate existing user changes from changes created by the maintenance
   task. Never assume every uncommitted file belongs to the current task.
5. Read command help when the installed SDK differs from the instructions in
   this skill. The installed CLI is authoritative for its version.

If `mainsequence` is unavailable, stop and report that the SDK CLI must be
installed or provided by the local development integration. Do not use an MCP
bearer token or an unpinned downloaded CLI as a substitute.

## Build Or Repair The Local Environment

Build the project environment with:

```bash
mainsequence project build_local_venv --path .
```

The command reads the project Python requirement, resolves `uv`, creates
`.venv`, and synchronizes dependencies. Do not manually parse
`pyproject.toml` or reconstruct those steps.

When an existing `.venv` is incompatible, show the detected mismatch and ask
before replacing it. Only then run:

```bash
mainsequence project build_local_venv --path . --recreate
```

Treat `.venv` as generated state, never as source code or durable project
documentation.

## Refresh Project Authentication

Refresh only the CLI-managed runtime authentication entries in the project
`.env`:

```bash
mainsequence project refresh_token --path .
```

Require normal Main Sequence CLI login first. Never print, inspect, summarize,
copy, or return access and refresh token values. Do not attempt to extract the
calling MCP host's protected bearer token. If authentication is unavailable,
stop and request login through the normal CLI or development integration.

## Update The Project SDK

Inspect the current status, preview the update, and then update when requested
or required by repository instructions:

```bash
mainsequence project sdk-status --path . --json
mainsequence project update-sdk --path . --dry-run
mainsequence project update-sdk --path .
```

`update-sdk` updates the lock and local environment. It does not publish the
working tree. After the SDK changes, refresh the managed agent context and run
the repository-specific validation required by `AGENTS.md`.

Do not automatically commit or push an SDK update unless the user also asked
to publish the project changes.

## Refresh Managed Skills And Instructions

Refresh the SDK-owned and platform-owned skills first, then update the managed
Main Sequence block in `AGENTS.md`:

```bash
mainsequence project update_agent_skills --path .
mainsequence project update AGENTS.md --path .
```

Verify `.agents/skills/mainsequence/PINNED_FROM.txt` after success:

- `sdk_version` must match the installed project SDK;
- platform manifest, ontology, and resource hashes must be present;
- project-owned skills outside `.agents/skills/mainsequence/` must remain
  untouched.

The skill update is staged and atomic. If it fails, report the failing lane and
preserve the previous valid managed tree. Because this operation can update
this skill, reload the refreshed `project-maintenance/SKILL.md` before starting
another maintenance routine.

## Publish With Canonical Project Sync

Use project sync only when the user intends to commit, tag, and push all
reviewed project changes.

Before execution:

```bash
git status --short
git diff --stat
mainsequence project sync --path . -m "<specific commit message>" --dry-run
```

Review every pending file because the command stages with `git add -A`. Do not
continue when unrelated or unexplained changes would be included.

After review:

```bash
mainsequence project sync --path . -m "<specific commit message>"
```

The canonical command always:

1. applies a patch version bump;
2. runs `uv lock`;
3. runs `uv sync`;
4. exports locked production requirements;
5. stages and commits the changes;
6. creates an annotated `v<version>` tag; and
7. pushes the branch and tag with `--follow-tags`.

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
- After a sync failure, inspect `git status`, `git log -1`, the current project
  version, tags pointing at `HEAD`, the upstream branch, and remote tags before
  deciding which step remains.

A blind sync retry may create another patch bump or collide with an existing
commit or tag.

## Validate And Report

Run only repository-relevant validation discovered from `AGENTS.md`, project
documentation, and the changed files. Report:

- the maintenance routine performed;
- the project path and installed SDK version;
- files or generated state changed;
- validation run and its result;
- whether changes remain local or were committed, tagged, and pushed;
- any remaining authentication, environment, Git, or webhook verification
  gap.

Never claim backend reconciliation succeeded solely because the local push
succeeded. Verify backend state separately when the task requires that claim.
