---
name: mainsequence-project
description: Use this skill for MainSequence-specific work in this Astro-scaffolded repository. This skill is scaffolded into newly created MainSequence projects together with AGENTS.md and the astro/ files so an agent can resume work with a clean separation between intent, current state, stable facts, and history. It does not define SDK behavior; it enforces a docs-first workflow against the latest MainSequence documentation and explains how to maintain astro/brief.md, astro/tasks.md, astro/record.md, astro/status.md, and astro/journal.md.
---

# MainSequence Project Runbook

Use this skill only for MainSequence-specific tasks in this repository.

## Why This Exists

This skill is copied into newly created MainSequence projects as part of the Astro scaffold.
Its purpose is to keep agent work reproducible by separating:

- goal and acceptance criteria in `astro/brief.md`
- current implementation work in `astro/tasks.md`
- stable project facts in `astro/record.md`
- latest verified state in `astro/status.md`
- historical investigation notes in `astro/journal.md`

Do not treat this skill as a frozen copy of MainSequence behavior.

The expected success condition for a task should also be written down explicitly, not implied.

## First Principle

The latest MainSequence docs are the source of truth for SDK, CLI, DataNode, jobs, artifacts,
RBAC, dashboard, release, market, portfolio, instrument, and platform behavior.

Canonical docs root:
`https://mainsequence-sdk.github.io/mainsequence-sdk/`

## Read Order

Before implementation, debugging, validation, or documentation work:

1. Read `AGENTS.md`.
2. Read the latest relevant MainSequence docs.
3. Read `astro/brief.md`.
4. Read `astro/status.md`.
5. Read `astro/tasks.md`.
6. Read `astro/record.md`.
7. If debugging or resuming, check `astro/journal.md`.

## Route by Task

Use the latest relevant documentation section for the task at hand.

Typical routing:
- project setup, local environment, CLI:
  tutorial setup docs plus CLI docs
- DataNodes:
  tutorial data-node chapters plus `knowledge/data_nodes.md`
- orchestration, jobs, schedules, images:
  orchestration docs
- constants, secrets, RBAC, releases:
  RBAC tutorial plus infrastructure docs
- file-based workflows:
  artifacts docs
- dashboards:
  dashboard docs
- markets, assets, portfolios, instruments:
  the relevant domain docs

## Workflow

1. Read `AGENTS.md`.
2. Read the latest relevant MainSequence docs.
3. Confirm the goal and acceptance criteria in `astro/brief.md`.
4. Confirm the latest known state in `astro/status.md`.
5. Confirm current priorities in `astro/tasks.md`.
6. Confirm project identifiers, checkout path, and orchestration notes in `astro/record.md`.
7. If there is a failure or repeated issue, check `astro/journal.md`.
8. Compare the current implementation against the latest docs.
9. Confirm project context with `mainsequence project current --debug`.
10. Run `mainsequence project refresh_token --path .` before validations or live checks.
11. If git push or pull is required, use `mainsequence project open-signed-terminal <PROJECT_ID>`.
12. If installed CLI behavior appears behind the docs, run `mainsequence project update-sdk --path .`.
13. Verify real platform state with the CLI or platform tooling when platform facts matter.
14. Update the Astro files appropriately.

## Define Success Up Front

Before implementation or validation, make the success condition explicit in `astro/brief.md`.

State:
- what should exist or work at the end
- what checks will prove it
- which platform objects or outputs must be verified
- what remains out of scope

A task is not complete just because code was edited. It is complete when the intended result is produced and verified.

## Astro File Update Rules

### Update `astro/brief.md` when:
- user intent changes
- scope changes
- project goal changes
- acceptance criteria changes
- success definition changes

### Update `astro/tasks.md` when:
- new actionable work is discovered
- priorities change
- a task is completed, blocked, or superseded

### Update `astro/record.md` when:
- project id or project name changes
- checkout path changes
- orchestration notes or stable operational identifiers change

### Update `astro/status.md` when:
- new evidence is checked
- latest state changes
- blockers appear or are resolved
- failures change the current situation
- next actions change
- the task now meets or no longer meets the stated success condition

Keep `astro/status.md` current, not historical.

### Update `astro/journal.md` when:
- a meaningful implementation milestone is reached
- a failure occurs
- a repeated issue is investigated
- a possible MainSequence SDK or platform issue is found
- important historical context should be preserved

Keep `astro/journal.md` append-only.

## Repo-Specific Rules

- Keep all formal docs under `docs/`.
- Keep `docs/SUMMARY.md` aligned with the docs structure.
- Keep reusable dashboard UI under `dashboards/components/`.
- Keep launcher scripts under `scripts/`.
- Keep tests under `tests/`.
- Keep recurring schedules in root-level `scheduled_jobs.yaml` when the project has shared recurring jobs.
- Manage project dependencies with `uv` instead of editing dependency files by hand.
- Do not hardcode machine-specific local paths in reusable docs or instructions.
- Do not hide failures.
- Prefer strict code.
- Fail fast in `DataNode` update paths.

## DataNode Safety

For any DataNode change:
- re-read the relevant current docs first
- keep the identifier and schema stable unless you are intentionally publishing a new dataset
- split config correctly across dataset meaning, updater scope, and runtime knobs
- keep updates incremental and use `UpdateStatistics`
- keep dependencies deterministic and out of `update()`
- implement metadata for production-quality tables
- do not log secrets
- always do the first development or test run in an explicit namespace before any non-namespaced run
- use `hash_namespace(...)` only for isolated testing and experimentation
- for high-volume nodes, test in a test namespace and smaller time range first

## Jobs, Releases, and Verification

- Sync the project before job or release work when repository state matters.
- For shared recurring jobs, prefer `scheduled_jobs.yaml` plus `mainsequence project schedule_batch_jobs`.
- Use project images when reproducibility matters.
- For deployed resources, verify the current project-resource and release workflow from the docs instead of assuming local-only files are deployable.
- Verify jobs, runs, logs, images, resources, and releases with the CLI instead of assuming they exist.


## Documentation Sync

When code, workflows, or platform assumptions change:
- update `docs/`
- keep `README.md` aligned with the docs map
- create follow-up tasks for unresolved documentation mismatches

## Possible MainSequence Issues

If behavior appears to be an SDK, docs, or platform problem:
- record what failed
- explain why it may be a MainSequence issue
- suggest a concrete improvement
- append the history to `astro/journal.md`
- track open work in `astro/tasks.md`
- reflect the current blocker and next action in `astro/status.md`
