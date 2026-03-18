---
name: mainsequence-project
description: Use this skill for MainSequence-specific work in this Astro-scaffolded repository. This skill does not define SDK behavior. It enforces a docs-first workflow against the latest MainSequence documentation and explains how to maintain astro/brief.md, astro/tasks.md, astro/record.md, astro/status.md, and astro/journal.md.
---

# MainSequence Project Runbook

Use this skill only for MainSequence-specific tasks in this repository.

## First Principle

The latest MainSequence docs are the source of truth for SDK, CLI, DataNode, market,
portfolio, instrument, dashboard, and platform behavior.

Canonical docs root:
`https://mainsequence-sdk.github.io/mainsequence-sdk/`

Do not use this skill as a frozen copy of MainSequence behavior.

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
- project setup, local environment, first DataNodes:
  getting started docs
- orchestration, jobs, schedules:
  orchestration docs
- markets, assets, portfolios:
  markets docs
- dashboards:
  dashboard docs
- if tutorial coverage is not enough:
  use examples, then reference, then changelog or project docs

## Workflow

1. Read `AGENTS.md`.
2. Read the latest relevant MainSequence docs.
3. Confirm the goal and acceptance criteria in `astro/brief.md`.
4. Confirm the latest known state in `astro/status.md`.
5. Confirm current priorities in `astro/tasks.md`.
6. Confirm project identifiers, checkout path, and orchestration notes in `astro/record.md`.
7. If there is a failure or repeated issue, check `astro/journal.md`.
8. Compare the current implementation against the latest docs.
9. Upgrade to the latest MainSequence SDK if relevant.
10. Run `mainsequence project refresh_token` before validations or live checks.
11. Verify real platform state with the CLI or platform tooling when platform facts matter.
12. Update the Astro files appropriately.

## Astro File Update Rules

### Update `astro/brief.md` when:
- user intent changes
- scope changes
- project goal changes
- acceptance criteria changes

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
- Keep reusable dashboard UI under `dashboards/components/`.
- Do not hardcode machine-specific local paths in reusable docs or instructions.
- Do not hide failures.
- Prefer strict code.
- Fail fast in `DataNode` update paths.

## DataNode Safety

For any DataNode change:
- re-read the relevant current docs first
- fail fast in update paths
- do not silently skip failures
- for high-volume nodes, test in a test namespace and smaller time range first
- only then run a full update or backfill

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