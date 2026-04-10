---
name: local-journal
description: Use this skill to maintain the project-state files under `astro/` in a Main Sequence repository. This skill owns reconciliation of current state, stable references, next actions, blockers, and historical notes after material work.
---

# Local Journal And Project State Maintenance

## Overview

Use this skill after material work to reconcile the project-state files under `astro/`.

This skill owns:

- `astro/brief.md`
- `astro/tasks.md`
- `astro/record.md`
- `astro/status.md`
- `astro/journal.md`

## This Skill Can Do

- update `astro/brief.md` when user intent, scope, or success criteria changed
- update `astro/tasks.md` when next actions, follow-up work, or open blockers changed
- update `astro/record.md` when stable commands, identifiers, paths, or operational notes changed
- update `astro/status.md` when verified state changed
- append to `astro/journal.md` when a milestone, failure, repeated issue, or investigation outcome should be preserved
- keep the five `astro/` files consistent with each other

## This Skill Must Not Claim

This skill must not claim ownership of:

- DataNode semantics
- SimpleTable semantics
- API design
- Command Center payload design
- job and release configuration semantics
- RBAC semantics
- market, VFB, dashboard, or pricing semantics

This skill maintains project-state files. It does not own domain implementation rules.

## Route Adjacent Work

- bootstrap and routing:
  `docs/astro/project_builder/SKILL.md`
- project audits and blocker classification:
  `docs/astro/maintenance/bug_auditor/SKILL.md`
- DataNodes:
  `docs/astro/data_publishing/data_nodes/SKILL.md`
- SimpleTables:
  `docs/astro/data_publishing/simple_tables/SKILL.md`
- FastAPI and application-facing APIs:
  `docs/astro/application_surfaces/api_surfaces/SKILL.md`
- Command Center workspaces:
  `docs/astro/command_center/workspace_builder/SKILL.md`
- AppComponents and custom forms:
  `docs/astro/command_center/app_components/SKILL.md`
- jobs, schedules, resources, releases, and artifacts:
  `docs/astro/platform_operations/orchestration_and_releases/SKILL.md`
- RBAC and sharing:
  `docs/astro/platform_operations/access_control_and_sharing/SKILL.md`
- assets and translation tables:
  `docs/astro/markets_platform/assets_and_translation/SKILL.md`
- Virtual Fund Builder:
  `docs/astro/markets_platform/virtualfundbuilder/SKILL.md`
- instruments and pricing:
  `docs/astro/markets_platform/instruments_and_pricing/SKILL.md`
- Streamlit dashboards:
  `docs/astro/dashboards/streamlit/SKILL.md`

## Read First

1. `docs/astro/AGENTS.md`
2. `docs/astro/project_builder/SKILL.md`
3. the domain skill that produced the material change
4. `astro/brief.md`
5. `astro/tasks.md`
6. `astro/record.md`
7. `astro/status.md`
8. `astro/journal.md`

## Inputs This Skill Needs

Before editing the `astro/` files, collect or infer:

- what materially changed
- what was verified versus assumed
- whether user intent or scope changed
- whether new follow-up tasks were created
- whether stable commands, identifiers, or paths changed
- whether the turn produced a milestone, failure, repeated issue, or investigation result worth preserving

## Required Decisions

For every maintenance pass, decide:

1. Does `astro/brief.md` need to change?
2. Does `astro/tasks.md` need to change?
3. Does `astro/record.md` need to change?
4. Does `astro/status.md` need to change?
5. Does `astro/journal.md` need a new entry?
6. Are the files still consistent with each other after the update?

## Build Rules

### 1. Update only after material work

Do not rewrite the `astro/` files for trivial lookups.

Use this skill after:

- implementation work
- verification work
- blocker discovery
- scope change
- documentation mismatch discovery
- new follow-up work discovery

### 2. Keep verified state separate from assumptions

`astro/status.md` must distinguish verified facts from assumptions or pending checks.

### 3. Keep stable references in `astro/record.md`

Put stable commands, ids, file paths, and operational references in `astro/record.md`, not in `astro/journal.md`.

### 4. Keep `astro/journal.md` append-only in spirit

Use `astro/journal.md` for historical notes:

- milestones
- failures
- repeated issues
- investigations
- suspected SDK or platform issues

Do not use it as the current source of truth for next actions or verified state.

### 5. Keep `astro/tasks.md` actionable

Write follow-up work as concrete tasks, not vague notes.

### 6. Keep `astro/brief.md` short and stable

It should reflect the current user goal and success condition, not the full execution history.

## Review Rules

When reviewing maintenance output, look for:

- material work happened but the `astro/` files were not reconciled
- `astro/status.md` contains guesses presented as facts
- `astro/tasks.md` contains stale or non-actionable items
- `astro/record.md` is missing stable references that future work will need
- `astro/journal.md` is being used as current state instead of history
- the five files disagree with each other

## Validation Checklist

Do not claim maintenance success until you have checked:

- every affected `astro/` file was considered
- verified facts and assumptions are clearly separated
- next actions are explicit
- stable references are captured
- historical notes were preserved when needed
- the `astro/` files are consistent with the actual repo and latest verified state

## This Skill Must Stop And Escalate When

- the material change is unclear
- the verified state is unknown
- the underlying domain skill has not been consulted
- the `astro/` files conflict in a way that cannot be resolved from available evidence

Do not guess through contradictory project state.
