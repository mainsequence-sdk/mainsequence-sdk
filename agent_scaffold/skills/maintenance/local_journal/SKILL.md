---
name: local-journal
description: Use this skill to maintain the project-state files under `agent_scaffold/` in a Main Sequence repository. This skill owns reconciliation of current state, stable references, next actions, blockers, and historical notes after material work.
---

# Local Journal And Project State Maintenance

## Overview

Use this skill after material work to reconcile the project-state files under `agent_scaffold/`.

This skill owns:

- `agent_scaffold/brief.md`
- `agent_scaffold/tasks.md`
- `agent_scaffold/record.md`
- `agent_scaffold/status.md`
- `agent_scaffold/journal.md`

## This Skill Can Do

- update `agent_scaffold/brief.md` when user intent, scope, or success criteria changed
- update `agent_scaffold/tasks.md` when next actions, follow-up work, or open blockers changed
- update `agent_scaffold/record.md` when stable commands, identifiers, paths, or operational notes changed
- update `agent_scaffold/status.md` when verified state changed
- append to `agent_scaffold/journal.md` when a milestone, failure, repeated issue, or investigation outcome should be preserved
- keep the five `agent_scaffold/` files consistent with each other

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
  `agent_scaffold/skills/project_builder/SKILL.md`
- project audits and blocker classification:
  `agent_scaffold/skills/maintenance/bug_auditor/SKILL.md`
- DataNodes:
  `agent_scaffold/skills/data_publishing/data_nodes/SKILL.md`
- SimpleTables:
  `agent_scaffold/skills/data_publishing/simple_tables/SKILL.md`
- FastAPI and application-facing APIs:
  `agent_scaffold/skills/application_surfaces/api_surfaces/SKILL.md`
- Command Center workspaces:
  `agent_scaffold/skills/command_center/workspace_builder/SKILL.md`
- AppComponents and custom forms:
  `agent_scaffold/skills/command_center/app_components/SKILL.md`
- jobs, schedules, resources, releases, and artifacts:
  `agent_scaffold/skills/platform_operations/orchestration_and_releases/SKILL.md`
- RBAC and sharing:
  `agent_scaffold/skills/platform_operations/access_control_and_sharing/SKILL.md`
- assets and translation tables:
  `agent_scaffold/skills/markets_platform/assets_and_translation/SKILL.md`
- Virtual Fund Builder:
  `agent_scaffold/skills/markets_platform/virtualfundbuilder/SKILL.md`
- instruments and pricing:
  `agent_scaffold/skills/markets_platform/instruments_and_pricing/SKILL.md`
- Streamlit dashboards:
  `agent_scaffold/skills/dashboards/streamlit/SKILL.md`

## Read First

1. `agent_scaffold/AGENTS.md`
2. `agent_scaffold/skills/project_builder/SKILL.md`
3. the domain skill that produced the material change
4. `agent_scaffold/brief.md`
5. `agent_scaffold/tasks.md`
6. `agent_scaffold/record.md`
7. `agent_scaffold/status.md`
8. `agent_scaffold/journal.md`

## Inputs This Skill Needs

Before editing the `agent_scaffold/` files, collect or infer:

- what materially changed
- what was verified versus assumed
- whether user intent or scope changed
- whether new follow-up tasks were created
- whether stable commands, identifiers, or paths changed
- whether the turn produced a milestone, failure, repeated issue, or investigation result worth preserving

## Required Decisions

For every maintenance pass, decide:

1. Does `agent_scaffold/brief.md` need to change?
2. Does `agent_scaffold/tasks.md` need to change?
3. Does `agent_scaffold/record.md` need to change?
4. Does `agent_scaffold/status.md` need to change?
5. Does `agent_scaffold/journal.md` need a new entry?
6. Are the files still consistent with each other after the update?

## Build Rules

### 1. Update only after material work

Do not rewrite the `agent_scaffold/` files for trivial lookups.

Use this skill after:

- implementation work
- verification work
- blocker discovery
- scope change
- documentation mismatch discovery
- new follow-up work discovery

### 2. Keep verified state separate from assumptions

`agent_scaffold/status.md` must distinguish verified facts from assumptions or pending checks.

### 3. Keep stable references in `agent_scaffold/record.md`

Put stable commands, ids, file paths, and operational references in `agent_scaffold/record.md`, not in `agent_scaffold/journal.md`.

### 4. Keep `agent_scaffold/journal.md` append-only in spirit

Use `agent_scaffold/journal.md` for historical notes:

- milestones
- failures
- repeated issues
- investigations
- suspected SDK or platform issues

Do not use it as the current source of truth for next actions or verified state.

### 5. Keep `agent_scaffold/tasks.md` actionable

Write follow-up work as concrete tasks, not vague notes.

### 6. Keep `agent_scaffold/brief.md` short and stable

It should reflect the current user goal and success condition, not the full execution history.

## Review Rules

When reviewing maintenance output, look for:

- material work happened but the `agent_scaffold/` files were not reconciled
- `agent_scaffold/status.md` contains guesses presented as facts
- `agent_scaffold/tasks.md` contains stale or non-actionable items
- `agent_scaffold/record.md` is missing stable references that future work will need
- `agent_scaffold/journal.md` is being used as current state instead of history
- the five files disagree with each other

## Validation Checklist

Do not claim maintenance success until you have checked:

- every affected `agent_scaffold/` file was considered
- verified facts and assumptions are clearly separated
- next actions are explicit
- stable references are captured
- historical notes were preserved when needed
- the `agent_scaffold/` files are consistent with the actual repo and latest verified state

## This Skill Must Stop And Escalate When

- the material change is unclear
- the verified state is unknown
- the underlying domain skill has not been consulted
- the `agent_scaffold/` files conflict in a way that cannot be resolved from available evidence

Do not guess through contradictory project state.
