---
name: local-journal
description: Use this skill to maintain the project-state files under `.agents/` in a Main Sequence repository. This skill owns reconciliation of current state, stable references, next actions, blockers, and historical notes after material work.
---

# Local Journal And Project State Maintenance

## Overview

Use this skill after material work to reconcile the project-state files under `.agents/`.

This skill owns:

- `.agents/brief.md`
- `.agents/tasks.md`
- `.agents/record.md`
- `.agents/status.md`
- `.agents/journal.md`

## This Skill Can Do

- update `.agents/brief.md` when user intent, scope, or success criteria changed
- update `.agents/tasks.md` when next actions, follow-up work, or open blockers changed
- update `.agents/record.md` when stable commands, identifiers, paths, or operational notes changed
- update `.agents/status.md` when verified state changed
- append to `.agents/journal.md` when a milestone, failure, repeated issue, or investigation outcome should be preserved
- keep the five `.agents/` files consistent with each other

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
  `.agents/skills/project_builder/SKILL.md`
- project audits and blocker classification:
  `.agents/skills/maintenance/bug_auditor/SKILL.md`
- DataNodes:
  `.agents/skills/data_publishing/data_nodes/SKILL.md`
- SimpleTables:
  `.agents/skills/data_publishing/simple_tables/SKILL.md`
- FastAPI and application-facing APIs:
  `.agents/skills/application_surfaces/api_surfaces/SKILL.md`
- Command Center workspaces:
  `.agents/skills/command_center/workspace_builder/SKILL.md`
- AppComponents and custom forms:
  `.agents/skills/command_center/app_components/SKILL.md`
- jobs, schedules, resources, releases, and artifacts:
  `.agents/skills/platform_operations/orchestration_and_releases/SKILL.md`
- RBAC and sharing:
  `.agents/skills/platform_operations/access_control_and_sharing/SKILL.md`
- assets and translation tables:
  `.agents/skills/markets_platform/assets_and_translation/SKILL.md`
- Virtual Fund Builder:
  `.agents/skills/markets_platform/virtualfundbuilder/SKILL.md`
- instruments and pricing:
  `.agents/skills/markets_platform/instruments_and_pricing/SKILL.md`
- Streamlit dashboards:
  `.agents/skills/dashboards/streamlit/SKILL.md`

## Read First

1. `AGENTS.md`
2. `.agents/skills/project_builder/SKILL.md`
3. the domain skill that produced the material change
4. `.agents/brief.md`
5. `.agents/tasks.md`
6. `.agents/record.md`
7. `.agents/status.md`
8. `.agents/journal.md`

## Inputs This Skill Needs

Before editing the `.agents/` files, collect or infer:

- what materially changed
- what was verified versus assumed
- whether user intent or scope changed
- whether new follow-up tasks were created
- whether stable commands, identifiers, or paths changed
- whether the turn produced a milestone, failure, repeated issue, or investigation result worth preserving

## Required Decisions

For every maintenance pass, decide:

1. Does `.agents/brief.md` need to change?
2. Does `.agents/tasks.md` need to change?
3. Does `.agents/record.md` need to change?
4. Does `.agents/status.md` need to change?
5. Does `.agents/journal.md` need a new entry?
6. Are the files still consistent with each other after the update?

## Build Rules

### 1. Update only after material work

Do not rewrite the `.agents/` files for trivial lookups.

Use this skill after:

- implementation work
- verification work
- blocker discovery
- scope change
- documentation mismatch discovery
- new follow-up work discovery

### 2. Keep verified state separate from assumptions

`.agents/status.md` must distinguish verified facts from assumptions or pending checks.

### 3. Keep stable references in `.agents/record.md`

Put stable commands, ids, file paths, and operational references in `.agents/record.md`, not in `.agents/journal.md`.

### 4. Keep `.agents/journal.md` append-only in spirit

Use `.agents/journal.md` for historical notes:

- milestones
- failures
- repeated issues
- investigations
- suspected SDK or platform issues

Do not use it as the current source of truth for next actions or verified state.

### 5. Keep `.agents/tasks.md` actionable

Write follow-up work as concrete tasks, not vague notes.

### 6. Keep `.agents/brief.md` short and stable

It should reflect the current user goal and success condition, not the full execution history.

## Review Rules

When reviewing maintenance output, look for:

- material work happened but the `.agents/` files were not reconciled
- `.agents/status.md` contains guesses presented as facts
- `.agents/tasks.md` contains stale or non-actionable items
- `.agents/record.md` is missing stable references that future work will need
- `.agents/journal.md` is being used as current state instead of history
- the five files disagree with each other

## Validation Checklist

Do not claim maintenance success until you have checked:

- every affected `.agents/` file was considered
- verified facts and assumptions are clearly separated
- next actions are explicit
- stable references are captured
- historical notes were preserved when needed
- the `.agents/` files are consistent with the actual repo and latest verified state

## This Skill Must Stop And Escalate When

- the material change is unclear
- the verified state is unknown
- the underlying domain skill has not been consulted
- the `.agents/` files conflict in a way that cannot be resolved from available evidence

Do not guess through contradictory project state.
