---
name: mainsequence-project
description: Use this skill to bootstrap work in a Main Sequence repository. This skill owns project context verification, docs-first behavior, success-definition discipline, and routing between specialized domain skills and the maintenance skill. It does not own domain implementation semantics or direct reconciliation of the project-state files under `.agents/`.
---

# Main Sequence Project Bootstrap

## Overview

Use this skill to start work correctly in a Main Sequence repository before moving into a domain skill.

This skill is for:

- establishing project context
- defining success up front
- enforcing a docs-first workflow
- deciding when `.agents/` maintenance is required
- verifying platform context before making claims
- routing work to the correct specialized skill

## This Skill Can Do

- determine the correct startup and read order
- define a concrete success condition before implementation starts
- verify current project and platform context
- decide which specialized skill owns the actual domain work
- decide when to hand off to `.agents/skills/mainsequence/maintenance/local_journal/SKILL.md`
- enforce standard Main Sequence repository structure expectations
- separate verified facts from assumptions
- record documentation mismatches and route follow-up work
- enforce the namespace-first safety rule for new or modified DataNodes

## This Skill Must Not Claim

This skill must not claim ownership of:

- DataNode engineering
- SimpleTable design
- FastAPI or application API design
- Command Center workspace payloads
- AppComponent contracts
- jobs, schedules, images, resources, or releases
- RBAC or sharing semantics
- assets, translation tables, or portfolio construction
- Streamlit dashboard implementation
- instruments or pricing-runtime semantics

Do not let this skill become a domain manual.

## Route Adjacent Work

- DataNodes:
  `.agents/skills/mainsequence/data_publishing/data_nodes/SKILL.md`
- SimpleTables:
  `.agents/skills/mainsequence/data_publishing/simple_tables/SKILL.md`
- platform data discovery before implementation:
  `.agents/skills/mainsequence/data_access/exploration/SKILL.md`
- FastAPI and application-facing APIs:
  `.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`
- Command Center workspace creation and widget mounting:
  `.agents/skills/mainsequence/command_center/workspace_builder/SKILL.md`
- AppComponents, custom forms, and widget-facing API contracts:
  `.agents/skills/mainsequence/command_center/app_components/SKILL.md`
- project-state reconciliation and local history under `.agents/`:
  `.agents/skills/mainsequence/maintenance/local_journal/SKILL.md`
- project status audits, blocker analysis, and upstream SDK assessment:
  `.agents/skills/mainsequence/maintenance/bug_auditor/SKILL.md`
- jobs, schedules, artifacts, images, resources, and releases:
  `.agents/skills/mainsequence/platform_operations/orchestration_and_releases/SKILL.md`
- RBAC and sharing:
  `.agents/skills/mainsequence/platform_operations/access_control_and_sharing/SKILL.md`
- assets, categories, and translation tables:
  `.agents/skills/mainsequence/markets_platform/assets_and_translation/SKILL.md`
- Virtual Fund Builder:
  `.agents/skills/mainsequence/markets_platform/virtualfundbuilder/SKILL.md`
- instruments and pricing:
  `.agents/skills/mainsequence/markets_platform/instruments_and_pricing/SKILL.md`
- Streamlit dashboards:
  `.agents/skills/mainsequence/dashboards/streamlit/SKILL.md`

## Read First

1. `AGENTS.md`
2. `.agents/skills/mainsequence/maintenance/local_journal/SKILL.md`
3. the latest relevant Main Sequence docs for the task
4. `.agents/brief.md`
5. `.agents/status.md`
6. `.agents/tasks.md`
7. `.agents/record.md`
8. `.agents/journal.md` when resuming, debugging, or investigating a repeated issue

Canonical documentation root:
`https://mainsequence-sdk.github.io/mainsequence-sdk/`

## Inputs This Skill Needs

Before starting non-trivial work, collect or infer:

- the user goal
- the concrete success condition
- the repository path and current project context
- whether live platform verification is required
- which specialized skill should own the domain behavior
- whether the maintenance skill will need to run after the domain step

If the user goal or project context is unclear, stop before routing domain work.

## Required Decisions

For every non-trivial task, decide:

1. What does success look like in observable terms?
2. Which specialized skill owns the domain behavior?
3. Does platform state need live verification?
4. Will the maintenance skill need to run after this step?
5. Are the docs and local implementation aligned, or is there a discrepancy to record?

## Build Rules

### 1. The latest docs are the source of truth

Do not rely on memory or copied snippets when the current Main Sequence docs should be checked.

### 2. Maintain the standard Main Sequence project structure

Also maintain these standard project areas when relevant:

- `src/`
- `scripts/`
- `tests/`
- `docs/`
- `api/`
- `dashboards/`
- `dashboards/components/`

If the project has recurring scheduled jobs, keep:

- `scheduled_jobs.yaml`

Treat `scheduled_jobs.yaml` as the repository-managed input file for batch job sync/create, not as a separate scheduler system.

Use the standard Main Sequence project structure unless the repository explicitly documents a different layout.

Repository-local execution paths for jobs must:

- be relative to the repository root
- use forward slashes, even on Windows
- point to a supported file inside the repository

Do not treat:

- `.env` as long-term documentation
- `.venv` as source code
- local absolute paths as reusable project instructions

### 3. Define success before implementation

Make the end state explicit before changing code, docs, or platform objects.

Do not start domain work with a vague target.

### 4. Verify project context before making platform claims

Use the CLI to confirm the active project and refresh credentials before live checks when needed.

When the result will be consumed programmatically or used as machine-readable evidence, prefer the CLI `--json` flag.

Typical bootstrap checks:

- `mainsequence project current --debug`
- `mainsequence project refresh_token --path .`

### 5. Route domain work instead of expanding the bootstrap skill

Once the task boundary is clear, move into the correct specialized skill.

Do not teach domain semantics here.

### 6. Hand off to maintenance after material work

After material domain work, route to:

- `.agents/skills/mainsequence/maintenance/local_journal/SKILL.md`

Use that handoff when the turn changed:

- verified state
- blockers
- next actions
- scope
- stable operational references
- a meaningful milestone
- a failure
- a repeated issue
- a suspected SDK or platform issue
- an investigation result worth preserving historically

### 7. Use namespaces first for new or modified DataNodes

Before first-running or validating a new or changed DataNode, use an explicit namespace before any non-namespaced run.

## Review Rules

When reviewing bootstrap behavior, look for:

- domain work happening without a clear owner skill
- material domain work finishing without a maintenance handoff
- implementation starting without a concrete success condition
- platform claims made without verification
- docs mismatches that were noticed but not recorded
- the bootstrap skill growing back into a catch-all domain manual

## Validation Checklist

Do not claim bootstrap success until you have checked:

- the correct project context is selected
- the relevant docs were checked
- the success condition is explicit
- the correct specialized skill was chosen
- the maintenance handoff was made when project state materially changed
- any platform-state claims were verified with CLI or platform tooling

## This Skill Must Stop And Escalate When

- the relevant docs cannot be accessed
- the project context is unclear
- the success condition is still ambiguous
- live platform state is required but has not been verified
- domain work is proceeding without the relevant specialized skill or docs

Do not guess through missing context.
