# AGENTS.md

This repository is a MainSequence Platform Project and uses the Astro scaffold.

Canonical MainSequence documentation root:
`https://mainsequence-sdk.github.io/mainsequence-sdk/`

## MainSequence Source-of-Truth Rule

For any task involving MainSequence code, CLI usage, DataNodes, orchestration, jobs, dashboards,
markets, assets, portfolios, instruments, or platform validation, always consult the latest
relevant MainSequence documentation before acting.

Rules:
- Treat the latest MainSequence docs as the source of truth for SDK, CLI, and platform behavior.
- Do not treat this file, local notes, or copied snippets as authoritative for MainSequence behavior.
- Do not rely on memory for MainSequence semantics when the docs should be checked.
- If the docs cannot be accessed, state that explicitly and do not claim the behavior was verified.

## Required Astro Scaffold

Maintain these files at the repository root:

- `astro/brief.md`
- `astro/tasks.md`
- `astro/record.md`
- `astro/status.md`
- `astro/journal.md`

Also maintain:

- `docs/`
- `dashboards/`
- `dashboards/components/`

## Astro File Responsibilities

### `astro/brief.md`
Purpose:
- translated user intent
- project goal
- acceptance criteria

Rules:
- keep it concise
- update it when the user goal, scope, or acceptance criteria changes
- do not use it as a task list
- do not use it as a journal

### `astro/tasks.md`
Purpose:
- prioritized actionable tasks
- current implementation work only

Rules:
- keep tasks actionable
- use checkboxes or explicit statuses
- keep tasks prioritized
- remove completed, obsolete, or superseded work
- do not use it as historical record

### `astro/record.md`
Purpose:
- stable project facts
- project id or project name
- local checkout path
- orchestration notes
- other stable operational identifiers or references

Rules:
- keep it factual and stable
- prefer portable path conventions such as:
  `<MAINSEQUENCE_WORKBENCH>/projects/project-ID`
- do not use it for transient progress notes
- do not use it as a journal or task list

### `astro/status.md`
Purpose:
- latest known project state
- evidence checked
- blockers or failures
- next actions

Rules:
- this is the current-state file
- overwrite it to keep it current
- do not make it historical
- summarize the latest verified state, what was checked, what is blocked, and what should happen next

### `astro/journal.md`
Purpose:
- append-only historical record

Record:
- what was implemented
- what failed
- what may have failed because of the MainSequence SDK or platform
- key decisions
- important historical snapshots
- prior error investigations and outcomes

Rules:
- append only
- do not overwrite history
- do not use it as the current status file
- use it when historical context matters, especially for repeated failures or prior fixes

## Mandatory Read Order

Before any non-trivial MainSequence-related task:

1. Read this file.
2. Read the latest relevant MainSequence docs.
3. Read `astro/brief.md`.
4. Read `astro/status.md`.
5. Read `astro/tasks.md`.
6. Read `astro/record.md`.
7. If debugging, resuming work, or investigating a failure, check `astro/journal.md`.

## Mandatory Startup Sequence

For any non-trivial MainSequence task:

1. Read the latest relevant MainSequence documentation.
2. Compare the implementation against the latest documented behavior.
3. Check `astro/status.md` for the latest verified state.
4. Check `astro/tasks.md` for current priorities.
5. Check `astro/record.md` for project identifiers, checkout path, and orchestration notes.
6. If an error appears, check `astro/journal.md` for the same or related error and any prior fix.
7. Upgrade to the latest MainSequence SDK when SDK behavior matters.
8. Before validations or live checks, run:
   `mainsequence project refresh_token`
9. Verify platform state with the CLI or platform tooling instead of guessing.

## Core Working Rules

- Keep documentation clear, concise, and accurate.
- Correct inconsistencies as soon as they are found.
- Prefer strict code.
- Avoid defensive guards on hot paths unless justified by a verified requirement.
- Fail fast, especially in `DataNode` update paths.
- Do not hide failures.
- Record the exact failing step, command, or workflow.
- If local code or local docs conflict with the latest MainSequence docs, record the discrepancy and create follow-up work.
- When unsure, verify.

## MainSequence Verification Rules

When platform state matters, verify it with the CLI and/or platform UI.

At minimum, verify relevant:
- `DataNodes`
- DataNode updates
- data availability
- jobs
- dashboards
- assets
- portfolios
- related platform objects used by the project

If live verification is not possible:
- state that clearly
- separate verified facts from assumptions
- provide the exact commands or checks still required

For high-volume `DataNodes`:
- test first in a test namespace
- use a smaller time range first
- only then run a full update or backfill

## File Maintenance Rules

Update files as follows:

- update `astro/brief.md` when intent, goal, or acceptance criteria changes
- update `astro/tasks.md` when priorities or actionable work changes
- update `astro/record.md` when stable project facts or orchestration notes change
- update `astro/status.md` whenever the latest verified state, blockers, evidence, or next actions change
- append to `astro/journal.md` for milestones, failures, repeated issues, suspected SDK issues, and important historical context

## Documentation Rules

- All formal project documentation must live under `docs/`.
- Documentation must follow an `MkDocs`-compatible structure.
- Navigation must match the actual file structure.
- The root `README.md` must remain the entry point and documentation map.
- Every major project area must have its own page under `docs/`.
- Operational and verification procedures must be documented under `docs/`.
- Any new feature, workflow, component, or integration must be reflected in documentation.

## Dashboard Rules

Before changing dashboard code, re-read the latest relevant dashboard docs.

Project rules:
- dashboard apps live under `dashboards/<app>/`
- reusable UI belongs in `dashboards/components/`
- keep dashboard code and docs aligned

## SDK / Platform Issue Handling

If something may be a MainSequence SDK, documentation, or platform issue:
- record what failed
- explain why it may be a MainSequence issue
- suggest a concrete improvement
- append the issue to `astro/journal.md`
- add actionable follow-up to `astro/tasks.md` if still open
- reflect the latest blocker in `astro/status.md`

## Path Conventions

Do not hardcode machine-specific personal paths in documentation or reusable instructions.

Prefer placeholders such as:
`<MAINSEQUENCE_WORKBENCH>/projects/project-ID`

If `astro/record.md` stores a checkout path, keep it portable unless a real local path is explicitly required for local execution notes.

## Output Style

- Be concise but complete.
- Prefer explicit facts over vague statements.
- Surface failures early.
- Distinguish verified facts from assumptions.