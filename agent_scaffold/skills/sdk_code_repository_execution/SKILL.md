---
name: mainsequence-sdk-code-repository-execution
description: Use the installed Main Sequence SDK and repository-local tools to verify CodeRepository context, apply local scaffold conventions, and route concrete implementation work after platform intent and ontology have been established.
---

# Main Sequence SDK CodeRepository Execution

## Overview

Use this SDK-owned execution skill after the platform `code-repository-design` skill
has established intent, CodeRepository ontology, the connected CodeRepository Blueprint, and
the observable definition of success. This file owns local SDK, CLI,
filesystem, and repository mechanics; it does not define the platform ontology
or replace `code-repository-design`.

This skill is for:

- establishing code repository context
- defining success up front
- enforcing a docs-first workflow
- verifying platform context before making claims
- routing work to the correct specialized skill

## This Skill Can Do

- determine the correct startup and read order
- define a concrete success condition before implementation starts
- verify current code repository and platform context
- decide which specialized skill owns the actual domain work
- enforce standard Main Sequence repository structure expectations
- separate verified facts from assumptions
- surface documentation mismatches to the user
- enforce the namespace-first safety rule for new or modified TimeIndexTableUpdaters

## This Skill Must Not Claim

This skill must not claim ownership of:

- TimeIndexTableUpdater engineering
- MetaTable design
- Command Center-serving FastAPI contract and release design
- jobs, schedules, images, resources, or releases
- RBAC or sharing semantics
- domain assets, translation tables, or construction logic
- unsupported application deployment targets
- pricing-runtime semantics

Do not let this skill become a domain manual.

## Route Adjacent Work

- TimeIndexTableUpdaters:
  `.agents/skills/mainsequence/data_publishing/time_index_table_updates/SKILL.md`
- MetaTables:
  `.agents/skills/mainsequence/data_publishing/meta_tables/SKILL.md`
- platform data discovery before implementation:
  `.agents/skills/mainsequence/data_access/exploration/SKILL.md`
- FastAPI APIs serving the Command Center frontend:
  `.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`
- CodeRepository audits, blocker analysis, and upstream SDK assessment:
  `.agents/skills/mainsequence/maintenance/bug_auditor/SKILL.md`
- local environment repair, CodeRepository authentication refresh, SDK updates,
  managed skill refresh, and canonical CodeRepository sync:
  `.agents/skills/mainsequence/maintenance/code_repository_maintenance/SKILL.md`
- jobs, schedules, artifacts, images, resources, and releases:
  `.agents/skills/mainsequence/platform_operations/orchestration_and_releases/SKILL.md`
- RBAC and sharing:
  `.agents/skills/mainsequence/platform_operations/access_control_and_sharing/SKILL.md`

## Read First

1. `AGENTS.md`
2. the latest relevant Main Sequence docs for the task

Canonical documentation root:
`https://mainsequence-sdk.github.io/mainsequence-sdk/`

## Inputs This Skill Needs

Before starting non-trivial work, collect or infer:

- the user goal
- the concrete success condition
- the repository path and current code repository context
- whether live platform verification is required
- which specialized skill should own the domain behavior

If the user goal or code repository context is unclear, stop before routing domain work.

## Resolve Local CodeRepository Context From Git

The containing Git worktree is the only source of repository, attached branch,
and exact commit identity. The SDK normalizes the non-secret repository remote,
maps it to CodeRepository and CodeRepositoryBranch through the platform API, resolves once for
the process, and reuses the immutable result. Authentication selects credentials
and permissions; it does not select source identity. A `git switch` performed in
a long-running process takes effect only in the next CLI invocation, worker,
script, or other process run.

Use `mainsequence code-repository current --debug --json` to verify that Git context resolves
to `code_repository_branch_status=resolved` and a nonempty `code_repository_branch_uid`. The UID
is an internal resolution result for branch-owned platform calls; it is not a
local or environment configuration input. Never require the user to look it up,
persist CodeRepository identity in `.env`, accept a branch environment override, or
infer a branch from collection order. Local and deployed code repository images use the
same Git algorithm. A detached checkout or an unregistered Git branch is
unresolved context and must block only live branch-owned operations.

Keep the platform boundaries explicit:

- use the Git-resolved logical CodeRepository UID for aggregate identity and CodeRepository operations;
- let the SDK resolve the current Git branch to CodeRepositoryBranch only when Jobs,
  images, releases, resources, pods, or other branch-owned APIs require it;
- treat GitHubRepositoryBinding as repository metadata and clone-location ownership;
  `git_ssh_url` is not CodeRepositoryBranch state.

For ordinary local implementation, work naturally in the current Git branch.
Do not make CodeRepositoryBranch selection a separate user workflow.
An unregistered local branch remains valid for ordinary local development, but
it has no CodeRepositoryBranch, Environment, or branch-derived MetaTables DataSource.
Only branch-owned operations fail. Register the branch before using Jobs,
images, releases, resources, platform-managed MetaTables/TimeIndexTableUpdaters, migrations,
pods, or other branch-owned platform APIs. Never fall back to another branch or
to an aggregate-level default DataSource.

## Required Decisions

For every non-trivial task, decide:

1. What does success look like in observable terms?
2. Which specialized skill owns the domain behavior?
3. Does platform state need live verification?
4. Are the docs and local implementation aligned, or is there a discrepancy to surface?

## Build Rules

### 1. The latest docs are the source of truth

Do not rely on memory or copied snippets when the current Main Sequence docs should be checked.

### 2. Maintain the standard Main Sequence CodeRepository structure

Also maintain these standard repository areas when relevant:

- `src/`
- `scripts/`
- `tests/`
- `docs/`
- `api/`

If the CodeRepository has recurring scheduled jobs or repository-managed releases,
keep backend-managed declarations as direct `.yaml` or `.yml` children of
`.mainsequence/workflows/`. Never create `scheduled_jobs.yaml`; retrieve and
validate the current workflow contract through the backend-owned CodeRepositoryBranch
workflow endpoints.

Use the standard Main Sequence CodeRepository structure unless the repository explicitly documents a different layout.

Repository-local execution paths for jobs must:

- be relative to the repository root
- use forward slashes, even on Windows
- point to a supported file inside the repository

Do not treat:

- `.env` as long-term documentation
- `.venv` as source code
- local absolute paths as reusable repository instructions

### 3. Define success before implementation

Make the end state explicit before changing code, docs, or platform objects.

Do not start domain work with a vague target.

### 4. Verify code repository context before making platform claims

Use the CLI to confirm the active CodeRepository and refresh credentials before live checks when needed.

When the result will be consumed programmatically or used as machine-readable evidence, prefer the CLI `--json` flag.

Typical bootstrap checks:

- `mainsequence code-repository current --debug`
- `mainsequence code-repository refresh-token --path .`

Do not proceed with a live branch-owned check unless `code-repository current` reports
the current Git branch and a resolved CodeRepositoryBranch UID.

### 5. Route domain work instead of expanding the bootstrap skill

Once the task boundary is clear, move into the correct specialized skill.

Do not teach domain semantics here.

### 6. Use namespaces first for new or modified TimeIndexTableUpdaters

Before first-running or validating a new or changed TimeIndexTableUpdater, use an explicit namespace before any non-namespaced run.

## Review Rules

When reviewing bootstrap behavior, look for:

- domain work happening without a clear owner skill
- implementation starting without a concrete success condition
- platform claims made without verification
- docs mismatches that were noticed but not surfaced
- the bootstrap skill growing back into a catch-all domain manual

## Validation Checklist

Do not claim bootstrap success until you have checked:

- the correct code repository context is selected
- the relevant docs were checked
- the success condition is explicit
- the correct specialized skill was chosen
- any platform-state claims were verified with CLI or platform tooling

## This Skill Must Stop And Escalate When

- the relevant docs cannot be accessed
- the code repository context is unclear
- the success condition is still ambiguous
- live platform state is required but has not been verified
- domain work is proceeding without the relevant specialized skill or docs

Do not guess through missing context.
