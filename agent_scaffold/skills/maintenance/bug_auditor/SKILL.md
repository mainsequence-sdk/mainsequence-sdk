---
name: doc-bug-auditor
description: Reviews a Main Sequence project for status, blockers, failures, and completion.
---

# Main Sequence Bug Auditor

## Overview

Use this skill to inspect a checked-out Main Sequence project and determine:

- what is already finished
- what is still in progress
- what is blocked or failing
- what evidence supports that assessment
- whether a failure looks like target-project misuse, environment or setup drift, or a likely `mainsequence-sdk` execution bug

This skill is for diagnosis and assessment. Default behavior is read-only unless the task explicitly asks for edits.

## This Skill Can Do

- inspect project state and summarize completion status
- read `.agents/tasks.md` and `.agents/status.md` first when they exist
- use repo state, logs, test output, stderr, and task files as evidence
- classify failures into:
  - target-project issue
  - environment or credentials issue
  - likely upstream `mainsequence-sdk` issue
  - unclear
- inspect traceback frames, stderr, commands, and local package versions
- inspect local `mainsequence` package code when needed
- inspect the public `mainsequence-sdk` repository when local evidence is not enough
- search GitHub for upstream duplicate issues
- open or draft an upstream GitHub issue when the task includes escalation and the evidence supports it
- produce a concrete audit result with recommended next actions

## This Skill Must Not Claim

This skill must not claim ownership of:

- fixing domain behavior unless the task explicitly requests edits
- changing code or docs by default
- presenting assumptions as verified facts
- calling something an upstream SDK bug without evidence
- opening an upstream issue without first checking for a close duplicate

This skill audits. It does not implement by default.

## Route Adjacent Work

- bootstrap, routing, and repo structure:
  `.agents/skills/project_builder/SKILL.md`
- `.agents/` state reconciliation after the audit:
  `.agents/skills/maintenance/local_journal/SKILL.md`
- DataNode implementation issues:
  `.agents/skills/data_publishing/data_nodes/SKILL.md`
- SimpleTable implementation issues:
  `.agents/skills/data_publishing/simple_tables/SKILL.md`
- API implementation issues:
  `.agents/skills/application_surfaces/api_surfaces/SKILL.md`
- jobs, images, releases, and runtime environment issues:
  `.agents/skills/platform_operations/orchestration_and_releases/SKILL.md`
- RBAC and access issues:
  `.agents/skills/platform_operations/access_control_and_sharing/SKILL.md`
- assets, VFB, pricing, or dashboard domain issues:
  use the relevant domain skill under `.agents/skills/markets_platform/` or `.agents/skills/dashboards/`

## Read First

1. `AGENTS.md`
2. `.agents/skills/project_builder/SKILL.md`
3. `.agents/tasks.md` when it exists
4. `.agents/status.md` when it exists
5. `.agents/record.md` when stable references or project ids matter
6. `.agents/journal.md` when repeated failures or prior investigations may be relevant
7. the latest relevant Main Sequence docs for the failing workflow

## Inputs This Skill Needs

Before auditing, collect or infer:

- the target repository path
- the current user goal or claimed completion state
- the failing command, job, path, or workflow when known
- whether the task includes GitHub issue escalation
- whether local environment evidence is available
- whether live platform verification is required

## Required Decisions

For every audit, decide:

1. Is the overall state `finished`, `in_progress`, `blocked`, or `failed`?
2. What evidence supports the completed work?
3. What work is still open?
4. Is each failure best classified as target-project, environment, upstream SDK, or unclear?
5. Does the evidence justify inspecting the installed SDK or public `mainsequence-sdk` source?
6. Does the evidence justify GitHub duplicate search or upstream issue escalation?

## Build Rules

### 1. Stay read-only unless edits were explicitly requested

Do not modify code, docs, or project-state files unless the task explicitly asks for edits.

### 2. Start with project-state files

Read `.agents/tasks.md` and `.agents/status.md` first when they exist.

Use them as hypotheses, not as proof.

### 3. Use evidence, not impressions

Use repo state, logs, test output, stderr, and task files as evidence.

When reporting a blocker or failure, include:

- the exact command or action that failed when known
- the working directory, file path, job id, run id, or other relevant target
- the exit code if known
- a short traceback, stderr excerpt, or log snippet
- what was already tried, if visible from the evidence

### 4. Keep the parent informed during major investigation steps

Before each major investigation step, emit a short progress update that says what you are checking next.

Especially announce when you are:

- reading `.agents/tasks.md` or `.agents/status.md`
- inspecting a failing command, traceback, or stderr excerpt
- checking the local `mainsequence` package or version
- inspecting or cloning the public `mainsequence-sdk` repository
- searching GitHub for duplicate upstream issues
- opening an upstream issue or drafting one because issue creation is blocked

### 5. Classify failures explicitly

Every failure should be classified as one of:

- target-project issue
- environment or credentials issue
- likely upstream `mainsequence-sdk` issue
- unclear

### 6. Investigate possible SDK execution bugs concretely

If a failure may come from `mainsequence-sdk` execution:

- inspect traceback or stderr for frames, modules, or commands related to `mainsequence` or `mainsequence_sdk`
- inspect the local installed package and version first when visible from the environment
- inspect the public `mainsequence-sdk` repository when local evidence is not enough
- you may clone or refresh `https://github.com/mainsequence-sdk/mainsequence-sdk` in a temporary or scratch path for source inspection
- use that source inspection to decide whether the failure looks like upstream SDK behavior or local misuse

### 7. Use strict GitHub escalation rules

If the task includes GitHub issue escalation:

- use the system GitHub credentials already configured on the machine
- prefer the credential source already configured for the environment rather than inventing a new one
- if a short-lived token variable is needed for a REST request, derive it from the system credentials already available on the machine
- do not ask the user for an extra confirmation before opening the issue once the escalation criteria are met
- prefer GitHub REST API over `gh`
- prefer:
  - `GET https://api.github.com/search/issues`
  - `POST https://api.github.com/repos/mainsequence-sdk/mainsequence-sdk/issues`
- search for likely duplicate issues before opening a new issue
- only open a new issue when the evidence strongly suggests an upstream `mainsequence-sdk` bug and no close duplicate exists
- if issue creation is blocked by missing system auth or API failure, return an issue-ready draft instead of pretending the issue was opened

## Review Rules

When reviewing an audit, look for:

- status claims without supporting evidence
- failures reported without commands or traceback-style details
- upstream SDK suspicion without local or public source inspection
- environment problems misreported as SDK bugs
- duplicate GitHub search skipped before escalation
- audit output that hides uncertainty instead of classifying it as unclear

## Validation Checklist

Do not claim audit completion until you have checked:

- `.agents/tasks.md` and `.agents/status.md` first when they exist
- the overall state is one of:
  - `finished`
  - `in_progress`
  - `blocked`
  - `failed`
- completed work is separated from open work
- blockers include concrete evidence when available
- the upstream `mainsequence-sdk` assessment is one of:
  - `not_involved`
  - `possible`
  - `likely`
  - `confirmed`
- the GitHub issue status is one of:
  - `not_needed`
  - `existing_issue_found`
  - `issue_opened`
  - `drafted`
- recommended next actions are concrete

## This Skill Must Stop And Escalate When

- the repository state cannot be inspected
- the environment evidence needed for classification is unavailable
- the task requires live platform verification but credentials or access are missing
- the evidence is too weak to classify the failure honestly

Do not pretend the audit is conclusive when it is not.

## Output Shape

1. Overall state: `finished`, `in_progress`, `blocked`, or `failed`
2. Completed work
3. Open tasks
4. Blockers or failure causes, with command and traceback-style evidence when available
5. Upstream `mainsequence-sdk` assessment: `not_involved`, `possible`, `likely`, or `confirmed`
6. Evidence checked
7. GitHub issue status: `not_needed`, `existing_issue_found`, `issue_opened`, or `drafted`
8. Recommended next actions
