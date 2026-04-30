---
name: mainsequence-orchestration-and-releases
description: Use this skill when the task is about operational execution in a Main Sequence project. This skill owns jobs, schedules, batch scheduling files, project images, run inspection, project resources, releases, and Artifacts as operational inputs. It does not own DataNode producer design, SimpleTable schema design, API route contracts, or RBAC policy.
---

# Main Sequence Orchestration And Releases

## Overview

Use this skill when the task is about getting project code to run on the platform in a controlled and verifiable way.

This skill is for:

- jobs
- schedules
- images
- project resources
- releases
- operational logs and run inspection
- Artifacts as job inputs or outputs

## This Skill Can Do

- create or review manual jobs
- create or review scheduled jobs
- decide when to use `scheduled_jobs.yaml`
- validate and submit batch job files
- decide when strict batch sync is appropriate
- create or select project images
- freeze jobs to a project image
- inspect job runs and logs
- reason about project resources and resource releases
- review Artifact-based workflows in operational pipelines

## This Skill Must Not Claim

This skill must not claim ownership of:

- DataNode producer behavior
- SimpleTable schema and row semantics
- FastAPI route contracts
- RBAC or sharing policy
- Streamlit dashboard implementation details

## Route Adjacent Work

- DataNodes:
  `.agents/skills/mainsequence/data_publishing/data_nodes/SKILL.md`
- SimpleTables:
  `.agents/skills/mainsequence/data_publishing/simple_tables/SKILL.md`
- APIs and FastAPI:
  `.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`
- predeployment mock API contract validation:
  `.agents/skills/mainsequence/command_center/api_mock_prototyping/SKILL.md`
- RBAC and sharing:
  `.agents/skills/mainsequence/platform_operations/access_control_and_sharing/SKILL.md`
- Streamlit dashboards:
  `.agents/skills/mainsequence/dashboards/streamlit/SKILL.md`

## Read First

1. `docs/tutorial/scheduling_jobs.md`
2. `docs/knowledge/infrastructure/scheduling_jobs.md`
3. `docs/knowledge/infrastructure/artifacts.md`

If the task touches deployed dashboards or APIs, also read the relevant domain skill/docs before changing the operational workflow.

If the task is about publishing a Command Center-facing API mainly to test AppComponent UX, bindings, or request/response contracts, read:

4. `.agents/skills/mainsequence/command_center/api_mock_prototyping/SKILL.md`

Do that before building an image or creating a FastAPI `ResourceRelease`.

## Inputs This Skill Needs

Before changing orchestration or release behavior, collect or infer:

- the execution target:
  - `execution_path`
  - app entrypoint
- whether the job is:
  - manual
  - interval
  - crontab
  - one-off
- the image strategy:
  - existing image
  - new image
- whether the workflow is:
  - direct CLI/client job creation
  - repository-managed batch scheduling
- whether Artifact inputs or outputs are part of the run
- whether the job should be reproducible against a pinned image

If the execution target or image strategy is unclear, stop before scheduling anything.

## Required Decisions

For every non-trivial orchestration task, decide:

1. Is this a one-off/manual workflow or a repository-managed recurring workflow?
2. Should the jobs live in `scheduled_jobs.yaml`?
3. Which pinned project image should the job use?
4. Is strict batch sync appropriate or dangerous?
5. Does the workflow depend on Artifacts?
6. Is the task actually a release/resource problem instead of only a job problem?

## Build Rules

### 1. Shared recurring jobs should be treated as code

For shared recurring workflows, prefer:

- `scheduled_jobs.yaml`
- `mainsequence project schedule_batch_jobs ...`

`scheduled_jobs.yaml` is the repository-managed input file for the bulk job sync/create flow. It is not a separate scheduling backend model.

In reviewed batch files, set `spot` explicitly. `spot: true` means the job may use lower-cost interruptible capacity, similar to GCP Spot or legacy preemptible capacity. `spot: false` means standard capacity.

Do not hide important recurring schedules in ad hoc shell history or one-off manual commands.

### 2. Jobs should run against pinned images

Jobs should be pinned to a project image.

Remember:

- images are built from pushed commits
- if a commit is not on the remote, it cannot be used for an image
- unpinned jobs are not an acceptable default in a managed Main Sequence project

### 3. Jobs must be verifiable after creation

Do not stop at creation.

Use the standard CLI execution loop when execution success matters:

- `mainsequence project jobs list`
- `mainsequence project jobs run <JOB_ID>`
- `mainsequence project jobs runs list <JOB_ID>`
- `mainsequence project jobs runs logs <JOB_RUN_ID> --max-wait-seconds 900`

Verify:

- the job exists
- the run was triggered manually when immediate validation matters, or has already been triggered by the scheduler
- the logs and run status match expectations

### 4. Batch scheduling is powerful and dangerous

Use `--strict` only when the batch file is intended to be the full desired state.

Do not use strict mode casually in shared environments.

### 5. Artifacts are operational file primitives

Use `Artifact` when the operational unit is a file.

Examples:

- vendor drops
- generated reports
- model files
- input spreadsheets

Do not force a file workflow into a table workflow too early.

### 6. Resources and releases are part of deployment, not just code

For deployed dashboards, APIs, or agents:

- the local file is not enough
- the project resource must exist
- the release must exist
- the release must point at the intended image or resource version

### 6.1 Do not publish an API just to test AppComponent contracts

If the goal is to validate Command Center AppComponent UX, request rendering, response rendering, published outputs, or downstream bindings, do not jump straight to:

- image build
- project resource creation
- `ResourceRelease` creation

Use the predeployment mock workflow first:

- `.agents/skills/mainsequence/command_center/api_mock_prototyping/SKILL.md`

That workflow exists to validate the contract in `apiTargetMode: "mock-json"` before spending time on deployment.

Only publish the real FastAPI API after the AppComponent contract is stable.

## Review Rules

When reviewing an orchestration task, look for:

- schedules that should have been version-controlled
- direct job creation where a batch file should exist
- missing or wrong `related_image_id`
- jobs tied to moving repository state instead of a pinned image
- no run/log verification after creation
- unsafe use of `--strict`
- workflows depending on laptop-specific file paths instead of Artifacts
- image or release work being used as a substitute for predeployment AppComponent/API contract validation
- tasks that are really resource/release problems rather than simple job problems

## Validation Checklist

Do not claim success until you have checked:

- the execution target is correct
- the job mode is correct:
  - manual
  - interval
  - crontab
  - one-off
- the pinned image choice is intentional
- the job exists after creation or sync
- runs and logs were inspected when execution success matters
- resources and releases were verified when deployment success matters
- Command Center-facing API publishing is not being used just to test AppComponent UX that should have been validated first in `mock-json` mode

If the workflow uses `scheduled_jobs.yaml`, also check:

- the file shape is valid
- the jobs list is intentional
- strict mode is either intentionally on or intentionally off
- the file is being treated as the reviewed input to the bulk job sync/create flow
- `spot` is explicit and matches the job's interruption tolerance

If the workflow uses Artifacts, also check:

- the bucket and artifact identity are intentional
- the workflow no longer depends on a fragile local path

## This Skill Must Stop And Escalate When

- the execution target is unclear
- the image strategy is unclear but reproducibility matters
- strict batch sync could delete jobs and the desired state is not explicit
- the workflow depends on local file paths that should be platform Artifacts
- the real need is AppComponent/API contract validation before deployment rather than release execution itself
- the task is actually about RBAC policy rather than orchestration
- the task is actually about producer semantics rather than platform execution

Do not guess through operational state.
