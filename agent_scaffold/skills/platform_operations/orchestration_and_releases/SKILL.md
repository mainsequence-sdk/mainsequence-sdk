---
name: mainsequence-orchestration-and-releases
description: Use this skill for Main Sequence jobs, schedules, backend-managed code-repository workflow files, code repository images, run inspection, resources, releases, and operational Artifacts. It does not own TimeIndexTableUpdater behavior, MetaTable schemas, API contracts, application UI design, or RBAC policy.
---

# Main Sequence Orchestration And Releases

## Overview

Use this skill when the task is about getting CodeRepository code to run on the platform in a controlled and verifiable way.

This skill is for:

- jobs
- schedules
- images
- code repository resources
- releases
- operational logs and run inspection
- Artifacts as job inputs or outputs

## This Skill Can Do

- author and validate Job, FastAPI, and Static Site workflow declarations
- inspect exact CodeRepository images, Jobs, ResourceReleases, and deployment runs
- run an existing Job manually and inspect logs
- update supported properties on an existing Job or release
- reason about automatic deployment and repository-event image promotion
- review Artifact-based workflows in operational pipelines

## This Skill Must Not Claim

This skill must not claim ownership of:

- TimeIndexTableUpdater producer behavior
- MetaTable schema and row semantics
- Command Center FastAPI wire contracts
- RBAC or sharing policy
- application UI implementation details

## Route Adjacent Work

- TimeIndexTableUpdaters:
  `.agents/skills/mainsequence/data_publishing/time_index_table_updates/SKILL.md`
- MetaTables:
  `.agents/skills/mainsequence/data_publishing/meta_tables/SKILL.md`
- Command Center FastAPI provider implementation and contract validation:
  `.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`
- RBAC and sharing:
  `.agents/skills/mainsequence/platform_operations/access_control_and_sharing/SKILL.md`

## Read First

1. `AGENTS.md`
2. <https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/infrastructure/scheduling_jobs/>
3. <https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/infrastructure/artifacts/>
4. <https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/infrastructure/owner_observability/> when inspecting logs or resource usage

If the task touches deployed FastAPI APIs, also use
`.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md` before
changing the operational workflow.

## Inputs This Skill Needs

Identify the repository source path or Job execution path, the product kind,
schedule and compute intent, and whether future qualifying repository events
should promote the target. For an existing target, obtain its public UID before
running or updating it. The repository workflow is the creation input; callers
do not select image, resource, Job, or release UIDs for creation.

## Required Decisions

1. Which repository workflow declaration owns the target?
2. Is the Job manual, interval, crontab, or one-off?
3. Should future exact repository events promote its image automatically?
4. Are Artifacts inputs or outputs of the run?
5. Which existing Job or release needs observation or an explicit operation?

## Build Rules

### 1. Shared recurring jobs should be treated as code

For shared recurring workflows, use direct `.yaml` or `.yml` children of
`.mainsequence/workflows/`. Do not create `scheduled_jobs.yaml`; it is not a
supported input.

The backend owns workflow parsing, validation, defaults, permissions, and
application. Retrieve the current template from
`GET /api/v1/code-repository-branches/{uid}/workflow-template/`, validate the proposed
`path` and `content` with
`POST /api/v1/code-repository-branches/{uid}/validate-workflow/`, then commit the file.
Do not reproduce the parser or construct an interpreted deployment payload in
the SDK or CodeRepository code.

Every file requires the backend-advertised `api_version`, a name, and resource
declarations. Use the current template for accepted fields and resource kinds.
There is no prune or strict-delete mode; removing a declaration does not delete
an existing backend resource.

Do not hide important recurring schedules in ad hoc shell history or one-off manual commands.

### 2. Every Job requires one exact image

Django derives the initial image from the exact repository event when it
applies a `kind: job` workflow declaration. A later qualifying event may
promote another exact image under the Job's automatic deployment policy.
There is no dynamic, blank-image, branch-tip, or `latest` execution mode.
A caller does not create an image or Job by selecting a stored image UID.

### 3. Jobs must be verifiable after creation

Do not stop at creation.

Use the standard CLI execution loop when execution success matters:

- `mainsequence code-repository jobs list`
- `mainsequence code-repository jobs run <JOB_UID>`
- `mainsequence code-repository jobs run <JOB_UID> --arg=<ARG>` for repeatable
  manual per-run arguments, including values that start with `-`
- `mainsequence code-repository jobs run <JOB_UID> -- <ARG>...` for manual
  passthrough arguments
- `mainsequence code-repository jobs runs list <JOB_UID>`
- `mainsequence code-repository jobs runs logs <JOB_RUN_UID> --max-wait-seconds 900`

The Python equivalent for one manual run is:

```python
job.run_job(
    command_args=[
        "--start-date",
        "2026-09-08T16:43:00Z",
        "--family",
        "jobs",
    ]
)
```

Treat each argument as one opaque argv string. Do not join arguments into a
shell command, and do not treat `command_args` as a replacement for the saved
Job entrypoint.

Current support matrix:

| Invocation | Argument support |
| --- | --- |
| `Job.run_job(command_args=[...])` | Supported for that manual run |
| `mainsequence code-repository jobs run ... --arg/-- ...` | Supported for that manual run |
| `Job.scheduled_command_args` | Persisted list copied into future scheduler-created runs |
| `mainsequence code-repository jobs update --scheduled-arg ...` | Replaces the persisted list on an existing Job |
| `.mainsequence/workflows/*.yaml` Job declaration | Supports `scheduled_command_args` as an ordered `list[str]` |

Keep `scheduled_command_args` separate from manual `command_args`. Retrieve the
backend workflow template and use only its advertised contract. Preserve every
list entry exactly; never collapse argv into a shell string. Updating the Job
changes only future scheduler-created runs, while existing `JobRun.command_args`
snapshots stay immutable.

Verify:

- the job exists
- the run was triggered manually when immediate validation matters, or has already been triggered by the scheduler
- manual per-run arguments are preserved as separate argv entries when used
- scheduled arguments are persisted on the Job or validated workflow declaration
- a scheduler-created run snapshots the configured list without merging manual arguments
- the logs and run status match expectations

Use owner-scoped observability rather than infrastructure discovery:

- `JobRun.get_logs()` and `JobRun.get_resource_usage()`
- `ResourceRelease.get_logs()` and `ResourceRelease.get_resource_usage()`
- `Agent.get_logs()` and `Agent.get_resource_usage()`
- `AgentSession.get_logs()` for one fixed session
- `mainsequence code-repository jobs runs logs <JOB_RUN_UID>`
- `mainsequence code-repository jobs runs resource-usage <JOB_RUN_UID>`
- `mainsequence code-repository resources logs <RESOURCE_RELEASE_UID>`
- `mainsequence code-repository resources resource-usage <RESOURCE_RELEASE_UID>`
- `mainsequence agent logs <AGENT_UID>`
- `mainsequence agent resource-usage <AGENT_UID>`
- `mainsequence agent session logs <AGENT_SESSION_UID>`

Do not ask the user for an Environment UID for these owner operations. The SDK
preserves the backend-owned capability scope. Do not discover Knative services,
revisions, pods, namespaces, or provider resources to retrieve telemetry.

DeploymentRun build and orchestration logs remain a separate product surface;
do not parse them as application-runtime `OwnerLogPage` rows.

### 4. Workflow application is backend-owned

Repository events apply valid workflow files independently. An invalid file is
not applied and does not block another valid file. After pushing, inspect the
repository-event action result and resulting deployment runs; a successful Git
push alone does not prove deployment success.

### 5. Artifacts are operational file primitives

Use `Artifact` when the operational unit is a file.

Artifact and Bucket operations derive their Organization Environment from the
process-frozen current Git branch and registered `CodeRepositoryBranch`. Do not ask the
user to select an Environment UID or branch UID.

Examples:

- vendor drops
- generated reports
- model files
- input spreadsheets

Do not force a file workflow into a table workflow too early.

### 6. Resources and releases are part of deployment, not just code

For deployed APIs, agents, or other supported resources:

- the local file is not enough
- the code repository resource must exist
- the release must exist
- the release must point at the intended image or resource version

### 6.1 Automatic ResourceRelease deployment

`automatic_deployment` is the automated deployment opt-in flag on a `ResourceRelease`. It means repository synchronization can rotate an existing release to the latest synced CodeRepository commit for the same resource path.

When `automatic_deployment=True`, repository-sync events may create a unified `DeploymentRun` with `target_type="resource_release"` and source `repository_event`. That run:

- reads the CodeRepository's current synced commit
- resolves the current code repository resource at the release's existing resource path
- resolves supporting resources required by the release kind
- creates or resolves the code repository image for that commit
- redeploys the existing release to the current resource, README, and code repository image
- records state, phase, outcome, revision context, artifact context, steps, logs, result, and errors on the deployment run

This is not a local development shortcut. It does not deploy unpushed local files. The repository must be pushed, the code repository must be synced, and code repository resource discovery must find the resource at the same path for the current commit.

Enable `automatic_deployment` only when:

- the release should track the CodeRepository's synced version
- the resource path is stable across commits
- the current synced branch/version is an acceptable deployment source for that release
- required supporting resources are available for the current commit
- the team accepts CI/CD-style rotation for this release

Keep `automatic_deployment` disabled when:

- the release should not promote on future repository events
- each release rotation needs human approval
- the resource path or entrypoint is still moving
- API or widget contracts are not stable enough for automatic rotation
- the current branch/CodeRepository sync target is not the intended deployment source

Declare opted-in releases in the repository workflow and validate the file
against the backend template. Repository events create deployment runs.
`DeploymentRun.filter(target_type="resource_release", target_uid=release.uid)`
inspects runs for an existing release.

Every release also exposes its immutable revision lifecycle:

- `desired_revision` is the public UID of the accepted revision being materialized
- `active_revision` is the public UID of the ready revision currently serving behind the stable release URL
- the two UIDs may differ while a deployment is running or after a deployment fails
- `revision_retention_count` is the positive release-owned retention setting; omission in the workflow uses the backend default
- configure retention in the workflow or update an existing release with `release.patch(revision_retention_count=5)`

Treat `active_revision` and `desired_revision` as read-only public identities. Do not substitute provider revision names, deployment names, or internal database IDs.

Inspect the unified run's `state`, `phase`, `outcome`, `steps`, `logs`, and `error` fields. Do not use legacy resource-release deployment status fields or filters.
Detail responses also expose `builder_image` and `builder_runtime`; these are empty strings when the run has no static-site builder metadata.

## Review Rules

When reviewing an orchestration task, look for:

- schedules that should have been version-controlled
- a missing repository workflow declaration for a new Job
- a workflow with a caller-selected image or generated platform UID
- jobs tied to moving repository state instead of an exact image
- client code that interprets automatic deployment as a branch-tip or `latest` selector
- no run/log verification after creation
- unsafe use of `--strict`
- workflows depending on laptop-specific file paths instead of Artifacts
- `automatic_deployment` enabled without an explicit decision about repository-sync CI/CD rotation
- assumptions that automatic deployment will deploy local unpushed changes
- automatic release rotation where the resource path or required supporting resources are not stable
- tasks that are really resource/release problems rather than simple job problems

## Validation Checklist

Do not claim success until you have checked:

- the execution target is correct
- the job mode is correct:
  - manual
  - interval
  - crontab
  - one-off
- the exact repository event and backend-owned image preparation are intentional
- standalone Job automatic deployment and tag policy are intentionally enabled or disabled
- the Job exists after repository workflow application
- runs and logs were inspected when execution success matters
- resources and releases were verified when deployment success matters
- `automatic_deployment` is intentionally enabled or disabled on each release
- automatic deployment runs were inspected when repository-sync rotation matters
- automatic deployment results match the intended commit, resource, README, image, and terminal status

If the workflow uses `.mainsequence/workflows/`, also check:

- the current backend template and supported `api_version` were used
- backend validation succeeded before commit
- the file is a direct `.yaml` or `.yml` child of the workflow directory
- repository-event and deployment results were inspected after push

If the workflow uses Artifacts, also check:

- the bucket and artifact identity are intentional
- the workflow no longer depends on a fragile local path

## This Skill Must Stop And Escalate When

- the execution target is unclear
- the image strategy is unclear but reproducibility matters
- the backend rejects the workflow version, resource kind, or requested field
- a Job workflow cannot resolve an exact source image
- an automatically deployed Job has no persisted synchronized CodeRepositoryBranch commit
- the workflow depends on local file paths that should be platform Artifacts
- automatic deployment is requested but the deployment source branch/current synced CodeRepository version is unclear
- automatic deployment is requested but the resource path or required README is not stable
- the task is actually about RBAC policy rather than orchestration
- the task is actually about producer semantics rather than platform execution

Do not guess through operational state.
