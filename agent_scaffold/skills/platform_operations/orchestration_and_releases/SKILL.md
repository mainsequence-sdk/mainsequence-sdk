---
name: mainsequence-orchestration-and-releases
description: Use this skill for Main Sequence jobs, schedules, backend-managed project workflow files, project images, run inspection, resources, releases, Streamlit deployment, and operational Artifacts. It does not own DataNode behavior, MetaTable schemas, API contracts, Streamlit design, or RBAC policy.
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
- Streamlit dashboard deployment as `streamlit_dashboard` releases
- operational logs and run inspection
- Artifacts as job inputs or outputs

## This Skill Can Do

- create or review manual jobs
- create or review scheduled jobs
- author backend-managed declarations under `.mainsequence/workflows/`
- validate workflow files through the ProjectBranch workflow endpoints
- create or select project images
- freeze jobs to a project image
- inspect job runs and logs
- reason about project resources and resource releases
- decide whether a `ResourceRelease` should opt into `automatic_deployment`
- inspect automatic deployment run state for release rotations
- create or review Streamlit dashboard deployment through project resources and `streamlit_dashboard` releases
- review Artifact-based workflows in operational pipelines

## This Skill Must Not Claim

This skill must not claim ownership of:

- DataNode producer behavior
- MetaTable schema and row semantics
- Command Center FastAPI wire contracts
- RBAC or sharing policy
- Streamlit dashboard implementation details
- Streamlit layout, styling, sidebar/session behavior, page structure, or UI helper design

## Route Adjacent Work

- DataNodes:
  `.agents/skills/mainsequence/data_publishing/data_nodes/SKILL.md`
- MetaTables:
  `.agents/skills/mainsequence/data_publishing/meta_tables/SKILL.md`
- Command Center FastAPI provider implementation and contract validation:
  `.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`
- RBAC and sharing:
  `.agents/skills/mainsequence/platform_operations/access_control_and_sharing/SKILL.md`

## Read First

1. `docs/tutorial/scheduling_jobs.md`
2. `docs/knowledge/infrastructure/scheduling_jobs.md`
3. `docs/knowledge/infrastructure/artifacts.md`
4. `docs/tutorial/dashboards/streamlit/streamlit_integration_2.md` when deploying or verifying a Streamlit dashboard
5. `docs/knowledge/dashboards/streamlit/index.md` when deployment metadata or SDK/dashboard boundary questions matter

If the task touches deployed FastAPI APIs, also read the relevant API skill/docs before changing the operational workflow.

If the task asks to design, build, style, or restructure a Streamlit app, do not treat that as Main Sequence platform skill work. Streamlit implementation is app-owned project code. This skill only owns deployment and verification of an already-authored dashboard.

## Inputs This Skill Needs

Before changing orchestration or release behavior, collect or infer:

- the execution target:
  - `execution_path`
  - app entrypoint
  - Streamlit `app.py` resource path when deploying a dashboard
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
  - backend-managed project workflow declarations
  - Streamlit dashboard release creation
- whether a `ResourceRelease` should be manually pinned or opted into repository-sync automatic deployment
- whether Artifact inputs or outputs are part of the run
- whether the job should be reproducible against a pinned image
- for Streamlit dashboard deployment, the `README.md` resource next to `app.py`

If the execution target or image strategy is unclear, stop before scheduling anything.

## Required Decisions

For every non-trivial orchestration task, decide:

1. Is this a one-off/manual workflow or a repository-managed recurring workflow?
2. Should the jobs live in a `.mainsequence/workflows/*.yaml` declaration?
3. Which pinned project image should the job use?
4. Does the workflow depend on Artifacts?
5. Is the task actually a release/resource problem instead of only a job problem?
6. For Streamlit dashboard deployment, do the selected app resource, README resource, and project image all refer to the intended pushed commit?
7. For a `ResourceRelease`, should repository sync be allowed to rotate the release automatically through `automatic_deployment`?

## Build Rules

### 1. Shared recurring jobs should be treated as code

For shared recurring workflows, use direct `.yaml` or `.yml` children of
`.mainsequence/workflows/`. Do not create `scheduled_jobs.yaml`; it is not a
supported input.

The backend owns workflow parsing, validation, defaults, permissions, and
application. Retrieve the current template from
`GET /api/v1/project-branches/{uid}/workflow-template/`, validate the proposed
`path` and `content` with
`POST /api/v1/project-branches/{uid}/validate-workflow/`, then commit the file.
Do not reproduce the parser or construct an interpreted deployment payload in
the SDK or project code.

Every file requires the backend-advertised `api_version`, a name, and resource
declarations. Use the current template for accepted fields and resource kinds.
There is no prune or strict-delete mode; removing a declaration does not delete
an existing backend resource.

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
- `mainsequence project jobs run <JOB_UID>`
- `mainsequence project jobs runs list <JOB_UID>`
- `mainsequence project jobs runs logs <JOB_RUN_UID> --max-wait-seconds 900`

Verify:

- the job exists
- the run was triggered manually when immediate validation matters, or has already been triggered by the scheduler
- the logs and run status match expectations

### 4. Workflow application is backend-owned

Repository events apply valid workflow files independently. An invalid file is
not applied and does not block another valid file. After pushing, inspect the
repository-event action result and resulting deployment runs; a successful Git
push alone does not prove deployment success.

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

### 6.1 Streamlit dashboard deployment is a release workflow

Main Sequence owns the deployment boundary for Streamlit dashboards, not the dashboard UI design.

For Streamlit dashboard deployment:

- the dashboard `app.py` must already exist in the repository
- the dashboard `README.md` must exist next to `app.py`
- both files must be committed and pushed
- the project image must be built from the intended pushed commit
- `mainsequence project project_resource list` must discover the dashboard app and README resources
- create the release with `mainsequence project project_resource create_dashboard`
- the dashboard release kind is `streamlit_dashboard`

Do not prescribe Streamlit page layout, styling, sidebar/session patterns, or helper/component architecture. Those are application-owned implementation details.

Validate the deployment path, not the Streamlit design.

### 6.2 Automatic ResourceRelease deployment

`automatic_deployment` is the automated deployment opt-in flag on a `ResourceRelease`. It means repository synchronization can rotate an existing release to the latest synced project commit for the same resource path.

When `automatic_deployment=True`, repository-sync events may create a unified `DeploymentRun` with `target_type="resource_release"` and source `repository_event`. That run:

- reads the project's current synced commit
- resolves the current project resource at the release's existing resource path
- resolves the current adjacent `README.md` when the release kind requires one, such as Streamlit dashboards
- creates or resolves the project image for that commit
- redeploys the existing release to the current resource, README, and project image
- records state, phase, outcome, revision context, artifact context, steps, logs, result, and errors on the deployment run

This is not a local development shortcut. It does not deploy unpushed local files. The repository must be pushed, the project repository must be synced, and project resource discovery must find the resource at the same path for the current commit.

Enable `automatic_deployment` only when:

- the release should track the project's synced repository version
- the resource path is stable across commits
- the current synced branch/version is an acceptable deployment source for that release
- dashboard README requirements are satisfied for the current commit
- the team accepts CI/CD-style rotation for this release

Keep `automatic_deployment` disabled when:

- the release must stay pinned to a manually selected image or resource version
- each release rotation needs human approval
- the resource path or entrypoint is still moving
- API or widget contracts are not stable enough for automatic rotation
- the current branch/project sync target is not the intended deployment source

Create opted-in releases with the CLI flags that exist:

- `mainsequence project project_resource create_dashboard --automatic-deployment`
- `mainsequence project project_resource create_fastapi --automatic-deployment`
- use `--no-automatic-deployment` when the decision is explicitly to keep the release pinned/manual

The SDK surface also accepts:

- `ProjectResource.create_dashboard(..., automatic_deployment=True)`
- `ProjectResource.create_fastapi(..., automatic_deployment=True)`
- `ResourceRelease.create(..., automatic_deployment=True)`
- `ResourceRelease.deploy_current_version()` for an SDK-triggered manual deployment run; it returns `DeploymentRun`
- `DeploymentRun.filter(target_type="resource_release", target_uid=release.uid)` to inspect runs for one release

Do not claim there is a CLI command for `deploy_current_version` unless the local CLI actually exposes one. In this SDK, the manual detail action is available through the client model.

Inspect the unified run's `state`, `phase`, `outcome`, `steps`, `logs`, and `error` fields. Do not use legacy resource-release deployment status fields or filters.
Detail responses also expose `builder_image` and `builder_runtime`; these are empty strings when the run has no static-site builder metadata.

## Review Rules

When reviewing an orchestration task, look for:

- schedules that should have been version-controlled
- direct job creation where a batch file should exist
- missing or wrong `related_image_uid`
- jobs tied to moving repository state instead of a pinned image
- no run/log verification after creation
- unsafe use of `--strict`
- workflows depending on laptop-specific file paths instead of Artifacts
- `automatic_deployment` enabled without an explicit decision about repository-sync CI/CD rotation
- assumptions that automatic deployment will deploy local unpushed changes
- automatic release rotation where the resource path or dashboard README is not stable
- Streamlit dashboard deployment work drifting into app design or UI implementation ownership
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
- the job exists after direct creation or repository workflow application
- runs and logs were inspected when execution success matters
- resources and releases were verified when deployment success matters
- Streamlit dashboard releases use the intended app resource, README resource, image, and `streamlit_dashboard` release kind
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
- the workflow depends on local file paths that should be platform Artifacts
- automatic deployment is requested but the deployment source branch/current synced project version is unclear
- automatic deployment is requested but the resource path or required README is not stable
- the task is actually about RBAC policy rather than orchestration
- the task is actually about producer semantics rather than platform execution

Do not guess through operational state.
