# Scheduling Jobs

Part 4 of the tutorial shows the shortest path to getting a job running. This page explains the same topic from an infrastructure point of view: what a job is, where schedules should live, how images fit in, and when to use the CLI versus the Python client.

If your jobs consume spreadsheets, CSV drops, model files, or generated reports, pair this guide with the [Artifacts](./artifacts.md) guide.

## Quick Summary

In this guide, you will:

- understand the lifecycle from project code to scheduled execution
- manage jobs from the CLI
- create the same jobs from the Python client
- decide when to use backend-managed project workflows and when to create jobs directly
- inspect runs, logs, and frozen images

## The mental model

A scheduled workflow in Main Sequence usually has five moving parts:

1. **Project code**  
   Your launcher script, notebook, or YAML entrypoint lives in the repository.

2. **Environment**  
   The project needs a reproducible Python environment and dependency lockfile.

3. **Job**  
   A job tells the platform what to execute. That can be a repository file such as `scripts/simulated_prices_launcher.py`, or an app entrypoint.

4. **Schedule**  
   A schedule tells the platform when the job should run. Main Sequence supports interval schedules and crontab schedules.

5. **Job run**  
   Every execution creates a job run. That is the object you inspect when you want status, logs, start time, end time, or resource usage.

Some workflows also include an **Artifact**: a file stored in a platform bucket that a job or `TimeIndexTableUpdater` reads later.

If you keep that chain in mind, most operational decisions become straightforward.

## The recommended default

For shared projects, treat recurring schedules as part of the repository.

That means:

- define recurring Jobs and ResourceReleases under `.mainsequence/workflows/`
- validate each document against the current backend ProjectBranch workflow contract
- commit and push the validated file so repository events apply it
- use direct CLI or client-created jobs mainly for experiments, backfills, or one-off operational tasks

The removed `scheduled_jobs.yaml` format and `schedule_batch_jobs` CLI command
are not compatibility surfaces.

!!! tip "Default rule"
    If a job is important enough to run every day, it is usually important enough to review in version control.

## Two ways to create jobs

There are two valid workflows, and they serve different purposes.

### 1. Repository-managed jobs

This is the best option for long-lived schedules used by a team.

Retrieve the current YAML from
`GET /api/v1/project-branches/{uid}/workflow-template/`. Validate the proposed
repository `path` and `content` with
`POST /api/v1/project-branches/{uid}/validate-workflow/`, save the result as a
direct `.yaml` or `.yml` child of `.mainsequence/workflows/`, then commit it.

The backend owns the parser, accepted `api_version`, resource kinds, defaults,
permissions, and application semantics. Clients must not reproduce that logic.
Repository events process each file independently. There is no prune or
strict-delete mode, and removing a declaration does not delete its backend
resource.

This approach is easier to review, easier to reproduce, and much easier to reason about later.

### 2. Direct job creation

This is the faster option when you want to:

- test a new launcher quickly
- trigger a temporary backfill
- create a manual-only job
- provision jobs from Python code

The direct path can be done from the CLI or from the Python client.

## Working from the CLI

The CLI is the fastest operational tool once the project already exists locally.

### Sync the project first

Before creating or updating scheduled jobs, make sure the project state is consistent:

```bash
mainsequence project sync -m "Prepare scheduling changes"
```

That command is more than a `git push`. It updates the local environment, exports `requirements.txt`, creates a commit, and pushes the result in the platform-compatible flow.

### Apply a reviewed project workflow

After backend validation, commit the workflow through the standard sync:

```bash
mainsequence project sync -m "Update project workflow"
```

The push triggers backend repository processing. Inspect the repository-event
result and resulting jobs or deployment runs; Git success is not deployment
success. For reviewed job declarations, make compute intent explicit:

- `spot: false` for stable standard capacity
- `spot: true` only for retry-safe workloads that can be interrupted

If you want to validate one of the synchronized jobs immediately instead of waiting for the scheduler, use the same CLI loop:

```bash
mainsequence project jobs list
mainsequence project jobs run <JOB_UID>
mainsequence project jobs run <JOB_UID> --arg demo-from-cli
mainsequence project jobs run <JOB_UID> -- --name demo-from-cli
mainsequence project jobs runs list <JOB_UID>
mainsequence project jobs runs logs <JOB_RUN_UID> --max-wait-seconds 900
```

### Create a manual job

Use this when you want a job that only runs when someone triggers it:

```bash
mainsequence project jobs create \
  --name "Simulated Prices - Manual" \
  --execution-path scripts/simulated_prices_launcher.py \
  --related-image-uid <IMAGE_UID>
```

Every persisted job is exact-image backed, but creation has one image owner.
For a manually pinned job, you select the exact ready image. For an
automatically deployed job, you omit the image and the backend derives the
initial exact image from the ProjectBranch's persisted synchronized commit.

To opt a standalone job into future exact-image promotion:

```bash
mainsequence project jobs create \
  --name "Simulated Prices - Promoted" \
  --execution-path scripts/simulated_prices_launcher.py \
  --automatic-deployment \
  --automatic-redeployment-tag-regex '^v[0-9]+$'
```

Omit `--automatic-redeployment-tag-regex` to accept every otherwise-eligible
exact repository event. Do not pass `--related-image-uid` with
`--automatic-deployment`; the backend owns initial preparation. It reads its
persisted synchronized commit and never asks the client to inspect Git or
select a branch tip.

Then run it:

```bash
mainsequence project jobs list
mainsequence project jobs run <JOB_UID>
mainsequence project jobs run <JOB_UID> --arg demo-from-cli
mainsequence project jobs run <JOB_UID> -- --name demo-from-cli
mainsequence project jobs runs list <JOB_UID>
mainsequence project jobs runs logs <JOB_RUN_UID> --max-wait-seconds 900
```

### Create an interval schedule

Use interval schedules when the cadence is simple, for example every hour:

```bash
mainsequence project jobs create \
  --name "Simulated Prices - Hourly" \
  --execution-path scripts/simulated_prices_launcher.py \
  --related-image-uid <IMAGE_UID> \
  --schedule-type interval \
  --schedule-every 1 \
  --schedule-period hours
```

### Create a crontab schedule

Use crontab when you want calendar-based timing such as nightly runs:

```bash
mainsequence project jobs create \
  --name "Simulated Prices - Nightly" \
  --execution-path scripts/simulated_prices_launcher.py \
  --related-image-uid <IMAGE_UID> \
  --schedule-type crontab \
  --schedule-expression "0 0 * * *"
```

You can also add a start time or mark the schedule as one-off:

```bash
mainsequence project jobs create \
  --name "One-time Backfill" \
  --execution-path scripts/simulated_prices_launcher.py \
  --related-image-uid <IMAGE_UID> \
  --schedule-type crontab \
  --schedule-expression "0 2 * * *" \
  --schedule-start-time "2026-03-15T02:00:00Z" \
  --schedule-one-off
```

### Inspect runs and logs

Once the job exists, the basic operational loop is:

```bash
mainsequence project jobs list
mainsequence project jobs runs list <JOB_UID>
mainsequence project jobs runs logs <JOB_RUN_UID> --max-wait-seconds 900
```

The logs command polls while the run is still `PENDING` or `RUNNING`, so it works well as a simple live tail for operational checks.

### Bind a job to an exact project image

Every job requires this binding. The image represents one pushed, exact commit
and remains the job's current image until an explicit image change or a
qualifying backend-owned promotion succeeds.

List existing images:

```bash
mainsequence project images list
```

Create a new image:

```bash
mainsequence project images create
```

Then create the job against that image:

```bash
mainsequence project jobs create \
  --name "Simulated Prices - Frozen" \
  --execution-path scripts/simulated_prices_launcher.py \
  --related-image-uid <IMAGE_UID>
```

This is the only supported creation pattern. There is no dynamic, blank-image,
branch-tip, or `latest` job mode.

!!! note "Important"
    Project images are built from pushed commits. If a commit does not exist on the remote, it cannot be turned into a project image.

## Working from the Python client

The Python client is useful when job creation itself is part of your automation. It is the right tool when you want to provision jobs from code rather than from a shell session.

Run this code from the intended Project checkout. The SDK resolves the canonical
Git repository, attached branch, and exact HEAD commit once for the process,
maps them to ProjectBranch, and uses that immutable context for every Job
operation. Application code does not select a ProjectBranch UID.

Examples below use the public client imports:

```python
from datetime import UTC, datetime

from mainsequence.client import CrontabSchedule, IntervalSchedule, Job, JobRun
```

### Create a manual job

```python
manual_job = Job.create(
    name="Simulated Prices - Manual",
    execution_path="scripts/simulated_prices_launcher.py",
    related_image_uid="<PROJECT_IMAGE_UID>",
    cpu_request="0.25",
    memory_request="0.5",
)
```

Automatic deployment remains backend-owned and still requires the exact
initial image:

```python
promoted_job = Job.create(
    name="Simulated Prices - Promoted",
    execution_path="scripts/simulated_prices_launcher.py",
    cpu_request="0.25",
    memory_request="0.5",
    automatic_deployment=True,
    automatic_redeployment_policy={"tag_regex": r"^v[0-9]+$"},
)
```

### Create an interval-scheduled job

```python
hourly_job = Job.create(
    name="Simulated Prices - Hourly",
    execution_path="scripts/simulated_prices_launcher.py",
    related_image_uid="<PROJECT_IMAGE_UID>",
    task_schedule=IntervalSchedule(
        every=1,
        period="hours",
        start_time=datetime(2026, 3, 14, 8, 0, tzinfo=UTC),
    ),
    cpu_request="0.25",
    memory_request="0.5",
)
```

### Create a nightly crontab job

```python
nightly_job = Job.create(
    name="Simulated Prices - Nightly",
    execution_path="scripts/simulated_prices_launcher.py",
    related_image_uid="<PROJECT_IMAGE_UID>",
    task_schedule={
        "schedule": CrontabSchedule(
            expression="0 0 * * *",
            start_time=datetime(2026, 3, 14, 0, 0, tzinfo=UTC),
        ).model_dump(mode="json", exclude_none=True),
        "one_off": False,
    },
    cpu_request="0.25",
    memory_request="0.5",
)
```

### List jobs, trigger a run, and fetch logs

```python
jobs = Job.filter()

run_payload = nightly_job.run_job()
job_runs = JobRun.filter(job__uid=nightly_job.uid)

latest_run = job_runs[0]
logs = latest_run.get_logs()
usage = latest_run.get_resource_usage()
```

`logs` is an `OwnerLogPage` with opaque cursor pagination and enriched rows.
`usage` is a `ResourceUsagePage` containing aggregate CPU, memory, and disk
samples. The SDK takes the required Environment scope from the backend-owned
JobRun capability links; application code does not provide an Environment UID.

In practice, the client gives you the same lifecycle as the CLI:

- create the job
- run it immediately if needed
- inspect the resulting job runs
- read logs for the run you care about

Each `JobRun` response freezes the exact `runtime_image_uid`, image digest, and
commit selected at run creation. Launch uses that snapshot even if the job is
promoted later.

!!! note "One practical difference"
    The CLI applies safe defaults for `cpu_request`, `memory_request`, `spot`, and `max_runtime_seconds` when you omit them. The Python client expects you to pass the compute values yourself. A manual Job requires `related_image_uid`; an automatic Job rejects it and delegates exact initial-image preparation to the backend.

## What the schedule fields mean

The schedule model is intentionally small.

### Interval schedules

Use interval schedules when the rule is simply "every N units".

Example:

- every `1` hour
- every `15` minutes
- every `2` days

Supported periods are:

- `seconds`
- `minutes`
- `hours`
- `days`

### Crontab schedules

Use crontab schedules when the rule is tied to the calendar.

Example:

- `"0 0 * * *"` for midnight every day
- `"0 6 * * 1-5"` for 06:00 on weekdays

The platform expects a standard five-field expression:

- minute
- hour
- day of month
- month
- day of week

If you are used to six-field cron syntax from other systems, this is the place where people usually trip.

## Operational rules that matter

These are the details that prevent avoidable problems.

### Use repository-relative execution paths

`execution_path` must point to a file inside the repository, for example:

```text
scripts/simulated_prices_launcher.py
```

Do not pass:

- absolute filesystem paths
- directory paths
- paths with `..`

The SDK also restricts job entrypoints to `.py`, `.ipynb`, and `.yaml`.

### Use forward slashes in paths

Even on Windows, use:

```text
scripts/simulated_prices_launcher.py
```

not backslashes. The platform handles the path correctly.

### Choose images when reproducibility matters

If a job is operationally important, ask whether it should follow the latest project state or a frozen image.

- Follow the repository tip when you want quick iteration.
- Pin to a project image when you want repeatable execution and stable rollbacks.

### Keep recurring jobs reviewable

For production-like schedules, prefer validated declarations under
`.mainsequence/workflows/`.

That gives you:

- review in pull requests
- a visible history of schedule changes
- less ambiguity about why a job exists
- backend-owned validation and repository-event application

### Separate creation from observation

Creating a schedule is only half of the work. Always verify:

```bash
mainsequence project jobs list
mainsequence project jobs runs list <JOB_UID>
mainsequence project jobs runs logs <JOB_RUN_UID>
```

That simple loop catches most configuration mistakes quickly.

## Common mistakes

### "The job was created, but it does not run what I expect"

Usually this means one of three things:

- the `execution_path` points at the wrong launcher
- the repository changed after the job was created and the job was not pinned to an image
- the schedule exists, but the workflow declaration was not validated and reviewed first

### "The client example fails, but the CLI worked"

The most common reasons are missing compute values or a missing exact
`related_image_uid` on a manually pinned Job, or a ProjectBranch without a
synchronized commit for an automatically deployed Job. The CLI supplies
compute defaults and can interactively select a manual image. `Job.create()`
does neither. Automatic creation must omit the image UID.

### "The cron expression looks valid, but the API rejects it"

Make sure you are using five fields, not six.

### "The logs command exits too soon"

Use a larger wait window:

```bash
mainsequence project jobs runs logs <JOB_RUN_UID> --max-wait-seconds 900
```

or disable polling control if you want to handle it yourself.

## Related Reading

- [Artifacts](./artifacts.md)
- [CLI Deep Dive](../cli.md)
