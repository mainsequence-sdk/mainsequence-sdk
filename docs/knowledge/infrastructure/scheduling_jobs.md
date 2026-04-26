# Scheduling Jobs

Part 4 of the tutorial shows the shortest path to getting a job running. This page explains the same topic from an infrastructure point of view: what a job is, where schedules should live, how images fit in, and when to use the CLI versus the Python client.

If your jobs consume spreadsheets, CSV drops, model files, or generated reports, pair this guide with the [Artifacts](./artifacts.md) guide.

## Quick Summary

In this guide, you will:

- understand the lifecycle from project code to scheduled execution
- manage jobs from the CLI
- create the same jobs from the Python client
- decide when to use `scheduled_jobs.yaml` and when to create jobs directly
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

Some workflows also include an **Artifact**: a file stored in a platform bucket that a job or `DataNode` reads later.

If you keep that chain in mind, most operational decisions become straightforward.

## The recommended default

For shared projects, treat recurring schedules as part of the repository.

That means:

- define recurring jobs in `scheduled_jobs.yaml`
- submit the batch with `mainsequence project schedule_batch_jobs`
- use direct CLI or client-created jobs mainly for experiments, backfills, or one-off operational tasks

`scheduled_jobs.yaml` is not a separate platform scheduler model. It is the repository-managed input file for the batch job sync/create flow.

!!! tip "Default rule"
    If a job is important enough to run every day, it is usually important enough to review in version control.

## Two ways to create jobs

There are two valid workflows, and they serve different purposes.

### 1. Repository-managed jobs

This is the best option for long-lived schedules used by a team.

You define the jobs in `scheduled_jobs.yaml`, commit that file, and submit the batch with:

```bash
mainsequence project schedule_batch_jobs scheduled_jobs.yaml
```

That file is just the reviewed repository representation of the batch operation. The CLI reads it, validates each job with the same rules used for individual creation, and then submits the batch through the same bulk job sync/create path.

Before submission, the CLI shows the project's existing images and asks you to choose which image the batch should use. The selected image is then applied to every job in the submitted batch.

After that selection, the CLI asks for confirmation and explicitly warns that the whole batch will be scheduled on the same image.

Example:

```yaml
jobs:
  - name: "Simulated Prices"
    execution_path: "scripts/simulated_prices_launcher.py"
    task_schedule:
      type: "crontab"
      expression: "0 0 * * *"
    related_image_id: 77
    cpu_request: "0.25"
    memory_request: "0.5"
    spot: false
```

Each entry is validated with the same rules used by individual job creation. That means the job still needs a related image id, valid compute settings, and a valid target. If one definition is invalid, the batch submission fails before anything is sent.

Set `spot` explicitly in repository-managed job files. `spot: true` means the job is allowed to prefer lower-cost interruptible capacity, conceptually similar to GCP Spot capacity or legacy preemptible VMs. `spot: false` means the job should stay on standard capacity. For long-lived reviewed schedules, make that choice explicit in the YAML instead of leaving it implicit.

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

### Submit a reviewed batch file

Once your repository state is ready, apply the scheduled jobs batch explicitly:

```bash
mainsequence project schedule_batch_jobs scheduled_jobs.yaml
```

This command reads the YAML file, checks that it contains a top-level `jobs` list, validates each job with the same rules as `Job.create()`, and then submits the normalized list to the backend in one request. Internally, this is still the bulk job sync/create path, not a separate scheduler-specific configuration system.

Before submission, the CLI shows the project's existing images and asks you to choose which image the batch should use. That selected image is then applied to every job in the submitted batch.

If you want the file to act as the full desired state for project jobs, add strict mode:

```bash
mainsequence project schedule_batch_jobs scheduled_jobs.yaml --strict
```

In strict mode, jobs that exist remotely but are not present in the YAML file may be removed. Jobs that are still linked to dashboards or resource releases are protected and will appear as not deleted in the batch result. The default is `--no-strict`.

For reviewed batch files, also make the compute intent explicit:

- `spot: false` for stable standard capacity
- `spot: true` only for retry-safe workloads that can be interrupted

If you want to validate one of the synchronized jobs immediately instead of waiting for the scheduler, use the same CLI loop:

```bash
mainsequence project jobs list
mainsequence project jobs run <JOB_ID>
mainsequence project jobs run <JOB_ID> --command python --command -m --command jobs.daily
mainsequence project jobs runs list <JOB_ID>
mainsequence project jobs runs logs <JOB_RUN_ID> --max-wait-seconds 900
```

### Create a manual job

Use this when you want a job that only runs when someone triggers it:

```bash
mainsequence project jobs create \
  --name "Simulated Prices - Manual" \
  --execution-path scripts/simulated_prices_launcher.py \
  --related-image-id <IMAGE_ID>
```

Then run it:

```bash
mainsequence project jobs list
mainsequence project jobs run <JOB_ID>
mainsequence project jobs run <JOB_ID> --command python --command -m --command jobs.daily
mainsequence project jobs runs list <JOB_ID>
mainsequence project jobs runs logs <JOB_RUN_ID> --max-wait-seconds 900
```

### Create an interval schedule

Use interval schedules when the cadence is simple, for example every hour:

```bash
mainsequence project jobs create \
  --name "Simulated Prices - Hourly" \
  --execution-path scripts/simulated_prices_launcher.py \
  --related-image-id <IMAGE_ID> \
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
  --related-image-id <IMAGE_ID> \
  --schedule-type crontab \
  --schedule-expression "0 0 * * *"
```

You can also add a start time or mark the schedule as one-off:

```bash
mainsequence project jobs create \
  --name "One-time Backfill" \
  --execution-path scripts/simulated_prices_launcher.py \
  --related-image-id <IMAGE_ID> \
  --schedule-type crontab \
  --schedule-expression "0 2 * * *" \
  --schedule-start-time "2026-03-15T02:00:00Z" \
  --schedule-one-off
```

### Inspect runs and logs

Once the job exists, the basic operational loop is:

```bash
mainsequence project jobs list
mainsequence project jobs runs list <JOB_ID>
mainsequence project jobs runs logs <JOB_RUN_ID> --max-wait-seconds 900
```

The logs command polls while the run is still `PENDING` or `RUNNING`, so it works well as a simple live tail for operational checks.

### Freeze a job to a project image

If you need reproducibility, pin the job to an image instead of letting it follow the moving repository tip.

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
  --related-image-id <IMAGE_ID>
```

This is the right pattern when you need confidence that the job will keep running with the same code and base image even after the repository evolves.

!!! note "Important"
    Project images are built from pushed commits. If a commit does not exist on the remote, it cannot be turned into a project image.

## Working from the Python client

The Python client is useful when job creation itself is part of your automation. It is the right tool when you want to provision jobs from code rather than from a shell session.

Examples below use the public client imports:

```python
from datetime import UTC, datetime

from mainsequence.client import CrontabSchedule, IntervalSchedule, Job, JobRun
```

### Create a manual job

```python
manual_job = Job.create(
    name="Simulated Prices - Manual",
    project_id=123,
    execution_path="scripts/simulated_prices_launcher.py",
    related_image_id=77,
    cpu_request="0.25",
    memory_request="0.5",
)
```

### Create an interval-scheduled job

```python
hourly_job = Job.create(
    name="Simulated Prices - Hourly",
    project_id=123,
    execution_path="scripts/simulated_prices_launcher.py",
    related_image_id=77,
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
    project_id=123,
    execution_path="scripts/simulated_prices_launcher.py",
    related_image_id=77,
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
jobs = Job.filter(project=123)

run_payload = nightly_job.run_job()
job_runs = JobRun.filter(job__id=[nightly_job.id])

latest_run = job_runs[0]
logs = latest_run.get_logs()
```

In practice, the client gives you the same lifecycle as the CLI:

- create the job
- run it immediately if needed
- inspect the resulting job runs
- read logs for the run you care about

!!! note "One practical difference"
    The CLI applies safe defaults for `cpu_request`, `memory_request`, `spot`, and `max_runtime_seconds` when you omit them. The Python client expects you to pass the compute values yourself, and jobs still require `related_image_id`.

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

For production-like schedules, prefer `scheduled_jobs.yaml` plus `mainsequence project schedule_batch_jobs`.

That gives you:

- review in pull requests
- a visible history of schedule changes
- less ambiguity about why a job exists
- a repository-managed wrapper around the same bulk job sync/create operation

### Separate creation from observation

Creating a schedule is only half of the work. Always verify:

```bash
mainsequence project jobs list
mainsequence project jobs runs list <JOB_ID>
mainsequence project jobs runs logs <JOB_RUN_ID>
```

That simple loop catches most configuration mistakes quickly.

## Common mistakes

### "The job was created, but it does not run what I expect"

Usually this means one of three things:

- the `execution_path` points at the wrong launcher
- the repository changed after the job was created and the job was not pinned to an image
- the schedule exists, but the actual logic should have been stored in `scheduled_jobs.yaml` and reviewed first

### "The client example fails, but the CLI worked"

The most common reasons are missing compute values or a missing `related_image_id`. The CLI supplies compute defaults. `Job.create()` does not.

### "The cron expression looks valid, but the API rejects it"

Make sure you are using five fields, not six.

### "The logs command exits too soon"

Use a larger wait window:

```bash
mainsequence project jobs runs logs <JOB_RUN_ID> --max-wait-seconds 900
```

or disable polling control if you want to handle it yourself.

## Related Reading

- [Tutorial Part 4: Orchestration](../../tutorial/scheduling_jobs.md)
- [Artifacts](./artifacts.md)
- [CLI Deep Dive](../cli.md)
