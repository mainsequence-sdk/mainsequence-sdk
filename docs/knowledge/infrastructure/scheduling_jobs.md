# Scheduling Jobs

Part 4 of the tutorial shows the shortest path to getting a job running. This page explains the same topic from an infrastructure point of view: what a job is, where schedules should live, how images fit in, and when to use the CLI versus the Python client.

## Quick Summary

In this guide, you will:

- understand the lifecycle from project code to scheduled execution
- manage jobs from the CLI
- create the same jobs from the Python client
- decide when to use `project_configuration.yaml` and when to create jobs directly
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

If you keep that chain in mind, most operational decisions become straightforward.

## The recommended default

For shared projects, treat recurring schedules as part of the repository.

That means:

- define recurring jobs in `project_configuration.yaml`
- sync the project with `mainsequence project sync`
- use direct CLI or client-created jobs mainly for experiments, backfills, or one-off operational tasks

!!! tip "Default rule"
    If a job is important enough to run every day, it is usually important enough to review in version control.

## Two ways to create jobs

There are two valid workflows, and they serve different purposes.

### 1. Repository-managed jobs

This is the best option for long-lived schedules used by a team.

You define the job in `project_configuration.yaml`, commit it, and push it with:

```bash
mainsequence project sync -m "Add nightly simulated prices job"
```

Example:

```yaml
name: "Tutorial Job Configuration"
jobs:
  - name: "Simulated Prices"
    resource:
      script:
        path: "scripts/simulated_prices_launcher.py"
    schedule:
      type: "crontab"
      expression: "0 0 * * *"
```

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

### Create a manual job

Use this when you want a job that only runs when someone triggers it:

```bash
mainsequence project jobs create \
  --name "Simulated Prices - Manual" \
  --execution-path scripts/simulated_prices_launcher.py
```

Then run it:

```bash
mainsequence project jobs list
mainsequence project jobs run <JOB_ID>
```

### Create an interval schedule

Use interval schedules when the cadence is simple, for example every hour:

```bash
mainsequence project jobs create \
  --name "Simulated Prices - Hourly" \
  --execution-path scripts/simulated_prices_launcher.py \
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
  --schedule-type crontab \
  --schedule-expression "0 0 * * *"
```

You can also add a start time or mark the schedule as one-off:

```bash
mainsequence project jobs create \
  --name "One-time Backfill" \
  --execution-path scripts/simulated_prices_launcher.py \
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
    The CLI applies safe defaults for `cpu_request`, `memory_request`, `spot`, and `max_runtime_seconds` when you omit them. The Python client expects you to pass the compute values yourself.

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

For production-like schedules, prefer `project_configuration.yaml` plus `mainsequence project sync`.

That gives you:

- review in pull requests
- a visible history of schedule changes
- less ambiguity about why a job exists

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
- the schedule exists, but the actual logic should have been stored in `project_configuration.yaml` and reviewed first

### "The client example fails, but the CLI worked"

The most common reason is missing compute values. The CLI supplies defaults. `Job.create()` does not.

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
- [CLI Deep Dive](../cli.md)
