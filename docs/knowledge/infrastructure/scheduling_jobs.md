# Scheduling Jobs

Job definitions and schedules are accepted by the backend from CodeRepository workflow declarations. The backend owns schema validation, defaults, authorization, and lifecycle policy. The SDK keeps direct adapters for reading existing Jobs and JobRuns and issuing their API actions.

Retrieve the current workflow template from `GET /api/v1/code-repository-branches/{uid}/workflow-template/` and validate proposed content with `POST /api/v1/code-repository-branches/{uid}/validate-workflow/` before committing it. The repository workflow is the source of truth for future scheduled runs.

```python
from mainsequence.client import Job, JobRun

jobs = Job.filter()
job = jobs[0]
run_payload = job.run_job(command_args=["--family", "jobs"])
job_runs = JobRun.filter(job__uid=job.uid)
logs = job_runs[0].get_logs()
```

Each JobRun freezes the selected image and command arguments when it is created. Inspect the JobRun status and logs to determine whether a run succeeded. Repository sync and deployment orchestration are no longer SDK CLI operations.
