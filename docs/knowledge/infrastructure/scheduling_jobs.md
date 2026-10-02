# Scheduling Jobs

Jobs, FastAPI releases, and CodeRepository images are created from repository
workflow declarations. Put a direct `.yaml` or `.yml` child under
`.mainsequence/workflows/`, validate it with the CodeRepositoryBranch workflow
endpoint, commit it, and push the exact source commit. Django resolves the
resource and image identities and creates the deployment run. The SDK does not
create these objects by posting selected UIDs to collection endpoints.

## Create a Job from repository source

Retrieve the current template from
`GET /api/v1/code-repository-branches/{uid}/workflow-template/`. Declare a
`kind: job` resource with its repository-relative `execution_path`, compute
settings, and optional schedule. Validate the proposed repository `path` and
`content` with
`POST /api/v1/code-repository-branches/{uid}/validate-workflow/`.
The backend owns the accepted version, fields, defaults, permissions, and
application semantics; use the current template rather than copying an old
example as a schema.

Commit the workflow file and push it with Git as usual:

```bash
git add .mainsequence/workflows/jobs.yaml
git commit -m "Update job workflow"
git push
```

Inspect the repository-event result and the Job or deployment history after the
push. A successful Git push does not itself prove deployment success. Removing
a workflow declaration does not delete an existing Job.

## Deploy on every push or on release tags

The platform deploys from Git pushes according to the repository's
`.mainsequence/workflows/*.yaml`. A declaration's `tag_regex` decides which
pushes deploy:

- `tag_regex` omitted, or `null`: every push to the branch deploys.
- `tag_regex` set to a regular expression: a tag that matches it deploys the
  commit it points at, whether that is the branch's latest commit or an older
  commit on the branch. Every-push targets ignore tags.

Automatic deployment and its `tag_regex` are set in the workflow file only.
Use the current workflow template for where they go in a declaration. A
`ResourceRelease` or `Job` update does not accept them, and the SDK models
expose `automatic_deployment` and `automatic_redeployment_policy` as read-only
fields.

The Main Sequence platform does not create or name tags. Versions and release
tags belong to the repository: raise the version in `pyproject.toml` (for
example with `uv version --bump patch`) in the commit you want to release, and
let the repository's own CI create the tag. The following GitHub Actions
workflow is an example of such repository code, not a platform requirement. On
every push to `main` it runs the tests and then tags the version that
`pyproject.toml` declares, once:

```yaml
# Example: .github/workflows/release.yml in your repository
name: release
on:
  push:
    branches: [main]
concurrency:
  group: release-main
  cancel-in-progress: false
permissions:
  contents: write
jobs:
  release:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - uses: astral-sh/setup-uv@v6
      - run: uv sync --locked
      - run: uv run pytest
      - name: Tag the version declared in pyproject.toml
        run: |
          TAG="v$(uv version --short)"
          if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
            echo "$TAG already exists; nothing to release"
            exit 0
          fi
          git tag "$TAG" "$GITHUB_SHA"
          git push origin "refs/tags/$TAG"
```

With this example, a `tag_regex` such as `^v[0-9]+\.[0-9]+\.[0-9]+$` deploys
the commits that CI tagged, and a push that does not raise the version deploys
nothing.

After a dependency change, run `mainsequence code-repository sync` to refresh
`uv.lock`, the local environment and `requirements.txt`, then commit and push
those files with the change. The command makes no Git commit, tag or push.

## Schedules and arguments

Use an interval schedule for a regular period such as every hour, or a
five-field crontab expression for calendar timing. Define the schedule in the
repository workflow. `scheduled_command_args` is an ordered list of argv
entries copied into each future scheduler-created JobRun. Updating the saved
list changes only future runs; existing run snapshots stay unchanged.

Manual runs use only their own `command_args` and never inherit scheduled
arguments. Pass each argument separately:

```bash
mainsequence code-repository jobs run <JOB_UID> \
  --arg=--start-date \
  --arg=2026-09-08T16:43:00Z
mainsequence code-repository jobs run <JOB_UID> -- --family jobs
```

For an existing Job, `jobs update <JOB_UID> --scheduled-arg=<VALUE>` replaces
the saved list; `--clear-scheduled-args` clears it. The repository workflow may
reapply its declared value on a later event.

## Inspect and run an existing Job

```bash
mainsequence code-repository jobs list
mainsequence code-repository jobs run <JOB_UID>
mainsequence code-repository jobs runs list <JOB_UID>
mainsequence code-repository jobs runs logs <JOB_RUN_UID> --max-wait-seconds 900
mainsequence code-repository images list
```

The Job always refers to an exact persisted image. Each JobRun freezes the
image UID, digest, and commit selected at run creation, even if a later
repository event promotes a new image for the Job.

The Python client supports the same existing-Job operations:

```python
from mainsequence.client import Job, JobRun

jobs = Job.filter()
job = jobs[0]
run_payload = job.run_job(command_args=["--family", "jobs"])
job_runs = JobRun.filter(job__uid=job.uid)
logs = job_runs[0].get_logs()
usage = job_runs[0].get_resource_usage()
```

`logs` is an `OwnerLogPage` with opaque cursor pagination. `usage` contains
aggregate CPU, memory, and disk samples. Application code does not supply an
Organization Environment UID to read the run's application logs.

## Operational checks

Use repository-relative, forward-slash execution paths such as
`scripts/simulated_prices_launcher.py`. Validate the workflow before pushing.
If a scheduled run does not appear, inspect the applied Job, schedule, and
repository-event result. If a run starts but fails, inspect its frozen image
and logs. The CLI log command can wait longer with `--max-wait-seconds`.

## Related reading

- [Artifacts](./artifacts.md)
- [CLI Deep Dive](../cli.md)
