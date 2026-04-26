# Part 4: Orchestration

Now that you've built and tested your `DataNode`s locally, it's time to orchestrate them on the Main Sequence Platform.

## Quick Summary

In this part, you will:

- sync local project changes to the platform from the CLI
- create manual jobs from the CLI
- freeze jobs to project images for reproducible execution
- define recurring schedules as code in a batch file (`scheduled_jobs.yaml`), which is the repository-managed input for the batch job sync/create flow
- store and reuse platform-managed files with `Artifact` when a workflow starts from file drops instead of APIs

DataNodes created in this part: **none new** (you orchestrate DataNodes built in previous parts).

This chapter is intentionally CLI-only so the workflow stays reproducible and easy to automate.

Some project and job-management steps can also be done through the VS Code extension, but the main tutorial documents only the terminal flow. A separate GUI tutorial will live under `docs/tutorial_gui/`.

## Before You Start

Before creating or running jobs, make sure your CLI session is active and you are already in the project root directory.

```bash
cd /path/to/your/project
mainsequence login
mainsequence project refresh_token
mainsequence project current
mainsequence project jobs --help
```

- `mainsequence project current` should show the expected project id and local path.
- All CLI examples below assume your current working directory is the repository root for the tutorial project.
- If you are running commands from another directory, add `--path /path/to/project` where needed.
- If a command says you are not logged in, run `mainsequence login` again.
- If `mainsequence project jobs` is missing, update or reinstall the CLI/SDK so your installed command set matches the current documentation.
- The commands shown below work in `bash`, `zsh`, and PowerShell. The command text is the same even if your shell prompt looks different.

## 1) Update Your Environment

Before scheduling anything, make sure your environment is consistent and your latest changes are committed.

1. **Run a dry-run first (recommended)** to preview everything the sync command will do:

   ```bash
   mainsequence project sync -m "Tutorial files" --dry-run
   ```

2. **Run the full sync workflow**:

   ```bash
   mainsequence project sync -m "Tutorial files"
   ```

   You can also target by project id:

   ```bash
   mainsequence project sync [PROJECT_ID] -m "Tutorial files"
   ```

3. **What `mainsequence project sync` does for you**

   - Ensures your local `.venv` and `uv` tooling are ready.
   - Bumps the package version (`patch` by default; configurable with `--bump`).
   - Runs `uv lock` and `uv sync`.
   - Exports locked dependencies to `requirements.txt`.
   - Runs `git add -A`, creates your commit, and pushes to remote (unless `--no-push` is used).
   - Uses your project SSH key setup for secure push flow.

4. **Useful options**

   ```bash
   # Bump minor version instead of patch
   mainsequence project sync -m "Tutorial files" --bump minor

   # Commit changes but skip push
   mainsequence project sync -m "Tutorial files" --no-push
   ```

## 2) Scheduling Jobs

You can run jobs **manually** or **automatically** on a schedule.

### 2.1 Manual Run

You can create the same manual job from the terminal.

1. Create an unscheduled job:

   ```bash
   mainsequence project jobs create --name "Random Number Launcher - Manual Job" --execution-path scripts/random_number_launcher.py --related-image-id <IMAGE_ID>
   ```

   Notes:

   - `execution-path` must be relative to the repository root, for example `scripts/random_number_launcher.py`.
   - If the CLI asks whether to build a schedule, answer **No** for a manual job.
   - Jobs require a `related_image_id`.
   - If project images already exist, the CLI will prompt you to select one when `--related-image-id` is omitted.
   - If you want to run the Part 3 example instead, replace the execution path with `scripts/simulated_prices_launcher.py`.

2. List the jobs for the current project and note the job id:

   ```bash
   mainsequence project jobs list
   ```

3. Trigger the job manually:

   ```bash
   mainsequence project jobs run <JOB_ID>
   mainsequence project jobs run <JOB_ID> --command python --command -m --command jobs.daily
   ```

4. Inspect run history:

   ```bash
   mainsequence project jobs runs list <JOB_ID>
   ```

5. Stream logs for a specific run:

   ```bash
   mainsequence project jobs runs logs <JOB_RUN_ID> --max-wait-seconds 900
   ```

### 2.2 Frozen Jobs with Images

One important concept in building strong systems is being able to guarantee that they will run even when you modify the repository later. To do that, you can freeze a job against a project image. This image captures a pushed commit plus the selected base image, so the job can keep running the same way even if the repository changes afterward.

1. List existing project images:

   ```bash
   mainsequence project images list
   ```

2. Create a new project image when needed:

   ```bash
   mainsequence project images create
   ```

   Notes:

   - The CLI will show pushed commits and may prompt you for `project_repo_hash` if you do not pass one explicitly.
   - Only commits that already exist on the remote can be used to build an image.
   - If the image takes time to build, increase the wait window if needed, for example:

   ```bash
   mainsequence project images create --timeout 600 --poll-interval 15
   ```

3. Create a job pinned to that image:

   ```bash
   mainsequence project jobs create --name "Random Number Launcher - Frozen Image" --execution-path scripts/random_number_launcher.py --related-image-id <IMAGE_ID>
   ```

4. Verify the job and image linkage:

   ```bash
   mainsequence project jobs list
   mainsequence project images list
   ```

### 2.3 Automatic Schedule

As projects and workflows grow, you will usually want **automation described as code**. You can define jobs and schedules in a reviewed YAML file, keep it in the repository, and apply the whole batch from the CLI.

`scheduled_jobs.yaml` is not a separate scheduler object. It is just the repository-managed input file for the CLI batch job sync/create flow.

Create a file named **`scheduled_jobs.yaml`** at the **repository root**.

**Windows path example:** `C:\Users\<YourName>\mainsequence\<YourOrganization>\projects\tutorial-project\scheduled_jobs.yaml`

**macOS/Linux path example:** `/home/<YourName>/mainsequence/<YourOrganization>/projects/tutorial-project/scheduled_jobs.yaml`

Add the following content to schedule `simulated_prices_launcher.py` to run daily at midnight:

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

**Note:** In the YAML file, always use forward slashes (`/`) for `execution_path`, even on Windows. The platform will handle path conversion automatically.

Set `spot` explicitly in repository-managed job files. Use `spot: false` when the job should stay on standard capacity. Use `spot: true` only when the job can tolerate interruption and restart. Conceptually, this is similar to GCP Spot capacity (previously called preemptible VMs): lower-cost capacity that may be reclaimed by the platform.

Each entry under `jobs` is validated with the same rules used for individual job creation. That means:

- each job needs a valid `name`
- each job must define exactly one of `execution_path` or `app_name`
- each job must define a `related_image_id`
- each scheduled job must use a valid `task_schedule`
- compute settings such as `cpu_request` and `memory_request` must also be valid
- `spot` should be set intentionally so reviewers can see whether the job is allowed to run on interruptible capacity

#### Keep the file version-controlled

If you want to follow the same signed-terminal flow used earlier in the tutorial, commit and push this file with a signed terminal:

```bash
mainsequence project open-signed-terminal [PROJECT_ID]
```

**Note:** Replace `[PROJECT_ID]` with your actual project id (for example, `60`).

Then, in the new terminal window that opens, run:

```bash
git add scheduled_jobs.yaml
git commit -m "Add scheduled jobs batch"
git push
```

This keeps the scheduling configuration reviewable in git, even though the actual scheduling step happens through the CLI command below.

#### CLI

To validate the batch file and submit all jobs in it, run:

```bash
mainsequence project schedule_batch_jobs scheduled_jobs.yaml
```

This command reads `scheduled_jobs.yaml`, validates it, and submits the whole batch through the platform's bulk job create/sync path.

Before the batch is submitted, the CLI shows the project's available images and asks you to choose which one the batch should use. That selected image is applied to every job in the submitted batch.

After the image is selected, the CLI asks for confirmation and explicitly warns that all jobs will be scheduled on that same image.

You can also pass an explicit path:

```bash
mainsequence project schedule_batch_jobs /path/to/project/scheduled_jobs.yaml
```

If you want the submitted file to become the full source of truth for scheduled jobs, use strict mode:

```bash
mainsequence project schedule_batch_jobs scheduled_jobs.yaml --strict
```

With `--strict`, jobs that already exist remotely but are not listed in `scheduled_jobs.yaml` may be removed. Jobs that are still linked to dashboards or resource releases are protected and will show up as not deleted in the batch result. By default, strict mode is off.

After the batch is submitted, verify that the scheduled job exists:

```bash
mainsequence project jobs list
```

If you want to validate the job immediately instead of waiting for the scheduler, you can trigger one run manually:

```bash
mainsequence project jobs run <JOB_ID>
mainsequence project jobs run <JOB_ID> --command python --command -m --command jobs.daily
mainsequence project jobs runs list <JOB_ID>
mainsequence project jobs runs logs <JOB_RUN_ID> --max-wait-seconds 900
```

Once the scheduler has triggered the job, inspect runs and logs:

```bash
mainsequence project jobs runs list <JOB_ID>
mainsequence project jobs runs logs <JOB_RUN_ID> --max-wait-seconds 900
```

#### CLI direct-create alternative

If you want to create a scheduled job directly from the terminal without relying on `scheduled_jobs.yaml`, you can do that too:

```bash
mainsequence project jobs create --name "Simulated Prices" --execution-path scripts/simulated_prices_launcher.py --related-image-id <IMAGE_ID> --schedule-type crontab --schedule-expression "0 0 * * *"
```

This direct CLI approach is useful for quick experiments. For shared projects, the repository-based `scheduled_jobs.yaml` flow is usually better because the schedule stays reviewable and version-controlled while still feeding the same bulk job sync/create behavior.

## 3) Artifacts: Platform-Managed Files

Not every workflow starts from an API. In many teams, the first input is a file drop: a spreadsheet from a vendor, a CSV exported by another system, or a model file generated by a job.

In Main Sequence, those files can live as **Artifacts** in named buckets. That gives you a stable platform reference instead of depending on a local path such as `C:\\temp\\...` or `/tmp/...`.

This is the right point in the tutorial to introduce Artifacts because they usually sit next to jobs:

- one job uploads the file
- another job or `DataNode` reads it back
- the rest of the platform stops depending on your laptop folder

Here is a simple upload script using the Python client:

```python
import os
from pathlib import Path

from tqdm import tqdm

from mainsequence.client import Artifact
from mainsequence.logconf import logger

BUCKET_NAME = "Vector de precios"
upload_path = Path(os.environ["VECTOR_UPLOAD_PATH"])

vector_files = sorted(
    path
    for path in upload_path.iterdir()
    if path.name.startswith("VectorAnalitico") and path.suffix.lower() == ".xls"
)

logger.info("Uploading %s vectors...", len(vector_files))

for path in tqdm(vector_files):
    artifact = Artifact.upload_file(
        filepath=str(path),
        name=path.name,
        bucket_name=BUCKET_NAME,
        created_by_resource_name="vector-upload-script",
    )
    logger.info("Artifact available: %s (id=%s)", path.name, artifact.id)
```

Once the file is in the platform, you can read it back from a job or `DataNode`:

```python
import pandas as pd

from mainsequence.client import Artifact

source_artifact = Artifact.get(
    bucket__name="Vector de precios",
    name="VectorAnalitico_2026_03_15.xls",
)

vector_df = pd.read_excel(source_artifact.content)
```

The important idea is that the **bucket + artifact name** becomes the durable reference, not the original local filesystem path.

For a deeper explanation of jobs, schedules, images, runs, and the Python client, see [Scheduling Jobs](../knowledge/infrastructure/scheduling_jobs.md). For a deeper explanation of buckets, upload patterns, and using Artifacts from `DataNode`s, see [Artifacts](../knowledge/infrastructure/artifacts.md).
