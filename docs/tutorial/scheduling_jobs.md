# Part 4: Orchestration

Now that you've built and tested your `DataNode`s locally, it's time to orchestrate them on the Main Sequence Platform.

## Quick Summary

In this part, you will:

- sync local project changes to the platform from the CLI
- create manual jobs from the CLI
- freeze jobs to project images for reproducible execution
- define recurring schedules through backend-managed project workflow files
- store and reuse platform-managed files with `Artifact` when a workflow starts from file drops instead of APIs

DataNodes created in this part: **none new** (you orchestrate DataNodes built in previous parts).

This chapter is intentionally CLI-only so the workflow stays reproducible and easy to automate.

Some project and job-management steps can also be done through the VS Code extension, but the main tutorial documents only the terminal flow.

## Before You Start

Before creating or running jobs, make sure your CLI session is active and you are already in the project root directory.

```bash
cd /path/to/your/project
mainsequence login
mainsequence project refresh_token
mainsequence project current
mainsequence project jobs --help
```

- `mainsequence project current` should show the expected project UID and local path.
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

   You can also target by project UID:

   ```bash
   mainsequence project sync [PROJECT_UID] -m "Tutorial files"
   ```

3. **What `mainsequence project sync` does for you**

   - Ensures your local `.venv` and `uv` tooling are ready.
   - Applies the required patch package-version bump.
   - Runs `uv lock` and `uv sync`.
   - Exports locked production dependencies to `requirements.txt`.
   - Runs `git add -A` and creates your commit.
   - Requests the backend-owned default tag for the current ProjectBranch and
     creates that annotated tag (`v<version>` on `main`, branch-qualified elsewhere).
   - Pushes the branch and tag with `git push --follow-tags`.
   - Uses your project SSH key setup for secure push flow.

4. **Preview the fixed workflow without executing it**

   ```bash
   mainsequence project sync -m "Tutorial files" --dry-run
   ```

## 2) Scheduling Jobs

You can run jobs **manually** or **automatically** on a schedule.

### 2.1 Manual Run

You can create the same manual job from the terminal.

1. Create an unscheduled job:

   ```bash
   mainsequence project jobs create --name "Random Number Launcher - Manual Job" --execution-path scripts/random_number_launcher.py --related-image-uid <IMAGE_UID>
   ```

   Notes:

   - `execution-path` must be relative to the repository root, for example `scripts/random_number_launcher.py`.
   - If the CLI asks whether to build a schedule, answer **No** for a manual job.
   - Jobs require a `related_image_uid`.
   - If project images already exist, the CLI will prompt you to select one when `--related-image-uid` is omitted.
   - If you want to run the Part 3 example instead, replace the execution path with `scripts/simulated_prices_launcher.py`.

2. List the jobs for the current project and note the job UID:

   ```bash
   mainsequence project jobs list
   ```

3. Trigger the job manually:

   ```bash
   mainsequence project jobs run <JOB_UID>
   mainsequence project jobs run <JOB_UID> --arg demo-from-cli
   mainsequence project jobs run <JOB_UID> -- --name demo-from-cli
   ```

4. Inspect run history:

   ```bash
   mainsequence project jobs runs list <JOB_UID>
   ```

5. Stream logs for a specific run:

   ```bash
   mainsequence project jobs runs logs <JOB_RUN_UID> --max-wait-seconds 900
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
   mainsequence project jobs create --name "Random Number Launcher - Frozen Image" --execution-path scripts/random_number_launcher.py --related-image-uid <IMAGE_UID>
   ```

4. Verify the job and image linkage:

   ```bash
   mainsequence project jobs list
   mainsequence project images list
   ```

### 2.3 Automatic Schedule

For shared automation, store backend-managed declarations as direct `.yaml`
or `.yml` children of `.mainsequence/workflows/`. The old root-level
`scheduled_jobs.yaml` format and `schedule_batch_jobs` CLI command are removed.

Do not copy a historical example. First retrieve the current contract from:

```text
GET /api/v1/project-branches/{project_branch_uid}/workflow-template/
```

Edit the returned YAML using its advertised `api_version`, then validate the
proposed repository `path` and file `content` with:

```text
POST /api/v1/project-branches/{project_branch_uid}/validate-workflow/
```

The backend owns parsing, defaults, permissions, and validation. A workflow
file can declare Jobs and ResourceReleases supported by the current template.
The SDK and project must not implement a second parser or construct a separate
deployment payload.

Save the validated file under `.mainsequence/workflows/` and push it through
the normal project sync flow:

```bash
mainsequence project sync -m "Add scheduled project workflow"
```

Repository events apply valid workflow files independently. Removing a
declaration or deleting a file does not delete an existing backend resource;
there is no prune or strict-delete mode. After the push, inspect the repository
event result and resulting jobs or deployment runs. A successful push alone
does not prove that workflow application succeeded.

Verify the resulting job and, when immediate execution validation matters,
trigger and inspect one run:

```bash
mainsequence project jobs list
mainsequence project jobs run <JOB_UID>
mainsequence project jobs runs list <JOB_UID>
mainsequence project jobs runs logs <JOB_RUN_UID> --max-wait-seconds 900
```

#### CLI direct-create alternative

For a quick experiment, you can create a scheduled job directly from the terminal:

```bash
mainsequence project jobs create --name "Simulated Prices" --execution-path scripts/simulated_prices_launcher.py --related-image-uid <IMAGE_UID> --schedule-type crontab --schedule-expression "0 0 * * *"
```

For shared projects, prefer the validated `.mainsequence/workflows/` contract
because repository events, not a client-side batch upload, own application.

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
    logger.info("Artifact available: %s (uid=%s)", path.name, artifact.uid)
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
