# Part 1: Setting a Project (CLI)

## Introduction

If you are building a pricing engine, a research pipeline, or a dashboard, the bottleneck is usually infrastructure: repositories, environments, storage, compute, permissions, and deployment.

Main Sequence removes that setup overhead. A project gives you a production-ready workspace where code, data, and compute are already connected.

The Main Sequence platform allows you to:

1. rapidly build and deploy data products and data workflows as a unified API with a normalized structure through `DataNode`s
2. rapidly deploy RBAC-enabled dashboards on the platform
3. rapidly deploy agents using the Google Agent SDK

The key idea is that you can focus on development and deployment, while the platform handles the DevOps layer.

This chapter uses a **CLI-only** flow.

## Quick Summary

In this part, you will:

- create a new project from the CLI
- map the project to your local machine
- open the project folder and build the local Python environment
- learn the default project parameters used by the platform

DataNodes created in this part: **none**.

This tutorial is intentionally CLI-only.

Some of the setup steps can also be done through the VS Code extension, but that flow is kept separate. A dedicated GUI tutorial will live under `docs/tutorial_gui/`.

## 1. Install and Verify the CLI

Open a terminal and install:

```bash
pip install mainsequence
```

Then verify:

```bash
mainsequence --help
# or:
python -m mainsequence --help
```

## 2. Log In

```bash
mainsequence login [USER_EMAIL]
```

You can validate access with:

```bash
mainsequence project list
```

## 3. Check Existing Project Names

Before you create a new project, inspect the project names already visible in your organization:

```bash
mainsequence organization project-names
```

If your intended name already appears in that list, choose another one before you create the project.

This command helps you avoid name collisions. It does not replace your organization's naming rules, but it is the fastest way to check whether a tutorial name such as `tutorial-project-cli` is already in use.

You can also validate the exact candidate name directly:

```bash
mainsequence project validate-name "tutorial-project-cli"
```

This command checks whether the name is available, shows the normalized slug/library names the platform will derive from it, and prints suggested alternatives when the name is already taken.

## 4. Create a New Project

Run:

```bash
mainsequence project create [PROJECT_NAME]
```

Example:

```bash
mainsequence project create tutorial-project-cli
```

The CLI validates the project name before continuing, prompts for any missing values, and uses sensible defaults. It also polls the backend every 30 seconds until `is_initialized=true`.

!!! note "Default parameter: data source (`--data-source-id`)"
    The CLI defaults to the first available data source.

    In Main Sequence, data is accessed through a **Data Source** abstraction rather than being tied directly to a specific database or storage system. This lets you work with data without needing to make database-specific decisions about schemas, storage engines, or query languages. Your workflows remain consistent even if the underlying storage changes.

    _Add your organization guidance here (for example: the recommended data source, naming conventions, or storage constraints)._

!!! note "Default parameter: base image (`--default-base-image-id`)"
    Projects run in a secure, organization-approved compute environment.

    The **base image** defines the operating environment used to run your project, including the required runtime, dependencies, and system configuration. When a project is executed, Main Sequence runs the code from the repository using this image. This ensures a consistent and approved environment across development, testing, and deployment, helping support reliable CI/CD workflows.

!!! note "Default parameter: GitHub organization (`--github-org-id`)"
    The CLI defaults to the first available GitHub organization (if available).

    Main Sequence organizes projects as repositories either in your selected GitHub organization or in the default Main Sequence-managed repository system. This gives you control over where your project code is hosted and managed.

    _Add your organization guidance here (for example: which organization to use and the required permission model)._

!!! note "Default parameter: branch (`--branch`)"
    The default branch is `main`.

    Instead of switching branches for different deployments, each project is associated with a specific branch. This makes it easier to separate development and production deployments, test changes safely, and keep a consistent project structure and view across environments.

## 5. Confirm Project Creation

List projects and identify the new project ID:

```bash
mainsequence project list
```

You should see your new project with an ID, for example:

```text
ID   Project               Data Source  Class         Status     Local  Path
--   -------               -----------  -----         ------     -----  ----
130  tutorial-project-cli  Default DB   timescale_db  AVAILABLE  -      -
```

## 6. Set Up the Project Locally

Use the project ID from the previous step:

```bash
mainsequence project set-up-locally [PROJECT_ID]
```

Example:

```bash
mainsequence project set-up-locally 130
```

Verify mapping:

```bash
mainsequence project list
```

The `Local` column should show `Local`, and `Path` should point to your local folder.

## 7. Open a Signed Terminal (Required for Git Push/Pull)

Before running git commands, open a signed terminal so the project SSH key is loaded in `ssh-agent`.

### macOS/Linux (bash/zsh)

```bash
mainsequence project open-signed-terminal 130
```

### Windows (PowerShell)

```powershell
mainsequence project open-signed-terminal 130
```

Use the newly opened terminal for `git pull`, `git push`, and any other git operations against the project repository.

## 8. Move Into the Project You Just Created

### macOS/Linux (bash/zsh)

```bash
PROJECT_ID=130
PROJECT_PATH=$(mainsequence project list | awk -v id="$PROJECT_ID" '$1 == id {print $NF}')
cd "$PROJECT_PATH"
pwd
```

### Windows (PowerShell)

```powershell
$ProjectId = 130
$ProjectPath = (
  mainsequence project list |
  Select-String "^\s*$ProjectId\s+" |
  ForEach-Object { ($_ -split '\s{2,}')[-1] }
)
Set-Location $ProjectPath
Get-Location
```

If `open-signed-terminal` opened directly in the project folder, you can keep working there and skip this step.

## 9. Understand the Folder Structure Created

After `mainsequence project set-up-locally [PROJECT_ID]`, your local project folder should look similar to:

```text
tutorial-project-cli-130/
├── .env
├── pyproject.toml
├── requirements.txt
├── src/
├── dashboards/
├── tests/
├── Dockerfile          (if DEFAULT_BASE_IMAGE is available)
└── .dockerignore       (if DEFAULT_BASE_IMAGE is available)
```

Key points:
- `.env` is generated by the CLI during local setup and includes project runtime variables (for example `MAINSEQUENCE_ACCESS_TOKEN`, `MAINSEQUENCE_REFRESH_TOKEN`, `TDAG_ENDPOINT`, and `MAIN_SEQUENCE_PROJECT_ID`).
- `src/`, `dashboards/`, `tests/`, `pyproject.toml`, and `requirements.txt` come from the project repository scaffold.
- `Dockerfile` and `.dockerignore` are scaffolded only when a default base image is present.

## 10. Build the Local Environment

Inside the project folder, create and sync a local `.venv`:

```bash
mainsequence project build_local_venv --path .
```

This command:
- reads `pyproject.toml`,
- creates `.venv` with the requested Python version,
- runs `uv sync` into that `.venv`.

After this step, your folder will also include:

```text
tutorial-project-cli-130/
└── .venv/
```

After completion, your project is ready for local development.

For a deeper reference on the CLI workflow behind this setup, see [CLI Deep Dive](../knowledge/cli.md).
