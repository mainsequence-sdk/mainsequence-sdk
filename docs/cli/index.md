# MainSequence CLI

This page gives a practical overview of the `mainsequence` command-line interface.
For command-by-command behavior, use `--help` (for example: `mainsequence project --help`).
For a deeper workflow guide, see [CLI Deep Dive](../knowledge/cli.md).

## Installation

```bash
pip install mainsequence
```

## Authentication

```bash
mainsequence login you@company.com
mainsequence login you@company.com 127.0.0.1:8000 mainsequence-dev
mainsequence logout
```

Backend/base-folder overrides passed to `login` are terminal-session only. They do not rewrite the persisted CLI settings for other terminals.

If you prefer shell-managed environment variables:

```bash
mainsequence login you@company.com --export
mainsequence logout --export
```

## Core Command Groups

## Top-Level Commands

```bash
mainsequence --help
mainsequence doctor
mainsequence markets --help
mainsequence user
mainsequence settings show
mainsequence sdk latest
```

## Project Commands

```bash
mainsequence project --help
```

Most frequently used flows:

```bash
# Markets
mainsequence markets portfolios list
mainsequence markets asset-translation-table list
mainsequence markets asset-translation-table detail 12

# 1) List and create
mainsequence project list
mainsequence project images list
mainsequence project images list 123
mainsequence project create tutorial-project
mainsequence project images create
mainsequence project images create 123
mainsequence project images create 123 4a1b2c3d
mainsequence project images create 123 --timeout 600 --poll-interval 15
mainsequence project jobs list
mainsequence project jobs runs list 91
mainsequence project jobs runs logs 501
mainsequence project jobs runs logs 501 --max-wait-seconds 900
mainsequence project jobs run 91
mainsequence project jobs create --name daily-run --execution-path scripts/test.py
mainsequence project data-node-updates list
mainsequence project data-node-updates list 123

# 2) Set up locally
mainsequence project set-up-locally 123
mainsequence project refresh_token

# 3) Environment setup
mainsequence project build_local_venv
mainsequence project build_local_venv --path .
mainsequence project freeze-env --path .

# 4) Day-to-day sync
mainsequence project sync --path . -m "Update environment"
mainsequence project sync_project "Update environment"
mainsequence project sync_project "Update environment" --path .

# 5) Docker/devcontainer
mainsequence project build-docker-env --path .

# 6) SDK maintenance
mainsequence project sdk-status --path .
mainsequence project update-sdk --path .
```

## Settings

```bash
mainsequence settings show
mainsequence settings set-backend https://main-sequence.app
mainsequence settings set-base ~/mainsequence
```

## Troubleshooting

- Run `mainsequence doctor` to check config, auth visibility, and tool availability.
- If a command says not logged in, run `mainsequence login <email>` again.
- If your shell cannot use secure token storage, use `--export` mode.
- `mainsequence user` shows the authenticated MainSequence user through the SDK client `User.get_logged_user()` path.
- `mainsequence markets portfolios list` lists markets portfolios through the SDK client `Portfolio.filter()` path.
- `mainsequence markets asset-translation-table list` lists translation tables through the SDK client `AssetTranslationTable.filter()` path.
- `mainsequence markets asset-translation-table detail` fetches one translation table through `AssetTranslationTable.get()` and renders each rule as a readable `match => target` mapping in the terminal.
- `mainsequence project images list` lists project images using the SDK client `ProjectImage.filter()` path.
- `mainsequence project images create` only accepts pushed commits for `project_repo_hash`. If omitted, it lists commits from the current branch upstream (or remote refs as fallback), shows which commits already have image ids, and waits until `is_ready=true` by polling every 30 seconds for up to 5 minutes by default.
- `mainsequence project jobs list` lists project jobs through the SDK client `Job.filter()` path.
- `mainsequence project jobs list` shows a human-readable schedule summary from `task_schedule`.
- `mainsequence project data-node-updates list` lists data node updates through the SDK client `Project.get_data_nodes_updates()` path.
- `mainsequence project jobs runs list` lists job-run history through the SDK client `JobRun.filter(job__id=[job_id])` path.
- `mainsequence project jobs runs logs` fetches logs through the SDK client `JobRun.get_logs()` path, polls every 30 seconds by default while the job run is `PENDING` or `RUNNING`, and stops after 10 minutes unless you override `--max-wait-seconds` or disable it with `--max-wait-seconds 0`.
- `mainsequence project jobs run` triggers a manual run through the SDK client `Job.run_job()` path.
- `mainsequence project jobs create` creates jobs through the SDK client `Job.create()` path, uses the selected project image directly, expects `execution_path` relative to the content root, for example `scripts/test.py`, builds interval or crontab schedules interactively when requested, and defaults compute settings to `cpu_request=0.25`, `memory_request=0.5`, `spot=false`, `max_runtime_seconds=86400` when omitted.
