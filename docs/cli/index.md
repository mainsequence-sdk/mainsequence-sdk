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
mainsequence settings show
mainsequence sdk latest
```

## Project Commands

```bash
mainsequence project --help
```

Most frequently used flows:

```bash
# 1) List and create
mainsequence project list
mainsequence project create tutorial-project
mainsequence project list data_nodes_updates
mainsequence project list data_nodes_updates 123

# 2) Set up locally
mainsequence project set-up-locally 123
mainsequence project refresh_token

# 3) Environment setup
mainsequence project build_local_venv
mainsequence project build_local_venv --path .
mainsequence project freeze-env --path .

# 4) Day-to-day sync
mainsequence project sync --path . -m "Update environment"
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
