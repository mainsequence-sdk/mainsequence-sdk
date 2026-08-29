<p align="center">
  <img src="https://www.main-sequence.io/images/logos/MS_logo_long_black.png" alt="Main Sequence Logo" width="500"/>
</p>

# Main Sequence Python SDK

[![Docs](https://img.shields.io/badge/docs-online-blue)](https://mainsequence-sdk.github.io/mainsequence-sdk/)
[![Open Issues](https://img.shields.io/github/issues/mainsequence-sdk/mainsequence-sdk)](https://github.com/mainsequence-sdk/mainsequence-sdk/issues)
[![Last Commit](https://img.shields.io/github/last-commit/mainsequence-sdk/mainsequence-sdk)](https://github.com/mainsequence-sdk/mainsequence-sdk/commits/main/)
[![Maintained](https://img.shields.io/badge/maintained-actively-green.svg)](https://github.com/mainsequence-sdk/mainsequence-sdk/commits/main/)

The Main Sequence Python SDK is the client and development toolkit for the Main Sequence platform.

The Main Sequence platform allows you to:

1. rapidly build and deploy data products with normalized `MetaTable` contracts and `TimeIndexTableUpdater` producer workflows
2. rapidly deploy RBAC-enabled dashboards on the platform
3. rapidly deploy agents using the Google Agent SDK

The key idea is that you can focus on development and deployment, while the platform handles the DevOps layer.

## Repository Status

- Status: actively maintained
- Last commit: `2026-03-27` - [`6aa5dea`](https://github.com/mainsequence-sdk/mainsequence-sdk/commit/6aa5deab73586226c7b267abf2e3f02f4f7e7669) - `docs logo fix v.3.17.6`
- Open issues: [GitHub Issues](https://github.com/mainsequence-sdk/mainsequence-sdk/issues)
- Documentation: [Documentation Site](https://mainsequence-sdk.github.io/mainsequence-sdk/)
- Security policy: [SECURITY.md](SECURITY.md)
- Release history: [CHANGELOG.md](CHANGELOG.md)
- Major-version migrations: [Migration guides](docs/migrations/v8-code-repository-ontology.md)

## What this repository contains

This repository contains the SDK and documentation used to build and operate Main Sequence CodeRepositories.

Main package areas:

- `mainsequence.meta_tables`: `MetaTable`s, `TimeIndexTableUpdater`s, update workflows, and persistence
- `mainsequence.client`: API client models for CodeRepositories, jobs, time-index tables, sharing, and platform resources
- `mainsequence.cli`: the `mainsequence` command-line interface

Repository areas:

- `docs/`: knowledge guides, CLI docs, and generated reference docs
- `tests/`: automated tests

## Documentation map

The documentation is organized into three reading modes:

1. **Knowledge**: deeper conceptual guides
2. **CLI**: command-focused operational documentation
3. **Reference**: generated API reference

Recommended entry points:

- Beginner tutorial:
  - [MainSequence SDK tutorial CodeRepository](https://github.com/mainsequence-projects/mainsequence-sdk-tutorial)
- Knowledge:
  - [Time-Index Table Updaters](docs/knowledge/time_index_table_updates.md)
  - [MetaTables](docs/knowledge/meta_tables/index.md)
  - [Constants and Secrets](docs/knowledge/infrastructure/constants_and_secrets.md)
  - [Scheduling Jobs](docs/knowledge/infrastructure/scheduling_jobs.md)
  - [Streamlit Dashboards](docs/knowledge/dashboards/streamlit/index.md)
- Migrations:
  - [7.x to 8.0: CodeRepository ontology hard cut](docs/migrations/v8-code-repository-ontology.md)
  - [6.x to 7.0: Time-Index Table Updater hard cut](docs/migrations/v7-time-index-table-updater-hard-cut.md)
- CLI:
  - [CLI Overview](docs/cli/index.md)

## Quick start

Install the package:

```bash
pip install mainsequence
```

Authenticate:

```bash
mainsequence login
```

An already MCP-authenticated coding agent can establish the same persisted CLI
session with `mainsequence login --mcp`, then call the printed
`auth.cli_authorize` tool while the command waits. The backend supplies the
callback URI; tokens return directly to the CLI and are never exposed through
MCP.

Check that you can see your CodeRepositories:

```bash
mainsequence code-repository list
```

Create a new CodeRepository:

```bash
mainsequence code-repository create my-first-repository
```

Set it up locally:

```bash
mainsequence code-repository set-up-locally <CODE_REPOSITORY_UID>
cd my-first-repository
mainsequence code-repository build-local-venv --path .
```

From there, the normal learning path is:

1. model your first canonical table with a backend-managed `MetaTable`
2. create your first `TimeIndexTableUpdater` as an opinionated MetaTable-backed update workflow
3. understand sharing and RBAC for published tables
4. add an API or another application surface
5. schedule jobs
6. build dashboards or downstream consumers
7. package the CodeRepository as an agent-facing surface when it is ready

## Installation for development

This repository uses `pyproject.toml` and a development dependency group.

With `uv`:

```bash
uv sync --group dev
```

Or with `pip`, install the package and the docs/test tools you need separately.

## Common development commands

Run the CLI:

```bash
mainsequence --help
```

Run tests:

```bash
pytest
```

Serve the docs locally:

```bash
mkdocs serve
```

Build the docs:

```bash
mkdocs build
```

Lint the code:

```bash
ruff check .
```

Format the code:

```bash
black .
```

## How to read this repository

If you are evaluating the platform:

- start with the separate canonical tutorial CodeRepository linked above

If you are building a feature and already know the area:

- go straight to the relevant guide in `docs/knowledge/`

If you are operating CodeRepositories day to day:

- use `docs/cli/` and the `mainsequence --help` command tree

If you need the exact SDK surface:

- use `docs/reference/`

## Package metadata

- Package name: `mainsequence`
- Python: `>=3.13`
- CLI entry point: `mainsequence`

Package metadata is defined in [pyproject.toml](pyproject.toml).

## License

This repository is distributed under the terms described in [LICENSE](LICENSE).
