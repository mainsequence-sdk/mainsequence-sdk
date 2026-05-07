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

1. rapidly build and deploy data products and data workflows as a unified API with a normalized structure through `DataNode`s
2. rapidly deploy RBAC-enabled dashboards on the platform
3. rapidly deploy agents using the Google Agent SDK

The key idea is that you can focus on development and deployment, while the platform handles the DevOps layer.

## Project Status

- Status: actively maintained
- Last commit: `2026-03-27` - [`6aa5dea`](https://github.com/mainsequence-sdk/mainsequence-sdk/commit/6aa5deab73586226c7b267abf2e3f02f4f7e7669) - `docs logo fix v.3.17.6`
- Open issues: [GitHub Issues](https://github.com/mainsequence-sdk/mainsequence-sdk/issues)
- Documentation: [Documentation Site](https://mainsequence-sdk.github.io/mainsequence-sdk/)
- Security policy: [SECURITY.md](SECURITY.md)
- Release history: [CHANGELOG.md](CHANGELOG.md)

## What this repository contains

This repository contains the SDK and the documentation used to build and operate Main Sequence projects.

Main package areas:

- `mainsequence.tdag`: data orchestration, `DataNode`s, update workflows, and persistence
- `mainsequence.client`: API client models for projects, jobs, data node storages, assets, sharing, and platform resources
- `mainsequence.virtualfundbuilder`: portfolio construction and portfolio time series workflows
- `mainsequence.instruments`: pricing-oriented market data and instrument tooling
- `mainsequence.dashboards.streamlit`: reusable Streamlit scaffolding and dashboard helpers
- `mainsequence.cli`: the `mainsequence` command-line interface

Repository areas:

- `docs/`: tutorials, knowledge guides, CLI docs, and generated reference docs
- `examples/`: worked examples and usage patterns
- `tests/`: automated tests

## Documentation map

The documentation is organized into four reading modes:

1. **Tutorial**: the guided learning path
2. **Knowledge**: deeper conceptual guides
3. **CLI**: command-focused operational documentation
4. **Reference**: generated API reference

Recommended entry points:

- Tutorial:
  - [Setting a Project (CLI)](docs/tutorial/setting_a_project.md)
  - [Creating a Data Node](docs/tutorial/creating_a_simple_data_node.md)
  - [Working With Simple Tables](docs/tutorial/working_with_simple_tables.md)
  - [Create Your First API](docs/tutorial/create_your_first_api.md)
  - [Role-Based Access Control](docs/tutorial/role_based_access_control.md)
- Knowledge:
  - [Data Nodes](docs/knowledge/data_nodes.md)
  - [Command Center](docs/knowledge/command_center/index.md)
  - [Command Center Widget Data Contracts](docs/knowledge/command_center/widget_data_contracts.md)
  - [Simple Tables](docs/knowledge/simple_tables/simple_table.md)
  - [Constants and Secrets](docs/knowledge/infrastructure/constants_and_secrets.md)
  - [Scheduling Jobs](docs/knowledge/infrastructure/scheduling_jobs.md)
  - [Streamlit Helpers](docs/knowledge/dashboards/streamlit/index.md)
  - [Virtual Fund Builder](docs/knowledge/virtualfundbuilder/index.md)
  - [Instruments](docs/knowledge/instruments/index.md)
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

Check that you can see your projects:

```bash
mainsequence project list
```

Create a new project:

```bash
mainsequence project create my-first-project
```

Set it up locally:

```bash
mainsequence project set-up-locally <PROJECT_ID>
cd my-first-project
mainsequence project build_local_venv --path .
```

From there, the normal learning path is:

1. create your first `DataNode`
2. model app-facing data with `SimpleTable` when needed
3. add an API or another application surface
4. understand sharing and RBAC
5. schedule jobs
6. build dashboards or downstream consumers

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

- start with the tutorial in `docs/tutorial/`

If you are building a feature and already know the area:

- go straight to the relevant guide in `docs/knowledge/`

If you are operating projects day to day:

- use `docs/cli/` and the `mainsequence --help` command tree

If you need the exact SDK surface:

- use `docs/reference/`

## Package metadata

- Package name: `mainsequence`
- Python: `>=3.11`
- CLI entry point: `mainsequence`

Project metadata is defined in [pyproject.toml](pyproject.toml).

## License

This repository is distributed under the terms described in [LICENSE](LICENSE).
