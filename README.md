<p align="center">
  <img src="https://www.main-sequence.io/images/logos/MS_logo_long_black.png" alt="Main Sequence Logo" width="500"/>
</p>

# Main Sequence Python SDK

[![Docs](https://img.shields.io/badge/docs-online-blue)](https://mainsequence-sdk.github.io/mainsequence-sdk/)
[![Open Issues](https://img.shields.io/github/issues/mainsequence-sdk/mainsequence-sdk)](https://github.com/mainsequence-sdk/mainsequence-sdk/issues)
[![Last Commit](https://img.shields.io/github/last-commit/mainsequence-sdk/mainsequence-sdk)](https://github.com/mainsequence-sdk/mainsequence-sdk/commits/main/)
[![Maintained](https://img.shields.io/badge/maintained-actively-green.svg)](https://github.com/mainsequence-sdk/mainsequence-sdk/commits/main/)

The Main Sequence Python SDK is the client and development toolkit for the Main
Sequence platform. It provides authentication, typed platform resources,
CodeRepository development and release operations, Agent and A2A workflows,
jobs, sharing, observability, logging, tracing, and reusable scaffold tooling.

MetaTables is now an independent domain package, installed as
`mainsequence-metatable` and imported as `metatables`. Table models, time-index
table updates, SQLAlchemy schemas, local database interfaces, and Alembic
execution are no longer implemented by this distribution.

## Repository Status

- Status: actively maintained
- Open issues: [GitHub Issues](https://github.com/mainsequence-sdk/mainsequence-sdk/issues)
- Documentation: [Documentation Site](https://mainsequence-sdk.github.io/mainsequence-sdk/)
- Security policy: [SECURITY.md](SECURITY.md)
- Release history: [CHANGELOG.md](CHANGELOG.md)
- Major-version migrations: [Migration guides](docs/migrations/v8-code-repository-ontology.md)

## What This Repository Contains

Main package areas:

- `mainsequence.client`: API client models for users, CodeRepositories, jobs,
  Agents, artifacts, constants, secrets, sharing, releases, and other platform
  resources
- `mainsequence.cli`: the complete `mainsequence` command-line interface for
  authentication, platform resources, and local CodeRepository operations
- `mainsequence.instrumentation` and `mainsequence.logconf`: SDK tracing and
  logging integration
- `mainsequence.server`: optional server-side caller assertion verification
- `mainsequence.scaffold_skills`: reusable, version-pinned skill copying for
  this SDK and extension libraries

Repository areas:

- `agent_scaffold/`: version-matched SDK skills and managed `AGENTS.md` content
- `docs/`: knowledge guides, CLI docs, architecture decisions, and generated
  reference docs
- `tests/`: offline tests plus explicitly marked live-backend tests

## Documentation Map

Recommended entry points:

- [Documentation home](docs/index.md)
- [Authentication](docs/knowledge/infrastructure/auth.md)
- [CLI overview](docs/cli/index.md)
- [Git source and Environment context](docs/knowledge/infrastructure/context.md)
- [Constants and Secrets](docs/knowledge/infrastructure/constants_and_secrets.md)
- [Scheduling Jobs](docs/knowledge/infrastructure/scheduling_jobs.md)
- [Resource releases](docs/knowledge/infrastructure/resource_releases.md)
- [Generated API reference](docs/reference/index.md)
- [MetaTables extraction guide](docs/migrations/metatables-sdk-removal.md)
- [ADR 0034](docs/adr/0034-extract-metatables-python-package.md)

The beginner tutorial is maintained in the separate
[MainSequence SDK tutorial CodeRepository](https://github.com/mainsequence-projects/mainsequence-sdk-tutorial).

## Quick Start

Install and authenticate:

```bash
pip install mainsequence
mainsequence login
mainsequence doctor
```

An already MCP-authenticated coding agent can establish the same persisted CLI
session with `mainsequence login --mcp`, then call the printed
`auth.cli_authorize` tool while the command waits. The backend supplies the
callback URI; tokens return directly to the CLI and are never exposed through
MCP.

Inspect and create CodeRepositories:

```bash
mainsequence code-repository search
mainsequence code-repository create my-first-repository
mainsequence code-repository set-up-locally <CODE_REPOSITORY_UID>
cd my-first-repository
mainsequence code-repository build-local-venv --path .
```

Use `mainsequence --help` and `mainsequence code-repository --help` for the
installed command surface. The CLI includes Agent and AgentSession,
CodeRepository, jobs and runs, images, resources and releases, constants,
secrets, teams, sharing, scaffold, Docker, and local-development commands.

## Python Client

`mainsequence.client` provides `AuthLoaders`, `MainSequenceClient`, generic
request and response helpers, and typed platform adapters. The platform owns
authorization and lifecycle policy. The SDK resolves CodeRepository source
identity from the current Git checkout when an operation requires that context.

Discover deployments with `ResourceRelease.filter(name=...)` or resolve a known
release UID with `ResourceRelease.get(pk=...)`. For shared deployments owned by
another repository, use the explicitly scoped `filter_admin` path. See
[Resource releases](docs/knowledge/infrastructure/resource_releases.md) for
supported filters and examples.

Server integrations can install `mainsequence[server]` for the
framework-independent [caller assertion verifier](docs/knowledge/server/caller_assertions.md).
Applications remain responsible for resource authorization and storage
operations.

Install `mainsequence-metatable` and import `metatables` for table-domain work.
See the [MetaTables migration guide](docs/migrations/metatables-sdk-removal.md)
for the install step, old and new imports, retired table CLI commands, and the
legacy SDK option.

## Development

This repository requires Python 3.13 or newer and uses `pyproject.toml` with a
development dependency group.

```bash
uv sync --group dev
pytest
ruff check .
mkdocs build --strict
```

Live-backend tests are marked `live` and excluded from a normal test run. Run
them explicitly with `pytest -m live` when credentials and a target backend are
available.

Package metadata is defined in [pyproject.toml](pyproject.toml). This repository
is licensed under the MIT License; see [LICENSE](LICENSE).
