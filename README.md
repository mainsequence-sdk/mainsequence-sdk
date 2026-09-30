# Main Sequence Python SDK

The `mainsequence` package is a Python adapter for the Main Sequence platform. It provides authentication, a generic HTTP client, direct platform resource models, CodeRepository source identity and local-development operations, and optional logging and tracing. Domain contracts and workflows live in their owning packages.

## Install and authenticate

```bash
pip install mainsequence
mainsequence login
mainsequence doctor
```

Install the optional tracing stack if the application calls `mainsequence.instrumentation.setup_tracing()` or exports OTLP spans:

```bash
pip install 'mainsequence[tracing]'
```

`mainsequence login --mcp` can establish the same CLI session through an already authenticated Main Sequence MCP principal. `mainsequence login --access-token ... --refresh-token ...` imports a JWT pair. For runtime credentials, set `MAINSEQUENCE_AUTH_MODE=runtime_credential` and the backend-provided credential ID and secret.

The CLI contains `login`, `logout`, `settings`, `version`, and `doctor`, plus the retained `code-repository` local-development commands for checkout setup, token refresh, Python environment maintenance, Git synchronization, SDK/scaffold updates, skill installation, and signed terminal access. Use `mainsequence code-repository --help` for that surface. Use `mainsequence settings set-backend <URL>` to change the backend endpoint, and `mainsequence doctor --check-connection` to probe it.

## Python client

`mainsequence.client` provides `AuthLoaders`, `MainSequenceClient`, generic request and response helpers, and typed adapters for platform resources such as users, CodeRepositories, jobs, agents, artifacts, constants, and secrets. The platform owns authorization and lifecycle policy. The SDK resolves CodeRepository source identity from the current Git checkout when a platform API needs that context.

Server integrations can install `mainsequence[server]` for the framework-independent [caller assertion verifier](docs/knowledge/server/caller_assertions.md). Applications own resource authorization and storage operations.

MetaTables, time-index table updates, SQLAlchemy schemas, local database interfaces, and Alembic migrations are no longer included. Use the independently ported `metatables` package for that domain. See [the removal migration guide](docs/migrations/metatables-sdk-removal.md) for old and new imports, retired CLI commands, and the legacy SDK option.

Discover deployments with `ResourceRelease.filter(name=...)` or resolve a
known release UID with `ResourceRelease.get(pk=...)`. For shared deployments
owned by another repository, use the explicitly scoped `filter_admin` path.
See [Resource releases](docs/knowledge/infrastructure/resource_releases.md)
for supported filters and examples.

## Documentation and development

- [Documentation index](docs/index.md)
- [Authentication](docs/knowledge/infrastructure/auth.md)
- [CLI reference](docs/cli/index.md)
- [ADR 0034](docs/adr/0034-remove-metatables-from-sdk.md)

The project requires Python 3.13 or newer. Development dependencies are in `pyproject.toml`; run `uv sync --group dev`, `pytest`, and `mkdocs build` from the checkout.

This repository is licensed under the MIT License. See [LICENSE](LICENSE) for the complete terms.
