# ADR 0034: Remove MetaTables and leave a thin `mainsequence` SDK

Date: 2026-09-27

Status: Implemented on `metatables_removal`; release pending independent-package version confirmation

## Context and scope

The `mainsequence` SDK still contains two parts of the MetaTables Python API:

- `mainsequence.meta_tables` contains SQLAlchemy contracts, compiled SQL, hashing and schema naming, updater workflows, and client-side Alembic providers, scaffolding, and templates.
- `mainsequence.client.metatables` contains the MetaTable, TimeIndexMetaTable, update, run, DataSource, request, and response models.

The independent `metatables` package has already been ported by a separate owner. Its package and service are the destination for MetaTables functionality. **This ADR assigns no implementation, transport, parity, or testing work to that project.** Its task is to remove MetaTables and other SDK-owned functionality outside the thin-adapter boundary from the `mainsequence` checkout on branch `metatables_removal`.

This is an SDK package change. It does not change Django TS Manager, the independent FastAPI service, existing catalog records, physical tables, or consumer repositories. Previously released SDK versions remain available to consumers that still use the legacy Django TS service. Record the independent package version and its consumer migration guide before releasing an SDK version without the old imports; creating those artifacts is outside this ADR.

## Decision

Delete `mainsequence/meta_tables/` and `mainsequence/client/metatables/` from `metatables_removal`. Remove their SDK exports, routes, constants, CLI commands, documentation, bundled skills, and dependencies wherever those are MetaTables-specific. Removing documentation and skills means deleting SDK-owned MetaTables guides and skill files, not merely changing their import examples. Remove other domain and operational implementations that do not fit the retained SDK responsibilities below. This branch removes code; it does not add replacement implementations to the independent package or a service. The SDK must not retain an import shim, a forwarding implementation, or a required dependency on `metatables` to make unrelated platform calls work. Removed public Python and CLI interfaces are breaking changes and must be named in the release migration note.

The replacement imports below describe the already ported package for SDK consumers; they are not work to implement under this ADR:

| Removed SDK import | Independent package import |
| --- | --- |
| `mainsequence.meta_tables` | `metatables` |
| `mainsequence.meta_tables.<module>` | `metatables.<module>` |
| `mainsequence.meta_tables.time_index_table_updates.<module>` | `metatables.time_index_table_updates.<module>` |
| `mainsequence.meta_tables.migrations.<module>` | `metatables.migrations.<module>` |
| `mainsequence.client.metatables` | `metatables.client.metatables` or its public root exports |
| `mainsequence.client.metatables.core` | `metatables.client.metatables.core` |

## SDK functionality retained after removal

The required end state of this ADR is a thin platform adapter. The SDK owns authentication and the generic logic needed to call platform APIs. Domain packages own domain contracts and workflows; the backend owns authorization and resource policy. A method belongs in the SDK when it generically prepares a platform request, transports it, or reads its response. A method that creates domain data, manages a schema, or orchestrates a multi-step workflow does not remain merely because it is attached to a platform resource model.

| Retained area | Functionality |
| --- | --- |
| Authentication and configuration | Endpoint selection, CLI login and logout, stored user tokens, JWT and session-JWT refresh, runtime-credential exchange, and authenticated request headers. Existing `AuthLoaders` and auth providers remain SDK functionality. |
| Generic HTTP client | Request/session handling, timeouts, safe retries, pagination, serialization, error conversion, and authentication refresh used by direct platform API calls. Generic `make_request`, `MainSequenceClient`, `BaseObjectOrm`, and `BasePydanticModel` capabilities remain; this removal does not require a new transport implementation. |
| Platform resource adapters | Small typed request/response projections and direct API operations for users, organizations, CodeRepositories and branches, jobs and runs, agents, releases, artifacts, constants, secrets, sharing, and other platform-owned resources. The backend remains authoritative for permissions, defaults, lifecycle, and policy. |
| CodeRepository source identity | The minimum Git and authenticated-runtime context, branch resolution, Environment scoping, and process source-drift validation required by remaining platform operations under ADR 0031. MetaTables DataSource selection and validation leave this context. |
| Logging and instrumentation | Optional structured request logging and trace propagation used by retained platform operations. Importing the SDK should not fetch startup state, configure an exporter, mutate unrelated environment variables, or install a process-wide exception hook. |
| Minimal CLI | Authentication, endpoint/profile configuration, version, and connection diagnostics needed to use the adapter. Domain and development-operation commands do not remain in the core SDK. |

Existing entry points to preserve by capability include `AuthLoaders` and the JWT, session-JWT, and runtime-credential providers; `build_session` and generic `make_request`; `MainSequenceClient`; generic `BaseObjectOrm` request/list/detail methods and `BasePydanticModel` response parsing; the branch/Environment guards in `get_code_repository_context`; CLI `login` and `logout`; and explicit logging/trace setup. Their public compatibility and necessary behavior must be assessed without preserving unrelated side effects or domain logic in the same files.

MetaTables-specific SQLAlchemy, compiled SQL, DataFrame/dtype conversion, local DuckDB/SQLite access, updater execution, and client-side Alembic code belong to the independent `metatables` package. CodeRepository environment building, Docker/Git/SSH deployment work, agent-skill assembly, scaffold installation, and agent/A2A workflow composition beyond direct API adaptation also leave the thin SDK. This ADR removes those SDK implementations and documents retired interfaces; it does not implement their replacements. Backend authorization, scheduling, catalog, and lifecycle policy remain server responsibilities.

Platform-owned DataSource directory responses or read-only branch fields may remain where a non-MetaTables platform API still returns them. They must be represented without importing MetaTables models or running table-specific logic. In particular, retaining a CodeRepositoryBranch model does not retain `get_time_index_table_updates()` as an SDK MetaTables operation, and retaining a DataSource summary does not retain `TimeScaleDB` table methods.

The generic `mainsequence.client.DataSource` adapter exposes directory reads and runtime-credential connection lookup, including typed connection material and requested-resource validation. It reuses SDK authentication and leaves connection use and capability policy to the application. Platform caller assertion verification lives in the optional `mainsequence.server.caller_assertions` module with the `server` extra; it supplies verified identity without framework middleware or application authorization. Neither interface imports domain packages or local database engines.

Organization membership, ownership, and platform access decisions belong only to
the platform backend. The SDK must not require an expected Organization, compare
response ownership to the logged-in User, or perform permission preflight requests.
Platform resource fields may mirror the backend response without becoming SDK
policy. Authentication, response shape/resource identity checks, and backend error
propagation remain SDK responsibilities.

## Removal work on `metatables_removal`

1. Delete both MetaTables trees and remove `mainsequence.client` star exports of their classes. Remove MetaTables endpoint entries from the generic base model and retire `META_TABLES_CONSTANTS` / `TDAG_CONSTANTS` if no non-MetaTables consumer requires them.
2. Break the outside-tree type and runtime dependencies. `mainsequence.client.models_foundry` currently imports the removed `DataSource`, declares a typed `metatables_data_source`, implements `TimeScaleDB`, and parses branch time-index updates. `mainsequence.code_repository_context` stores and validates a MetaTables DataSource. Remove or replace those MetaTables-specific portions while preserving the unrelated platform model and source-identity contracts.
3. Remove MetaTables, time-index table, update, and Alembic commands from `mainsequence.cli`, including dynamic model strings in `cli/api.py` and the import-time `cli/migrations.py` registration. Preserve the authentication, configuration, version, and connection commands retained above. The release note points users to the independent package's documented workflow; this branch does not implement a new MetaTables CLI.
4. Inventory the remaining SDK modules, methods, CLI commands, packaged skills, and dependencies against the retained boundary above. Remove domain and operational implementations outside it, including local environment/Docker/Git/SSH orchestration, bundled skill assembly and scaffolding, and nontrivial agent/A2A workflow composition. Keep the direct platform API adapters they use when those adapters are independently useful. Record whether each retired public interface has an existing replacement or is intentionally removed; do not add replacement code in another project under this ADR.
5. Classify SDK tests that reference removed functionality. Remove tests for intentionally retired SDK surfaces, retain or adapt tests for the thin adapter, and leave MetaTables parity tests with the independent project owner. Delete the SDK-owned `docs/knowledge/meta_tables/` pages, `docs/knowledge/time_index_table_updates.md`, and `docs/_includes/time_index_table_updater_cycle.html` if it has no remaining SDK use. Delete the packaged `agent_scaffold/skills/data_publishing/meta_tables/`, `agent_scaffold/skills/data_publishing/meta_table_migrations/`, and `agent_scaffold/skills/data_publishing/time_index_table_updates/` skills; remove any other SDK-bundled MetaTables-specific document or skill found in the final inventory. Update `README.md`, `docs/SUMMARY.md`, SDK CLI/reference pages, `agent_scaffold/AGENTS.md`, and shared guides or skills that link to the deleted content. Retain historical ADRs and migration records as decision history, annotating SDK-location guidance that this decision supersedes.
6. Recheck package discovery, package data, and runtime dependencies. Remove dependencies needed only by deleted domain/tooling code. Document breaking old-to-new imports, retired commands, and the supported legacy-SDK path before publishing.

The previously recorded consumer inventory found seven outside-tree SDK client/CLI files and 18 SDK test files with direct old-package references at revision `a91d171ea97bf3d974037a578c53de1262007ff5`. It is a starting checklist, not an exhaustive list. Search literal imports, dynamic model references, package metadata, generated files, documentation, and bundled skills again on the actual removal branch.

## Completion criteria

- The diff is confined to `mainsequence-sdk` on `metatables_removal`; it makes no change to the independent package or either service.
- An installed SDK wheel contains neither deleted tree and can import and exercise its retained authentication, transport, resource adapters, source context, optional observability, and minimal CLI without `metatables` installed. No SDK import path, command, or packaged instruction still presents removed domain or operational implementations as available.
- The SDK documentation build has no live navigation or examples for SDK-owned MetaTables or updater APIs. MetaTables-specific knowledge pages and packaged skills are absent from the repository and wheel; shared pages and skills contain no broken links to them. Historical ADRs and migration records remain identifiable as history.
- The retained SDK behavior passes its relevant checks. MetaTables-specific and other retired workflow tests no longer run as SDK tests. Basic SDK imports and platform requests do not load MetaTables, Alembic, SQLAlchemy, pandas, or local database interfaces.
- Package metadata matches the remaining imports, and the release note states the breaking imports, replacement package, and legacy-SDK option. The independent package and its migration guide are identified by version or revision before the SDK release is published.

Completing these criteria finishes the SDK-only removal and establishes the thin-adapter boundary. It makes no claim about implementation work in the independent package or either service.

## Implementation record

The `metatables_removal` checkout removes the two MetaTables code trees, SDK table and local database helpers, MetaTables documentation and bundled skills, domain/operational CLI commands, skill scaffolding, and SDK-owned A2A message orchestration. It retains the auth/transport/resource adapter, source-identity, and optional observability surfaces listed above. The base dependency set keeps the trace API used by structured logging; configuring a tracing provider or OTLP exporter requires the `tracing` extra. Package discovery and generated reference pages now follow the retained modules. The consumer-facing breaking changes are in [the removal migration guide](../migrations/metatables-sdk-removal.md).

The SDK tests, source lint, strict docs build, and installed-wheel import/content checks pass on this branch. The compatible independent `metatables` package version and its owner-maintained migration guide are still release inputs; this branch does not claim that external package is verified or publish an SDK release.
