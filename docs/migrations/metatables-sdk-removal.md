# MetaTables SDK extraction

ADR 0034 moves the MetaTables Python domain out of `mainsequence` and into the
independent `metatables` distribution. The extraction does not reduce unrelated
Main Sequence platform functionality.

## Python imports

| Former SDK path | Domain package path |
| --- | --- |
| `mainsequence.meta_tables` | `metatables` |
| `mainsequence.meta_tables.<module>` | `metatables.<module>` |
| `mainsequence.meta_tables.time_index_table_updates.<module>` | `metatables.time_index_table_updates.<module>` |
| `mainsequence.meta_tables.migrations.<module>` | `metatables.migrations.<module>` |
| `mainsequence.client.metatables` | `metatables.client.metatables` or public root exports |

The SDK no longer exports MetaTable, TimeIndexMetaTable,
TimeIndexTableUpdate, DataSource, TimeScaleDB, the dtype codec, local
DuckDB/SQLite table interfaces, or MetaTables constants. It does not resolve a
MetaTables DataSource through `CodeRepositoryContext`, and
`CodeRepositoryBranch.get_time_index_table_updates()` is no longer an SDK
operation.

Use the installed `metatables` package and its version-matched documentation
for table modeling, queries, DataSources, update execution, and Alembic
migrations. The SDK retains the migration guidance skill only to route users to
the owning package and explain the import transition.

## CLI boundary

The retired CLI surfaces are limited to the table domain:

- `meta-table` and `meta_table`
- `time-index-table`
- `code-repository time-index-table-updates`
- `migrations`, whose implementation was tied to the extracted MetaTables
  Alembic package

Authentication, users, Agent and AgentSession operations, A2A execution,
CodeRepository lifecycle and local development, jobs and runs, images,
resources and releases, constants, secrets, teams, sharing, SDK utilities,
skills, diagnostics, structured output, and Docker/devcontainer workflows
remain supported by the `mainsequence` CLI.

## Unchanged SDK responsibilities

The extraction does not remove or replace:

- `Constant.create_constants_if_not_exist()`
- `ResourceRelease.wait_for_runtime_access()`
- AgentSession A2A message and task helpers
- runtime-access and A2A-handle caches used by the CLI
- CodeRepository scaffold installation and update commands
- logging, tracing, diagnostics, or process helpers

Consumers still using the legacy Django TS service can pin the last released
SDK version that contains the former imports while they migrate. Confirm the
compatible `metatables` version with the domain package release notes before
upgrading production consumers.
