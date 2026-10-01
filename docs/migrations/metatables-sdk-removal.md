# MetaTables SDK extraction

ADR 0034 moves the MetaTables Python domain out of `mainsequence` and into an
independent package. Its distribution is `mainsequence-metatable`; the import
and the command are `metatables`. The extraction does not reduce unrelated Main
Sequence platform functionality.

## Release boundary

The next final SDK release is `9.0.1`, marking the MetaTables extraction as a
breaking public API change. Development publishes `9.0.1.devN`; merging
`development` into `main` publishes final `9.0.1`.

The extraction already shipped in SDK `8.1.26` and `8.1.27` despite being a
breaking change. Those releases do not retain the former MetaTables imports or
commands. `9.0.1` introduces no additional removal; it corrects the release
classification. SDK `8.1.25` contains the former implementation, but its
compatibility with a current backend must be verified before pinning it.

Version `9.0.0` was previously published and yanked. Its distribution filenames
cannot be reused, so this major-version transition starts at `9.0.1`.

## Install

```bash
uv add mainsequence-metatable
uv run python -m metatables.sdk_compat
uv run metatables copy-metatables-skills --path .
```

Do not install the PyPI project named `metatables`: it is unrelated. The second
command checks that the installed SDK is compatible. The third copies the
package's skills into `.agents/skills/metatables/`.

The SDK no longer installs `alembic`, `sqlalchemy`, `pandas`, `numpy`,
`psycopg2-binary`, or `tqdm`; `mainsequence-metatable` depends on them. The
`local-data` extra is removed. Declare `duckdb` and `pyarrow` directly if your
code imports them.

## Python imports

| Former SDK path | Domain package path |
| --- | --- |
| `mainsequence.meta_tables` | `metatables` |
| `mainsequence.meta_tables.<module>` | `metatables.<module>` |
| `mainsequence.meta_tables.time_index_table_updates.<module>` | `metatables.updaters.<module>` |
| `mainsequence.meta_tables.migrations.<module>` | `metatables.migrations.<module>` |
| `mainsequence.client.metatables` | `metatables.models` or public root exports |

The SDK no longer exports MetaTable, TimeIndexMetaTable,
TimeIndexTableUpdate, DataSource, the dtype codec, local DuckDB/SQLite table
interfaces, or MetaTables constants. It does not resolve a MetaTables DataSource
through `CodeRepositoryContext`, and
`CodeRepositoryBranch.get_time_index_table_updates()` is no longer an SDK
operation.

Use the installed `metatables` package, its skills, and its version-matched
documentation for table modeling, queries, DataSources, update execution, and
Alembic migrations. The SDK scaffold keeps one MetaTables skill,
`maintenance/metatables_transition`, which covers only the move described on
this page.

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
