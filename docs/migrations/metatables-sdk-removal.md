# MetaTables SDK removal

ADR 0034 removes MetaTables from the `mainsequence` SDK on `metatables_removal`. The independent `metatables` package was ported separately; this SDK branch does not implement or release that package. Confirm the compatible package version and its migration guide with the owning team before publishing an SDK release containing this change.

## Python imports

| Former SDK path | Domain package path |
| --- | --- |
| `mainsequence.meta_tables` | `metatables` |
| `mainsequence.meta_tables.<module>` | `metatables.<module>` |
| `mainsequence.meta_tables.time_index_table_updates.<module>` | `metatables.time_index_table_updates.<module>` |
| `mainsequence.meta_tables.migrations.<module>` | `metatables.migrations.<module>` |
| `mainsequence.client.metatables` | `metatables.models` or its public root exports |

The SDK no longer exports MetaTable, TimeIndexMetaTable, TimeIndexTableUpdate, DataSource, TimeScaleDB, the dtype codec, local DuckDB/SQLite interfaces, or MetaTables constants. It no longer resolves a MetaTables DataSource in `CodeRepositoryContext` or exposes `CodeRepositoryBranch.get_time_index_table_updates()`. Branch responses no longer project a MetaTables DataSource.

`mainsequence.client.DataSource` and its runtime-connection lookup are removed. Use
`metatables.DataSource` with the MetaTables API for source registration and ordinary
`mainsequence.client.Secret` for platform Secrets. Secret operations retain their
existing Environment requirements. The optional `mainsequence[server]` extra provides [caller assertion verification](../knowledge/server/caller_assertions.md) without a web-framework dependency.

Other retired Python interfaces are `mainsequence.code_repository_skills`, `Constant.create_constants_if_not_exist()`, and `ResourceRelease.wait_for_runtime_access()`. `mainsequence.scaffold_skills.copy_scaffold_skills()` remains as a generic, library-owned copier with overlap and protected-checkout guards and a required version sentinel. `AgentSession.send_a2a_message()`, `get_a2a_task()`, `cancel_a2a_task()`, `wait_for_a2a_task()`, its runtime-access cache helpers, and the SDK-owned `AgentA2AProfile`, `A2ATask`, `A2AMessageSendResult`, and related A2A response types are removed. Direct Agent and AgentSession platform API adapters remain. Logging and trace setup are now explicit: importing `mainsequence` no longer fetches startup state, configures tracing, or installs a process-wide exception hook. Applications may call `refresh_application_logger_bindings()` when needed. Install `mainsequence[tracing]` before calling `mainsequence.instrumentation.setup_tracing()` or exporting OTLP spans; the base SDK retains trace-context logging without installing the tracing SDK or exporter.

## CLI and packaged content

The SDK CLI retains `login`, `logout`, `settings`, `version`, and `doctor`. Its former `meta-table`, `time-index-table`, `migrations`, CodeRepository deployment/setup, agent workflow, and skill-installation commands are retired. The SDK continues to ship the `agent_scaffold` bundle and reusable copier as Python package content, but the thin CLI does not install or assemble skills. The SDK scaffold retains the application migration workflow skill because it coordinates SDK source context with the independent package and platform runtime. Its commands and implementation come from `metatables`; table modeling, queries, and update skills remain owned by that package.

The retired top-level CLI groups are `meta-table` (and `meta_table`), `time-index-table`, `code-repository`, `agent`, `skills`, `constants`, `secrets`, `organization`, `sdk`, and `migrations`; the `user` and `copy-llm-instructions` commands also leave. The corresponding resource helper wrappers in `mainsequence.cli.api` are gone; use the retained Python platform resource adapters for direct API calls. Authentication compatibility is retained: login still accepts positional backend/base-folder arguments, base-folder options, and `--export-env`, and exports `MAINSEQUENCE_AUTH_MODE`. The removed settings/workflow commands and global `--json` option are not restored. `mainsequence version` replaces the former SDK-version CLI path.

## Existing consumers

Consumers still using the legacy Django TS service can pin the last released SDK version that includes these imports while they migrate. Do not upgrade them to an SDK release containing this removal until the independent package's compatible version and guide are confirmed. The exact final compatible SDK version and independent `metatables` version must be inserted into release notes by the release owner before publication.

The existing `mainsequence.bootstrap.prime_runtime_env()` credential/endpoint
bootstrap remains active at SDK import. It reads local configuration without
performing a platform identity request.
