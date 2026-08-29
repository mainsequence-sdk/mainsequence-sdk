# Migrating from 6.x to 7.0

Main Sequence SDK 7.0 replaces the DataNode vocabulary and wire contract with
explicit time-index-table update concepts. This is a hard cut: version 7 has no
deprecated aliases, forwarding modules, legacy routes, old CLI commands, or
runtime configuration converter.

## Release boundary

Upgrade the SDK only as part of the coordinated backend cutover. The SDK and
backend contracts must be from the same generation.

| SDK | Backend contract | Result |
| --- | --- | --- |
| `6.0.53` | Pre-cutover DataNode contract | Required rollback/reference pair |
| `7.0.0` | Canonical time-index-table update contract | Required cutover pair |
| `6.x` | Canonical contract | Unsupported |
| `7.x` | Pre-cutover contract | Unsupported |

The final source before the cut is the
[`v6.0.53` tag](https://github.com/mainsequence-sdk/mainsequence-sdk/tree/v6.0.53).
If a project must remain on the old contract temporarily, pin it explicitly:

```bash
pip install "mainsequence==6.0.53"
```

Do not use the pin as a compatibility layer. Move the backend and every
participating SDK process to the version 7 contract in the same release window.

## Public name changes

| 6.x name | 7.x name | Meaning |
| --- | --- | --- |
| `DataNode` | `TimeIndexTableUpdater` | Executable Python producer behavior |
| `DataNodeConfiguration` | `TimeIndexTableUpdateConfig` | Hashed updater configuration |
| `APIDataNode` | `TimeIndexTableRef` | Read-only reference to a canonical existing table |
| `DataAccessMixin` | `TimeIndexTableAccessMixin` | Shared table-read helpers |
| `APIPersistManager` | `TimeIndexTableReader` | Read access for an existing table |
| `PersistManager` | `TimeIndexTableUpdateManager` | Updater synchronization and persistence |
| `DataNodeUpdate` | `TimeIndexTableUpdate` | Persisted backend update process |
| `DataNodeUpdateDetails` | `TimeIndexTableUpdateDetails` | Operational state for an update process |
| `LocalTimeSeriesHistoricalUpdate` | `TableUpdateRun` | One execution attempt |
| `RunConfiguration` | `TimeIndexTableUpdateConfiguration` | Schedule and run configuration |

`TimeIndexMetaTable` remains the table's storage identity and schema contract.
`UpdateRunner` also keeps its name.

The old `mainsequence.meta_tables.data_nodes` package no longer exists. Import
authoring types from `mainsequence.meta_tables`:

```python
from mainsequence.meta_tables import (
    TimeIndexTableRef,
    TimeIndexTableUpdateConfig,
    TimeIndexTableUpdater,
)
```

Typed API resources remain available from `mainsequence.client` or
`mainsequence.client.metatables` under their version 7 names.

## Migrate updater classes

Before:

```python
from mainsequence.meta_tables import DataNode, DataNodeConfiguration


class PricesConfig(DataNodeConfiguration):
    market: str


class PricesNode(DataNode):
    def __init__(self, config: PricesConfig):
        super().__init__(config=config, storage_table=PricesTable)
```

After:

```python
from mainsequence.meta_tables import (
    TimeIndexTableUpdateConfig,
    TimeIndexTableUpdater,
)


class PricesConfig(TimeIndexTableUpdateConfig):
    market: str


class PricesUpdater(TimeIndexTableUpdater):
    def __init__(self, config: PricesConfig):
        super().__init__(config=config, output_table=PricesTable)
```

Update framework-facing attributes at the same time:

| 6.x | 7.x |
| --- | --- |
| `storage_table` | `output_table` |
| `storage_metadata` | `output_metadata` |
| `data_node_update` | `table_update` |
| `local_persist_manager` on an updater | `update_manager` |
| `DATA_NODE_UPDATE_CLASS` | `TABLE_UPDATE_CLASS` |

Concrete project class names do not have to change. Renaming a class such as
`PricesNode` changes its import path and therefore its canonical update hash;
coordinate that additional identity change with the backend migration.

## Migrate `APIDataNode` separately

`APIDataNode` was not an updater despite its name. In version 7 it becomes the
read-only `TimeIndexTableRef` and can identify only a canonical
`TimeIndexMetaTable`. It has table-read and statistics operations, but no
`run`, `update`, `update_hash`, scheduler, or update-tree behavior.

Constructor migration:

| 6.x | 7.x |
| --- | --- |
| `APIDataNode.build_from_table_uid(uid)` | `TimeIndexTableRef.from_uid(uid)` |
| `APIDataNode.build_from_meta_table(table)` | `TimeIndexTableRef.from_meta_table(table)` |
| `APIDataNode.build_from_identifier(identifier)` | `TimeIndexTableRef.from_identifier(identifier)` |
| `APIDataNode.build_from_physical_identity(...)` | `TimeIndexTableRef.from_physical_identity(...)` |
| `APIDataNode.build_from_local_time_serie(update)` | `TimeIndexTableRef.from_table_update(update)` |

For example:

```python
prices = TimeIndexTableRef.from_uid(prices_table_uid)


class ReturnsUpdater(TimeIndexTableUpdater):
    def dependencies(self):
        return {"prices": prices}
```

The raw `APIDataNode(data_source_uid, physical_table_name, ...)` constructor and
ambiguous `build_from_table_name(...)` lookup are removed. Resolve the
`TimeIndexMetaTable` by UID, identifier, or full physical identity instead.
Code that used `api_node.local_persist_manager` should use `table_ref.reader`.

## Migrate typed client calls

Replace the resource classes and canonical fields together. Version 7 rejects
extra legacy fields during model parsing and before network requests.

Important field and method changes include:

| 6.x | 7.x |
| --- | --- |
| `data_node_storage` | `output_table` |
| `data_node_storage_uid` | `output_table_uid` |
| `time_serie_source_code` | `table_updater_source_code` |
| `time_serie_source_code_git_hash` | `table_updater_source_code_git_hash` |
| `related_table_uid` on update details/runs | `table_update_uid` |
| `create_historical_update(...)` | `create_table_update_run(...)` |
| `historical_update_uid` | `table_update_run_uid` |
| `CodeRepositoryBranch.get_data_nodes_updates()` | `CodeRepositoryBranch.get_time_index_table_updates()` |

The canonical collections are:

| Resource | Version 7 collection |
| --- | --- |
| `TimeIndexTableUpdate` | `time-index-table-updates` |
| `TimeIndexTableUpdateDetails` | `time-index-table-update-details` |
| `TableUpdateRun` | `table-update-runs` |

Old `local-time-series` routes and old dependency actions are not forwarded.
Custom integrations must use the canonical dependency actions:

- `POST .../update-dependencies/`
- `POST .../table-dependencies/`
- `DELETE .../dependencies/`

## Migrate CLI usage

The version 6 aliases are removed:

| 6.x | 7.x |
| --- | --- |
| `mainsequence data-node ...` | `mainsequence time-index-table ...` |
| `mainsequence data_node ...` | `mainsequence time-index-table ...` |
| `mainsequence data-node-storage ...` | `mainsequence time-index-table ...` |
| `mainsequence data_node_storage ...` | `mainsequence time-index-table ...` |
| `mainsequence code-repository data-node-updates list` | `mainsequence code-repository time-index-table-updates list` |

## Expect configuration and hash rotation

Version 7 accepts only updater configuration schema version 2:

```json
{
  "configuration_schema_version": 2,
  "table_updater_class_import_path": {
    "module": "pipelines.prices",
    "qualname": "PricesUpdater"
  }
}
```

The legacy `time_series_class_import_path` key, old dependency marker booleans,
old dependency `kind` values, and configuration without version `2` are
invalid. Canonical dependency values serialize as either:

```json
{
  "kind": "table_update",
  "update_hash": "...",
  "output_table_uid": "..."
}
```

or:

```json
{
  "kind": "time_index_table_ref",
  "time_index_meta_table_uid": "...",
  "data_source_uid": "..."
}
```

The schema and marker changes intentionally rotate updater hashes. The backend
must transform stored build configurations, hashes, dependency edges, and
related identities offline before the canonical application starts. The SDK
does not read or convert legacy payloads. Backend implementers should use the
shared exact cases in
[`tests/fixtures/time_index_table_update_hash_v2_golden.json`](https://github.com/mainsequence-sdk/mainsequence-sdk/blob/v7.0.0/tests/fixtures/time_index_table_update_hash_v2_golden.json)
as the conversion contract.

## Project migration checklist

1. Confirm the backend cutover and offline data migration are ready.
2. Pin or lock `mainsequence==7.0.0` for every participating process.
3. Replace the imports, base classes, properties, typed resources, and CLI
   commands listed above.
4. Rewrite every `APIDataNode` use as a canonical read-only
   `TimeIndexTableRef`; do not make it an updater subclass.
5. Remove hand-authored or cached legacy configuration payloads.
6. Review concrete updater-class renames because import paths affect hashes.
7. Run the project's tests and exercise updater construction, dependency
   traversal, reads, and one complete update lifecycle against the cutover
   backend.
8. Deploy all SDK processes in the coordinated release window.

Use these searches as a final review, adjusting excluded historical or
generated directories for the project:

```bash
rg -n 'mainsequence\.meta_tables\.data_nodes|\b(DataNode|DataNodeConfiguration|APIDataNode)\b' .
rg -n '\b(storage_table|storage_metadata|data_node_update|data_node_storage)\b' .
rg -n 'time_series_class_import_path|local-time-series|data-node-updates' .
```

Any live application match must be migrated. Historical release notes may keep
the old names when they describe an earlier version.
