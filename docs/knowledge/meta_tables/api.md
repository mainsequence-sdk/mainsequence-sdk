# SDK API And Backend Contract

This page maps the SDK objects to the TS Manager backend routes.

MetaTables use the backend route namespace:

```text
/orm/api/ts_manager/meta_table/
```

Use the MetaTable route namespace for registration, validation, introspection,
and compiled execution.

## SDK Modules

Transport models:

```python
from mainsequence.client import MetaTable
from mainsequence.client.metatables import (
    MetaTableCompiledSQLOperation,
    MetaTableContract,
    MetaTableRegistrationRequest,
    MetaTableValidateContractRequest,
)
```

SQLAlchemy helpers:

```python
from mainsequence.meta_tables import (
    AlembicMetaTableMigration,
    AlembicVersionMetaTable,
    PlatformManagedMetaTable,
    external_registered_registration_request_from_sqlalchemy_model,
    register_external_sqlalchemy_model,
)
from mainsequence.meta_tables.compiled_sql.v1 import compile_sqlalchemy_statement
```

SQLAlchemy is a core SDK dependency for MetaTable declarations. The SDK uses it
to inspect table contracts, compile governed SQL operations, and define the
provider-based Alembic migration scope
used by MetaTable schema migrations.

## Registration Primitive

Low-level SDK call:

```python
meta_table = MetaTable.register(request)
```

For platform-managed SQLAlchemy models this is SDK plumbing used by the
migration workflow, not a bootstrap call users should place in application
code. External-registered callers may still build a request and call this
primitive directly when the application owns the physical table lifecycle.

Backend route:

```text
POST /orm/api/ts_manager/meta_table/register/
```

Request fields:

| Field | Meaning |
| --- | --- |
| `data_source_uid` | TS Manager `DynamicTableDataSource` that owns connection, credentials, capabilities, and execution. |
| `management_mode` | `external_registered` or `platform_managed`. |
| `storage_hash` | Collision-resistant platform table identifier. |
| `identifier` | Optional logical MetaTable identifier, such as `Asset`. Non-empty values are globally unique per organization. Alembic migration preparation resolves provider MetaTables by authored SQLAlchemy table name instead. |
| `namespace` | Logical namespace, such as `sdk-examples`. |
| `description` | Optional discovery text. |
| `labels` | Optional table labels. |
| `protect_from_deletion` | Prevent accidental deletion through the platform. |
| `provisioning` | Platform-managed DDL options, such as `create_table` and `if_not_exists`. |
| `introspect` | Ask the backend to refresh the physical metadata snapshot during registration. |
| `table_contract` | Neutral relational contract. It does not include `data_source_uid`. |

For `platform_managed`, `storage_hash` is the logical table identity and
`table_contract.physical.table_name` is the authored SQLAlchemy table name that
Alembic sees. The two values are intentionally separate.

`PlatformManagedMetaTable` exists so SQLAlchemy table construction and
migration-managed registration derive the same configured `storage_hash` from
storage-relevant configuration while preserving the authored physical table
name. Prefix explicit table names with the project or package name.

`__metatable_extra_hash_components__` adds deterministic fields to the
`storage_hash` payload before registration. Use it only to disambiguate storage
identity for tables that could otherwise share the same storage-relevant shape;
changing it points at a different logical table.

## Contract Validation

Validate a new contract before registration:

```python
result = MetaTable.validate_contract(
    table_contract=request.table_contract,
    management_mode=request.management_mode,
    storage_hash=request.storage_hash,
)
```

Backend route:

```text
POST /orm/api/ts_manager/meta_table/validate-contract/
```

Validate a replacement contract for an existing table:

```python
result = meta_table.validate_existing_contract(
    table_contract=request.table_contract,
)
```

Backend route:

```text
POST /orm/api/ts_manager/meta_table/<uid>/validate-contract/
```

Validation normalizes column, index, and foreign-key fragments and returns the
normalized contract. It does not import application code.

## Schema Graph

SDK call for outgoing relationships:

```python
graph = meta_table.get_schema_graph(depth=1)
```

SDK call for outgoing and incoming relationships:

```python
graph = meta_table.get_schema_graph(depth=1, include_incoming=True)
```

Backend route:

```text
GET /orm/api/ts_manager/meta_table/<uid>/schema-graph/?depth=<n>&include_incoming=<bool>
```

Use this graph for dependency analysis. Graph edges include both `source_uid`
and `target_uid`, which is the shape needed to identify dependent MetaTables.

Return shape:

```python
{
    "root_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    "depth": 1,
    "include_incoming": True,
    "nodes": [...],
    "edges": [
        {
            "name": "fk_account_holdings_asset",
            "source_uid": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
            "target_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            "source_columns": ["asset_uid"],
            "target_columns": ["uid"],
            "on_delete": "restrict",
            "relationship_type": "meta_table_to_meta_table",
        }
    ],
}
```

For inbound dependencies:

```python
incoming_edges = [
    edge for edge in graph["edges"] if edge["target_uid"] == meta_table.uid
]
dependent_table_uids = [edge["source_uid"] for edge in incoming_edges]
```

Use `depth=1` when you only need direct dependents. Increase `depth` when you
need transitive relationship context; the backend enforces its supported maximum.

## Introspection

SDK call:

```python
result = meta_table.introspect()
snapshot = result["introspection_snapshot"]
```

Backend route:

```text
POST /orm/api/ts_manager/meta_table/<uid>/introspect/
```

The backend stores the snapshot on `MetaTable.introspection_snapshot`. The
current PostgreSQL path returns schema, table name, columns, indexes, and
constraints when the selected data source supports table introspection.

## Compiled Execution

SDK call:

```python
result = MetaTable.execute_operation(operation)
```

Backend route:

```text
POST /orm/api/ts_manager/meta_table/execute-operation/
```

Execution expects a `compiled-sql.v1` operation:

```python
operation = compile_sqlalchemy_statement(
    stmt,
    operation="select",
    scope_tables=[
        {"metaTableUid": asset_meta_table.uid, "alias": "asset", "access": "read"},
        {"metaTableUid": account_meta_table.uid, "alias": "account", "access": "read"},
    ],
    limits={"max_rows": 1000, "statement_timeout_ms": 15000},
)
result = MetaTable.execute_operation(operation)
```

The SDK accepts `metaTableUid` and `meta_table_uid` when building scope objects.
The serialized backend payload uses `meta_table_uid`.

Backend checks include:

- every scoped MetaTable is visible to the caller
- write operations require edit access
- all scoped tables are on the same data source
- the data source supports the requested compiled operation
- `version` is `compiled-sql.v1`
- `dialect` is `postgresql`
- `statement.paramstyle` is `pyformat`
- the SQL is one statement
- the SQL statement type matches `operation`
- parsed physical table references are covered by `scope.tables`

The response shape is:

```json
{
  "ok": true,
  "operation": "select",
  "dialect": "postgresql",
  "row_count": 25,
  "rows": [
    {
      "uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      "symbol": "BTC"
    }
  ],
  "truncated": false,
  "max_rows": 1000,
  "scope": {
    "meta_table_uids": ["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"],
    "data_source_uid": "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
  }
}
```

Rows are returned only for statements that produce rows. For result sets larger
than `max_rows`, `truncated` is `true`.

## Migration Execution

MetaTable schema migrations are executed by Alembic. The SDK migration CLI is a
provider adapter that:

1. loads the selected `AlembicMetaTableMigration` provider;
2. asks the backend for a temporary migration URI when a command needs database
   access and no explicit SQLAlchemy URL was supplied;
3. for `current`, `upgrade`, and `downgrade`, registers or resolves its
   `AlembicVersionMetaTable`;
4. for `upgrade` and `downgrade`, creates or resolves provider platform-managed
   MetaTable rows before Alembic runs and finalizes them after successful DDL;
5. calls Alembic `current`, `revision`, `upgrade`, or `downgrade` directly.

Backend coordination uses:

```text
POST /orm/api/ts_manager/meta_table/
POST /orm/api/ts_manager/dynamic_table/
POST /orm/api/ts_manager/dynamic_table_data_source/<uid>/migration-connection/
POST /orm/api/ts_manager/meta_table/finalize-managed/
```

The migration-connection request does not include MetaTable UIDs; the provider
and namespace define the migration context. The backend does not receive an
SDK-rendered SQL artifact and does not execute a client-defined migration
operation. After a successful Alembic `upgrade`, the CLI finalizes provider
MetaTable catalog rows through `finalize-managed/`.

## Backend Capabilities

The selected `DynamicTableDataSource` must expose the capability required by
the requested operation:

| Capability | Required for |
| --- | --- |
| `supports_table_registration` | all MetaTable registration |
| `supports_table_introspection` | `introspect=True` and the introspection action |
| `supports_table_ddl` | platform-managed physical table creation |
| `supports_compiled_select` | `select` operations |
| `supports_compiled_insert` | `insert` operations |
| `supports_compiled_update` | `update` operations |
| `supports_compiled_upsert` | `upsert` operations |
| `supports_compiled_delete` | `delete` operations |
| `supports_table_migration` | Alembic migration connections |

If a capability is missing, the backend returns a structured error instead of
falling back to direct database access.

This is the main platform-managed tradeoff: users get governed table creation
and execution without direct database credentials, but only for operations that
the selected data source explicitly supports.

## Search And Discovery

After registration, TS Manager stores:

- the `MetaTable` row
- normalized `MetaTableColumn` rows
- normalized `MetaTableIndex` rows
- normalized foreign-key projection rows
- an optional `introspection_snapshot`
- a `TableSearchIndex` document

Command Center and table discovery read those MetaTable records directly.
