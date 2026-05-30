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
from mainsequence.client.models_metatables import (
    MetaTableCompiledSQLOperation,
    MetaTableContract,
    MetaTableRegistrationRequest,
    MetaTableValidateContractRequest,
)
```

SQLAlchemy helpers:

```python
from mainsequence.meta_tables import (
    PlatformManagedMetaTable,
    external_registered_registration_request_from_sqlalchemy_model,
    metatable_configured_tablename,
    metatable_tablename,
    register_external_sqlalchemy_model,
)
from mainsequence.meta_tables.compiled_sql.v1 import compile_sqlalchemy_statement
```

The SQLAlchemy helpers are lazy. Importing `mainsequence.meta_tables` does
not require SQLAlchemy to be installed. SQLAlchemy-specific behavior is used
only when a helper or platform-managed class needs to compile, inspect, or
construct SQLAlchemy objects.

## Registration

SDK call:

```python
meta_table = MetaTable.register(request)
```

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
| `identifier` | Human logical table name, such as `Asset`. |
| `namespace` | Logical namespace, such as `sdk-examples`. |
| `description` | Optional discovery text. |
| `labels` | Optional table labels. |
| `protect_from_deletion` | Prevent accidental deletion through the platform. |
| `open_for_everyone` | Organization-wide visibility when the caller is allowed to set it. |
| `provisioning` | Platform-managed DDL options, such as `create_table` and `if_not_exists`. |
| `introspect` | Ask the backend to refresh the physical metadata snapshot during registration. |
| `table_contract` | Neutral relational contract. It does not include `data_source_uid`. |

For `platform_managed`, the backend requires:

```text
storage_hash == table_contract.physical.table_name
```

`PlatformManagedMetaTable` exists so SQLAlchemy table construction and
registration derive the same configured `storage_hash` from storage-relevant configuration.
The logical `identifier` is sent to the backend but does not rotate the
configured physical table name. The lower-level `metatable_tablename(...)`
helper remains available when callers need to set `__tablename__` explicitly.

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
- normalized `MetaTableForeignKey` rows
- an optional `introspection_snapshot`
- a `TableSearchIndex` document

Command Center and table discovery read those MetaTable records directly.
