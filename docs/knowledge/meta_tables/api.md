# SDK API And Backend Contract

This page maps the SDK objects to the TS Manager backend routes.

MetaTables use the backend route namespace:

```text
/api/v1/meta-tables/
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
    schema_table_name,
    sqlalchemy_naming_convention,
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
primitive directly when both catalog registration and physical DDL remain
external authority.

Backend route:

```text
POST /api/v1/meta-tables/register/
```

Request fields:

| Field | Meaning |
| --- | --- |
| `data_source_uid` | Canonical `DataSource.uid` that owns connection, capabilities, and execution. |
| `management_mode` | `external_registered` or `platform_managed`. |
| `schema_management` | Physical schema owner. Alembic providers use `mode="alembic_managed"` with provider metadata; backend-created tables use `backend_managed`; external registration resolves to `external_registered`. |
| `project_context` | Internal wire-only consistency context. It is not accepted as caller input by the SDK. The SDK derives the exact persisted `project_branch_uid` from the process-frozen Git repository and attached branch in both local and deployed project-code runs; the backend additionally verifies deployed equality against the authenticated runtime target. |
| `identifier` | Optional logical MetaTable identifier, such as `Asset`. A non-empty value is unique within its Organization Environment, or within the Organization for an Organization-scoped external row. Alembic migration preparation resolves provider MetaTables by authored SQLAlchemy table name instead. |
| `namespace` | Logical namespace, such as `sdk-examples`. |
| `description` | Optional discovery text. |
| `labels` | Optional table labels. |
| `protect_from_deletion` | Prevent accidental deletion through the platform. |
| `provisioning` | Platform-managed DDL options. Backend ownership requires `create_table=true`; Alembic ownership requires `create_table=false`; external registration forbids this field. |
| `introspect` | Ask the backend to refresh the physical metadata snapshot during registration. |
| `table_contract` | Neutral relational contract. It does not include `data_source_uid`. |

For `platform_managed`, `table_contract.physical.table_name` is the authored
SQLAlchemy table name that Alembic sees. The platform identity is the MetaTable
`uid`; the stable logical application identity is `identifier` when provided.

Do not treat management, schema ownership, and provisioning as one state:

| Catalog ownership | Schema ownership | Provisioning |
| --- | --- | --- |
| `platform_managed` | `backend_managed` | normally `active` after backend creation |
| `platform_managed` | `alembic_managed` | `reserved` before Alembic, `active` after finalize |
| `external_registered` | `external_registered` | `active` for an imported physical table |

The Alembic registry is the platform-managed, Alembic-managed root of its
provider. It is never an `external_registered` table.

`MetaTableRegistrationRequest` validates this matrix during construction, and
`MetaTable.register(...)` revalidates model and mapping inputs before HTTP.
`TimeIndexMetaTable.register(...)` has fixed platform catalog ownership and
applies the same backend-versus-Alembic DDL rule. Invalid combinations never
reach the backend.

`PlatformManagedMetaTable` exists so SQLAlchemy table construction and
migration-managed registration produce the same table contract while preserving
the authored physical table name. Prefix explicit table names with the project
or package name.
Use `schema_table_name(project_or_app, concept)` and
`sqlalchemy_naming_convention()` to keep authored table, index, constraint, and
Alembic version names collision-resistant and within PostgreSQL identifier
limits.

If a caller needs a deterministic contract fingerprint, use
`compute_metatable_contract_hash(model_or_table)`. It is an explicit utility,
not a MetaTable API field, and it includes the physical table name by default.

## Contract Validation

Validate a new contract before registration:

```python
result = MetaTable.validate_contract(
    table_contract=request.table_contract,
    management_mode=request.management_mode,
)
```

Backend route:

```text
POST /api/v1/meta-tables/validate-contract/
```

Validate a replacement contract for an existing table:

```python
result = meta_table.validate_existing_contract(
    table_contract=request.table_contract,
)
```

Backend route:

```text
POST /api/v1/meta-tables/<uid>/validate-contract/
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
GET /api/v1/meta-tables/<uid>/schema-graph/?depth=<n>&include_incoming=<bool>
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
POST /api/v1/meta-tables/<uid>/introspect/
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
POST /api/v1/meta-tables/execute-operation/
```

Execution expects a `compiled-sql.v1` operation:

```python
operation = compile_sqlalchemy_statement(
    stmt,
    operation="select",
    data_source_uid=data_source.uid,
    scope_tables=[
        {"metaTableUid": asset_meta_table.uid, "alias": "asset", "access": "read"},
        {"metaTableUid": account_meta_table.uid, "alias": "account", "access": "read"},
    ],
    limits={"max_rows": 1000, "statement_timeout_ms": 15000},
)
result = MetaTable.execute_operation(operation)
```

The SDK accepts `metaTableUid` and `meta_table_uid` when building scope objects.
The serialized backend payload uses `meta_table_uid`. If
`scope.data_source_uid` is omitted, the SDK leaves it out and the backend derives
the execution connection from the scoped MetaTables. An explicit UID remains
available when a caller intentionally selects an accessible execution context.

Backend checks include:

- every scoped MetaTable is visible to the caller
- write operations require edit access
- the selected data source supports the requested compiled operation
- `version` is `compiled-sql.v1`
- `dialect` is `postgresql`
- `statement.paramstyle` is `pyformat`
- the SQL is one statement
- the SQL statement type matches `operation`

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
  "pagination": {
    "limit": 1000,
    "offset": 0,
    "returned_count": 1,
    "has_more": false,
    "next_offset": null
  },
  "scope": {
    "meta_table_uids": ["aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"],
    "data_source_uid": "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
  }
}
```

Rows are returned only for statements that produce rows. For result sets larger
than one backend page, `MetaTable.execute_operation(...)` follows
`pagination.next_offset` automatically for select operations until it has
returned the requested `limits.max_rows` rows or `pagination.has_more` is
`false`. Include a deterministic `ORDER BY` in paginated select SQL.

## Migration Execution

MetaTable schema migrations are executed by Alembic. The SDK migration CLI is a
provider adapter that:

1. loads the selected `AlembicMetaTableMigration` provider;
2. asks the backend for a temporary migration URI when a command needs database
   access and no explicit SQLAlchemy URL was supplied;
3. for `current`, `upgrade`, and `downgrade`, reserves or resolves its
   platform-managed, Alembic-managed `AlembicVersionMetaTable` root;
4. for `upgrade` and `downgrade`, reserves provider platform-managed MetaTable
   children before Alembic runs and finalizes the root and children after
   successful DDL;
5. calls Alembic `current`, `revision`, `upgrade`, or `downgrade` directly.

Backend coordination uses:

```text
POST /api/v1/meta-tables/
POST /api/v1/time-index-meta-tables/
POST /api/v1/meta-tables/<meta_table_uid>/migration-connection/
POST /api/v1/meta-tables/finalize-managed/
```

The migration-connection request does not include MetaTable UIDs; the provider
and namespace define the migration context. The backend does not receive an
SDK-rendered SQL artifact and does not execute a client-defined migration
operation. After a successful Alembic `upgrade`, the CLI finalizes provider
MetaTable catalog rows through `finalize-managed/`.

## Backend Capabilities

The selected `DataSource` must expose the capability required by
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

Catalog and table discovery read those MetaTable records directly.
