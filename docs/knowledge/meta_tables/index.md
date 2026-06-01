# MetaTables

`MetaTable` is the SDK and TS Manager boundary for general relational tables.

Use it when your application already has table models, usually SQLAlchemy/Core
or SQLAlchemy ORM models, and you want Main Sequence to know about those tables
for metadata, permissions, discovery, search, and governed query execution.

`MetaTable` is not a replacement ORM. Your application keeps its normal model layer. The SDK turns
resolved table metadata and compiled query artifacts into TS Manager contracts.

## Mental Model

A `MetaTable` record binds these things together:

- a stable platform `uid`
- a collision-resistant `storage_hash`
- a registered TS Manager `DynamicTableDataSource`
- a logical `identifier` and `namespace`
- a physical table name
- a neutral table contract with columns, indexes, and foreign keys
- labels, ownership, and shareable access
- the latest backend introspection snapshot when available

The backend route namespace is:

```text
/orm/api/ts_manager/meta_table/
```

The SDK transport objects live in:

```python
from mainsequence.client import MetaTable
from mainsequence.client.models_metatables import MetaTableRegistrationRequest
```

Higher-level SDK helpers live in:

```python
from mainsequence.meta_tables import (
    MigrationManagedMetaTable,
    MigrationManagedTimeIndexMetaData,
    PlatformManagedMetaTable,
    PlatformTimeIndexMetaData,
    metatable_tablename,
    external_registered_registration_request_from_sqlalchemy_model,
    register_external_sqlalchemy_model,
)
from mainsequence.meta_tables.compiled_sql.v1 import compile_sqlalchemy_statement
```

Schema migrations use a client-defined registry MetaTable:

```python
from mainsequence.meta_tables.migrations import MigrationMetaTable
```

## Management Modes

MetaTables support two registration modes.

### `external_registered`

Use this when your app owns the physical table lifecycle.

Typical flow:

1. Define SQLAlchemy models.
2. Create and migrate tables with SQLAlchemy/Alembic or another migration tool.
3. Register the table metadata with TS Manager.
4. TS Manager introspects the table if the data source supports introspection.
5. TS Manager enforces permissions and can execute governed read/write operations through its data source.

The physical table name can be a normal application name such as `asset` or
`account`. The `storage_hash` is still the platform identity for the registered
table, but it does not have to equal the physical table name in this mode.

### `platform_managed`

Use this when TS Manager should create the physical table through the selected
`DynamicTableDataSource`.

Typical flow:

1. Define SQLAlchemy models with `PlatformManagedMetaTable` or `__tablename__ = metatable_tablename(...)`.
2. Build or register through the model class.
3. TS Manager receives a neutral registration contract extracted from SQLAlchemy metadata.
4. TS Manager validates the contract, applies supported DDL, stores projections, and returns a MetaTable `uid`.

For `platform_managed`, `storage_hash` is the logical table identity and
`table_contract.physical.table_name` is omitted from client requests. The
backend allocates the physical table name and returns it on the `MetaTable`.

That is why the SDK exposes `PlatformManagedMetaTable` and `metatable_tablename(...)`.
The platform-managed class computes the logical storage identity from
storage-relevant configuration, including the SQLAlchemy table shape. After
registration the SDK privately rebinds the SQLAlchemy table name to the backend
physical table name so compiled SQL targets the real table.

## Why Choose Platform-Managed

Platform-managed mode is useful when the application should not connect to the
database directly or should not own DDL credentials.

The practical benefits are:

- TS Manager uses the configured `DynamicTableDataSource` connection, so client
  code does not need direct database credentials.
- Logical storage identities come from `storage_hash`, while backend physical
  table names are allocated by TS Manager.
- Creation, permission checks, introspection, search-document refresh, and
  Command Center discovery happen through one platform path.
- Hosted or restricted environments can create relational tables without giving
  every application direct database access.
- The server applies a neutral contract. It does not import the user's ORM code
  or become a second ORM.

Choose `external_registered` when you already have a migration system, need
complex DDL that TS Manager does not support, or intentionally want the
application to own the physical table lifecycle.

## What Belongs In The Contract

The table contract is neutral JSON. It is not a SQLAlchemy object.

It contains:

- `version`: currently `relational-table.v1`
- `physical.table_name` for external-registered tables. Platform-managed client
  requests leave this empty because the backend owns the physical name.
- `columns`
- `indexes`
- `foreign_keys`
- optional authoring metadata

The selected data source does not belong inside `table_contract`. In normal
project execution it is resolved from the active Main Sequence session, the same
way DataNode resolves its data source.

```python
request = Asset.build_registration_request()

assert request.table_contract.physical.table_name is None
```

## Storage Hashes

`storage_hash` is the platform table identity. It prevents collisions better
than letting every app register a human table name such as `asset`.

For platform-managed tables, prefer the class API when the name should rotate
with the SQLAlchemy table shape:

```python
class Asset(PlatformManagedMetaTable, Base):
    __metatable_namespace__ = "sdk-examples"
    __metatable_identifier__ = "Asset"
    __metatable_extra_hash_components__ = {"storage_name": "asset"}
```

For time-indexed DataNode storage, use `PlatformTimeIndexMetaData` instead of
the generic `PlatformManagedMetaTable`. It uses the same storage-hash machinery,
but also includes `time_index_name` and `index_names` in the stable identity and
registers through the TimeIndexMetaData endpoint.

For in-place schema migrations, use the migration-managed bases from the first
version of the table. `MigrationManagedMetaTable` and
`MigrationManagedTimeIndexMetaData` use stable identifier-addressed storage
identity, while the contract hash still rotates as columns, indexes, foreign
keys, and types change. `MigrationManagedTimeIndexMetaData` keeps the
TimeIndexMetaData endpoint and time-index validation; it only changes the
storage identity rule.

When two backend-managed tables could otherwise have the same storage-relevant
shape, add `__metatable_extra_hash_components__` with stable deterministic
values:

```python
__metatable_extra_hash_components__ = {"storage_name": "account_holdings"}
```

This attribute is part of storage identity. Changing it creates a different
table. It is not for labels, descriptions, runtime options, test isolation,
backend UIDs, data-source UIDs, or updater scope.

For explicit low-level naming, use the helper as the SQLAlchemy table name:

```python
__tablename__ = metatable_tablename(
    namespace="sdk-examples",
    identifier="Asset",
)
```

The helper builds a PostgreSQL-safe name, keeps a readable prefix, and uses the
SDK's DataNode hash machinery for the digest when available. The generated name
stays within PostgreSQL's 63-character identifier limit.

## Backend Responsibilities

After registration, TS Manager stores a `MetaTable` row plus projection rows for
columns, indexes, and foreign keys. Those projections power serializers, search
documents, discovery, and query validation.

## Finding Foreign-Key Dependents

Use the schema graph when you need to know which MetaTables depend on another
MetaTable. The graph includes source and target table UIDs on every FK edge,
which is the information needed for dependency analysis.

```python
from mainsequence.client import MetaTable

asset_table = MetaTable.get("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
graph = asset_table.get_schema_graph(depth=1, include_incoming=True)

incoming_edges = [
    edge for edge in graph["edges"]
    if edge["target_uid"] == asset_table.uid
]

dependent_table_uids = [edge["source_uid"] for edge in incoming_edges]
```

Each inbound edge describes one FK dependency:

```python
for edge in incoming_edges:
    print(edge["source_uid"], edge["source_columns"], "->", edge["target_columns"])
```

Do not use `incoming_fks` as the main dependency API. It is a serialized FK
projection on the table response. `get_schema_graph(include_incoming=True)` is
the graph API because its edges include both `source_uid` and `target_uid`.

For compiled execution, TS Manager:

- resolves every `meta_table_uid` in the declared scope
- checks object permissions
- checks data-source capabilities
- validates operation kind and dialect
- binds parameters through the backend driver
- enforces limits and timeouts
- uses SQL parsing as a defense-in-depth check

The server does not import your SQLAlchemy models and does not rebuild ORM
filters. The SDK compiles statements locally and sends a governed execution
artifact.

## More Guides

- [SDK API And Backend Contract](api.md)
- [Registering SQLAlchemy Tables](sqlalchemy.md)
- [Compiled SQL Execution](compiled_sql.md)
- [MetaTable Examples](../../../examples/meta_tables/README.md)
