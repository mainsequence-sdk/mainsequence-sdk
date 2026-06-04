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
- a neutral table contract with columns and table identity
- SQLAlchemy/Alembic metadata for physical DDL such as indexes and foreign keys
- labels, ownership, and shareable access
- the latest backend introspection snapshot when available

The backend route namespace is:

```text
/orm/api/ts_manager/meta_table/
```

The SDK transport objects live in:

```python
from mainsequence.client import MetaTable
from mainsequence.client.metatables import MetaTableRegistrationRequest
```

Higher-level SDK helpers live in:

```python
from mainsequence.meta_tables import (
    AlembicVersionMetaTable,
    PlatformManagedMetaTable,
    PlatformTimeIndexMetaData,
    external_registered_registration_request_from_sqlalchemy_model,
    register_external_sqlalchemy_model,
)
from mainsequence.meta_tables.compiled_sql.v1 import compile_sqlalchemy_statement
```

Schema migrations use Alembic. The SDK exposes a catalog binding for Alembic's
version table:

```python
from mainsequence.meta_tables.migrations import AlembicVersionMetaTable
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

Use this when Alembic should create and evolve the physical table while TS
Manager keeps the MetaTable catalog binding.

Typical flow:

1. Define SQLAlchemy models with `PlatformManagedMetaTable` and project-prefixed `__tablename__` values.
2. Add those models to `AlembicMetaTableMigration.metatable_models`.
3. Run `mainsequence migrations upgrade --provider ... head`.
4. Migration tooling reserves catalog rows for missing models, Alembic applies schema
   evolution SQL.

For `platform_managed`, `storage_hash` is the logical table identity and
`storage_hash` is the logical platform identity. The authored SQLAlchemy table
name is sent separately as `table_contract.physical.table_name` and should be
prefixed with the project or package name.

That is why the SDK exposes `PlatformManagedMetaTable`.
The platform-managed class computes the logical storage identity from
storage-relevant configuration, including the SQLAlchemy table shape, without
using that identity as the SQLAlchemy table name.

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

assert request.storage_hash != Asset.__table__.name
assert request.table_contract.physical.table_name == Asset.__table__.name
```

## Storage Hashes

`storage_hash` is the platform table identity. It prevents collisions better
than letting every app register a human table name such as `asset`.

For platform-managed tables, prefer the class API when the name should rotate
with the SQLAlchemy table shape:

```python
class Asset(PlatformManagedMetaTable, Base):
    __tablename__ = "sdk_examples__asset"
    __metatable_namespace__ = "sdk-examples"
    __metatable_identifier__ = "sdk_examples.Asset"
    __metatable_extra_hash_components__ = {"storage_name": "asset"}
```

For time-indexed DataNode storage, use `PlatformTimeIndexMetaData` instead of
the generic `PlatformManagedMetaTable`. It uses the same storage-hash machinery,
but also includes `time_index_name` and `index_names` in the stable identity and
registers through the TimeIndexMetaData endpoint.

For schema migrations, use Alembic with ordinary SQLAlchemy models. The SDK no
longer provides schema-migration MetaTable bases or custom operation lists.
Register `AlembicVersionMetaTable` as the catalog pointer to Alembic's version
table, then register or refresh changed MetaTable catalog bindings separately
after Alembic applies SQL.

When two backend-managed tables could otherwise have the same storage-relevant
shape, add `__metatable_extra_hash_components__` with stable deterministic
values:

```python
__metatable_extra_hash_components__ = {"storage_name": "account_holdings"}
```

This attribute is part of storage identity. Changing it creates a different
table. It is not for labels, descriptions, runtime options, test isolation,
backend UIDs, data-source UIDs, or updater scope.

For explicit physical naming, use a project-prefixed SQLAlchemy table name:

```python
__tablename__ = "sdk_examples__asset"
```

Keep authored physical table names within PostgreSQL's 63-character identifier
limit.

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

Use `get_schema_graph(include_incoming=True)` as the dependency API because its
edges include both `source_uid` and `target_uid`.

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
