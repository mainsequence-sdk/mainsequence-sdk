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
from mainsequence.tdag.meta_tables import (
    PlatformManagedMetaTable,
    PlatformTimeIndexMetaData,
    metatable_tablename,
    external_registered_registration_request_from_sqlalchemy_model,
    register_external_sqlalchemy_model,
    compile_sqlalchemy_statement,
)
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

For `platform_managed`, the backend requires:

```text
storage_hash == table_contract.physical.table_name
```

That is why the SDK exposes `PlatformManagedMetaTable` and `metatable_tablename(...)`.
The platform-managed class computes the physical table name from storage-relevant
configuration, including the SQLAlchemy table shape. The logical `identifier`
is sent to the backend but does not rotate the configured physical table name.

## Why Choose Platform-Managed

Platform-managed mode is useful when the application should not connect to the
database directly or should not own DDL credentials.

The practical benefits are:

- TS Manager uses the configured `DynamicTableDataSource` connection, so client
  code does not need direct database credentials.
- Table names come from `storage_hash`, which avoids collisions between users
  registering common names like `asset`, `account`, or `orders`.
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
- `physical.table_name`
- `columns`
- `indexes`
- `foreign_keys`
- optional authoring metadata

The selected data source does not belong inside `table_contract`. In normal
project execution it is resolved from the active Main Sequence session, the same
way DataNode resolves its data source.

```python
request = Asset.build_registration_request()

assert request.table_contract.physical.table_name == request.storage_hash
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
```

For time-indexed DynamicTable/DataNode storage, use `PlatformTimeIndexMetaData` instead
of the generic `PlatformManagedMetaTable`. It uses the same storage-hash
machinery, but also includes `time_index_name` and `index_names` in the stable
identity and registers through the DynamicTable/TimeIndexMetaData endpoint.

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
