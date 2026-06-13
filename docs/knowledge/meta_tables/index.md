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
    PlatformTimeIndexMetaTable,
    external_registered_registration_request_from_sqlalchemy_model,
    register_external_sqlalchemy_model,
    schema_table_name,
    sqlalchemy_naming_convention,
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
`account`. The platform identity is the MetaTable `uid`; the logical
application identity is the optional `identifier`.

### `platform_managed`

Use this when Alembic should create and evolve the physical table while TS
Manager keeps the MetaTable catalog binding.

Typical flow:

1. Define SQLAlchemy models with `PlatformManagedMetaTable` and project-prefixed `__tablename__` values, preferably from `schema_table_name(project_or_app, concept)`.
2. Add those models to `AlembicMetaTableMigration.metatable_models`.
3. Run `mainsequence migrations upgrade --provider ... head`.
4. Migration tooling reserves catalog rows for missing models, Alembic applies schema
   evolution SQL.

For `platform_managed`, the authored SQLAlchemy table name is sent as
`table_contract.physical.table_name` and should be prefixed with the project or
package name. Alembic owns the physical table lifecycle, while the backend owns
MetaTable uniqueness and reconciliation through `uid`, `identifier`, data
source, and physical table name.

That is why the SDK exposes `PlatformManagedMetaTable`.
The platform-managed class builds the neutral table contract from
storage-relevant SQLAlchemy metadata without replacing the SQLAlchemy table
name.

## Why Choose Platform-Managed

Platform-managed mode is useful when the application should not connect to the
database directly or should not own DDL credentials.

The practical benefits are:

- TS Manager uses the configured `DynamicTableDataSource` connection, so client
  code does not need direct database credentials.
- Physical table names are explicit SQLAlchemy/Alembic names, while platform
  references use MetaTable UIDs and logical identifiers.
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
- `physical.table_name`, the authored physical table name.
- `columns`
- `indexes`
- `foreign_keys`
- optional authoring metadata

The selected data source does not belong inside `table_contract`. In normal
project execution it is resolved from the active Main Sequence session, the same
way DataNode resolves its data source.

```python
request = Asset.build_registration_request()

assert "storage_hash" not in request.model_dump(mode="json", exclude_none=True)
assert request.table_contract.physical.table_name == Asset.__table__.name
```

## Contract Fingerprints

`storage_hash` is no longer a MetaTable field or platform identity. Use
`uid`, `identifier`, and `physical_table_name` for MetaTable identity and
lookup.

If you need a deterministic contract fingerprint for drift checks, cache keys,
or custom stability validation, call the explicit utility:

```python
from mainsequence.meta_tables import compute_metatable_contract_hash

contract_hash = compute_metatable_contract_hash(Asset)
```

The utility includes the physical table name by default, so two same-shaped
tables with different authored table names produce different fingerprints.

For platform-managed tables, prefer explicit, project-prefixed SQLAlchemy table
names:

```python
ASSET_TABLE_NAME = schema_table_name("sdk_examples", "asset")


class Asset(PlatformManagedMetaTable, Base):
    __tablename__ = ASSET_TABLE_NAME
    __metatable_namespace__ = "sdk-examples"
    __metatable_identifier__ = "sdk_examples.Asset"
```

For time-indexed DataNode storage, use `PlatformTimeIndexMetaTable` instead of
the generic `PlatformManagedMetaTable`. It includes `time_index_name` and
`index_names` in the authored table contract and registers through the
TimeIndexMetaTable endpoint.

For schema migrations, use Alembic with ordinary SQLAlchemy models. The SDK no
longer provides schema-migration MetaTable bases or custom operation lists.
Register `AlembicVersionMetaTable` as the catalog pointer to Alembic's version
table, then register or refresh changed MetaTable catalog bindings separately
after Alembic applies SQL.

When a custom fingerprint needs additional stable inputs beyond the default
contract and physical table name, pass `extra_components` to the utility:

```python
compute_metatable_contract_hash(
    AccountHoldings,
    extra_components={"storage_name": "account_holdings"},
)
```

These components are not MetaTable identity. Do not use them for labels,
descriptions, runtime options, test isolation, backend UIDs, data-source UIDs,
or updater scope.

For explicit physical naming, use a project-prefixed SQLAlchemy table name:

```python
__tablename__ = schema_table_name("sdk_examples", "asset")
```

Keep authored physical table names within PostgreSQL's 63-character identifier
limit. `schema_table_name()` preserves the project/app prefix and adds a stable
hash suffix when a generated name would exceed that limit.

## Backend Responsibilities

After registration, TS Manager stores a `MetaTable` row plus the metadata needed
for serializers, search documents, discovery, and query validation. Alembic and
the database remain the authority for physical indexes and foreign keys; any
backend FK/index discovery is reflected physical metadata, not an SDK-managed
registration contract.

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
