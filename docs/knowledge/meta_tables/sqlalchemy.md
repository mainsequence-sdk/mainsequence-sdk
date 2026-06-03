# Registering SQLAlchemy Tables As MetaTables

This guide shows how to use SQLAlchemy models as the authoring layer while
registering neutral `MetaTable` contracts with TS Manager.

SQLAlchemy is a core SDK dependency. The SDK uses it to inspect resolved table
metadata and build Pydantic transport objects for the backend.

## Imports

```python
import datetime
import uuid

from sqlalchemy import DateTime, Float, Index, MetaData, String, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from mainsequence.meta_tables import (
    MetaTableForeignKey,
    PlatformManagedMetaTable,
    PlatformTimeIndexMetaData,
)
```

## Define A Base

You can create a small project-local base and use `PlatformManagedMetaTable` only on
models that should be platform-managed.

```python
NAMING_CONVENTION = {
    "ix": "%(table_name)s_%(column_0_name)s_idx",
    "pk": "%(table_name)s_pkey",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)
```

For platform-managed tables, inherit `PlatformManagedMetaTable`. It derives the
logical `storage_hash` from storage-relevant configuration and the SQLAlchemy
table shape, and exposes registration helpers on the model class. The backend
owns the physical table name.

```python
class Account(PlatformManagedMetaTable, Base):
    __table_args__ = {"schema": "public"}

    __metatable_namespace__ = "sdk-examples"
    __metatable_identifier__ = "sdk_examples.Account"
    __metatable_description__ = "Accounts used as the parent entity for asset and holdings tables."
    __metatable_extra_hash_components__ = {"storage_name": "account"}
    __metatable_labels__ = ["sdk-example"]

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
```

The `__metatable_identifier__` attribute is logical backend metadata. It is
sent during registration but does not contribute to the configured
`storage_hash`. When present, it must be globally unique per organization and
is the stable lookup key used to resolve migrated MetaTables. The mapped
columns, indexes, and foreign keys do contribute to the configured storage
identity.

Prefix explicit table identifiers, explicit physical table names, and Alembic
version table names with the project or package name. Bare names such as
`Account`, `Asset`, or `alembic_version` can collide across projects sharing the
same organization or database schema.

Use `__metatable_extra_hash_components__` to add stable, deterministic
storage-identity components when two table classes could otherwise hash to the
same storage name. This is common for generic or repeated storage shapes, such
as several one-index time-series tables with the same column types.

```python
class DailyReturns(PlatformTimeIndexMetaData, Base):
    __metatable_namespace__ = "sdk-examples"
    __metatable_identifier__ = "sdk_examples.DailyReturns"
    __metatable_description__ = "Daily return observations keyed by time for tutorial assets."
    __metatable_extra_hash_components__ = {"storage_name": "daily_returns"}
```

Changing `__metatable_extra_hash_components__` changes the logical storage
identity and therefore points at a different table. Do not use it for labels,
descriptions, runtime parameters, test isolation, backend UIDs, data-source
UIDs, or per-run updater scope.

## Migration-Manage A Platform-Managed Table

Build the request first when you want to inspect the payload:

```python
request = Account.build_registration_request()

assert request.management_mode == "platform_managed"
assert request.storage_hash == Account.__table__.name
assert request.table_contract.physical.table_name is None
```

The data source is resolved from the active Main Sequence project/session, like
DataNode. Registration metadata belongs on the model class, but users should
not call `Account.register()` directly for platform-managed tables.

Put platform-managed models in the selected migration provider:

```python
migration = AlembicMetaTableMigration(
    package="sdk_examples",
    migration_namespace="sdk-examples",
    script_location="sdk_examples:migrations",
    target_metadata=Base.metadata,
    alembic_registry=ProjectAlembicVersion,
    metatable_models=[Account],
)
```

Then run:

```bash
mainsequence migrations upgrade --provider mainsequence_migrations:migration head
```

Migration tooling calls the managed reservation path for missing
provider-scoped models, lets Alembic create the initial physical table, binds
`Account.__table__.name` to the returned physical table name, and then
renders/applies Alembic SQL for schema creation and evolution.

## Foreign Keys

Foreign keys must reference registered target MetaTables by platform UID in the
backend contract. For platform-managed tables, declare the target model class
with `MetaTableForeignKey(TargetModel, column=...)`.

Do not use SQLAlchemy table fullnames or parent table column objects as the
public SDK declaration. Migration-managed registration may rebind
`Account.__table__.name` to the backend physical table name.
`MetaTableForeignKey` stores the target model class and target column as SDK
metadata. Migration tooling resolves parent targets and backend
`MetaTable.uid` values before rendering SQL.

Platform-managed `MetaTableForeignKey(...)` contracts omit physical constraint
names. Alembic, SQLAlchemy, and the database own the physical FK name; backend
reservation does not generate or manage it.

```python
class Asset(PlatformManagedMetaTable, Base):
    __table_args__ = (
        Index(None, "account_uid"),
        {"schema": "public"},
    )

    __metatable_namespace__ = "sdk-examples"
    __metatable_identifier__ = "sdk_examples.Asset"
    __metatable_description__ = "Assets associated with an owning account."

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    account_uid: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        MetaTableForeignKey(Account, column="uid", ondelete="RESTRICT"),
        nullable=False,
    )
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
```

Include `Asset` in the provider's `metatable_models`. The migration workflow
resolves unresolved parent targets first and keeps a local
`storage_hash -> MetaTable` registry so the same table is not registered twice
during migration-managed registration.

The SDK contract serializer extracts:

- columns
- nullable flags
- primary-key flags
- unique flags
- indexes named by SQLAlchemy's naming convention
- foreign-key relationships without physical constraint names
- FK source columns
- FK target MetaTable UID
- FK target columns
- backend type strings such as `VARCHAR(64)`

## Time-Indexed DataNode Storage

Use `PlatformTimeIndexMetaData` when the table is DynamicTable/DataNode storage rather
than a generic relational MetaTable. It inherits the platform-managed MetaTable
authoring behavior, but registers through:

```text
/orm/api/ts_manager/dynamic_table/register/
```

The client sends only the explicit time-indexed table contract:

- `data_source_uid`
- `storage_hash`
- `identifier`
- `namespace`
- `description`
- `time_index_name`
- `table_contract`, which owns columns, indexes, and foreign keys

The backend derives identity dimensions, update progress grain, uniqueness,
tail-delete scope, physical indexes, and the time-indexed profile from the
MetaTable contract.

```python
class AccountHoldings(PlatformTimeIndexMetaData, Base):
    __table_args__ = (
        Index(None, "account_uid"),
        {"schema": "public"},
    )

    __metatable_namespace__ = "sdk-examples"
    __metatable_identifier__ = "AccountHoldings"
    __metatable_description__ = "Time-indexed account holdings by account and unique instrument identifier."
    __time_index_name__ = "time_index"
    __index_names__ = ["time_index", "account_uid", "unique_identifier"]

    time_index: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    account_uid: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        MetaTableForeignKey(Account, column="uid", ondelete="RESTRICT"),
        nullable=False,
    )
    unique_identifier: Mapped[str] = mapped_column(String(255), nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)


request = AccountHoldings.build_registration_request()

assert request.time_index_name == "time_index"
assert request.table_contract["authoring"]["time_indexed"]["index_names"] == [
    "time_index",
    "account_uid",
    "unique_identifier",
]
```

Validation is intentionally strict:

- the first index must be `time_index_name`
- every index column must exist in the SQLAlchemy table
- every index column must be non-nullable
- the time-index column must be temporal and timezone-aware for remote storage
- the client must not send derived backend fields such as `identity_dimensions`,
  `index_progress`, `tail_delete`, `uniqueness`, or `physical_index_plan`

## Migration-Managed Tables

Use Alembic when a platform-managed table must evolve. The SDK no longer
provides schema-migration SQLAlchemy base classes or a parallel operation-list
migration language.

For generic MetaTables:

```python
class Account(PlatformManagedMetaTable, Base):
    __metatable_namespace__ = "sdk-examples"
    __metatable_identifier__ = "sdk-examples.Account"

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
```

For time-indexed DataNode storage:

```python
class AccountHoldings(PlatformTimeIndexMetaData, Base):
    __metatable_namespace__ = "sdk-examples"
    __metatable_identifier__ = "sdk-examples.AccountHoldings"
    __time_index_name__ = "time_index"
    __index_names__ = ["time_index", "account_uid"]

    time_index: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    account_uid: Mapped[uuid.UUID] = mapped_column(Uuid, nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
```

Alembic owns physical DDL. MetaTable registration remains a catalog binding and
can be refreshed after Alembic applies schema changes.

## External Registration

Use `external_registered` when your app or schema-management framework creates the
physical table.

```python
from mainsequence.meta_tables import (
    external_registered_registration_request_from_sqlalchemy_model,
    register_external_sqlalchemy_model,
)


request = external_registered_registration_request_from_sqlalchemy_model(
    Asset,
    data_source_uid=DATA_SOURCE_UID,
)

asset_meta_table = MetaTable.register(request)
```

In this mode:

- the physical table name can be `asset`
- TS Manager does not create the table
- TS Manager registers metadata and can introspect the existing table
- `storage_hash` remains the platform identity

## Complete Examples

The SDK repository includes complete Account/Asset examples:

- [platform_managed/account_asset.py](../../../examples/meta_tables/platform_managed/account_asset.py)
- [external_managed/account_asset.py](../../../examples/meta_tables/external_managed/account_asset.py)
- [compiled_sql_account_asset_query.py](../../../examples/meta_tables/compiled_sql_account_asset_query.py)

The platform-managed example is migration-first. The external-managed example
uses the low-level TS Manager registration primitive. The compiled query example
prints the generated operation unless you set `MAINSEQUENCE_META_TABLE_EXECUTE=1`.

## Validation Rules

The SDK intentionally fails early for ambiguous metadata:

- platform-managed tables must use `PlatformManagedMetaTable` or `metatable_tablename(...)` to derive the logical `storage_hash`
- SQLAlchemy models must expose schema through SQLAlchemy table metadata, usually `__table_args__`
- indexes must resolve to names, either explicitly or through SQLAlchemy naming conventions
- platform-managed foreign keys must use `MetaTableForeignKey(TargetModel, column=...)`
- the migration workflow resolves `MetaTableForeignKey` target models through their stable identifiers and binds their `MetaTable.uid`
- unsupported SQLAlchemy column types raise before registration

This is deliberate. TS Manager should receive a deterministic table contract,
not a guessed ORM interpretation.

## Labels And Permissions

Registration requests accept the same organizational metadata as backend
MetaTables:

```python
request = Asset.build_registration_request(
    labels=["assets", "reference"],
    description="Tradable asset master table.",
    protect_from_deletion=True,
)
```

Labels and descriptions help discovery and search. They do not affect the
storage hash.

## Common Mistakes

Do not hand-write a platform-managed table name:

```python
class Asset(Base):
    __tablename__ = "asset"  # wrong for platform_managed
```

Use:

```python
class Asset(PlatformManagedMetaTable, Base):
    __metatable_namespace__ = "sdk-examples"
    __metatable_identifier__ = "sdk_examples.Asset"
    __metatable_description__ = "Externally managed asset table registered as a governed MetaTable."
```

Do not put `data_source_uid` inside `table_contract`. It belongs to the
registration request.

Do not use SQLAlchemy objects as the backend protocol. The SDK extracts a
neutral contract; the server stores and validates that contract.
