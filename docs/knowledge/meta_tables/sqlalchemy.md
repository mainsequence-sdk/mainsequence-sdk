# Registering SQLAlchemy Tables As MetaTables

This guide shows how to use SQLAlchemy models as the authoring layer while
registering neutral `MetaTable` contracts with TS Manager.

SQLAlchemy is optional for the SDK package itself. Your application installs and
uses SQLAlchemy. The SDK helpers only inspect resolved table metadata and build
Pydantic transport objects for the backend.

## Imports

```python
import uuid

from sqlalchemy import ForeignKey, Index, MetaData, String, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from mainsequence.tdag.meta_tables import (
    PlatformManagedMetaTable,
)
```

## Define A Base

You can create a small project-local base and use `PlatformManagedMetaTable` only on
models that should be platform-managed.

```python
NAMING_CONVENTION = {
    "ix": "%(table_name)s_%(column_0_name)s_idx",
    "fk": "%(table_name)s_%(column_0_name)s_fkey",
    "pk": "%(table_name)s_pkey",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)
```

For platform-managed tables, inherit `PlatformManagedMetaTable`. It derives the
physical table name from storage-relevant configuration and the SQLAlchemy table
shape, and exposes registration helpers on the model class.

```python
class Account(PlatformManagedMetaTable, Base):
    __table_args__ = {"schema": "public"}

    __metatable_namespace__ = "sdk-examples"
    __metatable_identifier__ = "Account"

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
```

The `__metatable_identifier__` attribute is logical backend metadata. It is
sent during registration but does not contribute to the configured physical
table name. The mapped columns, indexes, and foreign keys do contribute to the
configured table name.

## Register A Platform-Managed Table

Build the request first when you want to inspect the payload:

```python
request = Account.build_registration_request(
    description="Example account table.",
    labels=["sdk-example"],
)

assert request.management_mode == "platform_managed"
assert request.storage_hash == Account.__table__.name
assert request.table_contract.physical.table_name == Account.__table__.name
```

The data source is resolved from the active Main Sequence project/session, like
DataNode. Pass `data_source=...` or `data_source_uid=...` only for advanced
scripts and tests outside that context.

Then send it:

```python
account_meta_table = Account.register(
    description="Example account table.",
    labels=["sdk-example"],
)
account_meta_table_uid = account_meta_table.uid
```

The backend validates the table contract, creates the physical table when the
data source supports DDL, stores the MetaTable row, synchronizes column/index/FK
projections, and returns a platform `uid`.

## Foreign Keys

Foreign keys must reference registered target MetaTables by platform UID in the
backend contract. In normal platform-managed use, register parent tables first;
the SDK inspects the SQLAlchemy foreign key and resolves the target MetaTable
from the same data source, schema, and physical table name.

```python
class Asset(PlatformManagedMetaTable, Base):
    __table_args__ = (
        Index(None, "account_uid"),
        {"schema": "public"},
    )

    __metatable_namespace__ = "sdk-examples"
    __metatable_identifier__ = "Asset"

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    account_uid: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        ForeignKey(
            f"{Account.__table__.fullname}.uid",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
```

After registering `Account`, build or register `Asset` normally:

```python
asset_request = Asset.build_registration_request()

asset_meta_table = Asset.register()
```

The SDK contract serializer extracts:

- columns
- nullable flags
- primary-key flags
- unique flags
- indexes named by SQLAlchemy's naming convention
- foreign keys named by SQLAlchemy's naming convention
- FK source columns
- FK target MetaTable UID
- FK target columns
- backend type strings such as `VARCHAR(64)`

## External Registration

Use `external_registered` when your app or migration framework creates the
physical table.

```python
from mainsequence.tdag.meta_tables import (
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

- [platform_managed_account_asset.py](../../../examples/meta_tables/platform_managed_account_asset.py)
- [external_registered_account_asset.py](../../../examples/meta_tables/external_registered_account_asset.py)
- [compiled_sql_account_asset_query.py](../../../examples/meta_tables/compiled_sql_account_asset_query.py)

The registration examples call TS Manager by default. The compiled query example
prints the generated operation unless you set `MAINSEQUENCE_META_TABLE_EXECUTE=1`.

## Validation Rules

The SDK intentionally fails early for ambiguous metadata:

- platform-managed tables must use `PlatformManagedMetaTable` or `metatable_tablename(...)` as the physical table name
- SQLAlchemy models must expose schema through SQLAlchemy table metadata, usually `__table_args__`
- indexes must resolve to names, either explicitly or through SQLAlchemy naming conventions
- foreign keys must resolve to names, either explicitly or through SQLAlchemy naming conventions
- foreign-key targets must be registered first, or supplied explicitly through `target_meta_tables` or `target_meta_table_uid_by_fullname` for edge cases
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
    open_for_everyone=False,
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
    __metatable_identifier__ = "Asset"
```

Do not put `data_source_uid` inside `table_contract`. It belongs to the
registration request.

Do not use SQLAlchemy objects as the backend protocol. The SDK extracts a
neutral contract; the server stores and validates that contract.
