# Registering SQLAlchemy Tables As MetaTables

This guide shows how to use SQLAlchemy models as the authoring layer while
registering neutral `MetaTable` contracts with TS Manager.

SQLAlchemy is optional for the SDK package itself. Your application installs and
uses SQLAlchemy. The SDK helpers only inspect resolved table metadata and build
Pydantic transport objects for the backend.

## Imports

```python
import uuid

from sqlalchemy import ForeignKey, Index, String, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from mainsequence.tdag.meta_tables import (
    metatable_tablename,
    platform_managed_registration_request_from_sqlalchemy_model,
    register_platform_managed_sqlalchemy_model,
)
```

## Define A Base

You can create a small project-local base or mixin. The SDK does not require a
specific base class.

```python
class Base(DeclarativeBase):
    pass
```

For platform-managed tables, make `__tablename__` the SDK-derived storage hash.

```python
ACCOUNT_TABLE_NAME = metatable_tablename(
    namespace="example.assets",
    identifier="Account",
)


class Account(Base):
    __tablename__ = ACCOUNT_TABLE_NAME
    __table_args__ = {"schema": "public"}

    __metatable_namespace__ = "example.assets"
    __metatable_identifier__ = "Account"

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
```

The `__metatable_namespace__` and `__metatable_identifier__` attributes let the
SDK infer logical metadata later during registration.

## Register A Platform-Managed Table

Build the request first when you want to inspect the payload:

```python
request = platform_managed_registration_request_from_sqlalchemy_model(
    Account,
    data_source_uid=DATA_SOURCE_UID,
)

assert request.management_mode == "platform_managed"
assert request.storage_hash == Account.__table__.name
assert request.table_contract.physical.table_name == Account.__table__.name
```

Then send it:

```python
from mainsequence.client import MetaTable

account_meta_table = MetaTable.register(request)
account_meta_table_uid = account_meta_table.uid
```

Or use the convenience helper:

```python
account_meta_table = register_platform_managed_sqlalchemy_model(
    Account,
    data_source_uid=DATA_SOURCE_UID,
)
```

The backend validates the table contract, creates the physical table when the
data source supports DDL, stores the MetaTable row, synchronizes column/index/FK
projections, and returns a platform `uid`.

## Foreign Keys

Foreign keys must reference registered target MetaTables by platform UID in the
contract. That means you normally register parent tables first.

```python
ASSET_TABLE_NAME = metatable_tablename(
    namespace="example.assets",
    identifier="Asset",
)
ASSET_ACCOUNT_INDEX_NAME = f"{ASSET_TABLE_NAME[:47]}_account_uid_idx"
ASSET_ACCOUNT_FK_NAME = f"{ASSET_TABLE_NAME[:46]}_account_uid_fkey"


class Asset(Base):
    __tablename__ = ASSET_TABLE_NAME
    __table_args__ = (
        Index(ASSET_ACCOUNT_INDEX_NAME, "account_uid"),
        {"schema": "public"},
    )

    __metatable_namespace__ = "example.assets"
    __metatable_identifier__ = "Asset"

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    account_uid: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        ForeignKey(
            f"{Account.__table__.fullname}.uid",
            name=ASSET_ACCOUNT_FK_NAME,
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
```

After registering `Account`, pass its MetaTable UID when building the `Asset`
contract:

```python
asset_request = platform_managed_registration_request_from_sqlalchemy_model(
    Asset,
    data_source_uid=DATA_SOURCE_UID,
    target_meta_table_uid_by_fullname={
        Account.__table__.fullname: account_meta_table.uid,
    },
)

asset_meta_table = MetaTable.register(asset_request)
```

The SDK contract serializer extracts:

- columns
- nullable flags
- primary-key flags
- unique flags
- explicitly named indexes
- explicitly named foreign keys
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

The examples are dry-run by default. They print the generated contracts unless
you set `MAINSEQUENCE_META_TABLE_REGISTER=1` or
`MAINSEQUENCE_META_TABLE_EXECUTE=1`.

## Validation Rules

The SDK intentionally fails early for ambiguous metadata:

- platform-managed tables must use `metatable_tablename(...)` as the physical table name
- SQLAlchemy models must expose a schema, either on `__table_args__` or via the helper's `schema=...`
- indexes must be explicitly named
- foreign keys must be explicitly named
- foreign-key targets must be supplied through `target_meta_table_uid_by_fullname`
- unsupported SQLAlchemy column types raise before registration

This is deliberate. TS Manager should receive a deterministic table contract,
not a guessed ORM interpretation.

## Labels And Permissions

Registration requests accept the same organizational metadata as backend
MetaTables:

```python
request = platform_managed_registration_request_from_sqlalchemy_model(
    Asset,
    data_source_uid=DATA_SOURCE_UID,
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
class Asset(Base):
    __tablename__ = metatable_tablename(
        namespace="example.assets",
        identifier="Asset",
    )
```

Do not put `data_source_uid` inside `table_contract`. It belongs to the
registration request.

Do not use SQLAlchemy objects as the backend protocol. The SDK extracts a
neutral contract; the server stores and validates that contract.
