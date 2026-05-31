# Part 2: Working With MetaTables

## Quick Summary

In this tutorial, you will:

- understand `MetaTable` as the canonical platform table abstraction
- define simple relational tables with SQLAlchemy models
- register backend-managed `MetaTable`s
- let TS Manager create the physical tables through a configured data source
- register a parent table before a child table with a foreign key
- insert and read rows through governed MetaTable operations
- understand how a `DataNode` later becomes an opinionated MetaTable-backed update workflow

The concrete examples in this repository live under
[`examples/meta_tables`](../../examples/meta_tables/README.md).

## 1. MetaTable Is The Canonical Table

Use a `MetaTable` when your application needs a platform table contract: table
UID, physical table name, data source, schema, labels, sharing, and governed
read/write operations. `MetaTable` is the base storage concept in Main
Sequence.

This chapter starts with row-oriented relational data because it makes the table
contract visible without introducing update scheduling or DataNode dependencies
yet.

Good examples:

- accounts
- counterparties
- account groups
- mappings
- application records that need stable primary keys and relational constraints

The next tutorial introduces `DataNode` as an opinionated MetaTable-backed
update workflow. Use a `DataNode` when the table should be produced or refreshed
by SDK update logic, dependencies, and scheduling. Use `MetaTable` directly when
you need the table contract and governed operations without that update layer.

New tutorial code should use `MetaTable` for governed tables.

## 2. Backend-Managed Means `platform_managed`

The SDK calls backend-managed MetaTables `platform_managed`.

In this mode:

- your project defines the table contract locally
- TS Manager creates or validates the physical table
- the application does not need direct DDL credentials
- table permissions and governed execution go through the platform

For backend-managed tables, the SDK-derived `storage_hash` is the logical table
identity. TS Manager owns the physical table name and returns it during
registration. Use `PlatformManagedMetaTable` when the logical identity should be
derived from the SQLAlchemy table shape.

This tutorial assumes SQLAlchemy is available in the project environment.

## 3. Define A Simple Backend-Managed MetaTable

Create a small SQLAlchemy model for account records:

```python
import uuid

from sqlalchemy import Index, MetaData, String, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from mainsequence.meta_tables import PlatformManagedMetaTable


NAMESPACE = "sdk-examples"
SCHEMA = "public"


class Base(DeclarativeBase):
    metadata = MetaData(
        naming_convention={
            "ix": "%(table_name)s_%(column_0_name)s_idx",
            "fk": "%(table_name)s_%(column_0_name)s_fkey",
            "pk": "%(table_name)s_pkey",
        }
    )


class Account(PlatformManagedMetaTable, Base):
    __table_args__ = (
        Index(None, "region"),
        {"schema": SCHEMA},
    )

    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = "Account"
    __metatable_extra_hash_components__ = {"storage_name": "account"}

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    account_code: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    region: Mapped[str] = mapped_column(String(32), nullable=False)
```

The important pieces are:

- `PlatformManagedMetaTable` derives the initial `__tablename__` / `storage_hash` from storage-relevant configuration and table shape
- `__table_args__` declares the SQLAlchemy table schema used for storage-hash derivation
- `NAMESPACE` is a plain logical grouping for these SDK examples
- `__metatable_identifier__` is logical backend metadata and does not rotate the configured storage identity
- `__metatable_extra_hash_components__` adds a stable storage-identity component so similarly shaped tables cannot collide
- `uid` is an application-level primary key, not a backend row id

`__metatable_extra_hash_components__` is part of storage identity. Use stable
values such as `{"storage_name": "account"}` or
`{"storage_name": "account_limit"}`. Do not use labels, descriptions, runtime
options, backend UIDs, data-source UIDs, or test-specific values there.

## 4. Register The Parent MetaTable

Register the table through the class API:

```python
account_meta_table = Account.register(
    description="Tutorial backend-managed account table.",
    labels=["tutorial"],
)

print(account_meta_table.uid)
print(account_meta_table.physical_table_name)
```

The SDK extracts a neutral table contract from SQLAlchemy metadata and sends it
to TS Manager. The backend does not import your SQLAlchemy model.
The data source is resolved from the active Main Sequence project/session, like
DataNode.

The returned `uid` is the public platform reference for this table. After
registration, the SDK privately rebinds `Account.__table__.name` to the returned
`physical_table_name`, so SQLAlchemy statements compile against the backend
physical table. Keep the returned `uid` in
configuration if another API, dashboard, or job needs to read or write the
table later.

## 5. Add A Simple Related Table

Foreign keys reference a registered target `MetaTable` by UID in the backend
contract. In normal platform-managed use, register parent tables first; the SDK
then inspects the SQLAlchemy foreign key and resolves the target MetaTable from
the same data source and logical storage identity.

Use the parent column object in the SQLAlchemy `ForeignKey`. Do not build the
target from `Account.__table__.fullname`; registration can rebind that fullname
to the backend physical table name.

```python
from sqlalchemy import ForeignKey


class AccountLimit(PlatformManagedMetaTable, Base):
    __table_args__ = (
        Index(None, "account_uid"),
        {"schema": SCHEMA},
    )

    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = "AccountLimit"
    __metatable_extra_hash_components__ = {"storage_name": "account_limit"}

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    account_uid: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        ForeignKey(
            Account.__table__.c.uid,
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    limit_type: Mapped[str] = mapped_column(String(64), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
```

Register the child after the parent is registered:

```python
limit_meta_table = AccountLimit.register(
    description="Tutorial backend-managed account limit table.",
    labels=["tutorial"],
)
```

The SDK extracts the foreign key source columns, target MetaTable UID, target
columns, and `on_delete` rule into the registration contract.

## 6. Insert Rows Through Governed Operations

MetaTable operations are not raw unrestricted database access. The request
declares:

- operation kind
- SQL text and bound parameters
- table scope by MetaTable UID
- read/write access per scoped table
- optional row and timeout limits

Example insert:

```python
from mainsequence.client import MetaTable
from mainsequence.meta_tables.compiled_sql.v1 import build_operation


def qualified_name(meta_table: MetaTable) -> str:
    return meta_table.physical_table_name


operation = build_operation(
    operation="insert",
    sql=f"""
        INSERT INTO {qualified_name(account_meta_table)}
            (account_code, name, region)
        VALUES
            (%(account_code)s, %(name)s, %(region)s)
        ON CONFLICT (account_code) DO UPDATE SET
            name = EXCLUDED.name,
            region = EXCLUDED.region
        RETURNING uid, account_code, name, region
    """,
    parameters={
        "account_code": "ACME",
        "name": "Acme Capital",
        "region": "US",
    },
    scope={
        "tables": [
                {
                    "meta_table_uid": account_meta_table.uid,
                    "alias": "accounts",
                    "access": "write",
                }
        ]
    },
    limits={"max_rows": 100, "statement_timeout_ms": 15000},
)

insert_result = MetaTable.execute_operation(operation)
```

Only use physical table names returned by registered `MetaTable` objects when
building SQL strings. User input belongs in bound parameters, not SQL text.

## 7. Read Rows

Read operations use the same governed execution path with read scope:

```python
operation = build_operation(
    operation="select",
    sql=f"""
        SELECT uid, account_code, name, region
        FROM {qualified_name(account_meta_table)}
        WHERE (%(region)s IS NULL OR region = %(region)s)
        ORDER BY account_code
        LIMIT %(limit)s
    """,
    parameters={"region": "US", "limit": 50},
    scope={
        "tables": [
                {
                    "meta_table_uid": account_meta_table.uid,
                    "alias": "accounts",
                    "access": "read",
                }
        ]
    },
    limits={"max_rows": 50, "statement_timeout_ms": 15000},
)

rows = MetaTable.execute_operation(operation)
```

The exact response shape is backend-defined, but the request contract is always
the same: compiled SQL plus declared MetaTable scope.

## 8. How MetaTables Fit With DataNodes

Use them together:

- `MetaTable`s publish canonical table contracts and governed access
- `DataNode`s are opinionated producers for MetaTable-backed datasets, usually time-indexed or derived from other resources
- APIs and dashboards consume those contracts instead of reaching into private project code

A common project shape is:

- backend-managed `MetaTable` for account, counterparty, or mapping records
- `DataNode` for daily metrics backed by a governed table contract
- FastAPI route that combines both into a stable response

## 9. Further Reading

- [Creating a Data Node](creating_a_simple_data_node.md)
- [MetaTables Overview](../knowledge/meta_tables/index.md)
- [Registering SQLAlchemy Tables](../knowledge/meta_tables/sqlalchemy.md)
- [Compiled SQL Execution](../knowledge/meta_tables/compiled_sql.md)
- [MetaTable Examples](../../examples/meta_tables/README.md)
