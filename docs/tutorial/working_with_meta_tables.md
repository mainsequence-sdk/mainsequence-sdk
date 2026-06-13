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

For backend-managed tables, the MetaTable `uid` is the platform identity and
the optional `identifier` is the stable logical application identity. The
authored SQLAlchemy `__tablename__` is the physical table name Alembic uses, so
prefix it with the project or package name. Use `PlatformManagedMetaTable` to
derive the neutral MetaTable contract from the SQLAlchemy table shape. Use
Alembic for schema migrations; the SDK does not provide a separate
schema-migration MetaTable base.

This tutorial assumes SQLAlchemy is available in the project environment.

## 3. Define A Simple Backend-Managed MetaTable

Create a small SQLAlchemy model for account records:

```python
import uuid

from sqlalchemy import ForeignKey, Index, MetaData, String, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from mainsequence.meta_tables import (
    PlatformManagedMetaTable,
    schema_table_name,
    sqlalchemy_naming_convention,
)


PROJECT_NAME = "sdk_examples"
NAMESPACE = "sdk-examples"
ACCOUNT_TABLE_NAME = schema_table_name(PROJECT_NAME, "account")


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=sqlalchemy_naming_convention())


class Account(PlatformManagedMetaTable, Base):
    __tablename__ = ACCOUNT_TABLE_NAME
    __table_args__ = (
        Index(None, "region"),
    )

    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = f"{PROJECT_NAME}.Account"
    __metatable_description__ = "Tutorial accounts used as parent rows for governed related tables."
    __metatable_labels__ = ["tutorial"]

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    account_code: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    region: Mapped[str] = mapped_column(String(32), nullable=False)
```

The important pieces are:

- `PlatformManagedMetaTable` derives the neutral MetaTable contract from storage-relevant configuration and table shape
- `schema_table_name(PROJECT_NAME, "account")` creates an explicit project-prefixed SQLAlchemy table name used by Alembic
- omit SQLAlchemy schema metadata for the default PostgreSQL schema; use `__table_args__` only for non-default schemas or other table args
- `NAMESPACE` is a plain logical grouping for these SDK examples
- `__metatable_identifier__` is logical backend metadata and does not affect physical table naming
- `uid` is an application-level primary key, not a backend row id

Prefix explicit table identifiers and explicit physical table names with the
project or package name. Prefer `schema_table_name(project_or_app, concept)` so
the same convention is used everywhere. Bare names such as `Account`, `Asset`,
or `alembic_version` are easy to collide across projects sharing one
organization or database schema.

If you need a deterministic fingerprint for drift checks or custom stability
validation, call `compute_metatable_contract_hash()` explicitly. It includes the
physical table name by default and is not a MetaTable identity.

## 4. Add The Parent MetaTable To Migrations

Platform-managed tables are migration-first. Add the table to the selected
`AlembicMetaTableMigration.metatable_models` list and run migrations:

```bash
mainsequence migrations upgrade --provider sdk_examples.migrations:migration head
```

The SDK extracts a neutral table contract from SQLAlchemy metadata and sends it
to TS Manager. The backend does not import your SQLAlchemy model.
The data source is resolved from the active Main Sequence project/session, like
DataNode.

The returned `uid` is the public platform reference for this table. After
registration, the SDK binds that `MetaTable.uid` and storage metadata to the
model while preserving the authored SQLAlchemy table name. Keep the returned
`uid` in configuration if another API, dashboard, or job needs to read or write
the table later.

## 5. Add A Simple Related Table

Foreign keys are normal SQLAlchemy/Alembic DDL metadata. The SDK does not
serialize FK relationships into the MetaTable registration contract and does
not resolve target MetaTable UIDs for foreign keys. Prefer explicit table names
prefixed with the project or package name so SQLAlchemy FK string targets do not
collide across projects sharing the same schema.

```python
ACCOUNT_LIMIT_TABLE_NAME = schema_table_name(PROJECT_NAME, "account_limit")


class AccountLimit(PlatformManagedMetaTable, Base):
    __tablename__ = ACCOUNT_LIMIT_TABLE_NAME
    __table_args__ = (
        Index(None, "account_uid"),
    )

    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = f"{PROJECT_NAME}.AccountLimit"
    __metatable_description__ = "Account limit records keyed to the owning tutorial account."
    __metatable_labels__ = ["tutorial"]

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    account_uid: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        ForeignKey(f"{ACCOUNT_TABLE_NAME}.uid", ondelete="RESTRICT"),
        nullable=False,
    )
    limit_type: Mapped[str] = mapped_column(String(64), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
```

Add the child and parent to the same migration provider. The provider reserves
the MetaTable rows, Alembic creates/evolves the physical tables and constraints,
and finalization reconciles the MetaTable catalog after the upgrade:

```python
migration = build_metatable_migration_provider(
    ...,
    metatable_models=[Account, AccountLimit],
)
```

The SDK reserves the MetaTable rows for the provider. Alembic reads the
SQLAlchemy metadata and owns the FK DDL during revision generation and upgrade.

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
        "data_source_uid": account_meta_table.data_source_uid,
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
        "data_source_uid": account_meta_table.data_source_uid,
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

For select operations, `limits.max_rows` is the total row count requested by the
SDK call. If the backend returns a paginated response, the SDK follows
`pagination.next_offset` automatically until that count is satisfied or the
backend reports no more rows. Keep an explicit `ORDER BY` in paginated select
SQL so pages are stable.

The exact response shape is backend-defined, but the request contract is always
the same: compiled SQL plus execution data source and declared MetaTable scope.

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
- [MetaTable Migrations](metatable_migrations.md)
- [MetaTables Overview](../knowledge/meta_tables/index.md)
- [Registering SQLAlchemy Tables](../knowledge/meta_tables/sqlalchemy.md)
- [Compiled SQL Execution](../knowledge/meta_tables/compiled_sql.md)
- [MetaTable Examples](../../examples/meta_tables/README.md)
