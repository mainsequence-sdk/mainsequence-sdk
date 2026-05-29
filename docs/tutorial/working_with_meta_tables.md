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

- customers
- counterparties
- account groups
- mappings
- application records that need stable primary keys and relational constraints

The next tutorial introduces `DataNode` as an opinionated MetaTable-backed
update workflow. Use a `DataNode` when the table should be produced or refreshed
by SDK update logic, dependencies, and scheduling. Use `MetaTable` directly when
you need the table contract and governed operations without that update layer.

Legacy row-table authoring helpers are deprecated. New tutorial code should use
`MetaTable`.

## 2. Backend-Managed Means `platform_managed`

The SDK calls backend-managed MetaTables `platform_managed`.

In this mode:

- your project defines the table contract locally
- TS Manager creates or validates the physical table
- the application does not need direct DDL credentials
- table permissions and governed execution go through the platform

For backend-managed tables, the physical table name must be the SDK-derived
storage hash. Use `PlatformManagedMetaTable` when the table name should be
derived from the SQLAlchemy table shape.

This tutorial assumes SQLAlchemy is available in the project environment.

## 3. Define A Simple Backend-Managed MetaTable

Create a small SQLAlchemy model for customer records:

```python
import uuid

from sqlalchemy import Index, MetaData, String, Uuid
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from mainsequence.tdag.meta_tables import PlatformManagedMetaTable


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


class Customer(PlatformManagedMetaTable, Base):
    __table_args__ = (
        Index(None, "region"),
        {"schema": SCHEMA},
    )

    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = "Customer"

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    customer_code: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    region: Mapped[str] = mapped_column(String(32), nullable=False)
```

The important pieces are:

- `PlatformManagedMetaTable` derives `__tablename__` from storage-relevant configuration and table shape
- `__table_args__` declares the SQLAlchemy table schema used for table-name derivation
- `NAMESPACE` is a plain logical grouping for these SDK examples
- `__metatable_identifier__` is logical backend metadata and does not rotate the configured physical name
- `uid` is an application-level primary key, not a backend row id

## 4. Register The Parent MetaTable

Register the table through the class API:

```python
customer_meta_table = Customer.register(
    description="Tutorial backend-managed customer table.",
    labels=["tutorial"],
)

print(customer_meta_table.uid)
print(customer_meta_table.physical_table_name)
```

The SDK extracts a neutral table contract from SQLAlchemy metadata and sends it
to TS Manager. The backend does not import your SQLAlchemy model.
The data source is resolved from the active Main Sequence project/session, like
DataNode.

The returned `uid` is the public platform reference for this table. Keep it in
configuration if another API, dashboard, or job needs to read or write the
table later.

## 5. Add A Simple Related Table

Foreign keys reference a registered target `MetaTable` by UID in the backend
contract. In normal platform-managed use, register parent tables first; the SDK
then inspects the SQLAlchemy foreign key and resolves the target MetaTable from
the same data source and physical table name.

```python
from sqlalchemy import ForeignKey


class CustomerLimit(PlatformManagedMetaTable, Base):
    __table_args__ = (
        Index(None, "customer_uid"),
        {"schema": SCHEMA},
    )

    __metatable_namespace__ = NAMESPACE
    __metatable_identifier__ = "CustomerLimit"

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    customer_uid: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        ForeignKey(
            f"{Customer.__table__.fullname}.uid",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    limit_type: Mapped[str] = mapped_column(String(64), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
```

Register the child after the parent is registered:

```python
limit_meta_table = CustomerLimit.register(
    description="Tutorial backend-managed customer limit table.",
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
import uuid

from mainsequence.client import MetaTable
from mainsequence.tdag.meta_tables import build_compiled_sql_v1_operation


def qualified_name(meta_table: MetaTable) -> str:
    return meta_table.physical_table_name


customer_uid = str(uuid.uuid4())
operation = build_compiled_sql_v1_operation(
    operation="insert",
    sql=f"""
        INSERT INTO {qualified_name(customer_meta_table)}
            (uid, customer_code, name, region)
        VALUES
            (%(uid)s, %(customer_code)s, %(name)s, %(region)s)
        ON CONFLICT (uid) DO UPDATE SET
            customer_code = EXCLUDED.customer_code,
            name = EXCLUDED.name,
            region = EXCLUDED.region
        RETURNING uid, customer_code, name, region
    """,
    parameters={
        "uid": customer_uid,
        "customer_code": "ACME",
        "name": "Acme Capital",
        "region": "US",
    },
    scope={
        "tables": [
            {
                "meta_table_uid": customer_meta_table.uid,
                "alias": "customers",
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
operation = build_compiled_sql_v1_operation(
    operation="select",
    sql=f"""
        SELECT uid, customer_code, name, region
        FROM {qualified_name(customer_meta_table)}
        WHERE (%(region)s IS NULL OR region = %(region)s)
        ORDER BY customer_code
        LIMIT %(limit)s
    """,
    parameters={"region": "US", "limit": 50},
    scope={
        "tables": [
            {
                "meta_table_uid": customer_meta_table.uid,
                "alias": "customers",
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

- backend-managed `MetaTable` for customer, account, or mapping records
- `DataNode` for daily metrics backed by a governed table contract
- FastAPI route that combines both into a stable response

## 9. Further Reading

- [Creating a Data Node](creating_a_simple_data_node.md)
- [MetaTables Overview](../knowledge/meta_tables/index.md)
- [Registering SQLAlchemy Tables](../knowledge/meta_tables/sqlalchemy.md)
- [Compiled SQL Execution](../knowledge/meta_tables/compiled_sql.md)
- [MetaTable Examples](../../examples/meta_tables/README.md)
