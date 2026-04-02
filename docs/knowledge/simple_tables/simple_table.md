# What Is a `SimpleTable`?

`SimpleTable` is the schema model you use when your data is not naturally a time series.

Use it for tables such as:

- customers
- portfolios
- mappings
- reference lists
- balance snapshots where time is just another column

If your data is fundamentally built around `time_index` and `unique_identifier`, you usually want a `DataNode` table instead.

## The Core Idea

A `SimpleTable` defines one logical row.

That definition becomes:

- the Python row model you instantiate in code
- the schema sent to the backend
- the typed surface used for filtering

Example:

```python
import datetime
from typing import Annotated

from pydantic import Field

from mainsequence.tdag.simple_tables import ForeignKey, Index, Ops, SimpleTable


class CustomerRecord(SimpleTable):
    customer_code: Annotated[str, Index(unique=True), Ops(filter=True, order=True)] = Field(
        ...,
        title="Customer Code",
        description="Stable customer identifier.",
    )
    name: Annotated[str, Ops(filter=True, order=True)] = Field(
        ...,
        title="Name",
        description="Human-readable customer name.",
    )
    region: Annotated[str, Index(), Ops(filter=True)] = Field(
        ...,
        title="Region",
        description="Commercial region for the customer.",
    )


class CustomerBalanceRecord(SimpleTable):
    customer_id: Annotated[
        int,
        ForeignKey("customers", on_delete="cascade"),
        Index(),
        Ops(filter=True),
    ] = Field(
        ...,
        title="Customer Id",
        description="Foreign key to the customer table.",
    )
    as_of_date: Annotated[datetime.date, Ops(filter=True, order=True)] = Field(
        ...,
        title="As Of Date",
        description="Balance snapshot date.",
    )
    balance_usd: Annotated[float, Ops(filter=True, order=True)] = Field(
        ...,
        title="Balance USD",
        description="Customer balance in USD.",
    )
```

## Important: Do Not Declare `id`

!!! important
    `SimpleTable` rows always have a backend-managed `id`, but users must not declare that field in subclasses.

    Why:

    - the backend assigns the row id
    - the row id is not part of schema hashing
    - allowing users to declare it would create collisions and inconsistent schemas

Correct:

```python
class CustomerRecord(SimpleTable):
    customer_code: Annotated[str, Index(unique=True), Ops(filter=True, order=True)] = Field(...)
```

Wrong:

```python
class CustomerRecord(SimpleTable):
    id: int
    customer_code: Annotated[str, Index(unique=True), Ops(filter=True, order=True)] = Field(...)
```

The runtime behavior is:

- inserts do not send an `id`
- reads return rows with `id`
- later updates and deletes can use that returned `id`

So `id` is a system field available at runtime, not part of the user-authored schema DSL.

!!! warning
    A unique index helps lookup and uniqueness constraints, but it does not replace the backend-managed `id` for overwrite/upsert payloads.

    If a `SimpleTableUpdater.update()` implementation returns `(records, True)`, those records should already include backend ids.

## Why `Annotated[...]` Is Used

`Annotated[...]` keeps the field type and the schema/runtime metadata together.

This:

```python
customer_code: Annotated[str, Index(unique=True), Ops(filter=True, order=True)]
```

means:

- the value is a `str`
- it has a unique index
- it can be used in filters
- it can be used in ordering

That is the main pattern to remember.

## `Field(...)` vs `Annotated[...]`

These two pieces do different jobs.

`Annotated[...]` carries schema/runtime behavior:

- `Index(...)`
- `ForeignKey(...)`
- `Ops(...)`

`Field(...)` carries validation and documentation metadata:

- required vs optional
- title
- description

Example:

```python
customer_code: Annotated[str, Index(unique=True), Ops(filter=True, order=True)] = Field(
    ...,
    title="Customer Code",
    description="Stable customer identifier.",
)
```

## Indexes

Declare an index with `Index(...)`.

Plain index:

```python
region: Annotated[str, Index(), Ops(filter=True)] = Field(...)
```

Unique index:

```python
customer_code: Annotated[str, Index(unique=True), Ops(filter=True, order=True)] = Field(...)
```

Use indexes for fields you expect to:

- filter often
- join often
- treat as a business key

Do not add them to every column automatically.

A business key such as `customer_code` is still useful, but it is not the overwrite key for row mutations. For overwrite/upsert flows, resolve the business key back to the backend `id` first.

## `Ops(...)`

`Ops(...)` tells the system how a field participates in row operations.

Example:

```python
Ops(filter=True, order=True)
```

Available flags:

- `insert`
- `update`
- `filter`
- `order`

Meaning:

- `insert=True`: the field can be part of inserted rows
- `update=True`: the field can be changed in updates or upserts
- `filter=True`: the field can be used in filters
- `order=True`: the field can be used in ordering

If you omit `Ops(...)`, the default for normal user fields is permissive:

- insertable
- updatable
- filterable
- not orderable

## Foreign Keys

Declare a relation with `ForeignKey(...)`.

Example:

```python
customer_id: Annotated[
    int,
    ForeignKey("customers", on_delete="cascade"),
    Index(),
    Ops(filter=True),
] = Field(...)
```

This means:

- the stored value is an integer
- it refers to the updater exposed under the `"customers"` dependency key
- the backend should enforce the declared delete behavior
- the field is indexed
- the field is filterable

`ForeignKey.target` is the dependency key declared by the owning updater's
`dependencies()` method, not a `SimpleTable` class.

Example:

```python
def dependencies(self) -> dict[str, SimpleTableUpdater]:
    return {"customers": self.customers_updater}
```

The client-side schema payload keeps the resolved foreign-key contract small:

- `target`
- `on_delete`

The backend is responsible for mapping that relation to the physical table details.

## `SimpleTable` vs `SimpleTableUpdater`

The split is:

- `SimpleTable` defines the schema and row model
- `SimpleTableUpdater` owns the actual backend table, hashing, dependencies, and read/write workflow

Example:

```python
from mainsequence.tdag.simple_tables import SimpleTableUpdater


class CustomersUpdater(SimpleTableUpdater):
    SIMPLE_TABLE_SCHEMA = CustomerRecord

    def update(self) -> tuple[list[CustomerRecord], bool]:
        return (
            [
                CustomerRecord(customer_code="ACME", name="Acme Capital", region="US"),
                CustomerRecord(customer_code="BETA", name="Beta Treasury", region="EU"),
            ],
            True,
        )
```

That updater can:

- build or resolve the backend table
- insert rows returned by `update()`
- execute typed filters
- expose the resolved table identity through its storage

## Why Backend `id` Matters in Practice

Because `id` is assigned by the backend, the usual pattern is:

1. insert rows without `id`
2. read them back
3. use the returned `id` for sparse upserts or deletes

That is exactly what the example in
[`examples/data_nodes/simple_tables.py`](../../../examples/data_nodes/simple_tables.py)
does for the `CustomerRecord`, `CustomerBalanceRecord`, and `CustomerDebtRecord` tables.

## A Good Mental Model

Ask these questions when designing a `SimpleTable`:

1. What does one row represent?
2. Which fields are business lookup fields?
3. Which fields should be indexed?
4. Which fields should be filterable?
5. Which fields should be orderable?
6. Which fields point to another simple table?

That gives you a clean, row-oriented schema that can still live inside the same dependency graph system as `DataNode`.
