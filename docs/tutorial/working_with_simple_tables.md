# Working With Simple Tables

## Quick Summary

In this tutorial, you will:

- model non-time-series data with `SimpleTable`
- build a small three-table relation: customers, balances, and debts
- attach those tables to `SimpleTableUpdater` classes
- insert, read, sparse-upsert, and delete rows
- run typed filters, including join filters
- inspect the resulting backend tables from the CLI

The concrete example for this tutorial lives in
[`examples/data_nodes/simple_tables.py`](../../examples/data_nodes/simple_tables.py).

Before going further, it is important to frame the feature correctly.

As the name suggests, `SimpleTable` is designed to create small ORM-like tables inside a project, with a heavily reduced database configuration compared with a full application ORM. The spirit of MainSequence is fast iteration inside a normalized working environment: define a schema, attach it to an updater, persist rows, and query them back without having to design a full database layer.

That makes `SimpleTable` a good fit for:

- reference data
- small relational record sets inside a project
- application-facing tables that support lightweight insert, read, update, and delete workflows

It is not meant to replace a full ORM or a full database application model. If you need more complex transactional behavior, richer relational modeling, custom migration workflows, or a broader application database layer, you should not rely on `SimpleTable` for that.

## 1. When To Use `SimpleTable`

Use `SimpleTable` when your data is row-oriented but not naturally normalized as a time series.

Good examples:

- customers
- counterparties
- portfolios
- mapping tables
- balance snapshots where time is just another column

If your data is fundamentally organized around `time_index` and `unique_identifier`, you usually want a `DataNode` table instead.

`SimpleTable` still fits into the same graph-oriented MainSequence workflow:

- it has a schema
- it has an updater
- it has `update_hash` and `storage_hash`
- it can depend on other updaters

## 2. Important: `id` Is Backend-Managed

!!! important
    Every simple-table row has an `id`, but you must **not** declare that field yourself.

    Why:

    - the backend assigns it
    - it must not participate in schema hashing
    - allowing users to define it would create collisions

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

The normal lifecycle is:

1. insert rows without `id`
2. read them back
3. use the returned `id` for sparse upserts or deletes

This is especially important for foreign-key workflows. Downstream rows usually need parent ids that were assigned by the backend during earlier inserts.

## 3. Define the Table Schemas

The tutorial example uses three tables:

- `CustomerRecord`
- `CustomerBalanceRecord`
- `CustomerDebtRecord`

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
        ForeignKey(CustomerRecord, on_delete="cascade"),
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


class CustomerDebtRecord(SimpleTable):
    balance_id: Annotated[
        int,
        ForeignKey(CustomerBalanceRecord, on_delete="cascade"),
        Index(),
        Ops(filter=True),
    ] = Field(
        ...,
        title="Balance Id",
        description="Foreign key to the balance snapshot row.",
    )
    debt_type: Annotated[str, Ops(filter=True, order=True)] = Field(
        ...,
        title="Debt Type",
        description="Debt category associated with the balance row.",
    )
    debt_usd: Annotated[float, Ops(filter=True, order=True)] = Field(
        ...,
        title="Debt USD",
        description="Debt amount in USD.",
    )
```

What each schema feature is doing:

- `Annotated[...]` keeps the field type and metadata together
- `Index(...)` declares useful read paths
- `Ops(...)` declares which fields are filterable or orderable
- `ForeignKey(...)` declares the table-to-table relation

For a deeper explanation of the schema DSL, see
[`docs/knowledge/simple_tables/simple_table.md`](../knowledge/simple_tables/simple_table.md).

## 4. Attach Each Table To an Updater

`SimpleTable` defines the schema. `SimpleTableUpdater` owns the real backend table and the actual read/write workflow.

The customer updater is the simplest case:

```python
class CustomersUpdater(SimpleTableUpdater):
    SIMPLE_TABLE_SCHEMA = CustomerRecord

    @staticmethod
    def build_seed_rows() -> list[dict[str, object]]:
        return [
            {"customer_code": "ACME", "name": "Acme Capital", "region": "US"},
            {"customer_code": "BETA", "name": "Beta Treasury", "region": "EU"},
        ]

    def update(self) -> tuple[list[CustomerRecord], bool]:
        return (
            [CustomerRecord.model_validate(row) for row in self.build_seed_rows()],
            True,
        )
```

The balance updater depends on the customer updater:

```python
class CustomerBalancesUpdater(SimpleTableUpdater):
    SIMPLE_TABLE_SCHEMA = CustomerBalanceRecord

    def __init__(
        self,
        configuration: CustomerBalancesUpdaterConfiguration,
        *,
        customers_updater: CustomersUpdater | None = None,
        hash_namespace: str | None = None,
        test_node: bool = False,
    ):
        self.customers_updater = customers_updater or CustomersUpdater(
            configuration=CustomersUpdaterConfiguration()
        )
        super().__init__(
            configuration=configuration,
            hash_namespace=hash_namespace,
            test_node=test_node,
        )
```

Its `update()` method first reads customer rows back from the backend, because those rows now carry the backend-assigned `id` values needed for `customer_id`.

That is the key architectural shift relative to the old user-defined-id model:

- parent rows are inserted first
- parent rows are queried back
- downstream tables use the returned ids as foreign keys

The debt updater follows the same pattern, but one step lower:

- it queries customers to map `customer_code -> customer.id`
- it queries balances to map `(customer_id, as_of_date) -> balance.id`
- then it builds `CustomerDebtRecord` rows with `balance_id`

## 5. Build the Remote Tables

The example creates the three updaters and builds their remote objects:

```python
customers_updater = CustomersUpdater(configuration=CustomersUpdaterConfiguration())
balances_updater = CustomerBalancesUpdater(
    configuration=CustomerBalancesUpdaterConfiguration(),
    customers_updater=customers_updater,
)
debts_updater = CustomerDebtsUpdater(
    configuration=CustomerDebtsUpdaterConfiguration(),
    balances_updater=balances_updater,
)

customers_updater.verify_and_build_remote_objects()
balances_updater.verify_and_build_remote_objects()
debts_updater.verify_and_build_remote_objects()
```

At that point each updater has:

- its own `update_hash`
- its own `storage_hash`
- a resolved backend storage object

The example prints all three so you can see the distinction clearly.

## 6. Insert Rows

The example inserts customer rows first:

```python
for record in customer_seed:
    customers_updater.insert(record)
```

Then it reads them back:

```python
inserted_customers = customers_updater.execute_filter(
    CustomerRecord.filters.customer_code.in_(customer_codes)
)
```

Those returned rows now include backend ids. The example builds:

```python
customer_id_by_code = {
    record.customer_code: record.id
    for record in inserted_customers
    if record.id is not None
}
```

and uses that mapping to create the balance rows.

This is the intended pattern whenever downstream rows depend on backend-assigned ids.

## 7. Read Rows Back With Typed Filters

Simple-table filters are built from the table class and executed through the updater.

Example:

```python
us_customers = customers_updater.execute_filter(
    CustomerRecord.filters.region.eq("US")
)
```

More specific reads can combine conditions:

```python
from mainsequence.tdag.simple_tables import and_

balance_rows = balances_updater.execute_filter(
    and_(
        CustomerBalanceRecord.filters.customer_id.in_(list(customer_id_by_code.values())),
        CustomerBalanceRecord.filters.as_of_date.eq(datetime.date(2026, 3, 22)),
    )
)
```

For a deeper filtering guide, see
[`docs/knowledge/simple_tables/filtering.md`](../knowledge/simple_tables/filtering.md).

## 8. Sparse Upsert Existing Rows

After the balance rows are read back, the example uses the returned backend ids for a sparse upsert:

```python
SimpleTableStorage.upsert_records_into_table(
    simple_table_id=balances_updater.data_node_storage.id,
    records=[
        {"id": inserted_balance_by_customer_id[customer_id_by_code["ACME"]].id, "balance_usd": 150_000.0},
        {"id": inserted_balance_by_customer_id[customer_id_by_code["BETA"]].id, "balance_usd": 91_250.0},
    ],
)
```

This is exactly why backend-managed ids are workable:

- insert first
- read back ids
- upsert by id later

In other words, you do not need a user-declared primary key in the schema to mutate rows after they exist.

## 9. Delete Rows

Delete also uses the backend id returned by earlier reads:

```python
for record in inserted_debts:
    debts_updater.delete(record.id)
```

The example then re-queries the table and asserts that the demo rows are gone.

That same pattern is repeated for:

- debt rows
- balance rows
- customer rows

## 10. Build Join Filters

Single-table filters are enough for many workflows, but the example also shows how to build join filters.

First resolve the runtime tables:

```python
resolved_balances = balances_updater.resolve_table()
resolved_customers = customers_updater.resolve_table()
resolved_debts = debts_updater.resolve_table()
```

Then create join aliases:

```python
customer_join = resolved_balances.join("customer", target=resolved_customers)
debt_join = resolved_balances.join("debt", target=resolved_debts)
```

Then build a combined filter:

```python
from mainsequence.tdag.simple_tables import and_, or_

complex_filter = and_(
    CustomerBalanceRecord.filters.as_of_date.between(
        datetime.date(2026, 3, 1),
        datetime.date(2026, 3, 31),
    ),
    or_(
        customer_join.filters.region.eq("US"),
        debt_join.filters.debt_type.eq("margin"),
    ),
    or_(
        debt_join.filters.debt_usd.gte(10_000.0),
        CustomerBalanceRecord.filters.balance_usd.gte(140_000.0),
    ),
)

rows = balances_updater.execute_filter(
    complex_filter,
    joins=[customer_join, debt_join],
)
```

Important details:

- `customer` and `debt` are aliases
- the updater still executes the read
- the returned rows are still `CustomerBalanceRecord` rows
- joins are used to constrain which base rows match

## 11. Run the End-to-End Example

The example file exposes:

```python
build_test_simple_tables()
```

That helper does all of the following in one repeatable run:

- builds the three remote tables
- clears previous demo rows
- inserts customers, balances, and debts
- reads them back and verifies the inserted data
- performs a sparse upsert on balances
- reads the balances back and verifies the changes
- deletes the demo rows
- reads again and verifies that the rows are gone
- prints example request payloads for typed filters and join filters

If your `.env` is configured for a live backend, you can run:

```bash
.venv/bin/python examples/data_nodes/simple_tables.py
```

## 12. Inspect the Tables From the CLI

The CLI gives you storage-level visibility into the resulting simple tables.

List simple-table storages:

```bash
mainsequence simple_table list
```

Inspect one storage:

```bash
mainsequence simple_table detail 41
```

Delete one storage:

```bash
mainsequence simple_table delete 41
```

These commands operate on `SimpleTableStorage`, not on individual row ids.

They are useful for:

- inspecting the compiled schema
- reviewing column and relation metadata
- checking the build configuration used for the storage

## 13. How `SimpleTable` Fits With `DataNode`

`SimpleTable` is not a separate world. It is a different table model inside the same dependency system.

That means you can:

- use simple tables for master/reference data
- use data nodes for time-series analytics
- wire them into the same application or project workflow

Typical pattern:

- simple tables hold stable entities and relationships
- data nodes compute normalized analytical outputs that depend on them

Or the reverse:

- a data pipeline computes something
- a simple table exposes the application-facing records built from that output

## 14. Next Steps

After this tutorial, the most useful follow-ups are:

- read the schema guide in
  [`docs/knowledge/simple_tables/simple_table.md`](../knowledge/simple_tables/simple_table.md)
- read the filtering guide in
  [`docs/knowledge/simple_tables/filtering.md`](../knowledge/simple_tables/filtering.md)
- run the example in
  [`examples/data_nodes/simple_tables.py`](../../examples/data_nodes/simple_tables.py)
- inspect the created storages with `mainsequence simple_table list` and `detail`
