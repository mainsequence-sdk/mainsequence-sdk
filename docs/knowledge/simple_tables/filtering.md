# Filtering `SimpleTable` Data

For `SimpleTable`, the user-facing rule is:

- the table class builds the filter expression
- the updater executes it

So your normal flow is:

```python
customer_filter = CustomerRecord.filters.region.eq("US")
rows = customers_updater.execute_filter(customer_filter)
```

You do not need to pass `storage_hash` manually. The updater already knows which backend table it owns.

## Important: `id` Is Runtime-Available, Not User-Declared

!!! important
    Do not declare `id` in your `SimpleTable` subclasses.

    But after rows are stored and read back, `id` is still available as a system field on returned records. That means you can filter on it at runtime:

    ```python
    CustomerRecord.filters.id.in_([100, 101])
    ```

    That is valid because `id` exists on real rows returned by the backend, even though it is not part of the schema you author.

## Basic Pattern

### 1. Build the filter expression

```python
customer_filter = CustomerRecord.filters.region.eq("US")
```

### 2. Execute it through the updater

```python
rows = customers_updater.execute_filter(customer_filter)
```

The result is a typed list of row models:

```python
list[CustomerRecord]
```

## Supported Operators

The common operators are:

- `eq`
- `ne`
- `lt`
- `lte`
- `gt`
- `gte`
- `in_`
- `not_in`
- `contains`
- `starts_with`
- `ends_with`
- `is_null`
- `between`

Examples:

```python
CustomerRecord.filters.region.eq("US")
CustomerRecord.filters.customer_code.starts_with("AC")
CustomerRecord.filters.id.in_([100, 101])

CustomerBalanceRecord.filters.balance_usd.gte(100_000.0)
CustomerBalanceRecord.filters.as_of_date.between(
    datetime.date(2026, 3, 1),
    datetime.date(2026, 3, 31),
)
```

## Combining Filters

Use `and_(...)` and `or_(...)` to compose more complex logic.

```python
from mainsequence.tdag.simple_tables import and_, or_

customer_filter = and_(
    or_(
        CustomerRecord.filters.region.eq("US"),
        CustomerRecord.filters.region.eq("EU"),
    ),
    CustomerRecord.filters.customer_code.starts_with("A"),
)

rows = customers_updater.execute_filter(customer_filter)
```

## Filtering on Foreign-Key Fields

If a table stores a foreign-key id, you can filter on that id directly.

Example:

```python
balance_filter = and_(
    CustomerBalanceRecord.filters.customer_id.in_([100, 101]),
    CustomerBalanceRecord.filters.as_of_date.eq(datetime.date(2026, 3, 22)),
)

rows = balances_updater.execute_filter(balance_filter)
```

This is usually the first thing you do after reading parent rows and collecting their backend ids.

## Join Filters

Simple-table joins are also expressed through filters, but the updater still executes them.

The key pieces are:

- `resolve_table()` binds the runtime table identity to the schema
- `join(...)` creates a join alias
- joined fields are referenced as `alias.field`

Example with the tutorial tables:

```python
from mainsequence.tdag.simple_tables import and_, or_

resolved_balances = balances_updater.resolve_table()
resolved_customers = customers_updater.resolve_table()
resolved_debts = debts_updater.resolve_table()

customer_join = resolved_balances.join("customer", target=resolved_customers)
debt_join = resolved_balances.join("debt", target=resolved_debts)

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
- the updater resolves the actual backend table identity through its storage
- the returned rows are still `CustomerBalanceRecord` rows
- joins affect which base rows match the filter

## When To Use `resolve_table()`

For simple single-table reads, you usually do not need it.

This is enough:

```python
rows = customers_updater.execute_filter(
    CustomerRecord.filters.region.eq("US")
)
```

You normally use `resolve_table()` when:

- you need join handles
- you want to inspect the request payload
- you want to build alias-qualified filters

## Order Keys

If a field was declared with `Ops(order=True)`, you can build validated order keys:

```python
CustomerRecord.filters.customer_code.order_key()
CustomerRecord.filters.name.order_key(descending=True)
```

That is useful when you want ordering metadata derived from the same schema surface as the filters.

## Good User Mental Model

Think in two layers:

1. `SimpleTable` describes the rows you want
2. `SimpleTableUpdater` knows which real backend table to query

That is why this is the right user flow:

```python
filter_expr = CustomerRecord.filters.region.eq("US")
rows = customers_updater.execute_filter(filter_expr)
```

and not a lower-level request where users manually pass backend table identity.
