# Filtering `SimpleTable` Data

This page explains the **user-facing filtering model** for `SimpleTable`.

The important idea is:

- `SimpleTable` builds the filter
- `SimpleTableUpdater` executes the filter

So as a user, you should think in two steps:

1. describe **which rows you want**
2. give that filter to the updater that owns the real backend table

## The Main Mental Model

When you write:

```python
CustomerRecord.filters.region.eq("US")
CustomerRecord.filters.customer_code.starts_with("AC")
CustomerRecord.filters.id.in_([100, 101])
```

you are **not querying the backend yet**.

You are building a **filter expression object**.

That filter expression is then passed to:

```python
SimpleTableUpdater.execute_filter(...)
```

Why?

Because the updater is the object that knows:

- which actual backend table is being used
- what the `storage_hash` is
- how to connect to the backend
- how to turn backend rows back into `SimpleTable` instances

The `SimpleTable` class defines the language of the filter.
The updater is the execution interface.

## The Correct User Flow

### Step 1: build a filter expression

```python
customer_filter = CustomerRecord.filters.region.eq("US")
```

### Step 2: execute it through the updater

```python
customers_updater.execute_filter(customer_filter)
```

That is the right conceptual flow.

You should **not** have to manually pass:

- `storage_hash`
- backend table identifiers
- raw filter payloads

Those are updater concerns.

## What `CustomerRecord.filters...` Returns

This line:

```python
CustomerRecord.filters.id.in_([100, 101])
```

returns a filter expression object.

More specifically:

- a single field operator like `.eq(...)` or `.in_(...)` returns a `Condition`
- combining expressions with `and_(...)` or `or_(...)` returns a grouped expression

For the user, the most important takeaway is:

- these are structured filter objects
- they are not backend calls
- they are safe to compose before execution

## Why The Updater Must Execute The Filter

The same `SimpleTable` class can exist in multiple runtime contexts.

For example:

- different updaters
- different namespaces
- different data sources
- different backend storages

So this expression:

```python
CustomerRecord.filters.region.eq("US")
```

does **not** say which concrete backend table should be queried.

It only says:

- “I want rows where `region == "US"`”

The updater resolves the missing part:

- “which actual table do you mean?”

That is why `execute_filter(...)` belongs on `SimpleTableUpdater`.

## Basic Field Filters

Assume this table:

```python
class CustomerRecord(SimpleTable):
    id: int
    customer_code: Annotated[str, Index(unique=True), Ops(filter=True, order=True)] = Field(...)
    name: Annotated[str, Ops(filter=True, order=True)] = Field(...)
    region: Annotated[str, Index(), Ops(filter=True)] = Field(...)
```

Then you can build filters like these.

### Equality

```python
CustomerRecord.filters.region.eq("US")
CustomerRecord.filters.id.eq(100)
```

### Inequality

```python
CustomerRecord.filters.region.ne("EU")
```

### Membership

```python
CustomerRecord.filters.id.in_([100, 101, 102])
CustomerRecord.filters.region.not_in(["APAC", "LATAM"])
```

### Text filters

```python
CustomerRecord.filters.customer_code.contains("AC")
CustomerRecord.filters.customer_code.starts_with("AC")
CustomerRecord.filters.name.ends_with("Treasury")
```

### Null checks

```python
CustomerRecord.filters.region.is_null()
CustomerRecord.filters.region.is_null(False)
```

## Numeric and Date Filters

Assume:

```python
class CustomerBalanceRecord(SimpleTable):
    id: int
    customer_id: Annotated[int, ForeignKey(CustomerRecord, on_delete="cascade"), Index(), Ops(filter=True)] = Field(...)
    as_of_date: Annotated[datetime.date, Ops(filter=True, order=True)] = Field(...)
    balance_usd: Annotated[float, Ops(filter=True, order=True)] = Field(...)
```

Then you can write:

### Comparisons

```python
CustomerBalanceRecord.filters.balance_usd.gt(100_000.0)
CustomerBalanceRecord.filters.balance_usd.gte(100_000.0)
CustomerBalanceRecord.filters.balance_usd.lt(250_000.0)
CustomerBalanceRecord.filters.balance_usd.lte(250_000.0)
```

### Ranges

```python
CustomerBalanceRecord.filters.balance_usd.between(50_000.0, 200_000.0)

CustomerBalanceRecord.filters.as_of_date.between(
    datetime.date(2026, 3, 1),
    datetime.date(2026, 3, 31),
)
```

### Foreign-key filters

```python
CustomerBalanceRecord.filters.customer_id.eq(100)
CustomerBalanceRecord.filters.customer_id.in_([100, 101])
```

## Combining Filters

Real reads often need multiple conditions.

Use:

- `and_(...)`
- `or_(...)`

### AND

```python
from mainsequence.tdag.simple_tables import and_

customer_filter = and_(
    CustomerRecord.filters.region.eq("US"),
    CustomerRecord.filters.customer_code.starts_with("A"),
)
```

Then execute:

```python
customers_updater.execute_filter(customer_filter)
```

### OR

```python
from mainsequence.tdag.simple_tables import or_

customer_filter = or_(
    CustomerRecord.filters.region.eq("US"),
    CustomerRecord.filters.region.eq("EU"),
)
```

Then execute:

```python
customers_updater.execute_filter(customer_filter)
```

### Nested logic

```python
from mainsequence.tdag.simple_tables import and_, or_

balance_filter = and_(
    CustomerBalanceRecord.filters.as_of_date.eq(datetime.date(2026, 3, 22)),
    or_(
        CustomerBalanceRecord.filters.balance_usd.gte(100_000.0),
        CustomerBalanceRecord.filters.customer_id.in_([100, 101]),
    ),
)
```

Then execute:

```python
balances_updater.execute_filter(balance_filter)
```

## What The Updater Should Return

The natural result of:

```python
customers_updater.execute_filter(customer_filter)
```

should be:

```python
list[CustomerRecord]
```

And for balances:

```python
list[CustomerBalanceRecord]
```

That keeps the whole flow typed and user-friendly:

- you build the filter with the schema class
- you get rows back as schema instances

## Real Examples

### Example: find US customers

```python
customer_filter = CustomerRecord.filters.region.eq("US")
rows = customers_updater.execute_filter(customer_filter)
```

### Example: find customers by ids

```python
customer_filter = CustomerRecord.filters.id.in_([100, 101])
rows = customers_updater.execute_filter(customer_filter)
```

### Example: find balances above a threshold

```python
balance_filter = CustomerBalanceRecord.filters.balance_usd.gte(100_000.0)
rows = balances_updater.execute_filter(balance_filter)
```

### Example: find balances for one date and a customer set

```python
from mainsequence.tdag.simple_tables import and_

balance_filter = and_(
    CustomerBalanceRecord.filters.as_of_date.eq(datetime.date(2026, 3, 22)),
    CustomerBalanceRecord.filters.customer_id.in_([100, 101]),
)

rows = balances_updater.execute_filter(balance_filter)
```

### Example: nested business logic

This example means:

- date is in March 2026
- and either the customer is in a selected set
- or the balance is large
- and the balance is not zero

```python
from mainsequence.tdag.simple_tables import and_, or_

balance_filter = and_(
    CustomerBalanceRecord.filters.as_of_date.between(
        datetime.date(2026, 3, 1),
        datetime.date(2026, 3, 31),
    ),
    or_(
        CustomerBalanceRecord.filters.customer_id.in_([100, 101, 102]),
        CustomerBalanceRecord.filters.balance_usd.gte(250_000.0),
    ),
    CustomerBalanceRecord.filters.balance_usd.not_in([0.0]),
)

rows = balances_updater.execute_filter(balance_filter)
```

## Why This Is Better Than Passing `storage_hash`

You could imagine a lower-level API where you manually build a request and pass `storage_hash`.

But that is not the right primary user model.

Why:

- users should not need to know backend table identity
- the updater already knows its storage
- the updater already knows its connection context
- the updater is the right place to translate filter expressions into backend reads

So the better API is:

```python
filter_expr = CustomerRecord.filters.region.eq("US")
rows = customers_updater.execute_filter(filter_expr)
```

not:

```python
# lower-level, not the main user flow
...
```

## Filterability Rules

You can only filter on fields that were declared filterable with `Ops(filter=True)`.

Example:

```python
region: Annotated[str, Index(), Ops(filter=True)] = Field(...)
```

If a field is not filterable, trying to build a filter on it should raise an error.

That is useful because it prevents unsupported filters from being silently accepted.

## Dates and Datetimes

You can use normal Python values in filter expressions:

```python
CustomerBalanceRecord.filters.as_of_date.eq(datetime.date(2026, 3, 22))
```

The filter object can later be serialized safely by the execution layer.

So you write normal Python code; the updater/backend layer handles the transport format.

## Ordering

Some fields can also be marked with `Ops(order=True)`.

That supports building validated order keys such as:

```python
CustomerRecord.filters.customer_code.order_key()
CustomerRecord.filters.name.order_key(descending=True)
```

But the core filtering story is still:

- build filter expressions on `SimpleTable`
- execute them through `SimpleTableUpdater`

So filtering is the main thing to focus on first.

## A Good User Style

A good pattern is:

```python
from mainsequence.tdag.simple_tables import and_

balance_filter = and_(
    CustomerBalanceRecord.filters.customer_id.in_([100, 101]),
    CustomerBalanceRecord.filters.as_of_date.eq(datetime.date(2026, 3, 22)),
    CustomerBalanceRecord.filters.balance_usd.gte(100_000.0),
)

rows = balances_updater.execute_filter(balance_filter)
```

This keeps the code easy to read:

- the schema defines the fields
- the filter describes the rows
- the updater performs the read

## Summary

For `SimpleTable` filtering, the correct user-facing model is:

1. build a filter expression from the table class
2. pass that expression to `SimpleTableUpdater.execute_filter(...)`
3. let the updater resolve the real backend table and return typed rows

So these lines:

```python
CustomerRecord.filters.region.eq("US")
CustomerRecord.filters.customer_code.starts_with("AC")
CustomerRecord.filters.id.in_([100, 101])
```

should be understood as:

- declarative filter expressions
- not backend calls by themselves
- inputs to `SimpleTableUpdater.execute_filter(...)`
