# What Is a `SimpleTable`?

`SimpleTable` is the table model you use when your data is **not naturally a time series**.

That is the main idea.

If your data looks like:

- customers
- accounts
- products
- reference lists
- mappings
- balances where time is just one field among others

then `SimpleTable` is usually the right fit.

If your data is fundamentally built around:

- `time_index`
- `unique_identifier`
- normalized time-series storage

then you are usually in `DataNode` territory instead.

## The Concept

A `SimpleTable` is a **schema declaration**.

It defines:

- what columns exist
- what their Python types are
- which fields are indexed
- which fields are filterable
- which fields participate in ordering
- which fields point to another `SimpleTable`

Here is a small example:

```python
class CustomerRecord(SimpleTable):
    id: int
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
```

This class is doing two jobs at once:

1. it defines the shape of one row
2. it defines the schema of the backend table

## Why `SimpleTable` Exists

Time-series storage is optimized around a very specific model:

- one time axis
- one identity axis
- append or overwrite patterns that follow that model

But many useful tables do not fit that shape.

Examples:

- one row per customer
- one row per legal entity
- one row per portfolio
- one row per account relationship
- one row per snapshot record keyed by a business id

For these cases, `SimpleTable` gives you a lighter model:

- plain columns
- primary key
- optional foreign keys
- optional indexes
- filtering and querying support
- row-oriented insert / upsert / delete flows

## The Required `id` Field

In current usage, a `SimpleTable` normally has an `id` field:

```python
class CustomerRecord(SimpleTable):
    id: int
```

This acts as the primary key in the schema layer.

You usually do not need to annotate `id` with extra metadata for the common case.

The system already treats it specially:

- it is the primary key
- it is filterable
- it is orderable
- it is not meant to be updated like a normal mutable field

## Why `Annotated[...]` Is Used

Python’s `Annotated[...]` lets you attach schema metadata to a type.

Example:

```python
customer_code: Annotated[str, Index(unique=True), Ops(filter=True, order=True)]
```

The base type is still:

```python
str
```

But now it also carries extra declarations:

- `Index(unique=True)`
- `Ops(filter=True, order=True)`

This is useful because it keeps the field definition compact and readable:

- the type says what kind of value the field holds
- the annotations say how the system should treat that field

## Reading `Annotated[...]` in Plain English

Take this field:

```python
customer_code: Annotated[str, Index(unique=True), Ops(filter=True, order=True)]
```

You can read it like this:

- the value is a string
- it should have a unique index in the backend
- it can be used in filters
- it can be used in ordering

That is the pattern to keep in mind.

## `Field(...)` Is Still Important

You will usually combine `Annotated[...]` with `pydantic.Field(...)`:

```python
customer_code: Annotated[str, Index(unique=True), Ops(filter=True, order=True)] = Field(
    ...,
    title="Customer Code",
    description="Stable customer identifier.",
)
```

The two parts do different jobs:

- `Annotated[...]` carries schema/runtime metadata
- `Field(...)` carries validation/documentation metadata

Typical `Field(...)` usage:

- `...` means the field is required
- `title=...` gives a human-readable label
- `description=...` documents the field

## Indexes

Indexes are declared with `Index(...)`.

### Plain index

```python
region: Annotated[str, Index(), Ops(filter=True)] = Field(...)
```

This means:

- create a backend index for the column
- it is useful for filtering or lookup workloads
- uniqueness is **not** enforced

### Unique index

```python
customer_code: Annotated[str, Index(unique=True), Ops(filter=True, order=True)] = Field(...)
```

This means:

- create an index
- enforce uniqueness

In practice, a unique index is a good fit for business keys like:

- customer code
- account number
- external identifier

### Named index

You can also provide a name:

```python
region: Annotated[str, Index(name="idx_customer_region"), Ops(filter=True)] = Field(...)
```

This is optional. If you do not give a name, the backend can generate one.

## What an Index Means

An index is not just “extra metadata”.

It is a statement about how the field will be used.

Use an index when the field is likely to be:

- filtered often
- joined often
- used as a lookup key
- expected to be unique

Do **not** add indexes to every field automatically.

Indexes make reads faster for the indexed access pattern, but they also add maintenance cost on writes.

Good examples:

- `customer_code`
- `region`
- foreign-key fields like `customer_id`

Less obvious examples:

- free-text description fields
- rarely filtered columns

## `Ops(...)`: What the Field Is Allowed To Do

`Ops(...)` describes how the field participates in record operations.

Example:

```python
Ops(filter=True, order=True)
```

Available flags are:

- `insert`
- `update`
- `filter`
- `order`

### Meaning of each flag

- `insert=True`
  - field can be part of inserted data
- `update=True`
  - field can be changed in updates/upserts
- `filter=True`
  - field can be used in query filters
- `order=True`
  - field can be used for ordering

### Example

```python
name: Annotated[str, Ops(filter=True, order=True)] = Field(...)
```

This means:

- it is a normal data column
- users can filter on it
- users can sort on it

### If `Ops(...)` is omitted

For normal fields, the default behavior is permissive:

- insertable
- updatable
- filterable
- not orderable

For `id`, the system applies a special default appropriate for a primary key.

## Foreign Keys

A foreign key is declared with `ForeignKey(...)`.

Example:

```python
customer_id: Annotated[
    int,
    ForeignKey(CustomerRecord, on_delete="cascade"),
    Index(),
    Ops(filter=True),
] = Field(...)
```

This means:

- the column stores an integer id
- that id points to `CustomerRecord`
- if the referenced customer is deleted, the delete policy is `"cascade"`
- the field is indexed
- the field can be filtered

### Why the field type is still `int`

The stored value is still the referenced row id:

```python
customer_id: int
```

The `ForeignKey(...)` metadata explains what that integer means.

### Common delete policies

Current supported values are:

- `"cascade"`
- `"restrict"`
- `"set_null"`

Use them the same way you would think about relational database foreign keys.

## Example: A Parent / Child Relationship

```python
class CustomerRecord(SimpleTable):
    id: int
    customer_code: Annotated[str, Index(unique=True), Ops(filter=True, order=True)] = Field(...)
    name: Annotated[str, Ops(filter=True, order=True)] = Field(...)
    region: Annotated[str, Index(), Ops(filter=True)] = Field(...)


class CustomerBalanceRecord(SimpleTable):
    id: int
    customer_id: Annotated[
        int,
        ForeignKey(CustomerRecord, on_delete="cascade"),
        Index(),
        Ops(filter=True),
    ] = Field(...)
    as_of_date: Annotated[datetime.date, Ops(filter=True, order=True)] = Field(...)
    balance_usd: Annotated[float, Ops(filter=True, order=True)] = Field(...)
```

Read this schema as:

- `CustomerRecord` is the parent table
- `CustomerBalanceRecord` depends on it
- each balance row points to one customer
- balances can be filtered by customer
- balances can be filtered or ordered by date and amount

## How a `SimpleTable` Gets Data

A `SimpleTable` only defines the schema.

To populate it in a pipeline, you normally pair it with a `SimpleTableUpdater`.

Example:

```python
class CustomersUpdater(SimpleTableUpdater):
    SIMPLE_TABLE_SCHEMA = CustomerRecord

    def update(self) -> tuple[list[CustomerRecord], bool]:
        return (
            [
                CustomerRecord(id=100, customer_code="ACME", name="Acme Capital", region="US"),
                CustomerRecord(id=101, customer_code="BETA", name="Beta Treasury", region="EU"),
            ],
            True,
        )
```

That means:

- the updater owns the table lifecycle
- `CustomerRecord` defines the schema
- `update()` returns rows to insert or upsert

So the conceptual split is:

- `SimpleTable` = what the table looks like
- `SimpleTableUpdater` = how rows get created, changed, or deleted

## Validation Behavior

Because `SimpleTable` is a Pydantic model, rows are validated as normal model instances.

Example:

```python
record = CustomerRecord(
    id=100,
    customer_code="ACME",
    name="Acme Capital",
    region="US",
)
```

If a required field is missing, or a type is wrong, validation fails before the row is sent.

This is useful because:

- your row schema is explicit
- backend writes are cleaner
- code that creates rows is easier to trust

## A Good Way To Think About Design

When designing a `SimpleTable`, ask:

### 1. What is one row?

Examples:

- one customer
- one balance snapshot
- one account relationship

### 2. What is the primary key?

In the common pattern, that is `id`.

### 3. Which fields are business lookup keys?

Those are often good candidates for:

- `Index(unique=True)`
- `Ops(filter=True)`

### 4. Which fields should be filterable?

Typical examples:

- region
- customer_id
- as_of_date
- balance_usd

### 5. Which fields should be orderable?

Typical examples:

- names
- dates
- numeric values used in reports

### 6. Which fields reference another table?

Those should usually be:

- typed as `int`
- annotated with `ForeignKey(...)`
- indexed

## Practical Example

This is a realistic small schema:

```python
class CustomerRecord(SimpleTable):
    id: int
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
```

Why this design makes sense:

- `id`: primary key
- `customer_code`: unique business identifier
- `name`: readable field used in filters and sorting
- `region`: indexed filter field

## Common Mistakes

### Putting metadata in `Field(...)` instead of `Annotated[...]`

Use:

```python
Annotated[str, Index(), Ops(filter=True)]
```

for schema/runtime behavior.

Use:

```python
Field(title="...", description="...")
```

for validation/documentation metadata.

### Forgetting to index a foreign key

This is usually a bad idea:

```python
customer_id: Annotated[int, ForeignKey(CustomerRecord)] = Field(...)
```

This is usually better:

```python
customer_id: Annotated[int, ForeignKey(CustomerRecord), Index(), Ops(filter=True)] = Field(...)
```

### Marking everything as indexed

Only add indexes where they match real query patterns.

### Forgetting `Ops(filter=True)` on fields you plan to query

If a field is not declared filterable, the filter DSL will reject it.

## Related Reading

After understanding `SimpleTable` itself, the next useful page is:

- [`filtering.md`](/Users/jose/code/MainSequenceClientSide/mainsequence-sdk/docs/knowledge/simple_tables/filtering.md)

That page explains how to query `SimpleTable` data with field-based filters, boolean combinations, and request builders.
