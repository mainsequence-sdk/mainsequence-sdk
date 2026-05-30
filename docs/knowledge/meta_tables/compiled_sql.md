# Compiled SQL Execution

MetaTables let applications keep SQLAlchemy/Core ergonomics while TS Manager
handles authentication, authorization, routing, limits, and execution through a
registered data source.

The protocol is `compiled-sql.v1`.

That version string is a TS Manager wire-contract version. It is not a
SQLAlchemy version. The SDK sets it automatically and validates it with
Pydantic before sending the operation.

## Why Compile On The Client

Your app should build queries with its normal tools:

```python
stmt = (
    select(Asset.uid, Asset.symbol)
    .join(Account, Asset.account_uid == Account.uid)
    .where(Account.uid == account_uid)
    .where(Asset.symbol.ilike("%BTC%"))
)
```

The SDK compiles the SQLAlchemy/Core statement into plain SQL plus bound
parameters:

```json
{
  "operation": "select",
  "version": "compiled-sql.v1",
  "dialect": "postgresql",
  "statement": {
    "sql": "SELECT asset.uid, asset.symbol FROM public.asset AS asset JOIN public.account AS account ON asset.account_uid = account.uid WHERE account.uid = %(account_uid)s",
    "parameters": {
      "account_uid": "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
    },
    "paramstyle": "pyformat"
  },
  "scope": {
    "tables": [
      {
        "meta_table_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "alias": "asset",
        "access": "read"
      },
      {
        "meta_table_uid": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        "alias": "account",
        "access": "read"
      }
    ]
  }
}
```

TS Manager does not import your SQLAlchemy models and does not interpret ORM
relationship paths. It receives the compiled artifact and the declared table
scope.

## Build A Payload Manually

Use this when you already have SQL text and parameters:

```python
from mainsequence.meta_tables.compiled_sql.v1 import build_operation


operation = build_operation(
    operation="select",
    sql="SELECT asset.uid, asset.symbol FROM public.asset AS asset WHERE asset.symbol ILIKE %(symbol_1)s",
    parameters={"symbol_1": "%BTC%"},
    scope={
        "tables": [
            {
                "metaTableUid": asset_meta_table.uid,
                "alias": "asset",
                "access": "read",
            }
        ]
    },
    limits={"max_rows": 1000, "statement_timeout_ms": 15000},
)
```

The return value is a `MetaTableCompiledSQLOperation` Pydantic object.

The SDK validates:

- `version == "compiled-sql.v1"`
- `dialect == "postgresql"`
- `paramstyle == "pyformat"`
- non-empty SQL
- non-empty declared scope
- positive limits when supplied
- operation kind: `select`, `insert`, `update`, `delete`, or `upsert`

The SDK accepts `metaTableUid` and `meta_table_uid` when constructing Pydantic
objects. When `MetaTable.execute_operation(...)` sends the payload, it serializes
the field as `meta_table_uid`, which is the TS Manager server contract.

## Compile A SQLAlchemy Statement

Use `compile_sqlalchemy_statement(...)` when your app has SQLAlchemy installed.
The SDK imports SQLAlchemy lazily only when this helper is called.

```python
from sqlalchemy import select

from mainsequence.meta_tables.compiled_sql.v1 import compile_sqlalchemy_statement


stmt = (
    select(Asset.uid, Asset.symbol)
    .join(Account, Asset.account_uid == Account.uid)
    .where(Account.uid == account_uid)
    .where(Asset.symbol.ilike("%BTC%"))
)

operation = compile_sqlalchemy_statement(
    stmt,
    operation="select",
    scope_tables=[
        {
            "metaTableUid": asset_meta_table.uid,
            "alias": "asset",
            "access": "read",
        },
        {
            "metaTableUid": account_meta_table.uid,
            "alias": "account",
            "access": "read",
        },
    ],
    limits={"max_rows": 1000, "statement_timeout_ms": 15000},
)
```

Then execute through TS Manager:

```python
from mainsequence.client import MetaTable


result = MetaTable.execute_operation(operation)
```

The backend endpoint is:

```text
POST /orm/api/ts_manager/meta_table/execute-operation/
```

## Scope Is Required

`scope.tables` is not optional. It is the primary authorization contract.

Every table referenced by the compiled SQL should appear in scope:

```python
scope_tables=[
    {"metaTableUid": asset_meta_table.uid, "alias": "asset", "access": "read"},
    {"metaTableUid": account_meta_table.uid, "alias": "account", "access": "read"},
]
```

Access can be:

- `read`
- `write`

For write operations, scope the mutated table with `access="write"`.

TS Manager may parse the SQL as a defense-in-depth check, but the declared
MetaTable scope is the platform permission contract.

## Backend Execution Rules

TS Manager validates and executes the operation through the selected
`DynamicTableDataSource`.

It checks:

- every scoped MetaTable exists and is visible to the caller
- the caller has the requested access
- scoped tables belong to a compatible data source
- the operation kind matches the SQL statement kind
- the dialect is supported
- the statement is single-statement
- parameters are bound, not string-interpolated
- row limits and statement timeouts are respected
- writes are gated by object permission and data-source capabilities

The first backend implementation caps execution at `max_rows <= 10000` and
`statement_timeout_ms <= 60000`, even if the client sends larger values.

Read-only raw SQL remains separate. Writable MetaTable operations should use
compiled operation envelopes with declared scope.

## Writes

The same protocol family supports `insert`, `update`, `upsert`, and `delete`
when the backend data source allows those operations.

Example shape:

```python
operation = build_operation(
    operation="upsert",
    sql=(
        "INSERT INTO public.asset (uid, symbol) "
        "VALUES (%(uid)s, %(symbol)s) "
        "ON CONFLICT (uid) DO UPDATE SET symbol = EXCLUDED.symbol"
    ),
    parameters={"uid": str(asset_uid), "symbol": "BTC"},
    scope={
        "tables": [
            {
                "metaTableUid": asset_meta_table.uid,
                "alias": "asset",
                "access": "write",
            }
        ]
    },
)
```

Do not send arbitrary writable SQL without scope. TS Manager expects the
operation kind, declared MetaTable scope, and backend capability gates.

## Common Mistakes

Do not pass SQLAlchemy objects to `MetaTable.execute_operation(...)`:

```python
MetaTable.execute_operation(stmt)  # wrong
```

Compile first:

```python
operation = compile_sqlalchemy_statement(
    stmt,
    operation="select",
    scope_tables=[{"metaTableUid": asset_meta_table.uid, "access": "read"}],
)
MetaTable.execute_operation(operation)
```

Do not omit joined tables from scope. If `Asset` joins `Account`, both
MetaTables must be declared unless the backend adapter explicitly supports
another authorization model.
