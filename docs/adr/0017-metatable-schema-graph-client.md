# ADR 0017: MetaTable Schema Graph Client API

Date: 2026-05-31

Status: Accepted

## Context

MetaTable foreign keys are stored as canonical `MetaTableForeignKey` projection
rows. A table exposes outgoing relationships through `foreign_keys` and incoming
relationships through `incoming_fks`.

The `incoming_fks` projection is useful for display counts and direct FK
metadata, but it is not enough for dependency analysis because callers need the
dependent source table identity. The backend already exposes that complete graph
through:

```text
GET /orm/api/ts_manager/meta_table/<uid>/schema-graph/?depth=<n>&include_incoming=<bool>
```

That route returns graph edges with both `source_uid` and `target_uid`, so it is
the correct client-side surface for questions like "which MetaTables depend on
this MetaTable?".

## Decision

Add an SDK instance method:

```python
graph = meta_table.get_schema_graph(include_incoming=True)
```

The method calls the existing backend schema graph detail route using the
MetaTable public UID. It returns the backend payload as a dictionary with:

- `root_uid`
- `depth`
- `include_incoming`
- `nodes`
- `edges`

Inbound dependencies are edges where `edge["target_uid"] == meta_table.uid`.
The dependent table is `edge["source_uid"]`.

## API Contract

`MetaTable.get_schema_graph(...)` accepts:

- `depth: int = 1`
- `include_incoming: bool = False`
- `timeout: int | float | tuple[float, float] | None = None`

Example:

```python
graph = meta_table.get_schema_graph(depth=1, include_incoming=True)
incoming_edges = [
    edge for edge in graph["edges"] if edge["target_uid"] == meta_table.uid
]
```

## Non-Goals

This does not add a second foreign-key ownership model. Foreign keys remain
owned by the MetaTable contract and `MetaTableForeignKey` projection rows.

This does not reinterpret `incoming_fks` as the dependency graph API.
`incoming_fks` remains a serialized projection on a MetaTable response. Graph
queries should use `get_schema_graph(include_incoming=True)`.
