# ADR 0015: APIDataNode UID Runtime Identity

Date: 2026-05-28

Status: Accepted

## Related ADRs

- ADR 0006: TDAG Public UID Identifiers
- ADR 0007: Client-Wide UID Public Identity
- ADR 0009: CLI Public Resource Identity

## Context

The SDK has migrated public resource identity from integer `id` values to
stable `uid` values. Most TDAG backend lookups already use data source UID
filters, but the `APIDataNode` runtime path still contains legacy
`data_source_id` assumptions.

The current failure mode appears when reading a published DataNode through
`APIDataNode.build_from_identifier(...)`:

```python
table = DataNodeStorage.get(identifier=identifier)
ts = cls(data_source_id=table.data_source.id, storage_hash=table.storage_hash)
```

The `APIDataNode` constructor then asserts that `data_source_id` is an integer:

```python
assert isinstance(data_source_id, int)
```

That is wrong after the UID migration. A `DataNodeStorage` response may expose a
`DynamicTableDataSource` whose public identity is `uid`, and the legacy
integer `id` is no longer a valid runtime requirement.

There is a second bug after the assertion:

```python
APIPersistManager(
    storage_hash=self.storage_hash,
    data_source_id=self.data_source_id,
    data_source_uid=getattr(self.data_source, "uid", None),
)
```

`APIDataNode.build_from_identifier(...)` does not attach a data source object to
`self.data_source`, so `getattr(self.data_source, "uid", None)` can be `None`.
At the same time, `APIPersistManager` already requires `data_source_uid` and
uses it for the backend lookup:

```python
DataNodeStorage.get_or_none(
    storage_hash=self.storage_hash,
    data_source__uid=self.data_source_uid,
    include_relations_detail=True,
)
```

So the runtime is internally inconsistent:

- APIDataNode factories still pass `data_source_id`.
- APIDataNode constructor still requires `data_source_id`.
- APIPersistManager already requires `data_source_uid`.
- DataNodeStorage filters expose `data_source__uid`, not `data_source__id`.

ADR 0007 explicitly noted that some constructor-level compatibility arguments
remained in local/API initialization paths. This ADR closes that gap.

## Decision

`APIDataNode` runtime identity must be UID-only.

The final contract is:

- `APIDataNode.__init__` accepts `data_source_uid: str`, not
  `data_source_id: int`.
- `APIDataNode` stores `self.data_source_uid`.
- `APIDataNode` does not store or require `self.data_source_id`.
- `APIPersistManager` accepts and requires `data_source_uid`.
- `APIPersistManager` does not accept or store `data_source_id`.
- `APIDataNode.build_from_identifier(...)` passes
  `table.data_source.uid`.
- `APIDataNode.build_from_local_time_serie(...)` passes
  `source_table.data_source.uid`.
- `APIDataNode.build_from_table_id(...)` is removed.
- The replacement helper is `APIDataNode.build_from_table_uid(...)`.
- `build_from_table_uid(...)` resolves `DataNodeStorage` by `uid`, not `id`.
- TDAG APIDataNode pickle markers require `data_source_uid`.
- The old `data_source_id` pickle marker fallback is removed.

This is intentionally not a backwards-compatible migration. Integer data source
IDs are not part of the public runtime contract anymore.

## Non-Goals

- Do not preserve `build_from_table_id(...)` as an alias.
- Do not keep `data_source_id` as an optional constructor argument.
- Do not silently coerce strings into integer IDs.
- Do not infer data source UID from `self.data_source` when the factory already
  knows the UID.
- Do not keep old local pickle compatibility for markers that only contain
  `data_source_id`.

## Implementation Tasks

- [x] Change `APIDataNode.__init__` to accept `data_source_uid: str` and remove
      the `data_source_id` argument, assertion, and attribute.
- [x] Add a strict validation error when `data_source_uid` is missing or empty.
- [x] Update `APIDataNode.build_from_local_time_serie(...)` to pass
      `source_table.data_source.uid`.
- [x] Rename `APIDataNode.build_from_table_id(...)` to
      `APIDataNode.build_from_table_uid(...)`.
- [x] Update `build_from_table_uid(...)` to call `DataNodeStorage.get(uid=...)`.
- [x] Update `APIDataNode.build_from_identifier(...)` to pass
      `table.data_source.uid`.
- [x] Update `_set_local_persist_manager(...)` to call:

      ```python
      APIPersistManager(
          storage_hash=self.storage_hash,
          data_source_uid=self.data_source_uid,
      )
      ```

- [x] Remove `data_source_id` from `APIPersistManager.__init__`.
- [x] Remove `self.data_source_id` from `APIPersistManager`.
- [x] Remove `data_source_id` fallback reads from TDAG pickle deserialization.
- [x] Verify APIDataNode serialization writes only `data_source_uid`.
- [x] Update all SDK call sites from `build_from_table_id(...)` to
      `build_from_table_uid(...)`.
- [x] Update docs and tutorials that mention `build_from_table_id` or table ID
      lookup for APIDataNode.
- [x] Add focused tests proving `APIDataNode.build_from_identifier(...)` works
      when `DynamicTableDataSource.id` is absent but `uid` is present.
- [x] Add focused tests proving `APIDataNode.build_from_table_uid(...)` uses
      `DataNodeStorage.get(uid=...)`.
- [x] Add focused tests proving `APIPersistManager` receives only
      `data_source_uid`.
- [x] Add focused tests proving old `data_source_id` pickle markers are rejected.

## Consequences

Existing code that calls `APIDataNode(..., data_source_id=...)` or
`APIDataNode.build_from_table_id(...)` will fail and must be migrated.

Existing local pickles that only contain `data_source_id` will no longer be
readable. This is acceptable because this ADR removes the compatibility path
instead of keeping an ambiguous identity contract.

The public read path becomes consistent with the rest of the TDAG UID contract:
the published table is resolved by `DataNodeStorage.uid` or identifier, and the
data source is resolved by `DynamicTableDataSource.uid`.
