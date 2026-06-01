# MetaTable Migrations

MetaTable migrations are a small packaged-SQL workflow for contract changes that
need backend execution but client-owned migration state.

The protocol version is:

```text
metatable-migration.v1
```

This is not an Alembic runtime. The SDK packages SQL files and manifest
metadata, stores them in a client-defined `MigrationMetaTable`, and asks TS
Manager to apply a referenced registry row.

## Architecture

The registry is a normal platform-managed MetaTable declared by the client.

```python
from mainsequence.meta_tables.migrations import MigrationMetaTable


class MarketsMigration(MigrationMetaTable, Base):
    __metatable_namespace__ = "msm"
    __metatable_identifier__ = "markets_migrations"
    __metatable_description__ = "Packaged ms-markets contract migrations."

    release_channel: Mapped[str | None] = mapped_column(String(64))
```

`MigrationMetaTable` supplies the required migration columns. Project packages
can add release, compatibility, ownership, or diagnostic columns.

The backend does not own a separate migration artifact table. It receives a
reference to a row in this registry, validates the row, locks the migration
stream, executes SQL, refreshes affected MetaTables, and updates status.

## Manifest

A packaged migration manifest is YAML or JSON:

```yaml
version: metatable-migration-manifest.v1
migration_namespace: markets
revision: "001"
down_revision:
direction: upgrade
expected_current_revision:
sql_path: migrations/001.up.sql
affected_tables:
  - identifier: AssetTag
    namespace: msm
```

SQL files are loaded from the same installed Python package via
`importlib.resources`.

## Sync And Apply

```python
from mainsequence.meta_tables.migrations import (
    apply_migration,
    load_packaged_migration,
    sync_packaged_migration,
)


packaged = load_packaged_migration(
    "msm",
    "migrations/001_asset_tag.yaml",
    new_contract_models={"AssetTag": AssetTag},
)

sync_result = sync_packaged_migration(
    MarketsMigration,
    packaged,
    data_source_uid=DATA_SOURCE_UID,
    extension_fields={"release_channel": "stable"},
)

result = apply_migration(
    sync_result["meta_table"],
    sync_result["row"],
    dry_run=True,
)
```

`sync_packaged_migration(...)` registers the registry MetaTable if needed and
upserts the packaged migration row using `compiled-sql.v1`.

`apply_migration(...)` sends a `metatable-migration.v1` request and returns a
typed `MetaTableMigrationApplyResponse`.

The request contains:

- `migration_meta_table_uid`
- `migration_row_uid`
- `data_source_uid`
- `package`
- `migration_namespace`
- `revision`
- checksums
- expected contract hashes
- idempotency and lock keys
- `dry_run`

The apply request does not send executable SQL directly. The backend reads SQL
from the referenced registry row.

## Contract Hash Rotation

A contract migration changes the MetaTable contract. The SDK must send both sides
of that change:

- `old_contract_hashes`: what the backend must see before running SQL.
- `new_contract_hashes`: what the backend must see after SQL execution and
  introspection/refresh.
- `new_contracts`: the post-migration contract payloads the backend uses to
  refresh platform MetaTable metadata.

The affected table is addressed by stable `identifier`. The contract hash is
allowed to rotate; the logical table identifier is not.

This example uses SQLAlchemy classes only to compute old and new contract
payloads for the migration row. In-place migration targets must inherit from
`MigrationManagedMetaTable`, or from `MigrationManagedTimeIndexMetaData` for
time-indexed storage. Do not apply the changed declaration through normal
shape-addressed `PlatformManagedMetaTable.register(...)` or
`PlatformTimeIndexMetaData.register(...)`; in-place contract changes must go
through the migration apply endpoint so the backend can resolve the existing
table, run SQL, and refresh the MetaTable contract.

For a simple migration that adds a `status` column to `Asset`, model the two
contract versions explicitly:

```python
class AssetBeforeMigration(MigrationManagedMetaTable, BeforeBase):
    __metatable_namespace__ = "sdk-examples"
    __metatable_identifier__ = "sdk-examples.Asset"

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)


class AssetAfterMigration(MigrationManagedMetaTable, AfterBase):
    __metatable_namespace__ = "sdk-examples"
    __metatable_identifier__ = "sdk-examples.Asset"

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
```

Then load the packaged SQL with both declarations:

```python
packaged = load_packaged_migration(
    "examples.meta_tables.migrations",
    "packaged/002_add_asset_status.yaml",
    old_contract_models={"sdk-examples.Asset": AssetBeforeMigration},
    new_contract_models={"sdk-examples.Asset": AssetAfterMigration},
)

row = build_migration_registry_row(
    packaged,
    data_source_uid=DATA_SOURCE_UID,
)
```

The registry row now contains the contract rotation:

```python
row.old_contract_hashes["sdk-examples.Asset"]
row.new_contract_hashes["sdk-examples.Asset"]
row.new_contracts["sdk-examples.Asset"]
```

Those two hashes must be different for a real contract change. At apply time, the
backend validates the old hash before execution and the new hash after it
refreshes the affected MetaTable.

In a real package release, the same import path usually represents the table
before and after the code update. The example keeps both versions in one file,
so it pins the same `__metatable_identifier__` on both classes to model the
same logical MetaTable.

For time-indexed storage, use the time-indexed migration base so the class still
builds a `TimeIndexMetaTableRegistrationRequest` and keeps the existing
time-index/index validation:

```python
class HoldingsAfterMigration(MigrationManagedTimeIndexMetaData, AfterBase):
    __metatable_namespace__ = "sdk-examples"
    __metatable_identifier__ = "sdk-examples.AccountHoldings"
    __time_index_name__ = "time_index"
    __index_names__ = ["time_index", "account_uid"]

    time_index: Mapped[datetime.datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    account_uid: Mapped[uuid.UUID] = mapped_column(Uuid, nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)
    status: Mapped[str | None] = mapped_column(String(32), nullable=True)
```

`MigrationManagedTimeIndexMetaData` is still accepted by migration packaging as
a `MigrationManagedMetaTable` target, but it registers through the
`TimeIndexMetaData` endpoint and produces `table_kind: time_indexed` contracts.

## Backend Contract

TS Manager should expose:

```text
POST /orm/api/ts_manager/meta_table/apply-migration/
POST /orm/api/ts_manager/meta_table/migration-status/
```

Apply behavior:

1. Resolve `migration_meta_table_uid`.
2. Read `migration_row_uid`.
3. Recompute manifest and SQL checksums from the row.
4. Acquire the `(data_source_uid, package, migration_namespace)` lock.
5. Check the current revision after lock acquisition.
6. Validate old contract hashes.
7. Stop on `dry_run`.
8. Execute SQL transactionally when the database supports transactional DDL.
9. Introspect affected physical tables.
10. Create, import, or refresh affected MetaTables.
11. Validate new contract hashes.
12. Update the registry row status or append run rows.

## Example

Preview the example registry row and operation payloads:

```bash
python -m examples.meta_tables.migrations.client_defined_registry
```

Preview a contract-hash rotation for a changed MetaTable:

```bash
python -m examples.meta_tables.migrations.contract_hash_rotation
```

Register the example registry and sync the packaged migration row:

```bash
export MAINSEQUENCE_META_TABLE_MIGRATION_DATA_SOURCE_UID="<dynamic-data-source-uid>"
export MAINSEQUENCE_META_TABLE_MIGRATION_SYNC=1
python -m examples.meta_tables.migrations.client_defined_registry
```

Apply it after the backend migration endpoint is available:

```bash
export MAINSEQUENCE_META_TABLE_MIGRATION_APPLY=1
python -m examples.meta_tables.migrations.client_defined_registry
```

Apply the contract-rotation example by using the same environment variables and
running:

```bash
python -m examples.meta_tables.migrations.contract_hash_rotation
```
