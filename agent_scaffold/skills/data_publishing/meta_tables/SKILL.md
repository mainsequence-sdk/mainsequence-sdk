---
name: mainsequence-meta-tables
description: Use this skill when the task is about defining, registering, querying, migrating, or reviewing Main Sequence MetaTables. This skill owns SQLAlchemy table contracts, backend-managed table registration, external table registration, Alembic-based MetaTable migrations, governed compiled SQL operations, foreign keys, indexes, and validation rules. It does not own DataNode producers, API route contracts, scheduling, releases, or sharing policy.
---

# Main Sequence MetaTables

## Overview

Use this skill when the task changes row-oriented project tables that are not naturally time-series DataNodes.

This skill is for schema-driven application tables registered through TS Manager as `MetaTable` resources.

## This Skill Can Do

- define SQLAlchemy/Core or ORM table models for `MetaTable` registration
- choose `platform_managed` or `external_registered` management mode
- register platform-managed tables through the model class API
- build registration requests from resolved SQLAlchemy metadata when inspection is useful
- define indexes and foreign keys in the table contract
- design governed compiled SQL read and write operations
- design Alembic-based contract evolution for MetaTables
- package Alembic-rendered SQL artifacts for backend apply
- review table contracts for physical-name, namespace, and identifier issues
- review whether a task should be a `MetaTable` or a `DataNode`

## This Skill Must Not Claim

This skill must not claim ownership of:

- DataNode producer contracts
- FastAPI route contracts
- widget response contracts
- workspace payloads
- job scheduling, image pinning, or releases
- RBAC or sharing policy

If the user is still in the discovery process and does not yet know what data exists on the platform, use the exploration skill first and return here after discovery is complete.

## Route Adjacent Work

- discovery-only data inventory before table implementation:
  `.agents/skills/mainsequence/data_access/exploration/SKILL.md`
- DataNodes:
  `.agents/skills/mainsequence/data_publishing/data_nodes/SKILL.md`
- APIs and FastAPI:
  `.agents/skills/mainsequence/application_surfaces/api_surfaces/SKILL.md`
- Command Center workspaces:
  `.agents/skills/mainsequence/command_center/workspace_builder/SKILL.md`
- AppComponents and custom forms:
  `.agents/skills/mainsequence/command_center/app_components/SKILL.md`
- Jobs, images, resources, and releases:
  `.agents/skills/mainsequence/platform_operations/orchestration_and_releases/SKILL.md`

## Read First

1. `docs/tutorial/working_with_meta_tables.md`
2. `docs/knowledge/meta_tables/index.md`
3. `docs/knowledge/meta_tables/sqlalchemy.md`
4. `docs/knowledge/meta_tables/compiled_sql.md`
5. `docs/knowledge/meta_tables/migrations.md`
6. `docs/knowledge/meta_tables/api.md`

## Inputs This Skill Needs

Before changing code, collect or infer:

- the row entities that should exist
- the business keys
- relation shape between tables
- expected read patterns
- expected mutation patterns
- whether TS Manager should create the physical table
- for `external_registered`, the target `DynamicTableDataSource` UID
- for contract changes, the `AlembicVersionMetaTable` binding
- for contract changes, the package, migration namespace, revision, parent revision, rendered SQL artifact, affected table identifiers, and old/new SQLAlchemy contract declarations

If ownership of the physical table lifecycle is unclear, stop before choosing a management mode.

## Required Decisions

For every non-trivial task, decide:

1. Is this table really row-oriented, or should it be a DataNode?
2. What is the business key?
3. Should the table be `platform_managed` or `external_registered`?
4. What namespace and identifier define the logical table identity?
5. Are foreign-key dependencies aligned with registration order?
6. What governed operations should be allowed by the declared table scope?
7. If the table already exists and its contract changes, is this an Alembic migration rather than normal registration?
8. For a migration, what Alembic revision transition and version-table binding will the backend validate?

## Build Rules

### 1. SQLAlchemy metadata is the authoring source

Keep the application table model as the authoring source for the neutral table contract.

Do not hand-build contract fragments when the SQLAlchemy helper can derive them.

### 2. Use storage-hash physical names for backend-managed tables

For `platform_managed`, inherit from `PlatformManagedMetaTable`.

The mixin derives the SQLAlchemy physical table name from storage-relevant configuration and table shape. Do not hand-write `__tablename__` for normal backend-managed tables.

When a platform-managed table must support in-place contract migrations from its
first version, use Alembic. Keep the SDK model as a normal
`PlatformManagedMetaTable` or `PlatformTimeIndexMetaData` catalog contract, and
apply physical schema changes through the Alembic migration workflow.

Schema must come from SQLAlchemy table metadata, usually `__table_args__ = {"schema": "public"}` or the tuple form ending in `{"schema": ...}`. Do not add a separate MetaTable-specific schema attribute.

Always declare `__metatable_description__` on the model. The description must
explain the table's business intention, row grain, and expected use, not only
the schema. Column-level descriptions stay in `mapped_column(info={...})`.
Every mapped column must include `info={"label": ..., "description": ...}`.
The column description must explain what the value means in this table and how
it is used, not just restate the column name or dtype.

Use `__metatable_extra_hash_components__` when two backend-managed tables could
otherwise produce the same storage hash because their storage-relevant shape is
identical or intentionally generic. The value must be stable and deterministic,
usually a small mapping such as `{"storage_name": "account_holdings"}`.

This attribute is part of storage identity. Changing it defines a different
table. Do not use it for labels, descriptions, runtime options, test isolation,
backend UIDs, data-source UIDs, or updater scope. Use `hash_namespace` for test
or experiment isolation.

Register through the class API:

```python
class Account(PlatformManagedMetaTable, Base):
    __metatable_namespace__ = "sdk-examples"
    __metatable_identifier__ = "Account"
    __metatable_extra_hash_components__ = {"storage_name": "account"}
    __metatable_description__ = (
        "Customer account master records used to scope balances, holdings, and "
        "account-level limits."
    )
    __metatable_labels__ = ["sdk-example"]

    uid: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        primary_key=True,
        info={
            "label": "Account UID",
            "description": "Stable account identifier referenced by dependent tables.",
        },
    )
    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        info={
            "label": "Name",
            "description": "Display name used to identify the account in examples.",
        },
    )


account_meta_table = Account.register()
```

Registration metadata belongs on the class. Do not pass description, labels,
provisioning, data-source UID, hash namespace, time-index fields, or storage
layout into `register()`.

For platform-managed registration, the data source is resolved from the active Main Sequence project/session, the same way DataNode does. Do not require or thread a `data_source_uid` through normal platform-managed example code.

Only call `build_registration_request()` when the task explicitly needs to inspect or validate the payload before registration.

For SDK examples, use a plain namespace constant:

```python
NAMESPACE = "sdk-examples"
```

Do not add an environment variable for namespace in examples.

Do not add generic labels such as `"meta-table"` or `"platform-managed"` to examples. Keep labels specific to the example or domain.

Do not add a `MAINSEQUENCE_META_TABLE_REGISTER` toggle in registration examples. Registration examples should register directly.

### 3. Register parent tables before child tables

Foreign-key contracts reference the target `MetaTable` UID.

For `PlatformManagedMetaTable`, define foreign keys with
`MetaTableForeignKey(TargetModel, column=...)`. Do not write raw SQLAlchemy
table fullnames, `Parent.__table__.c.<column>` targets, or explicit target UID
maps in the platform-managed path. Registration is the lifecycle path:
`register()` recursively registers unresolved target model classes, stores each
returned `MetaTable` in a local process registry keyed by `storage_hash`, and
uses the target `MetaTable.uid` in the child FK contract.

Do not require users to provide foreign-key names. `MetaTableForeignKey(...)`
accepts `name=...` only as an override; when omitted, the SDK derives a stable
PostgreSQL-safe contract name from the child table and source column after the
column is attached to the SQLAlchemy table.

Use this pattern:

```python
account_uid: Mapped[uuid.UUID] = mapped_column(
    Uuid,
    MetaTableForeignKey(Account, column="uid", ondelete="RESTRICT"),
    nullable=False,
)
```

Every participating table must include `__metatable_description__` describing
both the schema and the table's intention.

Example registration order:

```python
asset_meta_table = Asset.register()
```

The child registration registers `Account` first if it has not already been
registered in the current process. The local registry prevents duplicate backend
registration attempts for the same `storage_hash` and raises a clear error for
recursive registration cycles.

For `external_registered`, there is no platform-managed parent lookup. Register
the parent first, then build the child registration request with
`target_meta_tables={Account: account_meta_table}`:

```python
account_meta_table = MetaTable.register(account_request)
asset_request = external_registered_registration_request_from_sqlalchemy_model(
    Asset,
    data_source_uid=data_source_uid,
    target_meta_tables={Account: account_meta_table},
)
asset_meta_table = MetaTable.register(asset_request)
```

### 4. Schema changes use Alembic

Do not apply in-place contract changes by changing a `PlatformManagedMetaTable`
SQLAlchemy class and calling normal registration again. Shape-addressed
`PlatformManagedMetaTable` storage identity changes when columns, indexes,
foreign keys, or constraints change, so new code cannot reliably recover the
previous shape-derived table.

For contract evolution, use Alembic plus `mainsequence.meta_tables.migrations`:

- declare/register an `AlembicVersionMetaTable` catalog binding
- render SQL from Alembic revisions
- call `MetaTable.apply_migration(...)` with a `metatable-migration.v1`
  request containing the Alembic-rendered SQL artifact

`alembic_version_meta_table_uid` is the UID of the catalog binding for Alembic's
version table. It is not the UID of the table being migrated.

Package the backend apply request with Alembic-rendered SQL:

```python
operation = AlembicMigrationOperation(
    data_source_uid=data_source_uid,
    alembic_version_meta_table_uid=alembic_version_meta_table.uid,
    package="sdk-examples",
    migration_namespace="default",
    revision="002_add_asset_status",
    manifest=manifest,
    sql=alembic_rendered_sql,
    statement_boundaries=[],
    dry_run=True,
)
```

The SQL must be Alembic-rendered from the installed package. After SQL apply
succeeds, register or refresh application MetaTable catalog bindings separately.

Do not use SDK-managed migration artifact tables, artifact sync helpers, or custom
`operations()` migration modules.

### 5. Governed operations declare scope

Compiled SQL operations must declare the `MetaTable` UID scope and read/write access for every table they touch.

Do not execute unrestricted SQL outside the MetaTable operation contract.

### 6. Physical names come from registered resources

Only use physical table names returned by registered `MetaTable` objects when composing SQL strings.

Do not hardcode platform-managed physical names manually.

## Review Rules

When reviewing an existing MetaTable workflow, look for:

- missing namespace or identifier
- missing `__metatable_description__`, or a description that only repeats column names instead of table intention
- mapped columns without `info.label` and `info.description`
- backend-managed models that do not inherit `PlatformManagedMetaTable`
- schema changes that bypass Alembic or try to use SDK operation lists
- backend-managed examples that use namespace environment variables instead of a plain `sdk-examples` namespace
- duplicate schema sources outside SQLAlchemy table metadata
- external tables registered with unstable physical names
- platform-managed examples that manually sequence parent registration instead
  of relying on `MetaTableForeignKey(...)` recursive registration
- external child registrations that do not map foreign-key targets to registered parent `MetaTable.uid` values
- contract changes attempted through normal registration instead of an Alembic migration
- migration apply requests that confuse `alembic_version_meta_table_uid` with an affected table UID
- migration requests that include arbitrary SQL instead of Alembic-rendered package SQL
- compiled SQL operations without complete table scope
- raw SQL that hardcodes stale physical names
- a table that should really be modeled as a DataNode instead

## Validation Checklist

Do not claim success until you have checked:

- the table contract matches the intended row contract
- the table has an intention-rich `__metatable_description__`
- every mapped column has an intention-rich `info.description`
- indexes are intentional
- foreign keys resolve to the correct dependency targets
- management mode is correct
- backend-managed physical names match the storage hash
- registration returns a `MetaTable.uid`
- compiled SQL operations declare table scope
- migrations use Alembic-rendered SQL
- migration requests reference the registered Alembic version-table binding
- migration apply/status helpers use typed `metatable-migration.v1` request and response models

For related tables, also check:

- aliases are readable
- platform-managed child registration recursively resolves parent
  `MetaTableForeignKey(...)` targets
- external child registration requests map FK targets to the registered parent UIDs
- query results still match the expected response contract

## This Skill Must Stop And Escalate When

- physical table lifecycle ownership is unclear
- the target data source is unknown for an `external_registered` workflow
- the task really requires a time-series published table
- the workflow requires direct database credentials outside TS Manager governance
- a requested contract change lacks a stable affected table identifier or old contract anchor
- the user expects the SDK to autogenerate migrations or parse arbitrary request-body SQL
- the task is actually an API or orchestration problem

Do not guess through registration or execution semantics.
