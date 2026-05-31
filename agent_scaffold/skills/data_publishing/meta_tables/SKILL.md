---
name: mainsequence-meta-tables
description: Use this skill when the task is about defining, registering, querying, or reviewing Main Sequence MetaTables. This skill owns SQLAlchemy table contracts, backend-managed table registration, external table registration, governed compiled SQL operations, foreign keys, indexes, and validation rules. It does not own DataNode producers, API route contracts, scheduling, or sharing policy.
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
5. `docs/knowledge/meta_tables/api.md`

## Inputs This Skill Needs

Before changing code, collect or infer:

- the row entities that should exist
- the business keys
- relation shape between tables
- expected read patterns
- expected mutation patterns
- whether TS Manager should create the physical table
- for `external_registered`, the target `DynamicTableDataSource` UID

If ownership of the physical table lifecycle is unclear, stop before choosing a management mode.

## Required Decisions

For every non-trivial task, decide:

1. Is this table really row-oriented, or should it be a DataNode?
2. What is the business key?
3. Should the table be `platform_managed` or `external_registered`?
4. What namespace and identifier define the logical table identity?
5. Are foreign-key dependencies aligned with registration order?
6. What governed operations should be allowed by the declared table scope?

## Build Rules

### 1. SQLAlchemy metadata is the authoring source

Keep the application table model as the authoring source for the neutral table contract.

Do not hand-build contract fragments when the SQLAlchemy helper can derive them.

### 2. Use storage-hash physical names for backend-managed tables

For `platform_managed`, inherit from `PlatformManagedMetaTable`.

The mixin derives the SQLAlchemy physical table name from storage-relevant configuration and table shape. Do not hand-write `__tablename__` for normal backend-managed tables.

Schema must come from SQLAlchemy table metadata, usually `__table_args__ = {"schema": "public"}` or the tuple form ending in `{"schema": ...}`. Do not add a separate MetaTable-specific schema attribute.

Always declare `__metatable_description__` on the model. The description must
explain the table's business intention, row grain, and expected use, not only
the schema. Column-level descriptions stay in `mapped_column(info={...})`.

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


account_meta_table = Account.register(labels=["sdk-example"])
```

Pass `description=...` only when the call intentionally overrides the class
default.

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

For `PlatformManagedMetaTable`, register parent tables first and then register
child tables normally. Define SQLAlchemy foreign keys with parent column
objects, not stringified table fullnames, because platform registration may
rebind `Parent.__table__.name` to the backend physical table name.

Use this pattern:

```python
account_uid: Mapped[uuid.UUID] = mapped_column(
    Uuid,
    ForeignKey(Account.__table__.c.uid, ondelete="RESTRICT"),
    nullable=False,
)
```

Do not use `ForeignKey(f"{Account.__table__.fullname}.uid")`.

The SDK inspects SQLAlchemy foreign-key constraints and resolves each target
`MetaTable` by looking up the already registered parent table by stable logical
storage identity in the same data source.

Example registration order:

```python
account_meta_table = Account.register(...)
asset_meta_table = Asset.register(...)
```

The child registration will fail if the parent table has not already been registered, because there is no target `MetaTable.uid` for the backend FK contract.

Do not pass `target_meta_tables` in the normal platform-managed path. Use
explicit FK target mappings only for edge cases where automatic lookup is
ambiguous or impossible. When you need an explicit mapping, key it by the parent
model class, not a mutable table fullname:

```python
asset_meta_table = Asset.register(
    target_meta_tables={Account: account_meta_table},
)
```

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

### 4. Governed operations declare scope

Compiled SQL operations must declare the `MetaTable` UID scope and read/write access for every table they touch.

Do not execute unrestricted SQL outside the MetaTable operation contract.

### 5. Physical names come from registered resources

Only use physical table names returned by registered `MetaTable` objects when composing SQL strings.

Do not hardcode platform-managed physical names manually.

## Review Rules

When reviewing an existing MetaTable workflow, look for:

- missing namespace or identifier
- missing `__metatable_description__`, or a description that only repeats column names instead of table intention
- backend-managed models that do not inherit `PlatformManagedMetaTable`
- backend-managed examples that use namespace environment variables instead of a plain `sdk-examples` namespace
- duplicate schema sources outside SQLAlchemy table metadata
- external tables registered with unstable physical names
- platform-managed child tables that are registered before parent tables
- external child registrations that do not map foreign-key targets to registered parent `MetaTable.uid` values
- compiled SQL operations without complete table scope
- raw SQL that hardcodes stale physical names
- a table that should really be modeled as a DataNode instead

## Validation Checklist

Do not claim success until you have checked:

- the table contract matches the intended row contract
- the table has an intention-rich `__metatable_description__`
- indexes are intentional
- foreign keys resolve to the correct dependency targets
- management mode is correct
- backend-managed physical names match the storage hash
- registration returns a `MetaTable.uid`
- compiled SQL operations declare table scope

For related tables, also check:

- aliases are readable
- platform-managed parent tables are registered before child tables
- external child registration requests map FK targets to the registered parent UIDs
- query results still match the expected response contract

## This Skill Must Stop And Escalate When

- physical table lifecycle ownership is unclear
- the target data source is unknown for an `external_registered` workflow
- the task really requires a time-series published table
- the workflow requires direct database credentials outside TS Manager governance
- the task is actually an API or orchestration problem

Do not guess through registration or execution semantics.
