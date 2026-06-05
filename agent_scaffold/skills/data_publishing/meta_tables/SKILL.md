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
- define indexes and foreign keys in SQLAlchemy metadata for Alembic-owned DDL
- design governed compiled SQL read and write operations
- design provider-based Alembic contract evolution for MetaTables
- run the documented `mainsequence migrations ...` lifecycle for Alembic-backed MetaTable changes
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
2. For migration work, `docs/tutorial/metatable_migrations.md`
3. `docs/knowledge/meta_tables/index.md`
4. `docs/knowledge/meta_tables/sqlalchemy.md`
5. `docs/knowledge/meta_tables/compiled_sql.md`
6. `docs/knowledge/meta_tables/migrations.md`
7. `docs/knowledge/meta_tables/api.md`

## Inputs This Skill Needs

Before changing code, collect or infer:

- the row entities that should exist
- the business keys
- relation shape between tables
- expected read patterns
- expected mutation patterns
- whether TS Manager should create the physical table
- for `external_registered`, the target `DynamicTableDataSource` UID
- for contract changes, the selected `AlembicMetaTableMigration` provider or provider module path
- for contract changes, the provider's `AlembicVersionMetaTable` binding and whether it has been registered
- for contract changes, the intended Alembic revision, parent/current revision, target revision, and updated SQLAlchemy declarations

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
8. For a migration, which provider object controls `target_metadata`, `script_location`, `alembic_registry`, and `metatable_models`?
9. For a migration, has the provider's Alembic version-table binding been registered?

## Build Rules

### 0. Creation And Deletion Are SDK-Managed

Do not build custom migrations for creating or deleting MetaTables.

MetaTable creation and deletion are handled through `mainsequence-sdk` and the
Main Sequence CLI. Assistants should define the SQLAlchemy MetaTable model,
select the SDK migration provider when schema evolution is needed, and use the
documented `mainsequence migrations ...` commands.

Do not hand-author create/delete migration files, backend migration payloads,
manifest rows, registry rows, or low-level SDK migration requests for MetaTable
creation or deletion.

The only migration workflow to recommend is the Main Sequence CLI lifecycle:

```bash
mainsequence migrations current --provider mainsequence_migrations:migration
mainsequence migrations revision --provider mainsequence_migrations:migration
mainsequence migrations upgrade --provider mainsequence_migrations:migration head
```

### 1. SQLAlchemy metadata is the authoring source

Keep the application table model as the authoring source for the neutral table contract.

Do not hand-build contract fragments when the SQLAlchemy helper can derive them.

### 2. Use explicit project-prefixed table names

For `platform_managed`, inherit from `PlatformManagedMetaTable`.

Declare an explicit project-prefixed SQLAlchemy `__tablename__`. Use
`schema_table_name(project_or_app, concept)` from `mainsequence.meta_tables` to
generate that name. The mixin derives only the logical `storage_hash` from
storage-relevant configuration and table shape; it must not use that hash as the
SQLAlchemy table name.

When a platform-managed table must support in-place contract migrations from its
first version, use Alembic. Keep the SDK model as a normal
`PlatformManagedMetaTable` or `PlatformTimeIndexMetaTable` catalog contract, and
apply physical schema changes through the Alembic migration workflow.

For `PlatformTimeIndexMetaTable`, declare `__cadence__` whenever the table has a
known stable observation interval, for example `1m`, `5m`, `1h`, `1d`, `1w`,
`1mo`, `1q`, or `1y`. Cadence is table metadata and belongs on the storage
model when possible.

Default-schema tables must leave SQLAlchemy `Table.schema` unset; do not write
`__table_args__ = {"schema": "public"}` for the default PostgreSQL schema. Set
schema metadata only for non-default schemas, using `__table_args__ = {"schema":
"custom_schema"}` or the tuple form ending in `{"schema": ...}`. Do not add a
separate MetaTable-specific schema attribute.

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

Prefix explicit table identifiers, explicit physical table names, and Alembic
version table names with the project or package name. Bare names such as
`Account`, `Asset`, or `alembic_version` can collide across projects sharing an
organization or database schema. Prefer `schema_table_name(...)` over
hand-built f-strings so project/app prefixes, bounded length, and separators are
consistent.

Register through the class API:

```python
from mainsequence.meta_tables import PlatformManagedMetaTable, schema_table_name

PROJECT_NAME = "sdk_examples"
ACCOUNT_TABLE_NAME = schema_table_name(PROJECT_NAME, "account")


class Account(PlatformManagedMetaTable, Base):
    __tablename__ = ACCOUNT_TABLE_NAME
    __metatable_namespace__ = "sdk-examples"
    __metatable_identifier__ = "sdk_examples.Account"
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

```

Registration metadata belongs on the class. Do not call `Account.register()`
directly for platform-managed models. Add platform-managed models to the
selected `AlembicMetaTableMigration.metatable_models` list and let
`mainsequence migrations upgrade --provider ... head` reserve, migrate, refresh,
and bind them.

For platform-managed migration registration, the data source is resolved from
the active Main Sequence project/session, the same way DataNode does. Do not
require or thread a `data_source_uid` through normal platform-managed example
code.

Only call `build_registration_request()` when the task explicitly needs to inspect or validate the payload before registration.

For SDK examples, use a plain namespace constant:

```python
NAMESPACE = "sdk-examples"
```

Do not add an environment variable for namespace in examples.

Do not add generic labels such as `"meta-table"` or `"platform-managed"` to examples. Keep labels specific to the example or domain.

Do not add a `MAINSEQUENCE_META_TABLE_REGISTER` toggle in platform-managed
examples. Platform-managed examples should be migration-first.

### 3. Keep foreign keys in SQLAlchemy/Alembic metadata

For `PlatformManagedMetaTable`, define foreign keys with normal SQLAlchemy
`ForeignKey(...)` / `ForeignKeyConstraint(...)` metadata. Do not write explicit
target `MetaTable.uid` maps in the platform-managed path. Migration is the
lifecycle path: the SDK reserves provider MetaTable rows, Alembic renders and
applies FK/index DDL from SQLAlchemy metadata, and finalization refreshes the
catalog after upgrade.

Prefer project-prefixed SQLAlchemy table names for explicit FK string targets.
Alembic, SQLAlchemy, and the database own physical FK/index names unless the
project explicitly names them in SQLAlchemy.

Use this pattern:

```python
account_uid: Mapped[uuid.UUID] = mapped_column(
    Uuid,
    ForeignKey("sdk_examples__account.uid", ondelete="RESTRICT"),
    nullable=False,
)
```

Every participating table must include `__metatable_description__` describing
both the schema and the table's intention.

Provider scope:

```python
migration = AlembicMetaTableMigration(
    ...,
    metatable_models=[Account, Asset],
)
```

Migration tooling reserves provider MetaTable rows in the provider-declared
model order. Include related parent and child models in the same selected
provider when Alembic must create or evolve their FK DDL.

For `external_registered`, register each external table contract directly. Do
not encode FK target MetaTable UIDs in the child registration request; FKs are
database DDL/introspection metadata, not SDK registration contract fields:

```python
account_meta_table = MetaTable.register(account_request)
asset_request = external_registered_registration_request_from_sqlalchemy_model(
    Asset,
    data_source_uid=data_source_uid,
)
asset_meta_table = MetaTable.register(asset_request)
```

### 4. Schema changes use Alembic

When doing migration work, first read
`docs/tutorial/metatable_migrations.md`. That document is the tutorial source
for the provider-based Alembic lifecycle.

Do not apply in-place contract changes by changing a `PlatformManagedMetaTable`
SQLAlchemy class and calling normal registration again. Shape-addressed
`PlatformManagedMetaTable` storage identity changes when columns, indexes,
foreign keys, or constraints change, so new code cannot reliably recover the
previous shape-derived table.

Do not modify Alembic revision files that have already been implemented/applied.
MetaTable migrations are database-backed history: once a revision may exist in a
database `alembic_version` table, changing that file corrupts the relationship
between source history and deployed state. For any follow-up schema change,
create a new Alembic revision on top of the current head.

For contract evolution, define or update one selected
`AlembicMetaTableMigration` provider:

- put the provider in `mainsequence_migrations.py:migration` or pass
  `--provider module.path:migration`
- set `package`, `migration_namespace`, `script_location`, and `target_metadata`
- set `alembic_registry` to an `AlembicVersionMetaTable` subclass
- list the post-apply catalog scope in `metatable_models`
- generate revisions, apply migrations, and refresh catalog bindings
  through `mainsequence migrations ...` commands

`alembic_version_meta_table_uid` is the UID of the catalog binding for Alembic's
version table. It is not the UID of the table being migrated.

Application MetaTable catalog sync resolves existing rows by the authored
SQLAlchemy table name used by the provider model. Keep that table name stable
when a class is renamed or moved but must keep the same platform identity.
When declaring an explicit identifier, explicit physical table name, or Alembic
version table name, prefix it with the project or package name rather than using
a bare table name. Use `schema_table_name(project_or_app, concept)` for the
physical table and Alembic version table names.

Do not ask users to construct backend migration payloads, call low-level
migration request models, or use SDK helper functions directly. The backend
request shape is reference material in the tutorial; the user-facing path is:

```bash
mainsequence migrations current --provider mainsequence_migrations:migration
mainsequence migrations revision --provider mainsequence_migrations:migration
mainsequence migrations upgrade --provider mainsequence_migrations:migration head
```

All migration commands prepare the provider, reserve provider-scoped
platform-managed MetaTables, bind backend names, and call Alembic directly.
`revision` accepts optional `-m/--message`; if omitted, the CLI uses
`migration`. `revision --autogenerate` is optional and requires an explicit
`--sqlalchemy-url` for the baseline database.

The SQL must be Alembic-rendered from the selected provider. After SQL apply
succeeds, register or refresh only the application MetaTable catalog bindings
listed in `migration.metatable_models`. Do not pass the Alembic version-table
data source into those application model registrations; each model uses its
own normal MetaTable data-source binding. A migration is not complete until
both backend SQL execution and catalog sync succeed.

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
- migration work that lacks a selected `AlembicMetaTableMigration` provider
- backend-managed examples that use namespace environment variables instead of a plain `sdk-examples` namespace
- duplicate schema sources outside SQLAlchemy table metadata
- external tables registered with unstable physical names
- platform-managed examples that try to sequence parent registration through FK metadata
- external child registrations that try to encode FK target MetaTable UIDs in registration contracts
- contract changes attempted through normal registration instead of an Alembic migration
- migration work that asks users to define backend payloads, artifact rows, or SDK request objects
- compiled SQL operations without complete table scope
- raw SQL that hardcodes stale physical names
- a table that should really be modeled as a DataNode instead

## Validation Checklist

Do not claim success until you have checked:

- the table contract matches the intended row contract
- the table has an intention-rich `__metatable_description__`
- every mapped column has an intention-rich `info.description`
- indexes are intentional
- foreign keys are present in SQLAlchemy metadata for Alembic when required
- management mode is correct
- authored physical names are explicit, project-prefixed SQLAlchemy table names
- registration returns a `MetaTable.uid`
- compiled SQL operations declare table scope
- migrations use Alembic-rendered SQL
- migrations are scoped by an `AlembicMetaTableMigration` provider
- the provider's Alembic version-table binding is registered before apply/current
- post-apply catalog registration is scoped to `migration.metatable_models`
- catalog sync resolves application MetaTables by exact `identifier`
- user-facing migration instructions stay on the documented CLI/provider lifecycle

For related tables, also check:

- aliases are readable
- platform-managed child tables and parent tables are included in the selected migration provider
- FK target strings use stable project-prefixed SQLAlchemy table names where explicit strings are authored
- query results still match the expected response contract

## This Skill Must Stop And Escalate When

- physical table lifecycle ownership is unclear
- the target data source is unknown for an `external_registered` workflow
- the task really requires a time-series published table
- the workflow requires direct database credentials outside TS Manager governance
- a requested contract change lacks a selected provider or registered Alembic version-table binding
- the user expects the SDK to invent schema changes without Alembic/provider metadata
- the task is actually an API or orchestration problem

Do not guess through registration or execution semantics.
