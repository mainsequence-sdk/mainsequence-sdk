---
name: mainsequence-metatable-migrations
description: Use this skill when the task is about Main Sequence MetaTable schema evolution, Alembic provider setup, migration scaffold generation, revision creation, migration upgrade/current commands, provider-scoped MetaTable reservation/finalization, or debugging migration lifecycle failures. This skill owns the migration-first workflow for platform-managed MetaTables and should be used instead of direct MetaTable.register() guidance.
---

# Main Sequence MetaTable Migrations

## Overview

Use this skill for platform-managed MetaTable schema lifecycle work. The
standard path is migration-first: declare SQLAlchemy MetaTable models, put them
in a provider, run `mainsequence migrations ...`, and let the SDK reserve,
migrate, finalize, and bind provider-scoped MetaTables.

Do not recommend direct `MetaTable.register()` or model `.register()` calls for
platform-managed application tables outside the migration workflow.

## Read First

1. `docs/tutorial/metatable_migrations.md`
2. `docs/knowledge/meta_tables/migrations.md`
3. `docs/knowledge/meta_tables/api.md`
4. `docs/adr/0020-metatable-migration-artifact-registry.md`
5. `docs/adr/0026-sdk-owned-migration-scaffolding.md`

## Required Decisions

- Which provider module owns the migration stream?
- Which package and migration namespace identify the provider?
- Which `AlembicVersionMetaTable` binding points to Alembic's version table?
- Which SQLAlchemy `MetaData` object is the target metadata?
- Which provider-scoped MetaTable models belong in `metatable_models`?
- Does a dynamic provider need `metadata_for_models(...)` instead of full
  package metadata?
- Does the CodeRepository need an `after_register_metatables` catalog hook, and does
  that hook use `context.metatable_models` and `context.registered_metatables`
  instead of importing a broader registry?

## Standard Workflow

Use SDK-owned scaffold and helpers when creating a migration package:

```bash
mainsequence migrations scaffold \
  --package msm \
  --module migrations \
  --namespace mainsequence.examples \
  --base msm.base:MarketsBase \
  --metadata msm.base:MarketsBase.metadata
```

The generated provider should use:

- `build_alembic_version_metatable(...)`
- `build_metatable_model_registry(...)`
- `build_metatable_migration_provider(...)`
- SDK-owned `run_mainsequence_alembic_env(...)`
- SDK-owned `script.py.mako`

Then use the normal lifecycle:

```bash
mainsequence migrations current --provider migrations:migration
mainsequence migrations revision --provider migrations:migration
mainsequence migrations upgrade --provider migrations:migration head
```

`revision` writes normal Alembic files. `upgrade` reserves the provider's
Alembic registry root and provider MetaTables, runs Alembic DDL through the
backend-issued migration credential, finalizes all provider-scoped catalog
rows, and runs the optional provider hook.

## Canonical Lifecycle Contract

Treat catalog ownership, physical schema ownership, and provisioning state as
independent axes:

| Resource stage | `management_mode` | `schema_management_mode` | `provisioning_status` |
| --- | --- | --- | --- |
| Alembic registry before migration | `platform_managed` | `alembic_managed` | `reserved` |
| Provider table before migration | `platform_managed` | `alembic_managed` | `reserved` |
| Registry or provider table after finalize | `platform_managed` | `alembic_managed` | `active` |
| Imported external table | `external_registered` | `external_registered` | `active` |

The Alembic registry is the provider's managed root. It has no parent
`alembic_version_meta_table_uid`; every provider table references its UID.
Never classify the registry as `external_registered`. Alembic owning its
physical DDL is represented by `schema_management_mode="alembic_managed"`, not
by external catalog ownership.

Do not handcraft or mutate lifecycle combinations. SDK request models enforce
three creation intents: platform/backend with backend table creation,
platform/Alembic with backend table creation disabled, and external/external
with no provisioning. Managed migration collection rows are always
platform-managed, Alembic-managed reservations and are validated before HTTP.

## Rules

- Keep Alembic as the schema migration engine; do not build custom operation
  lists or fake migration payload formats.
- Keep provider scope explicit. Do not scan all imported models or installed
  packages.
- For one-model or configured dynamic providers, use `metadata_for_models(...)`
  and a provider-specific `metatable_models` list.
- Never send or thread request-side `data_source_uid` through migration status
  or apply flows. Backend migration operations resolve the data source from the
  reserved Alembic version MetaTable UID.
- Do not create SDK reset/reconcile commands for stale reserved state. If stale
  reserved state exists, fail clearly and require an explicit backend/admin
  repair path.
- Ordinary `MetaTable.delete()` must not bypass Alembic schema-management
  protection. For a deliberate organization-admin teardown/reset only, use
  `MetaTable.delete_with_cascade(confirm_cascade_delete=True, ...)` and set
  `override_schema_management_protection=True` when deleting Alembic-managed
  tables. Decide explicitly whether referencing MetaTables and time-indexed Data
  Nodes are also deleted. This permanently deletes catalog rows and physical
  tables; never use it as migration reconciliation or an Alembic downgrade.
- Do not write direct backend migration request bodies in examples. Use the CLI
  and SDK provider APIs.
- Do not call platform-managed model `.register()` in normal application code.
  Registration is reserved for the migration workflow.
- Resolve CodeRepository/Git context once per CLI operation and reuse it for the
  registry reservation, provider reservations, and migration credential.
- Reject a returned registry unless it is a platform-managed,
  Alembic-managed root in `reserved` or `active` state. Do not silently reuse a
  legacy organization-scoped `external_registered` registry.
- Treat `ensure_alembic_registry()` as resolve-or-create. Resolve by DataSource
  UID, physical schema, and physical table name; then validate provider key,
  lifecycle modes, and the empty parent relationship before reuse. Never
  collection-create a compatible registry that already exists.

## Debugging

- If `current` fails before Alembic runs, inspect the provider import path and
  Alembic version MetaTable binding. Confirm the registry reservation is
  `platform_managed` + `alembic_managed`, has no parent registry UID, and
  carries the same CodeRepository/Git context as the migration operation.
- If `revision` autogenerate tries to create everything again, the local
  migration connection cannot see the provider's current physical tables.
- If `upgrade` fails during prepare, inspect provider model identifiers,
  physical table names, and reserved/active MetaTable rows.
- If `upgrade` fails during finalization, inspect whether Alembic created the
  physical tables for every provider-scoped model.
- If an after-register hook reports the wrong model count, ensure it uses the
  provider-scoped context, not a package-global model registry.
