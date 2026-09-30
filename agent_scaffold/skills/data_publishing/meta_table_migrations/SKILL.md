---
name: mainsequence-metatable-migrations
description: "Use for application-owned MetaTable schema evolution: defining provider scope, scaffolding a provider, authoring Alembic revisions, invoking approved migration execution, or diagnosing reservation and finalization failures. The Main Sequence SDK owns this workflow guidance; the installed metatables package owns the executable migration implementation and its version-specific command surface."
---

# Main Sequence MetaTable Migrations

## Authority

Use this skill for an application's MetaTable migration lifecycle. The
`mainsequence` scaffold owns the cross-component workflow and source-context
rules. The installed `metatables` package owns SQLAlchemy models, migration
providers, Alembic templates, CLI implementation, and MetaTables API contracts.

Check the installed `metatables` version and its `metatables migrations --help`
output before using a command. Do not restore or recommend the retired
`mainsequence migrations` CLI.

For table modeling, external registration, queries, or updater implementation,
use the corresponding skill shipped by `metatables`. API catalog/system
migrations are a separate operational history and are not application-provider
revisions.

## Source And Runtime Context

- Treat the current checkout as source identity. Never ask the user to select a
  CodeRepository branch or Organization Environment for a migration.
- Hosted execution uses the API deployment's resolved runtime and DataSource.
  Do not accept a caller-supplied DataSource, database URL, revision path, or
  branch override.
- An explicit `METATABLES_API_URL` selects a MetaTables API endpoint, not a
  branch, Environment, or storage binding.
- Provider aliases accepted by hosted execution are deployment configuration,
  not arbitrary Python import paths supplied over the API.

## Author The Migration

1. Define the SQLAlchemy table models and stable physical names with the
   installed `metatables` package.
2. Identify one provider module, migration namespace, target `MetaData`, model
   registry, and prefixed Alembic version-table binding.
3. Keep provider scope explicit. Do not scan all imported models or installed
   packages.
4. Scaffold only when the application does not already have a provider:

```bash
metatables migrations scaffold \
  --package ledger \
  --module ledger.migrations \
  --namespace ledger \
  --base ledger.tables:Base \
  --metadata ledger.tables:Base.metadata
```

5. Edit the generated registry so it returns exactly the models owned by that
   migration stream. Scaffolding alone does not select models or create tables.
6. Create and review a normal offline Alembic revision:

```bash
metatables migrations revision \
  --provider ledger.migrations:migration \
  --no-autogenerate \
  --message "create ledger"
```

Use `--source-root` and `--code-repository-root` when the application does not
use the default `src/` layout. Keep applied revisions immutable; add a new
revision for every later schema change.

## Execute Through The Approved Runtime

General client-side `current`, `upgrade`, `downgrade`, direct migration
connections, and database autogeneration are not supported. A local
`--sqlalchemy-url` does not bypass that boundary.

Application migrations execute inside the selected MetaTables API through its
approved application-migration workflow. The deployment must already contain
the reviewed provider code and map a public provider alias to that code. The API
selects its active runtime DataSource, reserves the provider catalog rows, runs
Alembic, and finalizes the physical bindings. Application code supplies the
approved alias only; it does not upload revisions or select storage.

Use the application's tested setup entry point when one exists. Do not replace
it with direct model registration or an undocumented request body.

## Lifecycle Invariants

- Managed provider tables and the Alembic registry are
  `platform_managed` + `alembic_managed`.
- Reservation precedes physical migration; successful reconciliation moves the
  catalog rows from `reserved` to `active`.
- The registry is the provider root and has no parent. Provider tables bind to
  that registry.
- Physical identity is DataSource UID + physical schema + physical table name.
  A logical identifier or contract hash does not replace that identity.
- Repeated setup must reuse compatible bindings and applied revision history.
- `.register()` is lifecycle plumbing, not the ordinary application table
  creation path. External registration cannot substitute for a managed
  migration registry.

## Failure Handling

- If provider loading fails, verify the import path, model registry, metadata,
  version-table binding, and installed `metatables` version.
- If reservation fails, compare provider scope and physical identities before
  changing code. Do not create a second logical row for the same table.
- If Alembic succeeds but finalization fails, physical DDL may already be
  committed. Inspect every per-table result and the actual schema before retrying.
- Treat an uncertain migration journal result as reconciliation work. Do not
  blindly retry, stamp, downgrade, cascade-delete, or register around it.
- Destructive catalog deletion is not migration recovery and never bypasses
  schema-management protection.

Verify provider scope, revision content, approved-provider configuration,
repeat execution, and final active bindings. Report separately what was checked
offline and what was exercised against a configured API.
