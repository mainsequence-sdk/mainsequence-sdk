# ADR 0026: SDK-Owned Migration Scaffolding And Helpers

Date: 2026-06-07

Status: Proposed

## Context

ADR 0020, ADR 0022, and ADR 0023 moved MetaTable schema evolution to Alembic,
with the SDK acting as a thin adapter around provider-scoped MetaTable
reservation, Alembic execution, and catalog finalization.

The `ms-markets` project now shows the next source of complexity. A normal
project migration package still has to hand-roll a lot of boilerplate:

- `src/migrations/__init__.py` defines namespace slugging, active namespace
  version locations, the Alembic version MetaTable class, and the provider
  object.
- `src/migrations/registry.py` filters, validates, and deduplicates provider
  MetaTable models.
- `src/migrations/env.py` wires Alembic to the SDK provider, target metadata,
  schema filters, include hooks, scoped connection, and migration role.
- `src/migrations/script.py.mako` repeats the same Alembic revision template.
- `src/migrations/versions/<namespace_slug>/` is derived from the configured
  migration namespace.
- Dynamic one-model providers, such as portfolio example storage migrations,
  need the same provider machinery but a different provider model scope.

Those pieces are mostly SDK conventions, not project business logic. Keeping
them in every project makes migrations harder to use and easier to miswire.
For example, a provider-scoped hook can accidentally import a broader global
registry than the provider being migrated. The SDK now passes
`context.metatable_models` and `context.registered_metatables` to hooks so
project hooks can use the exact provider scope, but the scaffolding still makes
it too easy to duplicate or bypass SDK conventions.

## Decision

The SDK should own migration scaffolding and common migration helper functions.

Create a dedicated MetaTable migration module, separate from backend client
MetaTable models but still inside the MetaTable SDK namespace. Migration-related
provider types, Alembic helpers, scaffolding, templates, and utility functions
should live under this module because the workflow is specific to
platform-managed MetaTables.

Target public import shape:

```python
from mainsequence.meta_tables.migrations import (
    AlembicMetaTableMigration,
    AlembicVersionMetaTable,
    build_metatable_model_registry,
    build_metatable_migration_provider,
    metadata_for_models,
    namespace_version_location,
    namespace_version_slug,
)
```

Project code should define models and project-specific naming policy. The SDK
should provide the migration wiring.

## SDK-Owned Module

Add a package such as:

```text
mainsequence/meta_tables/migrations/
  __init__.py
  provider.py
  registry.py
  alembic.py
  scaffold.py
  templates/
    env.py
    script.py.mako
```

Responsibilities:

- `provider.py`: `AlembicMetaTableMigration`, `AlembicVersionMetaTable`,
  `AlembicMetaTableCatalogRefreshContext`, provider construction helpers.
- `registry.py`: provider model filtering, identifier validation, and
  deduplication helpers.
- `alembic.py`: Alembic config helpers, namespace version path helpers, include
  hooks, role application, and metadata selection helpers.
- `scaffold.py`: filesystem scaffolding for project migration packages.
- `templates/`: SDK-owned `env.py` and `script.py.mako`.

Migration APIs should not live inside `mainsequence.client.metatables`. That
package is for backend client models and request/response DTOs. Migration
workflow code should remain under `mainsequence.meta_tables.migrations`, but
the current single-file module should become a package with focused submodules.

## Helper Functions

The SDK should provide the generic helpers currently duplicated by
`ms-markets`.

### Namespace Version Paths

```python
namespace_version_slug(namespace: str | None) -> str
namespace_version_location(
    namespace: str | None,
    *,
    prefix: str = "migrations:versions",
) -> str
```

Behavior:

- empty namespace maps to `default`;
- non-alphanumeric runs normalize to `_`;
- long slugs are shortened with a deterministic hash suffix;
- output is stable across machines and runs.

This replaces project-local helpers such as `namespace_version_slug()` and
`active_namespace_version_location()`.

### Provider Model Registry

```python
build_metatable_model_registry(
    *sources: Iterable[type[Any]] | Callable[[], Iterable[type[Any]]],
    base: type[Any] | None = None,
) -> list[type[Any]]
```

Behavior:

- accepts model lists or callables returning models;
- keeps only `PlatformManagedMetaTable` and `PlatformTimeIndexMetaTable`
  subclasses;
- optionally requires subclassing a project base such as `MarketsBase`;
- requires every included model to have a non-empty identifier;
- deduplicates by model object and validates duplicate identifiers;
- preserves source order.

This replaces project-local registry filtering and deduping.

### Provider Factory

```python
build_metatable_migration_provider(
    *,
    package: str,
    migration_namespace: str,
    target_metadata: Any,
    alembic_registry: type[AlembicVersionMetaTable],
    metatable_models: Sequence[type[Any]],
    script_location: str = "migrations:",
    version_location_prefix: str = "migrations:versions",
    after_register_metatables: Callable[[AlembicMetaTableCatalogRefreshContext], Any] | None = None,
) -> AlembicMetaTableMigration
```

Behavior:

- derives `version_locations` and `version_path` from `migration_namespace`;
- validates and normalizes provider models;
- wires the provider for normal CLI commands;
- supports both full package providers and one-model dynamic providers.

Projects should not need to manually calculate `version_locations` and
`version_path`.

### Alembic Version Table Factory

```python
build_alembic_version_metatable(
    *,
    class_name: str = "ProjectAlembicVersion",
    namespace: str,
    identifier: str,
    schema: str | None,
    table_name: str,
    column_name: str = "version_num",
) -> type[AlembicVersionMetaTable]
```

This removes the repeated hand-written subclass where the only meaningful
inputs are namespace, identifier, schema, table name, and version column.

Projects may still write a subclass directly when they need normal Python
class-level clarity.

### Metadata Selection

```python
metadata_for_models(models: Sequence[type[Any]]) -> MetaData
```

Behavior:

- creates an isolated SQLAlchemy `MetaData` containing only selected model
  tables;
- preserves table names, schema, columns, indexes, and naming conventions where
  SQLAlchemy supports safe copying;
- is intended for dynamic providers that migrate one or a few configured
  storage tables.

This helper must be implemented carefully because SQLAlchemy table copying can
lose or alter constraints if done naively. It should have dedicated tests for
columns, indexes, time-index unique grain indexes, schema, and `info`.

### Alembic Env Helpers

SDK-owned `env.py` should support:

- provider lookup through `context.config.attributes["mainsequence_migration_provider"]`;
- fallback import of the local provider object only in the scaffolded project
  file;
- target metadata from `migration.target_metadata`;
- provider `include_name` and `include_object` hooks;
- SDK role application through `apply_mainsequence_migration_role`;
- online execution with SDK-provided connection when present;
- online execution from `sqlalchemy.url` for direct Alembic compatibility;
- offline execution.

Project `env.py` should be thin or generated. It should not duplicate the SDK
implementation.

## CLI Scaffolding

Add a CLI command that writes the migration package skeleton:

```bash
mainsequence migrations scaffold \
  --package msm \
  --module migrations \
  --namespace mainsequence.examples \
  --base msm.base:MarketsBase \
  --metadata msm.base:MarketsBase.metadata \
  --models migrations.registry:metatable_provider_models \
  --alembic-version-name MarketsAlembicVersion
```

The command should create:

```text
src/migrations/
  __init__.py
  registry.py
  env.py
  script.py.mako
  versions/
    <namespace_slug>/
      __init__.py
```

Scaffolded files should be small:

- `__init__.py` imports SDK helpers and declares project inputs;
- `registry.py` lists project model sources and calls
  `build_metatable_model_registry(...)`;
- `env.py` delegates to SDK Alembic env helpers;
- `script.py.mako` is copied from the SDK template;
- version directories are derived from the namespace slug helper.

The scaffold command should be idempotent:

- create missing files/directories;
- refuse to overwrite changed existing files unless `--force` is passed;
- print exactly which files were created, skipped, or overwritten.

## Project-Owned Responsibilities

The SDK should not own:

- project model definitions;
- project table naming policy, such as `markets_table_name(...)`;
- project identifier policy, such as `markets_identifier(...)`;
- business-specific dynamic model construction;
- catalog refresh semantics;
- the list of model source functions for a package.

The project should own only the inputs that are genuinely project-specific.

## Hook Contract

`after_register_metatables` hooks must use the provider-scoped context:

```python
def refresh_catalog(context):
    models = context.metatable_models
    registered = context.registered_metatables
    by_model = dict(zip(models, registered, strict=True))
```

Hooks must not import a broader registry and compare it against the current
provider. A full package provider and a one-model dynamic provider are both
valid providers, and the hook must respect the provider scope it receives.

## Skills And Documentation

The current MetaTable skill should remain focused on MetaTable authoring:

- choosing `PlatformManagedMetaTable` vs `PlatformTimeIndexMetaTable`;
- declaring SQLAlchemy columns, indexes, cadence, descriptions, labels, and
  identifiers;
- using stable naming helpers such as `schema_table_name(...)`;
- understanding runtime binding and read/write behavior.

Migration workflow guidance is now large enough to deserve a dedicated
MetaTable migration skill. That skill should cover:

- when to create a migration provider;
- using `mainsequence migrations scaffold`;
- declaring provider inputs with SDK helpers;
- building full-package providers and one-model dynamic providers;
- using `metadata_for_models(...)` for scoped dynamic providers;
- generating revisions and running `current`, `revision`, and `upgrade`;
- writing or reviewing Alembic revision files;
- using provider-scoped `context.metatable_models` and
  `context.registered_metatables` inside hooks;
- diagnosing prepare/finalize failures without falling back to direct
  `MetaTable.register()` calls.

The MetaTable skill should link to the migration skill instead of duplicating
the full migration workflow. This keeps table authoring guidance short while
giving migrations their own operational checklist.

## Consequences

Benefits:

- new projects get a smaller migration setup;
- namespace-specific version directories become consistent;
- dynamic one-model providers become first-class;
- the SDK can test Alembic env behavior once instead of relying on each project;
- hook scope bugs become less likely because provider-scoped context is the
  normal path.

Costs:

- migration code needs to be reorganized from one MetaTable migration file into
  a MetaTable migration package;
- docs and examples must be updated to use SDK helpers;
- scaffolded projects need a clear ownership boundary between generated
  boilerplate and project inputs.

## Migration Plan

- Convert `mainsequence.meta_tables.migrations` into the dedicated MetaTable
  migration package and move workflow code into focused submodules.
- Add SDK namespace, registry, provider, metadata, and Alembic env helpers.
- Add SDK-owned `env.py` and `script.py.mako` templates.
- Add `mainsequence migrations scaffold`.
- Update tutorials, knowledge docs, and examples to use the helper-based
  provider shape.
- Add a dedicated MetaTable migration skill and update the existing MetaTable
  skill to delegate migration workflow guidance to it.
- Refactor `ms-markets` to replace local namespace slugging, registry dedupe,
  env.py internals, and revision template with SDK helpers.

## Implementation Tasks

- [x] Convert `mainsequence/meta_tables/migrations.py` into a
  `mainsequence/meta_tables/migrations/` package.
- [x] Move `AlembicMetaTableMigration`, `AlembicVersionMetaTable`,
  `AlembicMetaTableCatalogRefreshContext`, and Alembic config helpers into the
  new module.
- [x] Add `namespace_version_slug()` and `namespace_version_location()`.
- [x] Add `build_metatable_model_registry()`.
- [x] Add `build_metatable_migration_provider()`.
- [x] Add `build_alembic_version_metatable()`.
- [x] Add `metadata_for_models()` with tests for dynamic providers.
- [x] Add SDK-owned Alembic `env.py` and `script.py.mako` templates.
- [x] Add `mainsequence migrations scaffold`.
- [x] Add a dedicated MetaTable migration skill.
- [x] Update the existing MetaTable skill to link to the migration skill for
  migration workflow tasks.
- [x] Update migration tutorial, knowledge docs, and examples.
- [ ] Refactor `ms-markets` migration package to consume SDK helpers.
