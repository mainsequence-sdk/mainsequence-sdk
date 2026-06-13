# ADR 0028: MetaTable Storage Hash As Utility

Date: 2026-06-13

Status: Proposed

## Context

Earlier MetaTable ADRs and SDK docs treated `storage_hash` as a first-class
MetaTable identity. That made sense while platform-managed table creation and
DataNode storage were converging on one storage abstraction, but it now leaks
implementation machinery into the user-facing model.

For Alembic-managed MetaTables, the user-authored SQLAlchemy table name is the
physical database table name. Alembic evolves that table over time. The stable
platform reference is the MetaTable `uid`, and the stable human/application
reference is the logical `identifier`.

A shape-derived `storage_hash` has different semantics. It changes when
storage-relevant schema changes. That makes it useful as a deterministic
fingerprint, but risky as table identity in an Alembic lifecycle:

- Alembic means "same table, new schema revision".
- A schema-derived hash suggests "new storage identity".
- Exposing both as first-class user concepts makes migration behavior harder to
  reason about.
- The current configured hash path does not include the authored table name, so
  two same-shaped tables in the same namespace/schema can collide unless users
  remember to add `__metatable_extra_hash_components__`.

The backend is expected to remove `storage_hash` from the MetaTable resource
contract. The SDK should follow that direction instead of preserving
`storage_hash` as a required client-model field.

The existing hashing machinery is still valuable. Users may want a deterministic
contract fingerprint for validation, drift detection, cache keys, or custom
stability checks. That use case should remain available as an explicit utility,
not as a core MetaTable property.

## Decision

Remove `storage_hash` from user-facing and backend-facing MetaTable client
models as the backend contract removes it.

The SDK should not require, expose, filter by, or document `storage_hash` as a
MetaTable model field. This applies to:

- `MetaTable`
- `TimeIndexMetaTable`
- MetaTable registration request models
- managed reservation/finalization request and response models
- DataNode storage client paths that currently carry `remote_table_hash_id`
  only for compatibility

The authoritative identities become:

- `uid`: public platform identity for permissions, compiled SQL scope, sharing,
  and API references;
- `identifier`: stable logical application identity, globally unique where the
  backend enforces it;
- `physical_table_name`: SQL/Alembic/database binding;
- `data_source_uid`: storage connection ownership.

Keep deterministic hashing as an opt-in utility. The utility should produce a
contract or storage fingerprint for callers that explicitly ask for it, but the
result must not be a required MetaTable attribute.

The utility must include the physical table name by default. If two tables have
the same shape but different authored table names, the default utility output
must differ.

## Utility Shape

Expose a function with intent-focused naming, for example:

```python
from mainsequence.meta_tables import compute_metatable_contract_hash
```

The exact name can change during implementation, but the public API should make
clear that the value is a computed fingerprint, not platform identity.

Recommended behavior:

```python
compute_metatable_contract_hash(
    model_or_table,
    *,
    namespace: str | None = None,
    schema: str | None = None,
    include_table_name: bool = True,
    extra_components: Mapping[str, Any] | None = None,
) -> str
```

Default hash inputs:

- namespace, when provided or declared on the model;
- schema;
- physical table name;
- normalized column storage identity;
- normalized index storage identity;
- normalized foreign-key storage identity;
- time-index profile for `PlatformTimeIndexMetaTable` models;
- optional caller-provided extra components.

The utility may keep using the existing deterministic serialization and
PostgreSQL-safe shortening machinery. It should not reuse API names that imply
backend storage identity, such as `storage_hash`, unless they are retained only
as deprecated aliases.

## SDK Contract Changes

Client DTOs should stop requiring `storage_hash`.

Registration requests for Alembic-managed and platform-managed MetaTables should
send:

- data source UID;
- identifier;
- namespace;
- description;
- labels;
- physical table name through the table contract;
- table contract;
- schema-management metadata where applicable.

Reservation and finalization should resolve rows by backend-supported stable
identity:

- existing `uid` where known;
- logical `identifier` for provider-scoped model reconciliation;
- physical table name plus data source when resolving the database binding.

The backend, not the SDK hash, owns uniqueness and reconciliation rules for
MetaTable rows.

Compiled SQL scope should continue using `meta_table_uid`. SQL strings should
continue targeting the authored physical table names produced by SQLAlchemy and
Alembic.

## DataNode Consequences

DataNodes already moved most storage linkage to MetaTable UID. That direction
should continue.

Remove or replace compatibility fields and names that imply hash-based storage
identity:

- `remote_table_hash_id`
- `storage_hash` constructor arguments used to carry physical table names
- local fallback paths that prefer `storage_hash` after `physical_table_name`

When a DataNode or APIDataNode needs to bind storage, it should use a
`TimeIndexMetaTable` UID, logical identifier, or physical table name depending
on the workflow. A parameter carrying a physical table name should be named
`physical_table_name` or `table_name`, not `storage_hash`.

## Migration And Compatibility

This is a breaking client-model cleanup aligned with a backend contract removal.
The implementation may need a short compatibility window:

- accept backend responses that still include `storage_hash`;
- do not require callers to pass `storage_hash`;
- do not emit `storage_hash` in new SDK request payloads once backend support is
  available;
- keep deprecated aliases only when needed for existing user code, and make them
  forward to the utility or to `physical_table_name` as appropriate.

Compatibility aliases must not preserve the old mental model. Documentation,
examples, and skills should stop describing `storage_hash` as MetaTable identity.

## Consequences

### Positive

- Alembic-managed tables have one clear identity model: `uid`, `identifier`, and
  physical table name.
- Schema evolution no longer implies a new table identity just because a
  fingerprint changed.
- Users no longer need to learn `__metatable_extra_hash_components__` to avoid
  same-shape table collisions in normal authored-table workflows.
- Hashing remains available for explicit stability and drift-detection use
  cases.
- Client models align with the backend's planned MetaTable contract.

### Negative

- Existing code that reads or filters by `storage_hash` must migrate.
- Tests, docs, CLI output, and DataNode compatibility fields need broad cleanup.
- During transition, SDK code may need to accept both old and new backend
  payloads.
- Any backend code that used `storage_hash` for reconciliation must choose a
  replacement key, usually `uid`, `identifier`, or physical table name plus data
  source.

## Required Updates

- Remove `storage_hash` from MetaTable and TimeIndexMetaTable client models once
  backend responses no longer include it.
- Remove `storage_hash` from registration, reservation, and finalization request
  and response DTOs.
- Replace SDK runtime usage with `uid`, `identifier`, or `physical_table_name`
  according to the workflow.
- Rename APIDataNode and DataNode compatibility parameters that actually carry
  physical table names.
- Add a first-class hashing utility that includes physical table name by
  default.
- Deprecate existing hash builder names that imply backend storage identity.
- Update docs, tutorials, examples, and skills to describe hash output as an
  optional contract fingerprint.
