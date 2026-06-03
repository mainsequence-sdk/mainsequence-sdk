# ADR 0019: Class-Based MetaTable Foreign Keys

Date: 2026-05-31

Status: Accepted

## Context

Platform-managed MetaTable registration has a lifecycle boundary that ordinary
SQLAlchemy foreign keys do not understand.

SQLAlchemy accepts foreign-key targets as strings or column objects, for example:

```python
ForeignKey(Account.__table__.c.uid, ondelete="RESTRICT")
```

That is a valid SQLAlchemy declaration, but it is the wrong public abstraction
for platform-managed MetaTables. It exposes the SQLAlchemy table object as if it
were the platform storage identity. In this SDK, the platform storage identity is
the registered `MetaTable.uid`; `Model.__table__` is local SQLAlchemy authoring
state, not the public foreign-key contract.

The SDK currently needs a target `MetaTable.uid` in the foreign-key contract.
Using SQLAlchemy table fullnames or table column objects in public examples has
three problems:

- table names and fullnames are not the platform foreign-key identity
- column objects do not prove that the target MetaTable model has been
  registered
- callers are forced into manual sequential registration and manual target UID
  mapping, even though `register()` is already the SDK lifecycle method

The backend already supports the required contract shape. No new backend
endpoint is needed. The missing piece is an SDK-owned foreign-key declaration
that captures target MetaTable model intent and lets `register()` resolve the
target `MetaTable.uid`.

## Decision

Add an SDK-owned SQLAlchemy-compatible helper named `MetaTableForeignKey`.

Public usage:

```python
from mainsequence.meta_tables import MetaTableForeignKey, PlatformManagedMetaTable


class Account(PlatformManagedMetaTable, Base):
    __metatable_namespace__ = "tutorial"
    __metatable_identifier__ = "tutorial.account"
    __metatable_description__ = "Accounts that own asset positions."

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)


class AccountPosition(PlatformManagedMetaTable, Base):
    __metatable_namespace__ = "tutorial"
    __metatable_identifier__ = "tutorial.account_position"
    __metatable_description__ = "Positions keyed by owning account."

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    account_uid: Mapped[uuid.UUID] = mapped_column(
        Uuid,
        MetaTableForeignKey(Account, column="uid", ondelete="RESTRICT"),
        nullable=False,
    )
```

`MetaTableForeignKey(Account, column="uid", ...)` declares:

- the target is the `Account` MetaTable authoring model class
- the target column is `uid`
- SQLAlchemy should still see a normal local foreign-key relationship
- the MetaTable registration contract must use the registered `Account`
  `MetaTable.uid`, not `Account.__table__.fullname`

`MetaTableForeignKey` may be implemented as a function or constructor-style
class, but it must return a SQLAlchemy-compatible `ForeignKey` object for use in
`mapped_column(...)`. It must not monkey-patch or shadow `sqlalchemy.ForeignKey`.
The explicit SDK name is important because this is platform MetaTable behavior,
not generic SQLAlchemy behavior.

## Target Type

The target argument is a MetaTable authoring model class, not a backend
`MetaTable` client object.

The cached registration object on authoring models is typed as:

```python
__metatable__: ClassVar[MetaTable | None]
```

`MetaTableForeignKey` targets the class that owns that cache and registration
method, for example `Account`, not `Account.__metatable__`.

The public type should be compatible with `PlatformManagedMetaTable` subclasses,
including `PlatformTimeIndexMetaData` because it inherits the same registration
path.

## Registration Semantics

`PlatformManagedMetaTable.register()` remains the only public lifecycle path.

When registering a model, the SDK should:

1. inspect the model for `MetaTableForeignKey(...)` declarations
2. collect the target authoring model classes and target columns
3. recursively register unregistered target models before building the child
   contract
4. use each returned target `MetaTable.uid` in the child foreign-key contract
5. register the child model
6. bind each model only from the backend `MetaTable` returned by registration

Recursive registration must be get-or-create safe because platform registration
is keyed by the stable logical storage identity. Calling `Account.register()`
from `AccountPosition.register()` should be safe when `Account` already exists.

Recursive registration must not propagate child-specific identity fields to the
parent. The parent must resolve its own class-level namespace, identifier,
description, hash namespace, and extra hash components. Shared runtime inputs
such as `data_source`, `data_source_uid`, timeout, and compatible provisioning
options may be propagated.

If a target model is already bound, the SDK should use its cached
`MetaTable.uid`. If it is not bound, the SDK should call the target model's
registration method. If the target cannot be registered or resolved, child
registration must fail before sending a malformed contract.

## Local Process Registration Registry

Recursive registration must use a process-local registry keyed by
`storage_hash`.

The registry shape should be conceptually:

```python
{
    storage_hash: RegistrationState(
        status="in_progress" | "registered",
        model=TargetModel,
        meta_table=MetaTable | None,
        stack=[...],
    )
}
```

Before `register()` sends a backend registration request, it must compute the
model's `storage_hash` and check the local registry:

- if the storage hash is already `registered`, reuse the cached `MetaTable`,
  bind the current authoring model to it if needed, and return it
- if the storage hash is `in_progress` in the current recursive stack, raise a
  clear cycle error with the model/storage path
- if the storage hash is absent, insert an `in_progress` entry, recursively
  resolve/register FK target models, register the current model through the
  backend get-or-create path, then replace the entry with the returned
  `MetaTable`

This registry prevents duplicate registration work inside one Python process.
It also gives recursive registration a precise place to distinguish "already
registered locally" from "currently being registered and therefore cyclic".

The registry is only a local optimization and recursion guard. It is not a
persistent cache and must not replace the backend's get-or-create semantics.
Failed registrations must remove their `in_progress` entry so later attempts are
not poisoned by stale state.

Circular dependencies are not part of this decision. The implementation should
detect registration cycles through the local registry and raise a clear error
instead of attempting partial two-phase registration.

## Contract Extraction

The SDK foreign-key contract builder must prefer SDK metadata captured by
`MetaTableForeignKey`.

The contract source of truth is:

- target authoring model class
- target column name
- registered target `MetaTable.uid`
- local SQLAlchemy constraint metadata such as local columns, `ondelete`,
  `onupdate`, and the constraint name

The contract source of truth is not:

- `Account.__table__.fullname`
- `Account.__table__.name`
- a manually constructed string such as `"public.account.uid"`
- the post-registration physical table name

Internally, `MetaTableForeignKey` may use `Account.__table__.c["uid"]` to satisfy
SQLAlchemy's local relationship machinery. That use is private implementation
detail. Public docs, tutorials, and skills should not teach users to write it.

Platform-managed foreign-key contracts must not include physical constraint
names. Contract extraction must not derive names from local SQLAlchemy table or
column names, and any transient SQLAlchemy FK name must stay out of the
MetaTable contract. The SDK sends logical relationship intent only: source
columns, target MetaTable UID, target columns, and delete behavior. Alembic,
SQLAlchemy, and the database own the actual foreign-key constraint name; the
MetaTable backend does not manage it.

## Legacy Raw Target Paths

Platform-managed MetaTable foreign-key registration should not keep a second raw
target path based on SQLAlchemy table fullnames or caller-supplied target UID
maps. The SDK-owned path is `MetaTableForeignKey(TargetModel, column=...)`, and
registration resolves the target `MetaTable.uid` from the target model class.

## Non-Goals

This ADR does not add a backend endpoint.

This ADR does not make `ForeignKey(Account)` valid by patching SQLAlchemy.

This ADR does not make manual binding public. Registration remains the only
public lifecycle path.

This ADR does not solve circular foreign-key registration. Cycles should be
detected and rejected until a deliberate two-phase contract flow exists.

## Implementation Tasks

- [x] Add `MetaTableForeignKey` to `mainsequence.meta_tables`.
- [x] Type the helper target as a platform-managed MetaTable authoring model class,
  not as a backend `MetaTable` object.
- [x] Have the helper validate that `column` exists on the target model table at
  declaration or contract-build time with a clear error.
- [x] Have the helper attach SDK metadata to the SQLAlchemy FK object or constraint:
  target model class and target column name, without SDK-resolved physical naming
  metadata.
- [x] Keep the returned object compatible with `mapped_column(..., ForeignKey-like)`.
- [x] Preserve normal SQLAlchemy relationship behavior by internally constructing a
  valid SQLAlchemy foreign-key target column.
- [x] Update foreign-key contract extraction to prefer `MetaTableForeignKey`
  metadata over table fullname lookup.
- [x] Update registration to recursively resolve and register target model classes
  discovered from `MetaTableForeignKey`.
- [x] Add a process-local registration registry keyed by `storage_hash`.
- [x] Have `register()` check the local registry before calling backend
  registration.
- [x] Reuse locally registered `MetaTable` objects for repeated registrations of
  the same storage hash.
- [x] Track `in_progress` registration states in the local registry.
- [x] Remove failed `in_progress` entries so failed registrations do not poison
  later attempts.
- [x] Add cycle detection for recursive registration using the local registry.
- [x] Ensure recursive registration does not pass child namespace, identifier,
  description, hash namespace, or extra hash components to parent models.
- [x] Resolve target `MetaTable.uid` from the target model's returned or cached
  `MetaTable`.
- [x] Remove raw `target_meta_table_uid_by_fullname` from the platform-managed
  FK registration path.
- [x] Make missing target registration, missing target column, unresolved target UID,
  and registration cycles fail with direct errors.
- [x] Add tests showing parent registration is invoked before child registration.
- [x] Add tests showing child FK contracts contain the parent `MetaTable.uid`.
- [x] Add tests showing parent table physical-name mutation after registration does
  not break child FK contract generation.
- [x] Add tests showing `MetaTableForeignKey(Account, column="uid")` works without
  the caller touching `Account.__table__.fullname` or `Account.__table__.c.uid`.
- [x] Add tests showing platform-managed FK contracts omit names.
- [x] Add tests showing explicit `name=...` is rejected.
- [x] Add tests for cycle detection.
- [x] Add tests proving platform-managed FK registration does not require an
  explicit target UID mapping.
- [x] Update `docs/knowledge/meta_tables/sqlalchemy.md` to teach
  `MetaTableForeignKey(Account, column="uid", ...)`.
- [x] Update `docs/tutorial/working_with_meta_tables.md` with a complete parent and
  child registration example.
- [x] Update `docs/tutorial/creating_a_simple_data_node.md` anywhere it shows
  MetaTable-backed FK storage.
- [x] Update `agent_scaffold/skills/data_publishing/meta_tables/SKILL.md` to state
  that platform-managed FKs must use `MetaTableForeignKey(TargetModel, ...)`.
- [x] Update `agent_scaffold/skills/data_publishing/data_nodes/SKILL.md` to point
  storage-schema FK work to the MetaTable skill and use the same language when a
  DataNode example includes MetaTable storage.
- [x] Remove public skill/tutorial examples that use
  `ForeignKey(f"{Account.__table__.fullname}.uid")`.
- [x] Remove public skill/tutorial examples that use
  `ForeignKey(Account.__table__.c.uid)`.
- [x] Document that FK declarations should include table descriptions through
  `__metatable_description__` on every participating table.
- [x] Document that `MetaTable.uid` is the platform FK target identifier and that
  `storage_hash`, data source UID, table fullname, and physical table name must
  not be used as public FK identifiers.
