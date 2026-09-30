# ADR 0034: Extract the MetaTables Python API from `mainsequence`

Date: 2026-09-27

Status: Accepted; SDK extraction implemented

## Context

The `mainsequence` distribution currently owns two parts of one MetaTables Python API:

- `mainsequence.meta_tables` contains SQLAlchemy contracts, compiled SQL helpers, hashing and schema naming, time-index references and updater workflows, and client-side Alembic migration providers, registry, scaffolding, and templates.
- `mainsequence.client.metatables` contains the HTTP-facing MetaTable, TimeIndexMetaTable, update, run, DataSource, request, and response models.

Users should be able to install the independent `metatables` distribution and replace `from mainsequence.meta_tables import X` with `from metatables import X`, keeping the same public class, method, and behavior where that interface applies. The independent project also owns its FastAPI service. Its client library and reusable domain code live under `src/metatables/`; `api/` is the service transport boundary.

This ADR governs the **SDK package extraction and removal plan**. It does not authorize removing Django TS Manager, deleting catalog records, or changing physical tables. The SDK branch `metatables_removal` is the place for a later SDK-only removal, after the new package works. Existing released `mainsequence` versions remain available to consumers of the legacy Django service during adoption.

## Decision: destination and import contract

The new Python distribution is named `metatables` and must not require `mainsequence` at runtime. The full source-tree layout is mirrored, rather than only copying a few root exports:

| Current SDK import | Independent package import |
| --- | --- |
| `mainsequence.meta_tables` | `metatables` |
| `mainsequence.meta_tables.<module>` | `metatables.<module>` |
| `mainsequence.meta_tables.time_index_table_updates.<module>` | `metatables.time_index_table_updates.<module>` |
| `mainsequence.meta_tables.migrations.<module>` | `metatables.migrations.<module>` |
| `mainsequence.client.metatables` | `metatables.client.metatables`; its public names are also exported from `metatables` |
| `mainsequence.client.metatables.core` | `metatables.client.metatables.core` |

A user-facing import such as `from metatables import PlatformTimeIndexMetaTable, TimeIndexMetaTable` is the intended common form. The nested paths remain available for callers that currently import a specific submodule. The new project's repository-root Alembic migrations belong to its **service catalog**; `metatables.migrations` contains the mirrored **client-side** Alembic workflows. They must not be conflated.

The port must preserve public names, signatures, callable/class semantics, Pydantic field aliases and validation, return shapes, exceptions, local DataSource behavior, SQLAlchemy metadata, hashing, updater lifecycle, migration provider and template behavior, and package resources. Differences required by the independent service's routing or authentication must be documented at the operation level before a client interface is declared compatible. Keeping a method name while sending it to an unsupported endpoint is not parity.

## Current migration checkpoint

At SDK revision `a91d171ea97bf3d974037a578c53de1262007ff5`, the independent project's working tree contains structural copies of all 29 Python/template paths in the two SDK MetaTables trees. It also contains copies of 20 shared SDK support modules needed to work toward a `mainsequence`-free import graph. The new root initializer declares lazy exports for 45 names from the old `mainsequence.meta_tables` root and 63 names from the HTTP client root. These numbers describe an inventory, not verified functionality.

The source of truth for the file-by-file status is the independent project's `docs/python_package/mirror_manifest.md` and `docs/python_package/public_interface_inventory.md`. At this checkpoint, the client-to-FastAPI operation mapping, shared-helper behavior, dynamic import references, installation, and public-interface verification remain open. In particular, the copied TS HTTP models reject calls until their new-service transport is configured. No claim that the new client is ready for SDK removal follows from file presence.

## Extraction work

1. **Freeze and inventory the source surface.** Compare the full file tree, root and nested exports, public classes and methods, Alembic templates, dynamic import strings, CLI model references, and package assets against the SDK revision being extracted. Record every intentional difference.
2. **Close shared dependencies locally.** Resolve imports of `mainsequence.client.base`, `dtype_codec`, DataSource interfaces, exceptions, model helpers, utilities, value sets, repository context, instrumentation, and logging. Copying a helper is only a starting point; remove its transitive dependence on the SDK and decide which behavior belongs to MetaTables versus the platform. The wheel must install and import without `mainsequence`.
3. **Complete the client transport.** Map every HTTP-facing model operation to the independent FastAPI service, including its authentication, query and body format, pagination, error conversion, and timeout behavior. Keep Django-owned platform lookups, such as the DataSource directory, behind an explicit integration. A model must never silently fall back to Django's TS `/api/v1/` routes when it is supposed to use the independent service.
4. **Preserve the non-HTTP contracts.** Verify SQLAlchemy model registration, compiled SQL, dtype conversion, updater configuration and execution, local data interfaces, migration provider loading, generated Alembic imports, and bundled `.mako` resources. Rewrite literal `mainsequence.*` module references in newly generated artifacts without rewriting already deployed user code silently.
5. **Publish the import migration guide.** Give direct old-to-new examples for root, nested updater, migration, compiled SQL, and HTTP model imports. Document installation, service configuration, and any approved behavioral differences. Consumers of legacy Django TS records can keep a previously released SDK until their service adoption is decided.
6. **Verify the mirror before removal.** Compare exported symbols and signatures, run focused behavior tests and installed-wheel checks, exercise client operations against the FastAPI schema/service, and verify representative updater and migration workflows. Record failures as work to close; structural copying is not a passing gate. Full end-to-end adoption testing follows in the separate testing stage before any Django hard cut.

## SDK branch removal plan

After the independent package passes the mirror gate, the SDK-only change on
`metatables_removal` follows this boundary:

1. Remove the duplicated `mainsequence/meta_tables/` and `mainsequence/client/metatables/` trees from that branch. Do not remove unrelated platform models merely because they are currently imported from a MetaTables module.
2. Resolve every outside-tree dependency before deleting those trees. At the source revision above, these include `mainsequence/client/__init__.py` star exports and constants, `client/base.py` TS endpoint entries, `client/utils.py` constants, `client/models_foundry.py` DataSource/update imports and `TimeScaleDB` inheritance, and `cli/cli.py`, `cli/api.py`, and `cli/migrations.py` commands and dynamic model paths. Preserve unrelated client and CLI behavior; move or retire TS commands deliberately, with the replacement workflow documented.
3. Classify the existing SDK tests that reference old imports: move parity cases to the new project, retain tests for non-TS SDK behavior, and remove only tests for intentionally retired SDK surfaces. The removal branch must have no import-time dependency on deleted modules.
4. Update SDK knowledge pages, CLI/reference docs, package discovery and dependencies, and the release migration note in the same branch. Historical ADRs remain as decision history and should be annotated where the new ownership supersedes their SDK-location guidance.
5. Review the SDK branch diff and installed package behavior, then coordinate release order: usable independent `metatables` package first, consumer migration instructions second, SDK release without duplicated code later. The old published SDK may remain installed for legacy service consumers during parallel adoption.

The target project's consumer inventory identifies seven outside-tree SDK client/CLI files and 18 SDK test files with direct old-package references at the recorded source revision. That inventory is the removal checklist, not proof that every dynamic reference has been found. Search imports, string-based module references, generated files, package metadata, documentation, and downstream consumers again immediately before removal.

## Boundaries and consequences

- This ADR changes package ownership, not Django's runtime or data ownership. The later Django removal has its own stage and approval gate.
- The public Python interface is the compatibility target. The independent service can have a different HTTP wire contract, provided the new client's documented Python behavior is preserved or an exception is explicitly accepted.
- The `metatables_removal` branch is not ready merely because source files have been copied. Removal waits for a working independent package, documented consumer migration, and verification of the shared SDK code that will remain.
- This ADR itself makes no code change and deletes no SDK module. The SDK removal is assigned to a separate implementation pass.

## Implementation clarification

The extraction removes only MetaTables-owned Python code and its direct SDK
control surfaces: MetaTable and TimeIndexTable client models, DataSource and
local table-storage interfaces, dtype/table serialization, updater workflows,
and SDK-owned Alembic execution. It does **not** authorize a generally thin SDK
or removal of unrelated platform adapters.

Agent and AgentSession operations, A2A execution, CodeRepository lifecycle and
local development, jobs and runs, images, resources and releases, constants,
secrets, teams, sharing, observability, diagnostics, scaffold installation,
logging, and tracing remain SDK responsibilities unless a separate accepted ADR
changes their ownership.
