# Changelog

All notable changes to this project should be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows versioned releases.

## [Unreleased]

## [4.0.2] - 2026-05-25

### Added

- Added ADR `0009-cli-public-resource-identity` to define CLI-wide canonical public identifier rules by object family and to record that `Workspace` and `AgentSession` remain numeric until their client contracts migrate.

### Changed

- Migrated project-facing CLI arguments, local env write-paths, and current-project detection to prefer `project_uid` and `MAIN_SEQUENCE_PROJECT_UID`.
- Updated local project setup, token refresh, sync, and project resolution helpers to operate on public project references while keeping internal numeric compatibility adapters only where older client filters still require backend row ids.
- Aligned `Project` client helpers and quick-search models with the public UID contract used by the CLI migration.

## [4.0.1] - 2026-05-25

### Fixed

- Fixed CLI login persistence on macOS by verifying secure-store readback after login and falling back to backend-scoped local CLI auth storage when keychain readback is not usable in later CLI processes.
- Fixed cross-backend CLI auth collisions by scoping persisted auth entries to the active backend instead of one global shared token slot.

### Changed

- Updated CLI current-user profile enrichment to use `GET /user/api/user/get_user_details/` instead of deriving a backend user id from `/auth/rest-auth/user/`.

## [4.0.0] - 2026-05-25

### Added

- Added MetaTable client contracts for row-oriented relational application data, including registration, contract validation, introspection, governed compiled SQL operations, labels, and sharing.
- Added MetaTable tutorial and ADR guidance for backend-managed tables, governed SQL payloads, and future CLI parity.
- Added `DataNodeStorage.delete_after_date(...)` to call the dynamic-table tail-delete endpoint using POST and return authoritative post-delete table stats.

### Changed

- Released the SDK as version 4.0.0 to mark the shift to a general-purpose MainSequence platform SDK for several application domains instead of a domain-specific client package.
- Standardized row-oriented application data around MetaTables and DataNodes, with stable UID-based public resource identity.
- Removed unsupported `MAINSEQUENCE_TOKEN` authentication from the SDK runtime and auth loader paths. JWT access/refresh tokens are now the only supported authentication mechanism.
- Added `MAINSEQUENCE_AUTH_MODE=runtime_credential` for runtime credential authentication. This mode behaves like JWT access-only request auth, but refreshes by exchanging `MAINSEQUENCE_RUNTIME_CREDENTIAL_ID` and `MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET` for a new access token.
- Runtime credential auth writes exchanged access tokens to `MAINSEQUENCE_ACCESS_TOKEN` for the current process environment and does not use `MAINSEQUENCE_REFRESH_TOKEN`.
- Made `mainsequence login` runtime-credential-aware: when `MAINSEQUENCE_AUTH_MODE=runtime_credential`, it exchanges the configured runtime credential instead of opening browser login or persisting CLI JWT refresh tokens.
- Made `mainsequence project set-up-locally` and `mainsequence project refresh_token` runtime-credential-aware so local project `.env` files no longer require JWT refresh tokens in runtime credential mode.
- Updated pod/runtime detection and startup-state bootstrap to use execution markers plus JWT auth instead of the removed legacy token path.
- Stopped treating `MAINSEQUENCE_TOKEN=` as a managed project `.env` key during CLI auth refresh and project setup flows.
- Added label fields to update metadata models so backend responses containing `labels` deserialize correctly for local time-series update payloads.
- Bound the installed SDK version into structured logs as `sdk_version` to make deployed-image/version drift easier to diagnose.
- Preserved registry detail-only fields on `RegisteredWidgetType` responses and surfaced schema, IO, default presentation, and extra fields in the CLI detail view.
- Migrated DataNode source-configuration docs and CLI detail output to the multidimensional layout contract: use backend-derived `storage_layout` and `physical_index_plan` instead of the removed `table_partition` field.

### Removed

- Removed the legacy domain application packages from the core SDK tree so this repository stays focused on platform client primitives.
- Removed deprecated row-oriented table APIs, examples, tutorials, and scaffold guidance in favor of MetaTables.

## [3.19.17] - 2026-05-17

### Added

- Added `mainsequence data-node run_query` so the CLI can execute raw read-only SQL against published dynamic tables by storage UID through the SDK-backed `run_query(...)` method.

### Changed

- Documented the new raw query CLI commands in the CLI reference plus the data-node knowledge docs.

## [3.19.16] - 2026-05-17

### Added

- Added `DataNodeStorage.run_query(...)` so the client can execute read-only SQL against dynamic tables using the backend `run_query/` endpoint with plain-text SQL request bodies.

### Changed

- Documented the raw SQL query flow for dynamic tables, including the plain-text request contract and structured backend response envelope.

## [3.19.14] - 2026-05-16

### Fixed

- Made `mainsequence logout` perform a hard CLI logout for browser-login JWT sessions by calling `/auth/cli/revoke/`, with a legacy fallback to `/auth/jwt-token/logout/` on older backends and local-only clearing for runtime credential mode or other no-refresh-token sessions.

## [3.17.48] - 2026-04-14

### Added

- Added `NotificationTone` and `NotificationDefinition` to `mainsequence.client.command_center.app_component` so FastAPI and AppComponent APIs can return notification-banner response contracts with `x-ui-role: notification` and `x-ui-widget: banner-v1`.
- Added focused SDK tests covering notification payload validation and emitted schema metadata for the new AppComponent notification response contract.

### Changed

- Clarified the AppComponent skill so richer UI contracts are explicitly driven by `x-ui-role`, with `editable-form` for input-side contracts and `notification` for response-side contracts.
- Clarified the FastAPI/API skill so immediate client feedback should use `NotificationDefinition`, while long-running or subprocess-spanning work should use `mainsequence.client.Notification` for asynchronous user updates.

## [3.17.47] - 2026-04-14

### Added

- Added `mainsequence cc registered_widget_type detail <WIDGET_ID>` to inspect one registered Command Center widget type by `widget_id`.
- Updated registered widget type list/detail CLI output to include the backend row `id`.
- Added focused tests for local pod-project resolution, including invalid environment handling, warning behavior, caching, and `DataNodeUpdate.get_or_create(...)` failure when no local pod project is available.

### Fixed

- Cleaned up Ruff `F821` undefined-name issues across the SDK, TDAG, virtual fund builder, and CLI modules.
- Cleaned up Ruff `B904` exception chaining in CLI and utility code so wrapped exceptions now preserve their original cause.
- Fixed the real Ruff `B008` default-evaluation issue in the bond pricer and configured Ruff to ignore the standard Typer default-signature pattern in `mainsequence/cli/cli.py`.
- Reworked local pod-project resolution so SDK code validates `MAIN_SEQUENCE_PROJECT_ID`, resolves projects lazily, caches successful lookups, and surfaces clearer errors when a local pod project is required but unavailable.
- Updated job, DataNode update, and TDAG build-signature paths to use the new local pod-project resolution helpers instead of relying on a fragile global `POD_PROJECT` snapshot.

### Changed

- Documented the user-resolution boundary for agents and scripts: `User.get_logged_user()` is for request-bound identity contexts, while standalone authenticated CLI or script code should prefer `User.get_authenticated_user_details()`.

## [3.17.41] - 2026-04-13

### Added

- Added first-class label mutation support for labelable SDK objects through `LabelableObjectMixin.add_label()` and `remove_label()` documentation, plus shared CLI commands on `project`, `data-node`, and `cc workspace`.
- Added label documentation clarifying that object labels are organizational metadata only and do not affect runtime behavior or functionality.

### Fixed

- Fixed `mainsequence project schedule_batch_jobs` batch submission so the SDK sends top-level `project_id` instead of `project`, matching the backend `sync_jobs` contract.
- Fixed `mainsequence project jobs list` so the CLI always scopes job listing with `project__id=<PROJECT_ID>` instead of sending an incorrect `project=<PROJECT_ID>` filter that could return unscoped results.

### Changed

- Added first-class storage `namespace` support for DataNode storage models while keeping `hash_namespace` inside build configuration for identity construction.
- Added storage registration support to send top-level `namespace` metadata to the backend.
- Added storage and updater namespace filter support in the SDK:
  `namespace`, `namespace__contains`, `namespace__in`, `namespace__isnull`,
  `related_table__namespace__contains`, `related_table__namespace__in`, and
  `related_table__namespace__isnull`.
- Added CLI support for `namespace=...` on `mainsequence data-node list`.
- Updated CLI output and documentation to surface storage namespace information and examples.
- Documented `mainsequence project schedule_batch_jobs` in the CLI reference.
- Clarified that `scheduled_jobs.yaml` is the repository-managed input file for the bulk job sync/create flow.
- Documented explicit `spot` usage for reviewed batch job files, including the meaning of `spot: true` versus `spot: false`.

## [3.17.6] - 2026-03-27

### Changed

- Documentation and repository maintenance updates for the `3.17.6` release line.
