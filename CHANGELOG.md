# Changelog

All notable changes to this project should be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows versioned releases.

## [Unreleased]

### Changed

- Removed unsupported `MAINSEQUENCE_TOKEN` authentication from the SDK runtime and auth loader paths. JWT access/refresh tokens are now the only supported authentication mechanism.
- Added `MAINSEQUENCE_AUTH_MODE=runtime_credential` for runtime credential authentication. This mode behaves like JWT access-only request auth, but refreshes by exchanging `MAINSEQUENCE_RUNTIME_CREDENTIAL_ID` and `MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET` for a new access token.
- Runtime credential auth writes exchanged access tokens to `MAINSEQUENCE_ACCESS_TOKEN` for the current process environment and does not use `MAINSEQUENCE_REFRESH_TOKEN`.
- Made `mainsequence login` runtime-credential-aware: when `MAINSEQUENCE_AUTH_MODE=runtime_credential`, it exchanges the configured runtime credential instead of opening browser login or persisting CLI JWT refresh tokens.
- Made `mainsequence project set-up-locally` and `mainsequence project refresh_token` runtime-credential-aware so local project `.env` files no longer require JWT refresh tokens in runtime credential mode.
- Updated pod/runtime detection and startup-state bootstrap to use execution markers plus JWT auth instead of the removed legacy token path.
- Stopped treating `MAINSEQUENCE_TOKEN=` as a managed project `.env` key during CLI auth refresh and project setup flows.
- Added label fields to update metadata models so backend responses containing `labels` deserialize correctly for local time series and simple table update payloads.
- Added `DataNodeStorage.delete_after_date(...)` to call the dynamic-table tail-delete endpoint using POST and return authoritative post-delete table stats.
- Bound the installed SDK version into structured logs as `sdk_version` to make deployed-image/version drift easier to diagnose.

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
- Updated job, simple-table, DataNode update, and TDAG build-signature paths to use the new local pod-project resolution helpers instead of relying on a fragile global `POD_PROJECT` snapshot.

### Changed

- Documented the user-resolution boundary for agents and scripts: `User.get_logged_user()` is for request-bound identity contexts, while standalone authenticated CLI or script code should prefer `User.get_authenticated_user_details()`.

## [3.17.41] - 2026-04-13

### Added

- Added first-class label mutation support for labelable SDK objects through `LabelableObjectMixin.add_label()` and `remove_label()` documentation, plus shared CLI commands on `project`, `data-node`, `simple_table`, and `cc workspace`.
- Added label documentation clarifying that object labels are organizational metadata only and do not affect runtime behavior or functionality.

### Fixed

- Fixed `mainsequence project schedule_batch_jobs` batch submission so the SDK sends top-level `project_id` instead of `project`, matching the backend `sync_jobs` contract.
- Fixed `mainsequence project jobs list` so the CLI always scopes job listing with `project__id=<PROJECT_ID>` instead of sending an incorrect `project=<PROJECT_ID>` filter that could return unscoped results.

### Changed

- Added first-class storage `namespace` support for DataNode and SimpleTable storage models while keeping `hash_namespace` inside build configuration for identity construction.
- Added storage registration support to send top-level `namespace` metadata to the backend.
- Added storage and updater namespace filter support in the SDK:
  `namespace`, `namespace__contains`, `namespace__in`, `namespace__isnull`,
  `related_table__namespace__contains`, `related_table__namespace__in`, and
  `related_table__namespace__isnull`.
- Added CLI support for `namespace=...` on `mainsequence simple_table list` and `mainsequence data-node list`.
- Updated CLI output and documentation to surface storage namespace information and examples.
- Documented `mainsequence project schedule_batch_jobs` in the CLI reference.
- Clarified that `scheduled_jobs.yaml` is the repository-managed input file for the bulk job sync/create flow.
- Documented explicit `spot` usage for reviewed batch job files, including the meaning of `spot: true` versus `spot: false`.

## [3.17.6] - 2026-03-27

### Changed

- Documentation and repository maintenance updates for the `3.17.6` release line.
