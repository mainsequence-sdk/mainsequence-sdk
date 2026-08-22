# Changelog

All notable changes to this project should be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows versioned releases.

## [Unreleased]

### Fixed

- Made repository SSH key filenames collision-resistant across projects by deriving them from the
  canonical Git origin identity instead of the repository basename. Local setup, signed terminals,
  and project sync now register keys when needed and verify the forced identity before mutation;
  legacy basename-only key files remain untouched and are not reused as a fallback.

## [6.0.38] - 2026-08-22

### Fixed

- Made `mainsequence project sync --dry-run` non-mutating after its read-only
  project and Git preflight. It now returns before SSH key generation,
  dependency operations, backend tag requests, and Git changes.

## [6.0.37] - 2026-08-22

### Fixed

- Moved deploy-key registration from the ProjectBranch SDK surface to the
  owning Project. Local project setup now calls the project-scoped endpoint
  with the logical Project UID while retaining ProjectBranch selection only
  for clone readiness and branch choice.

## [6.0.36] - 2026-08-21

### Changed

- Documented backend-owned AgentSession user inheritance for direct and A2A
  execution. SDK callers provide parent-session provenance but never select a
  model-provider credential owner or forward provider credentials.

## [6.0.35] - 2026-08-20

### Removed

- Removed deprecated generic global-visibility fields from DataNodeUpdate and
  TimeIndexMetaTable client contracts and removed the corresponding persist-manager
  mutator. Access remains governed by organization ownership and explicit grants.

## [6.0.34] - 2026-08-20

### Fixed

- Aligned strict JobRun list and detail parsing with the backend's canonical
  read-only Project and ProjectBranch projections so CLI log retrieval accepts
  current project-scoped responses.

## [6.0.33] - 2026-08-20

### Fixed

- Aligned Agent list and detail parsing with the backend's canonical read-only
  ProjectBranch and Organization Environment projections, preserving required
  response keys whose values may be null for unscoped Agents.
- Added the required Organization Environment discovery scope to Agent listing
  and semantic search, including canonical semantic-search result projections
  and corresponding CLI and skill guidance.

## [6.0.32] - 2026-08-19

### Changed

- Jobs now use the platform's exact-image contract. Manually managed Jobs
  require an explicit ready image, while automatically deployed Jobs omit
  image selection so the backend derives and binds the exact image from the
  ProjectBranch's synchronized commit.
- Job and JobRun models and CLI output now expose exact image, commit,
  readiness, automatic-deployment policy, and immutable runtime image state.

### Removed

- Removed SDK and CLI support for dynamic, nullable, or `latest` Job image
  selection.

## [6.0.31] - 2026-08-18

### Fixed

- Compiled SQL operations no longer perform a logical Project lookup when
  `scope.data_source_uid` is omitted. The SDK now preserves the backend's
  optional field contract so branch-owned runtimes can derive execution from
  their declared MetaTable scope without using a runtime credential on a
  forbidden Project endpoint.

## [6.0.30] - 2026-08-18

### Changed

- FastAPI handlers now consume the authenticated human injected by the Main
  Sequence platform through `request.state.user` and
  `request.state.user_uid`, without SDK authentication setup.

### Removed

- Removed `mainsequence.client.fastapi.LoggedUserContextMiddleware` and the
  middleware-only FastAPI extra and Starlette dependency. Project applications
  must not install request identity middleware themselves.

## [6.0.29] - 2026-08-17

### Changed

- Documented the one-time FastAPI request-user middleware setup and clarified
  that the SDK automatically resolves the requesting human from local bearer
  authentication or the deployed gateway's trusted UID header.
- Updated the Command Center FastAPI skill to keep local/deployed identity
  handling out of route code and separate request identity from deployment and
  workload identities.

## [6.0.28] - 2026-08-17

### Changed

- Made runtime request identity UID-only. `User.get_logged_user()` now returns
  `RequestUserIdentity`; FastAPI middleware exposes `request.state.user` and
  `request.state.user_uid`, rejects removed numeric identity headers, and
  isolates identity context for every request.
- Changed permission-sharing and notification recipient APIs to public user and
  team UIDs, including CLI arguments, payload keys, response relationships, and
  rendered access state.

### Fixed

- Limited direct-development bearer validation to forwarding only the request
  Authorization header to `/api/v1/users/me/`, and reject bearer/header UID
  mismatches before application route execution.

## [6.0.27] - 2026-08-12

### Fixed

- Allowed server-owned MCP platform skills to use canonical hierarchical paths
  below `skills/` while preserving path traversal, naming, identity, ownership,
  collision, and content-integrity validation.
- Aligned coding-agent runtime access with the canonical
  `coding_agent_service_uid` response and report unavailable runtime state
  before attempting an A2A request.

### Added

- Added `AlembicVersionMetaTable` as an external catalog binding for Alembic's
  version table.
- Added typed SDK request and response models for Alembic SQL migration apply
  and status endpoints.

### Changed

- Made A2A calls treat the server-issued UID-based `rpc_url` as opaque; the
  standard message path, body, and authentication headers remain unchanged.
- Made `DataSource` the sole canonical database identity across ProjectBranch,
  MetaTable, DataNode persistence, SQLAlchemy registration, compiled SQL, and
  CLI discovery. Migration credentials are now requested through the owning
  Alembic registry MetaTable.
- Replaced the former branch-shaped `Project` SDK contract with the logical
  Project aggregate and added explicit `ProjectBranch` and `GitRepository`
  models. Branch-owned jobs, images, resources, releases, deployment runs,
  DataNodes, and coding-agent services now use ProjectBranch UIDs.
- Kept `MAIN_SEQUENCE_PROJECT_UID` as the only persisted local Project
  identity. The SDK resolves the active ProjectBranch from that logical
  Project and the checked-out Git branch; it does not introduce or read a
  `MAIN_SEQUENCE_PROJECT_BRANCH_UID` variable.
- Updated Project creation and bulk deletion to the canonical backend request
  contracts and moved branch actions, including summaries and repository
  browsing, onto `ProjectBranch`.
- Made SQLAlchemy a core SDK dependency for MetaTable declaration and compiled
  SQL support.
- Reworked MetaTable schema migration docs around Alembic-rendered SQL artifacts
  instead of SDK-managed artifact tables or custom operation plans.

### Removed

- Removed the redundant `ResourceRelease.subdomain` SDK field and CLI preview
  row in favor of UID-only public runtime routing.
- Removed `DynamicTableDataSource`, its deleted TS Manager endpoint, wrapper
  traversal, migration-connection models, and compatibility exports.

## [6.0.26] - 2026-08-11

### Changed

- Made the separate `mainsequence-sdk-tutorial` Project the sole source of
  beginner tutorial documentation and kept one repository link for discovery.
- Clarified that runnable applications own their code, migrations, fixtures,
  tests, and documentation in self-contained Project repositories.

### Removed

- Removed the SDK-hosted tutorial, example source tree, generated examples page,
  navigation entries, and examples-index generator.

## [6.0.25] - 2026-08-11

### Fixed

- Added the canonical detail-only `builder_image` and `builder_runtime` fields
  to `DeploymentRun` while preserving compatibility with collection responses
  that omit them.

## [6.0.24] - 2026-08-11

### Changed

- Removed the obsolete `ResourceReleaseAutomaticDeploymentRun` model and made
  resource-release deployment actions and queries use the unified
  `DeploymentRun` response and filter contract.

## [6.0.23] - 2026-08-11

### Fixed

- Accepted the documented `automatic_redeployment_policy` object returned for
  resource releases, preventing successful CLI release creation from being
  reported as a Pydantic `extra_forbidden` failure.

## [4.0.2] - 2026-05-25

### Changed

- Updated project-facing CLI arguments, local env write-paths, and current-project detection to prefer `project_uid` and `MAIN_SEQUENCE_PROJECT_UID`.
- Updated local project setup, token refresh, sync, and project resolution helpers to operate on public project references while keeping internal numeric adapters only where backend filters still require row ids.
- Aligned `Project` client helpers and quick-search models with the public UID contract.

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
- Updated pod/runtime detection and startup-state bootstrap to use execution markers plus JWT auth.
- Stopped treating `MAINSEQUENCE_TOKEN=` as a managed project `.env` key during CLI auth refresh and project setup flows.
- Added label fields to update metadata models so backend responses containing `labels` deserialize correctly for local time-series update payloads.
- Bound the installed SDK version into structured logs as `sdk_version` to make deployed-image/version drift easier to diagnose.
- Preserved registry detail-only fields on `RegisteredWidgetType` responses and surfaced schema, IO, default presentation, and extra fields in the CLI detail view.
- Updated DataNode source-configuration docs and CLI detail output to the multidimensional layout contract using backend-derived `storage_layout` and `physical_index_plan`.

### Removed

- Removed domain application packages from the core SDK tree so this repository stays focused on platform client primitives.
- Removed row-oriented table APIs, examples, tutorials, and scaffold guidance in favor of MetaTables.

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

- Made `mainsequence logout` perform a hard CLI logout for browser-login JWT sessions by calling `/auth/cli/revoke/`, with fallback local-only clearing for runtime credential mode or other no-refresh-token sessions.

## [3.17.47] - 2026-04-14

### Added

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
