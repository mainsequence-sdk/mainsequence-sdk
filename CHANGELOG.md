# Changelog

All notable changes to this project should be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows versioned releases.

## [Unreleased]

## [8.0.6] - 2026-08-30

### Changed

- Aligned `Job` with the canonical backend execution contract: jobs now use a
  required `.py` or `.yaml` `execution_path`, accept the read/write
  `description` field, and no longer expose `app_name` or notebook execution.

### Fixed

- Allowed `JobRun.get_logs()` to follow a backend capability link without an
  `organization_environment_uid` query parameter because the JobRun UID
  already fixes that scope. Other owner-log links and JobRun resource-usage
  links remain explicitly environment-scoped.

## [8.0.5] - 2026-08-30

### Changed

- Aligned CodeRepository creation with ADR-049 by replacing branch-wide
  `env_vars` with `bootstrap_organization_environment_uid`.
- Removed the obsolete provider-repository deletion option from logical
  CodeRepository bulk deletion; provider repositories and Git branches are
  preserved.

## [8.0.4] - 2026-08-29

### Fixed

- Aligned current ADR guidance with the v8 CodeRepository ontology, including
  the canonical `code_repository_context` wire field,
  `get_code_repository_context()` entry point, and
  `CodeRepositoryAlembicVersion` scaffold default ([#104](https://github.com/mainsequence-sdk/mainsequence-sdk/issues/104)).
- Added a published-documentation contract check that rejects retired Project
  identifiers while explicitly preserving historical changelog entries.

## [8.0.3] - 2026-08-29

### Fixed

- Accepted the backend-owned, read-only `code_repository_type` projection on
  `CodeRepositoryBranch` detail and Git-context responses ([#103](https://github.com/mainsequence-sdk/mainsequence-sdk/issues/103)).

## [8.0.2] - 2026-08-29

### Fixed

- Made `code-repository freeze-env` export the locked runtime dependency
  closure while excluding development dependency groups ([#102](https://github.com/mainsequence-sdk/mainsequence-sdk/issues/102)).

## [8.0.1] - 2026-08-29

### Fixed

- Aligned SDK guidance and platform-skill examples with the canonical
  `code_repository_design` and `code_repository_to_agent` contracts, removing
  the superseded repository-design vocabulary.

## [8.0.0] - 2026-08-29

### Changed

- Replaced the superseded Pod Manager repository SDK ontology with `CodeRepository`,
  `CodeRepositoryBranch`, and `GitHubRepositoryBinding` across models,
  fields, routes, filters, runtime context, CLI output, and scaffold guidance.
- Renamed the local CLI base option to `--code-repositories-base`, changed new
  managed checkout paths to `<base>/<organization>/code-repositories/`, and
  removed stale repository-era CLI examples and schema descriptions.
- Renamed the generated Alembic registry default to
  `CodeRepositoryAlembicVersion` and aligned the installed
  `code_repository_to_agent` skill path with the backend catalog.

### Migration

- Follow the [7.x to 8.0 CodeRepository ontology hard-cut guide](docs/migrations/v8-code-repository-ontology.md).
  Version 8 requires the coordinated canonical backend and does not provide
  aliases for the removed repository-domain contract.

## [7.0.2] - 2026-08-28

### Fixed

- Restored the ADR-0037 Git-native source resolver for authenticated project
  runtimes. Runtime credentials now verify the Git-resolved CodeRepositoryBranch
  instead of replacing repository, branch, and commit context with a no-Git
  projection; deployed source drift and missing Git fail closed
  ([#100](https://github.com/mainsequence-sdk/mainsequence-sdk/issues/100)).

## [7.0.1] - 2026-08-28

### Fixed

- Added an explicit strict `ScheduledUpdateNode` response model so non-empty
  Scheduler `pre_loads_in_tree`, `in_active_tree`, and `schedules_to`
  projections accept the backend's canonical node discriminator and
  output-table identity fields ([#99](https://github.com/mainsequence-sdk/mainsequence-sdk/issues/99)).

## [7.0.0] - 2026-08-27

### Changed

- Replaced the former DataNode SDK surface with the hard-cut time-index-table
  vocabulary: `TimeIndexTableUpdater`, `TimeIndexTableUpdateConfig`,
  `TimeIndexTableUpdate`, `TimeIndexTableUpdateDetails`, and `TableUpdateRun`.
  The old package, imports, endpoint registry entries, routes, fields, and CLI
  commands are not retained as aliases.
- Split read-only upstream access into `TimeIndexTableRef` and
  `TimeIndexTableReader`. References identify one canonical
  `TimeIndexMetaTable`, create table-lineage edges, and have no updater,
  scheduler, or execution behavior.
- Versioned updater build configuration at schema version 2, introduced the
  canonical updater/reference dependency discriminators, and intentionally
  rotated updater hashes. Legacy configuration shapes fail validation.

### Migration

- Follow the [6.x to 7.0 hard-cut migration guide](docs/migrations/v7-time-index-table-updater-hard-cut.md).
  Version `6.0.53` is the final pre-cutover SDK release; SDK and backend
  generations cannot be mixed during this flag-day migration.

## [6.0.53] - 2026-08-27

### Fixed

- Aligned strict `AgentSession` parsing with the backend-owned, read-only
  `runtime_capabilities` version map across list, detail, and handle-based
  get-or-create responses.

## [6.0.52] - 2026-08-27

### Added

- Added typed, explicitly confirmed cascade deletion for MetaTables, including
  reference-cascade controls, Alembic schema-protection override, SDK-owned
  Organization Environment context, and typed backend error propagation.

## [6.0.51] - 2026-08-27

### Fixed

- Installed backend-authenticated `runtime_project_context` during Knative
  runtime-credential exchange before resolving project state. Branch-owned
  deployed workloads now resolve their exact CodeRepositoryBranch and MetaTables Data
  Source without inspecting container Git, and SDK-owned runtime requests omit
  caller-selectable CodeRepositoryBranch and Organization Environment selectors.

## [6.0.50] - 2026-08-27

### Fixed

- Made every CLI login mode use the backend resolved by the active CLI
  configuration when `--backend` is omitted. MCP handoff creation and polling
  can no longer silently target the standard backend while `mainsequence
  doctor` reports a different configured backend; explicit backend precedence
  and different-backend projects-base validation remain unchanged.

## [6.0.49] - 2026-08-27

### Fixed

- Aligned strict `ResourceRelease` list, detail, and create parsing with the
  canonical revision lifecycle fields: positive `revision_retention_count` and
  nullable public `active_revision` / `desired_revision` UIDs. Release create
  and patch operations now validate and send positive retention settings.

## [6.0.48] - 2026-08-27

### Changed

- Replaced the retired project-qualified Organization Environment field names
  throughout SDK models, project-context resolution, MetaTables, CLI operations,
  tests, and documentation with canonical `organization_environment_uid` and
  `organization_environment_name` fields. No legacy aliases remain.
- Added owner-scoped logs and aggregate resource usage for JobRun,
  ResourceRelease, and Agent, plus fixed-owner AgentSession logs. The SDK follows
  authenticated same-backend capability links and does not expose Environment
  selection through owner method arguments.
- Kept DeploymentRun build and orchestration logs on their separate response
  contract while adding the SDK-resolved canonical Environment authorization
  query.

### Added

- Added sessionless `Agent.respond()` and `Agent.stream_response()` helpers that
  resolve Agent runtime access and use the canonical A2A response endpoints with
  optional inference and strict dictionary metadata.

## [6.0.40] - 2026-08-23

### Fixed

- Aligned MetaTable and TimeIndexMetaTable collection filtering with the backend's
  canonical `organization_environment_uid` query parameter. CLI table listings now
  derive the exact environment scope from the active Git-resolved CodeRepositoryBranch or accept an
  explicit administrative scope outside a registered CodeRepository checkout.

## [6.0.39] - 2026-08-23

### Changed

- Adopted the Git-native CodeRepository source-context contract for local and deployed CodeRepository code.
  The SDK freezes the containing repository identity, attached branch, and exact HEAD commit once
  per process, then resolves the authoritative CodeRepositoryBranch through the backend's canonical
  Git-context action.
- Scoped branch-owned Jobs, images, resources, releases, deployment runs, migrations, MetaTables,
  DataNodes, and code-repository-executor operations to the resolved CodeRepositoryBranch. Unregistered local
  branches remain usable for ordinary development and fail only when a branch-owned operation is
  requested.

### Removed

- Removed the SDK runtime repository-context environment projection and the local `.env` CodeRepository UID
  association. CodeRepository, CodeRepositoryBranch, repository branch, commit, and Organization Environment are
  no longer selected from process environment.
- Removed the retired CodeRepository-level default MetaTables DataSource fallback. CodeRepository-derived data
  access now requires the DataSource configured on the exact resolved CodeRepositoryBranch.

## [6.0.38] - 2026-08-22

### Fixed

- Preflight the exact backend-owned project-sync tag before mutation: preview the `uv` patch
  version, reject local and remote tag collisions, verify the applied bump matches the preview, and
  atomically push the explicit branch and tag refs.
- Made repository SSH key filenames collision-resistant across projects by deriving them from the
  canonical Git origin identity instead of the repository basename. Local setup, signed terminals,
  and project sync now register keys when needed and verify the forced identity before mutation;
  legacy basename-only key files remain untouched and are not reused as a fallback.
- Made `mainsequence project sync --dry-run` non-mutating after its read-only
  project and Git preflight. It now returns before SSH key generation,
  dependency operations, backend tag requests, and Git changes.

## [6.0.37] - 2026-08-22

### Fixed

- Moved deploy-key registration from the CodeRepositoryBranch SDK surface to the
  owning CodeRepository. Local project setup now calls the repository-scoped endpoint
  with the CodeRepository UID while retaining CodeRepositoryBranch selection only
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
  read-only CodeRepository and CodeRepositoryBranch projections so CLI log retrieval accepts
  current project-scoped responses.

## [6.0.33] - 2026-08-20

### Fixed

- Aligned Agent list and detail parsing with the backend's canonical read-only
  CodeRepositoryBranch and Organization Environment projections, preserving required
  response keys whose values may be null for unscoped Agents.
- Added the required Organization Environment discovery scope to Agent listing
  and semantic search, including canonical semantic-search result projections
  and corresponding CLI and skill guidance.

## [6.0.32] - 2026-08-19

### Changed

- Jobs now use the platform's exact-image contract. Manually managed Jobs
  require an explicit ready image, while automatically deployed Jobs omit
  image selection so the backend derives and binds the exact image from the
  CodeRepositoryBranch's synchronized commit.
- Job and JobRun models and CLI output now expose exact image, commit,
  readiness, automatic-deployment policy, and immutable runtime image state.

### Removed

- Removed SDK and CLI support for dynamic, nullable, or `latest` Job image
  selection.

## [6.0.31] - 2026-08-18

### Fixed

- Compiled SQL operations no longer perform a logical CodeRepository lookup when
  `scope.data_source_uid` is omitted. The SDK now preserves the backend's
  optional field contract so branch-owned runtimes can derive execution from
  their declared MetaTable scope without using a runtime credential on a
  removed repository endpoint.

## [6.0.30] - 2026-08-18

### Changed

- FastAPI handlers now consume the authenticated human injected by the Main
  Sequence platform through `request.state.user` and
  `request.state.user_uid`, without SDK authentication setup.

### Removed

- Removed `mainsequence.client.fastapi.LoggedUserContextMiddleware` and the
  middleware-only FastAPI extra and Starlette dependency. project applications
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
- Made `DataSource` the sole canonical database identity across CodeRepositoryBranch,
  MetaTable, DataNode persistence, SQLAlchemy registration, compiled SQL, and
  CLI discovery. Migration credentials are now requested through the owning
  Alembic registry MetaTable.
- Replaced the former branch-shaped repository SDK contract with the logical
  CodeRepository aggregate and added explicit `CodeRepositoryBranch` and `GitHubRepositoryBinding`
  models. Branch-owned jobs, images, resources, releases, deployment runs,
  DataNodes, and coding-agent services now use CodeRepositoryBranch UIDs.
- Kept the canonical CodeRepository UID as the only persisted local repository identity
  identity. The SDK resolves the active CodeRepositoryBranch from that logical
  CodeRepository and the checked-out Git branch; it does not introduce or read a
  superseded branch-identity environment variable.
- Updated CodeRepository creation and bulk deletion to the canonical backend request
  contracts and moved branch actions, including summaries and repository
  browsing, onto `CodeRepositoryBranch`.
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

- Made the separate `mainsequence-sdk-tutorial` repository the sole source of
  beginner tutorial documentation and kept one repository link for discovery.
- Clarified that runnable applications own their code, migrations, fixtures,
  tests, and documentation in self-contained repositories.

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

- Updated code-repository CLI arguments, local env write paths, and checkout detection to use `code_repository_uid`.
- Updated local code repository setup, token refresh, sync, and project resolution helpers to operate on public project references while keeping internal numeric adapters only where backend filters still require row ids.
- Aligned `CodeRepository` client helpers and quick-search models with the public UID contract.

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
- Reworked local pod execution-context resolution to validate the then-current numeric runtime identity, resolves projects lazily, caches successful lookups, and surfaces clearer errors when a local pod project is required but unavailable.
- Updated job, DataNode update, and TDAG build-signature paths to use the new local pod-project resolution helpers instead of relying on a fragile global `POD_PROJECT` snapshot.

### Changed

- Documented the user-resolution boundary for agents and scripts: `User.get_logged_user()` is for request-bound identity contexts, while standalone authenticated CLI or script code should prefer `User.get_authenticated_user_details()`.

## [3.17.41] - 2026-04-13

### Added

- Added first-class label mutation support for labelable SDK objects through `LabelableObjectMixin.add_label()` and `remove_label()` documentation, plus shared CLI commands on `code-repository`, `data-node`, and `cc workspace`.
- Added label documentation clarifying that object labels are organizational metadata only and do not affect runtime behavior or functionality.

### Fixed

- Fixed CodeRepository batch submission so the SDK sends the canonical CodeRepositoryBranch selector expected by the backend job synchronization contract.
- Fixed `mainsequence code-repository jobs list` so the CLI always scopes job listing through the canonical CodeRepository relation filter and cannot return unscoped results.

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
