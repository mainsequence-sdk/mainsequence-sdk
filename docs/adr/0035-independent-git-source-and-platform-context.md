# ADR 0035: Independent Git source and platform execution context

Date: 2026-09-28

Status: Accepted and implemented on `metatables_removal`; release pending.

Supersedes in part: [ADR 0031: Git-native process CodeRepositoryBranch context](0031-process-lifetime-code-repository-branch-context.md).

Preserves: the thin SDK ownership boundary in [ADR 0034](0034-remove-metatables-from-sdk.md).

Companion decision: [MetaTables ADR 0001: One API execution path with SQLite for local development](https://github.com/mainsequence-projects/MetaTables/blob/main/docs/adr/0001-unified-api-storage-and-local-sqlite.md).
Cross-repository draft links become available when the corresponding
documentation is published. The SDK implementation belongs on `metatables_removal`.

## Context

Before this decision, the SDK froze Git repository, branch/ref, and commit identity, then
resolves an optional platform CodeRepositoryBranch. Its context model already
represents `code_repository_branch_not_registered` as a valid nonfatal result.
However, obtaining that context still attempts platform resolution, and
`resolve_organization_environment_uid()` calls
`require_code_repository_branch_context()`. Consequently, operations on
Environment-owned resources can fail because the current Git branch is not
registered, even when the developer could authenticate and use an authorized
Environment independently.

This coupling affects Secrets, Constants, Buckets, and Artifacts through
`CurrentCodeRepositoryEnvironmentResourceMixin`. It also affects consumers such
as MetaTables that need local source identity without a registered platform
branch. Removing all branch checks would be incorrect: operations targeting
branch-owned Jobs and releases still need a real platform branch identity.

The required behavior is for a developer to work on an unregistered branch such
as `test` using the same SDK context mechanism and resource clients. SDK
authentication and generic source context remain SDK concerns. MetaTables owns
its own local catalog and storage; the SDK must not acquire database engines or
a separate local implementation to support that consumer.

## Decision

### Separate facts within one context mechanism

Distinguish three concerns:

| Concern | Meaning | Registration requirement |
| --- | --- | --- |
| Git source context | Actual checkout, canonical repository identity, attached branch/ref, and full commit SHA. | None. |
| Platform branch context | Optional registered CodeRepository/CodeRepositoryBranch identities for that exact source. | Required only by operations that need these platform resources. |
| Authenticated execution scope | User/Organization identity and, when needed, an authorized Environment with recorded provenance. | Environment access does not inherently require the current Git branch to be registered. |

These concerns are composed by one SDK context implementation. A consumer does
not select a separate local SDK or replace the normal request/session pipeline.
Source facts never grant permissions and a selected Environment never changes
the actual Git branch.

### Source discovery and optional platform resolution

Expose a public Git-source resolver that obtains and freezes local source facts
without authentication or a platform request. The public name is
`get_git_source_context()` in `mainsequence.code_repository_context`; it reuses
the existing Git parser, normalization, and source validation.

Retain `get_code_repository_context()` as the composition point for optional
platform enrichment, using the same frozen source object. Preserve the existing
registered and unregistered results and optional UID fields. Consumers that
only need source facts must use the source resolver rather than triggering
platform enrichment as an initialization side effect.

An unregistered result leaves platform UIDs absent. It does not register a branch,
substitute `main`, select a sibling branch, or fabricate a platform UID. A
directory/authentication outage is not classified as an unregistered branch.
Failed optional enrichment must not poison already valid local source facts or
disable unrelated SDK calls. Retrying failed platform resolution is explicit;
it does not silently replace an already established platform context.

### Environment resolution independent of branch registration

Keep `resolve_organization_environment_uid(operation)` as the common consumer
interface, but resolve an Environment according to these rules:

1. In an authenticated deployed runtime, use the runtime's authenticated target
   Environment. Validate applicable source and target consistency. Development
   settings cannot override this scope.
2. For a human-authenticated development process, accept an explicit SDK-managed
   development Environment selection after verifying its Organization and the
   caller's access through a supported platform interface.
3. Without an explicit selection, a registered branch may supply its configured
   Environment as the existing default. If both an explicit selection and a
   registered branch's configured Environment are present, require them to agree
   for this initial change; do not silently retarget current-branch operations.
4. With neither a selected authorized Environment nor a branch-derived one,
   raise an Environment-specific prerequisite error only when an operation
   actually needs an Environment. Authentication, local Git context, and
   Organization-level operations continue to work.

The explicit selection is configured through one typed SDK configuration
interface before the first Environment-scoped operation. Its exact CLI/config
spelling is an implementation detail to document with that interface. The
resolved context records the Environment UID, Organization UID, and whether the
source was an authenticated runtime, an explicit development selection, or a
registered branch. There is no arbitrary per-resource context override.

The platform remains the authorization authority on each request. Verifying a
selection does not grant access by itself or bypass later resource authorization.
Do not infer an Environment from a DataSource, collection order, another branch,
or an application-specific local workspace ID.

The existing service contract for human Environment selection and subsequent
resource requests must be verified before implementing the SDK selector. If a
required capability is missing, record the exact upstream contract dependency
and add a thin SDK operation once it exists. Do not implement private platform
HTTP calls or speculative endpoint fallbacks in MetaTables.

### Operation-specific prerequisites

`require_code_repository_branch_context()` remains strict for operations whose
actual target requires a platform branch. A request to create a branch-owned Job
cannot be fulfilled using only a Git branch name. Keep those errors explicit and
limited to the operation needing the missing resource.

Environment-owned resource clients use the common Environment resolver directly.
They do not require a registered branch as a proxy for Environment availability.
Update `CurrentCodeRepositoryEnvironmentResourceMixin` and its consumers
accordingly. Audit branch-scoped collection helpers separately; remove a branch
prerequisite only when the resource contract does not actually need it.

Explicit target-resource operations retain their own target ownership and
authorization semantics. Targeting a known resource must not mutate the frozen
current source or authenticated runtime scope.

### Behavior matrix

| Situation | Source/authentication | Environment-owned operations | Current platform-branch operations |
| --- | --- | --- | --- |
| Human, registered branch with an Environment | Available through the common resolvers. | Use the branch-derived default, or a matching explicit selection. | Use the resolved branch after ordinary authorization. |
| Human, unregistered `test` branch, authorized Environment selected | Git source and SDK authentication work. | Use the explicitly selected Environment through normal resource clients. | Fail only when the operation requires a registered target branch. |
| Human, unregistered branch, no Environment selected | Git source, authentication, and unrelated operations work. | Fail with an Environment-specific prerequisite message. | Fail with a branch-specific prerequisite message. |
| Platform lookup unavailable | Local source facts remain available; identity calls report their own failures. | Operations needing unavailable scope resolution fail explicitly. | Operations needing unavailable branch resolution fail explicitly. |
| Authenticated deployed runtime | Retain authenticated-target verification and source drift checks. | Use its authenticated Environment; reject conflicting development selection. | Retain required target and branch checks. |

Purely local MetaTables storage needs SDK User/Organization identity and Git
source facts, not a fabricated platform Environment or registered branch. Its
local workspace identity is owned by MetaTables under the companion ADR and is
never sent to platform endpoints as an Environment UID.

### State, errors, and compatibility

Preserve process-level synchronization and immutable snapshots. Source facts,
optional platform enrichment, and Environment selection have distinct resolution
state so one missing optional component does not invalidate the others. Forked
children reinitialize process-owned caches. Scope caches include authenticated
principal/Organization and selected Environment; token refresh for the same
principal does not change scope. Changing principal or selected scope requires
an explicit new/reset context, never accidental reuse of the old one.

Retain explicit source-drift validation for changed checkout, branch, or HEAD.
Changing branches takes effect through a documented fresh-process/context
workflow. Supporting unregistered branches does not authorize silent in-process
retargeting of deployed or branch-owned work.

Preserve existing public resolver names and registered-branch behavior where
possible. Additive source discovery and development Environment selection extend
capability, but error timing changes and the broader Environment contract must
be documented. Update tests that encode the old blanket branch prerequisite.
Missing Environment errors should be distinguishable from missing branch errors
while preserving a useful common context-error base.

### Ownership and exclusions

The SDK remains an authentication, context, and platform-request adapter under
ADR 0034. This decision adds no SQLite or DuckDB dependency, local catalog,
DataSource override, schema migration workflow, producer logic, or storage
implementation. MetaTables consumes the SDK's public context interfaces and
maintains one API workflow whose database differences are confined to thin
communication/dialect adapters.

## Relationship to ADR 0031

This decision supersedes only these ADR 0031 requirements:

- Successful platform enrichment as part of every source-context use.
- A registered current branch as the only way a human development process can
  obtain an Environment for Environment-owned resource operations.
- The prohibition on an explicit SDK-managed, authorized development Environment
  selection independent of the current branch's registration.

It preserves canonical Git identity, actual optional platform UIDs,
process-snapshot and fork behavior, source-drift checks, platform authorization,
authenticated deployed-runtime target checks, and strict prerequisites for
operations genuinely requiring registered platform branches.

ADR 0031 retains its source identity and runtime target rules. Its affected
clauses are marked as superseded; historical text is retained as decision history.

## Alternatives considered

- Deleting all branch guards would allow incomplete requests for resources that
  genuinely require a registered branch and would obscure their prerequisites.
- Falling back to another branch or its Environment would silently change the
  target of developer operations.
- Fabricating or automatically creating platform branch IDs would turn source
  discovery into resource provisioning and create unwanted platform records.
- Implementing context resolution inside MetaTables would duplicate SDK behavior
  and leave the same restriction in other SDK consumers.
- Adding a separate local SDK or database engine would violate ADR 0034 and
  create a second implementation to maintain.

## Implementation sequence and documentation

1. Verify the platform Environment-selection contract and record any required
   upstream capability before changing consumers.
2. Separate network-free source discovery from optional platform enrichment in
   the existing SDK context module.
3. Add explicit development Environment configuration and provenance-aware
   resolution while retaining authenticated runtime precedence.
4. Update Environment-scoped resource consumers and audit legitimate
   branch-owned operation guards.
5. Add the context and resource regression cases below. Update authentication,
   context, Secrets/Constants, Buckets/Artifacts, CLI/config, and migration notes
   together with implementation.
6. Mark the affected ADR 0031 clauses as superseded only when this ADR is
   accepted. Keep the MetaTables dependency prerequisite explicit until the
   corresponding SDK interfaces are published.

## Acceptance criteria

- A new unregistered `test` branch yields real frozen Git source context with
  no branch-directory or physical DataSource request.
- SDK login and unrelated platform calls work without current-branch
  registration. Importing the SDK does not trigger source or scope discovery.
- With an explicit authorized development Environment, Environment-owned
  resource requests carry the correct scope through existing transport and
  authentication. Wrong-Organization and unauthorized selections fail.
- With no Environment, only Environment-dependent operations report the missing
  prerequisite; source discovery and unrelated operations remain usable.
- Branch-owned operations still require their real target branch. No test
  creates a platform branch or substitutes another branch as a side effect.
- Registered default behavior, explicit-selection conflicts, deployed-runtime
  target checks, concurrent initialization, fork/reset behavior, source drift,
  and optional lookup failures have regression coverage.
- The same context/resource implementations serve registered and unregistered
  development cases; no MetaTables-specific resolver is introduced.
- Installed SDK checks continue to pass without MetaTables or local database
  engines, and documentation accurately distinguishes proposed from implemented
  behavior.

## Implemented interfaces and verified service contract

The implementation is in `mainsequence.code_repository_context`:

- `get_git_source_context()` and `validate_git_source_context()` read local Git
  without authentication or platform calls.
- `get_code_repository_context()` enriches the same immutable source snapshot.
- `DevelopmentEnvironmentSelection` and `configure_development_environment()`
  configure a human selection before the first Environment resolution attempt.
- `get_organization_environment_context()` records principal, Organization,
  Environment, API endpoint, process, and scope provenance.
- `resolve_organization_environment_uid()` remains the shared resource entry point.
- `retry_failed_context_resolution()` preserves source and any established scope;
  `reset_development_context()` starts fresh development state and clears selection.
  Active resolution cannot be reset; deployed runtimes require a fresh process.

The existing platform contract was verified from its Environment viewset,
serializer, and request-scope service on 2026-09-28:

- `GET /api/v1/users/me/` supplies the authenticated User and Organization.
- `GET /api/v1/organization-environments/` lists visible Environments;
  `GET /api/v1/organization-environments/{uid}/` applies the same visibility policy.
  `OrganizationEnvironment` is the SDK resource adapter. Its public ownership
  field is `organization_owner_uid`; SDK selection must match the User's
  Organization, including for administrators with broader directory visibility.
- Human Environment-owned resource requests already accept the SDK-owned
  `organization_environment_uid` body/query field and validate access against the
  authenticated Organization. These requests do not require a branch registration.
- Runtime `resolve-git-context` validates source against the authenticated target
  and its immutable commit when supplied. Runtime resource endpoints derive scope
  from that target and reject conflicting selectors. Runtime credentials and
  runtime-scoped session JWTs reject development configuration. `session_jwt`
  alone also supports forwarded human tokens and does not establish runtime
  authority; token scope labels only tighten local guards, and the platform
  verifies authentication and the runtime target before context is accepted.

No additional server endpoint is required. Verification is against the local
platform implementation and mocked SDK transport contracts, not a live production
deployment. Consumers require these supported public platform routes.

Platform enrichment and Environment scope both bind their cache to the
principal, Organization, and API endpoint. Each platform-context resolution
revalidates the principal via `users/me`; Environment resolution reuses that
verified identity. Same-principal token refresh preserves the snapshot, while
account or Organization changes fail until an explicit development reset or a
fresh runtime process. This incurs one identity request per successful resolver call. The initial Environment detail
lookup and immutable scope are cached; later resource authorization remains
server-owned. Credentials and reset operations must not change while resource
requests are in flight. Forked children clear process state and must configure
their development selection again.

The branch guard audit retained current-branch collection checks for `Job`,
`CodeRepositoryImage`, `CodeRepositoryResource`, `ResourceRelease`, and
`DeploymentRun`; their explicit target operations retain their existing semantics.
`Secret`, `Constant`, `Bucket`, and `Artifact` continue through the common
Environment mixin and standard SDK transport.

Regression coverage is in `tests/test_independent_context.py`,
`tests/test_code_repository_runtime_context.py`, and
`tests/test_environment_resource_context.py`. User-facing behavior and compatibility
changes are documented in the [context guide](../knowledge/infrastructure/context.md)
and [migration note](../migrations/independent-context.md). Until these SDK interfaces
are published, MetaTables retains its documented local SDK prerequisite. The
companion MetaTables ADR remains a plan; this implementation adds no local storage.
