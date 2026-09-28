# Git source and Environment context

Source identity, authentication, and resource scope are separate SDK concerns.
An attached Git branch such as `test` does not need a platform registration for
local source discovery, login, or Organization-level requests. Environment-owned
resources use the same clients and request transport on registered and
unregistered branches.

## Read local source without contacting the platform

```python
from mainsequence.code_repository_context import get_git_source_context

source = get_git_source_context()
print(source.canonical_repository_identity)
print(source.repository_branch, source.commit_sha)
```

This reads the actual checkout and freezes its repository root, normalized remote,
attached branch/ref, and full commit SHA. It makes no authentication, branch
directory, or DataSource request. The checkout needs a commit, an attached branch,
and an unambiguous remote; detached HEAD and an unidentified remote fail explicitly.
Pass `code_repository_dir` when the checkout is outside the working directory.

`get_code_repository_context()` enriches that same source snapshot through the
platform's `resolve-git-context` API. A branch with no visible registration returns
`status == "code_repository_branch_not_registered"` and absent platform UIDs.
Authentication failures, permission failures, and service outages raise their
own lookup errors. They do not invalidate the local source snapshot. Enrichment
records `principal_uid`, `organization_uid`, and `api_url`; subsequent lookups
revalidate identity without repeating branch discovery.

## Select a development Environment

Authenticate normally, then inspect the Environments visible to the account:

```python
from mainsequence.client import OrganizationEnvironment, User

user = User.get_authenticated_user_details()
environments = OrganizationEnvironment.filter()
for environment in environments:
    print(environment.uid, environment.name, environment.organization_owner_uid)
```

Choose an existing Environment explicitly. The SDK never chooses the first
result, substitutes another branch, or creates an Environment or branch as a
side effect. Configure the selection once, before the first Environment-scoped
operation:

```python
from mainsequence.client import Secret
from mainsequence.code_repository_context import (
    DevelopmentEnvironmentSelection,
    configure_development_environment,
    get_organization_environment_context,
)

configure_development_environment(
    DevelopmentEnvironmentSelection(
        organization_environment_uid="58218213-5e4e-43de-a5bd-6757f4e1c8f6",
    )
)
scope = get_organization_environment_context()
secret = Secret.get(name="VENDOR_API_KEY")
```

Replace the example UUID with one available to your account. Selection is typed
and process-local; it is not a login setting, an environment variable, or a new
CLI flag. Listing Environments and reading the authenticated user do not require
Git discovery or branch registration.

On first Environment resolution, the SDK checks the current branch's optional
platform registration, verifies the authenticated User and Organization using
`users/me`, and retrieves the selected Environment through the public
`organization-environments/{uid}/` endpoint. That endpoint enforces visibility;
the SDK also requires the Environment's owner to match the authenticated
Organization. Later resource requests remain subject to platform authorization.

The selection does not change the Git branch. If that branch is registered and
already has an Environment, the explicit selection must match it. A failed
branch lookup is not treated as proof that the branch is unregistered; resolve
the failure and retry. A local source-only consumer can continue using
`get_git_source_context()` independently.

## Scope rules

| Process | Environment resolution |
| --- | --- |
| Human, registered branch, no selection | Use the branch's Environment after access and Organization verification. |
| Human, unregistered branch, explicit selection | Use the selected authorized Environment. |
| Human, no selected or branch-derived Environment | Raise `CodeRepositoryEnvironmentContextRequiredError` only for Environment-dependent operations. |
| Authenticated runtime | Use the authenticated target, with source/target checks; development selection is rejected. |

`get_organization_environment_context()` returns an immutable snapshot containing
`organization_environment_uid`, `organization_uid`, `principal_uid`, `api_url`,
`process_id`, and provenance in `source`: `authenticated_runtime`,
`explicit_development`, or `registered_branch`.

Secrets, Constants, Buckets, and Artifacts consume this shared resolver. Their
methods still reject per-call Environment overrides. Human requests carry the
SDK-owned `organization_environment_uid` in the normal body/query field; runtime
requests omit that selector and the platform derives scope from the credential.

Current-branch collection operations on Jobs, CodeRepositoryImages,
CodeRepositoryResources, ResourceReleases, and DeploymentRuns still require a
registered branch. Explicit target-resource operations retain their existing
ownership and authorization rules.

## Lifetime, failures, and reset

Source discovery, optional platform enrichment, and Environment scope have
separate synchronized state. Concurrent first calls share immutable snapshots.
Importing the SDK does not resolve any of them. Forked children receive fresh
locks and empty state; configure their development selection again.

Each platform-context resolution rechecks `users/me` through the existing transport;
Environment resolution uses that same verified identity. This adds one identity
request per successful platform/Environment resolver call and detects a changed account or
Organization without trusting unverified token contents. Refreshing a token for
the same principal preserves the cached scope. A principal, Organization,
endpoint, or scope change raises `AuthenticatedContextChangedError`; it cannot
reuse the previous account's scope. The Environment detail lookup is cached;
resource authorization is enforced again by the server on every request.

Failed optional lookups are cached. After fixing a temporary failure:

```python
from mainsequence.code_repository_context import retry_failed_context_resolution

retry_failed_context_resolution()
```

This clears failed enrichment/Environment attempts and preserves frozen source
and any previously resolved scope. It cannot switch identity, refresh an already
resolved unregistered result, or recover changed Git facts. An Environment
attempt that failed before establishing scope can be retried after configuring
a selection.

After stopping current development work, use a fresh process or explicitly reset:

```python
from mainsequence.code_repository_context import (
    reset_development_context,
    validate_git_source_context,
)

validate_git_source_context()  # Network-free; raises if checkout, branch, or HEAD changed.
# Once the previous work is stopped:
reset_development_context()
# Configure the next selection and resolve the new checkout when needed.
```

Reset clears source, enrichment, Environment, and explicit selection; it retains
authentication. Reset/retry is rejected while a resolver is running. Do not change
credentials or reset while resource requests are in flight. Deployed runtimes
must start a fresh process and cannot use the development reset API.

The SDK does not implement local catalogs, SQLite/DuckDB storage, or a local
DataSource override. Consumers such as MetaTables use the public Git and User
interfaces and own their local workspace and storage behavior.
