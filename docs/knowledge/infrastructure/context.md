# Git source and Environment context

The SDK reads Git source facts independently of platform branch registration.
A missing Environment is allowed until an operation actually requires one.

## Local source discovery

```python
from mainsequence.code_repository_context import get_git_source_context

source = get_git_source_context()
print(source.canonical_repository_identity, source.repository_branch, source.commit_sha)
```

This reads the actual checkout without network calls, login, or platform
registration. `validate_git_source_context()` detects changes to the frozen
repository, branch, or commit. It never substitutes another branch.

## Optional platform metadata

```python
from mainsequence.code_repository_context import get_code_repository_context

context = get_code_repository_context()
print(context.status, context.organization_environment_uid)
```

This uses the normal SDK session to resolve the source through the platform.
An unregistered branch has status `code_repository_branch_not_registered` and no
platform branch or Environment UID. A registered branch may also have no
Environment. Both are valid results. Authentication, permission, and backend
failures are reported rather than treated as missing registration.

The platform result is cached for the process. Reading it again does not perform
an identity preflight or compare the authenticated account or Organization.
Start a fresh process after changing checkout or platform registration. Forked
processes resolve their own context.

## Operations that require scope

Constants, Secrets, Buckets, and Artifacts use the current registered branch's
Environment. If it has none, these operations raise
`CodeRepositoryEnvironmentContextRequiredError`. Operations requiring a registered
branch raise `CodeRepositoryBranchContextRequiredError` when it is unavailable.
The Environment error retains its existing branch-error superclass; both can be
caught through `CodeRepositoryContextError`.

For example, on an unregistered `test` branch, `get_git_source_context()` works and
`get_code_repository_context()` can return the unregistered snapshot.
`Secret.filter()` raises a missing-Environment error. Login, User directory calls,
and other unscoped operations remain available.

There is no Environment selection API or per-call Environment override. The SDK
does not choose another branch's Environment. Authenticated runtimes preserve the
existing checks against their backend-authenticated target.

Organization access decisions remain in the backend. Selecting a resource through
the SDK's normal context does not replace backend authorization. Local application
storage can use the Git source context without invoking platform-scoped resources.

See [ADR 0035](../../adr/0035-independent-git-source-and-platform-context.md).

## Release discovery by name

`ResourceRelease.filter(name=...)` and filtered `ResourceRelease.get(name=...)`
accept exact names while retaining the current CodeRepositoryBranch context.
For a shared deployment in another repository, use the existing explicit
`filter_admin` path with its owning branch, or retrieve a known release UID.
See [Resource releases](resource_releases.md) for supported filters, examples,
and the difference between collection rows and full release details.
