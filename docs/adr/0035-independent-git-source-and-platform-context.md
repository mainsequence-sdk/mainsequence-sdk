# ADR 0035: Independent Git source and optional platform context

Date: 2026-09-28

Status: Accepted and implemented on `metatables_removal`.

## Context

A developer may work on a Git branch that is not registered with the platform,
or on a registered branch that has no Environment. These are valid source
contexts. Requiring an Environment while discovering source facts prevents
unrelated work, including application-owned local storage development.

The SDK must defer prerequisite errors to the operations that need those
prerequisites. This does not introduce Environment selection or change existing
authentication, authorization, or process-context caching.

## Decision

1. `get_git_source_context()` reads and freezes the checkout, canonical repository
   identity, attached branch, ref, and exact commit. It performs no platform
   requests and requires no login or branch registration.
2. `get_code_repository_context()` enriches those same source facts using the
   existing SDK request/authentication pipeline. An unregistered branch returns
   `code_repository_branch_not_registered`, with platform UIDs and Environment
   absent. A registered branch may also have no Environment. Neither condition
   causes this general context resolver to raise.
3. Operations requiring a registered platform branch keep their branch guard.
   Operations requiring an Environment call `resolve_organization_environment_uid()`
   and raise `CodeRepositoryEnvironmentContextRequiredError` if none is present.
   Constants, Secrets, Buckets, and Artifacts continue to derive their Environment
   from the registered current branch. There is no explicit Environment override,
   synthetic registration, or fallback to another branch's Environment.
4. Platform metadata remains a process snapshot. Repeated access returns the
   cached context without `/users/me/` requests, account or endpoint binding, or
   Organization comparisons. Start a fresh process to resolve a changed checkout
   or newly registered branch; no development selector/reset lifecycle is added.
5. Authentication and genuine lookup failures propagate as errors. A rejected
   credential, forbidden lookup, or backend outage is not an unregistered result.
   Such failures do not invalidate independently discovered Git facts.
6. Existing authenticated runtime target checks remain. Git source must match
   the backend-authenticated repository, branch, and Environment target. Source
   validation detects changes to the frozen checkout. Forked processes establish
   their own snapshots.

## Authentication and authorization boundary

The SDK's established CLI credential and endpoint bootstrap remains available.
JWT, session-JWT, and runtime credentials use their existing providers. Context
resolution introduces no account revalidation requests or new authentication mode.

Organization membership, ownership, and permissions are enforced by the platform
backend. Context discovery does not require Organization metadata or perform an
Environment ownership preflight. Platform resource response fields remain data.
Caller assertion verification and authenticated runtime target validation remain
separate from Organization authorization.

## Capability matrix

| Current source | Git discovery | General platform context | Environment-owned operation | Branch-owned operation |
| --- | --- | --- | --- | --- |
| Unregistered branch | Available without authentication | Unregistered snapshot after normal platform lookup | Missing-Environment error | Missing-branch error |
| Registered branch without Environment | Available | Resolved snapshot with no Environment | Missing-Environment error | Available subject to normal backend authorization |
| Registered branch with Environment | Available | Resolved snapshot | Uses that branch's Environment | Available subject to normal backend authorization |
| Authenticated runtime | Available | Existing runtime target checks apply | Uses verified target context | Existing target guards apply |

Login, User/Organization directory calls, and other operations without branch or
Environment prerequisites do not acquire those prerequisites from this change.
Application-owned local storage uses Git facts without needing a platform Environment.

## Relationship to ADR 0031

This decision changes only the requirement to resolve platform registration while
reading local source facts, and clarifies where missing-prerequisite errors occur.
ADR 0031's branch-derived Environment, process snapshot, source drift, and deployed
runtime target rules remain. ADR 0034 removes the SDK's MetaTables implementation.

## Validation

Tests cover unregistered and registered branches without Environments, scoped
resource failures, unrelated calls after those failures, registered branch scope,
cached context without identity requests, backend lookup failures, concurrency,
fork isolation, source drift, and unchanged runtime target guards.
