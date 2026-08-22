# ADR 0031: Git-Native Process Project Context

Date: 2026-08-22

Status: Accepted

Platform decision: `tdag-django` ADR-0037, stable fingerprint
`git-native-project-source-context-v1`

## Context

Project code runs in local checkouts and platform-built runtime images. The SDK
previously used two identity mechanisms: local `.env` plus Git for developer
work, and authenticated ProjectBranch values projected into process environment
for deployed work. That split allowed source identity to depend on mutable
configuration even though both locations execute a Git checkout.

Platform ADR-0037 makes the checked-out Git repository the single source of
truth for repository, branch, and commit identity. Runtime authorization remains
backend-owned and target-derived; Git identity never grants permission.

## Ontology

| Concept | Responsibility |
| --- | --- |
| Git worktree | Identifies the repository, attached branch/ref, and exact commit executed by this process |
| `GitRepository` | Maps canonical Git repository identity to the logical platform `Project` |
| `Project` | Repository-level platform aggregate; it does not authorize a branch or Environment |
| `ProjectBranch` | Persisted platform identity for the exact Project and Git branch |
| `OrganizationProjectEnvironment` | Environment derived from the resolved ProjectBranch |
| `ProjectBranch.metatables_data_source` | Optional physical MetaTables DataSource for that exact branch |
| Runtime credential or human token | Authenticates and authorizes the caller; it does not select source identity |

## Decision

The SDK has one project-context entry point:

```python
get_project_runtime_context()
```

It uses the same algorithm in a local shell, CLI process, Job, FastAPI release,
Streamlit release, runtime-agent release, or Project Executor:

```text
containing Git worktree
  -> canonical non-secret repository identity
  -> attached refs/heads/<branch>
  -> exact full HEAD commit
  -> backend resolve-git-context contract
  -> GitRepository
  -> Project
  -> exact ProjectBranch
  -> OrganizationProjectEnvironment
  -> optional ProjectBranch MetaTables DataSource
```

Authentication mode affects credentials only. The resolver does not choose a
local or deployed identity path.

### Frozen source snapshot

The first call resolves and freezes:

```text
repository_root
canonical_repository_identity
repository_branch
repository_ref
commit_sha
project_uid | None
project_branch_uid | None
organization_project_environment_uid | None
metatables_data_source | None
status
process_id
```

Initialization is protected by a process-local lock. Concurrent callers receive
the identical immutable object. Resolution errors are cached. A forked child
discards inherited state and resolves once for its own process.

A deliberate `git switch` takes effect in the next process. Later validation in
the same process rejects a changed repository, branch, or HEAD as source-context
drift; it does not silently retarget platform work.

### Valid unresolved states

Resolution itself does not fail merely because no visible ProjectBranch maps to
the repository and branch:

```text
project_branch_not_registered
```

The canonical backend action deliberately exposes one not-found result rather
than leaking whether the repository or only its branch is registered. This
permits ordinary work on local repositories and new feature branches. Only an
operation that owns or enumerates branch-linked platform objects calls
`require_project_branch_context()` and fails before its backend request.

No unresolved state may fall back to `main`, a sibling ProjectBranch, a
Project-level DataSource, an Environment inferred from a DataSource, collection
order, or a caller-supplied ProjectBranch UID. The removed
`Project.default_metatables_data_source` contract is not restored.

### Retired environment contract

The SDK does not read, write, or inject these values as project identity:

```text
MAIN_SEQUENCE_PROJECT_UID
MAIN_SEQUENCE_PROJECT_BRANCH_UID
MAINSEQUENCE_REPOSITORY_BRANCH
MAIN_SEQUENCE_ORGANIZATION_PROJECT_ENVIRONMENT_UID
```

Local `.env` stores only unrelated user configuration plus supported endpoint
and authentication transport. Setup and refresh remove stale occurrences of all
four variables. Existing process values are ignored, not treated as fallbacks.

The SDK still supports authentication variables such as
`MAINSEQUENCE_ENDPOINT`, `MAINSEQUENCE_AUTH_MODE`, access/refresh tokens, and
runtime credentials. They answer where and as whom to call, not which project
source is running.

### Branch-owned consumers

Current-branch Job, ProjectImage, ResourceRelease, ProjectResource,
ProjectExecutor, migration, platform-managed MetaTable, and DataNode workflows
consume the frozen context. Ordinary branch-owned collections are scoped to the
resolved ProjectBranch. Explicit administrative enumeration uses separately
named admin APIs.

Parent-derived operations retain the persisted parent's ownership instead of
inventing another branch selector. Backend authorization remains the final
enforcement boundary and deployed runtime endpoints must require the
Git-resolved ProjectBranch to equal the authenticated runtime target.

## Consequences

- Local and deployed project code use one resolution algorithm.
- Source identity cannot be changed by environment injection.
- Every project-sensitive consumer sees one repository, branch, and commit for
  the complete process run.
- Unregistered branches remain usable for local development but cannot create,
  mutate, or enumerate branch-owned platform resources.
- Supported runtime images must preserve a sanitized attached Git checkout.
  Images without that contract must be rebuilt; the SDK has no permanent
  environment fallback.
- Repository mapping uses the canonical
  `POST /api/v1/project-branches/resolve-git-context/` action. That action also
  verifies exact equality with an authenticated JobRun or Knative runtime
  target.

## Rejected Alternatives

### Keep a deployed environment projection

Rejected because it recreates a second mutable source of repository and branch
identity and conflicts with platform ADR-0037.

### Resolve on every operation

Rejected because it repeats Git/backend work and lets one process change
ownership after a branch switch.

### Fail every operation on an unregistered branch

Rejected because local Git development does not require a persisted
ProjectBranch. Enforcement belongs at branch-owned platform operations.

### Accept explicit ProjectBranch UIDs for current-project work

Rejected because callers could select a branch that differs from the source
actually executing. Explicit Project UIDs retained by some CLI commands are
assertions against Git resolution, never selectors.

## Required Invariants

- Git is the only SDK source of repository, branch/ref, and commit identity.
- Exactly one SDK entry performs current-run platform mapping.
- Resolution runs once per process and once again after a fork.
- Detached HEAD, missing Git metadata, missing/ambiguous remote identity, and
  source drift fail explicitly.
- Unregistered repository or branch state does not abort unrelated local work.
- Branch-owned operations reject unresolved context before backend calls.
- The four retired identity variables never influence source resolution.
- No ProjectBranch, Environment, sibling-branch, or DataSource fallback exists.
- Backend authorization remains authoritative and must verify deployed target
  equality.
