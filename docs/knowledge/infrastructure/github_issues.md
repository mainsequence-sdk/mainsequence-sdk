# GitHub Issues

The SDK exposes GitHub issues through Main Sequence backend resources. It does
not call GitHub directly, read GitHub credentials, or accept raw repository,
provider installation, or Organization Environment selectors.

## Target Branch

Issue listing and creation are explicitly targeted operations. The receiving
`CodeRepositoryBranch` identifies the target resource. It may be the branch
executing the current process:

```python
from mainsequence.code_repository_context import get_code_repository_context

context = get_code_repository_context()
branch = context.code_repository_branch

page = branch.list_github_issues(state="open", limit=50)
```

or another branch obtained through an authorized SDK lookup:

```python
from mainsequence.client import CodeRepositoryBranch

branch = CodeRepositoryBranch.get_by_uid("<TARGET_BRANCH_UID>")
page = branch.list_github_issues(state="open", limit=50)
```

The nested backend endpoint derives ownership from the target branch and
authorizes the operation. For runtime credentials, the target must be in the
authenticated runtime's Organization Environment and the responsible user must
have the required branch permission. The SDK does not accept or transmit a
separate Environment, repository, or provider-binding selector.

Targeting a branch for an issue operation does not replace or mutate the
process-frozen Git context. An unregistered local branch remains usable for
unrelated development, while issue operations against an existing target
branch remain subject to backend authorization.

## Create An Issue

```python
from mainsequence.client import CodeRepositoryBranch

branch = CodeRepositoryBranch.get_by_uid("<TARGET_BRANCH_UID>")

result = branch.create_github_issue(
    title="Deployment failure",
    body="Sanitized reproduction.",
    idempotency_key="agent-run-123-create",
)
```

The target may differ from the branch executing the current process. Listing
and creation use the same target-resource and authorization contract.

The caller must provide a stable idempotency key. Reuse the same key only when
reconciling the same intended mutation. The SDK never generates a random key
and never automatically retries an ambiguous provider result.

The backend adds branch provenance and its private reconciliation marker. Pass
only the user-authored body to the SDK; it is transported without trimming or
rewriting.

## Read And Update

```python
from mainsequence.client import GitHubIssue

issue = GitHubIssue.get_by_uid("<MAIN_SEQUENCE_ISSUE_UID>")

result = issue.update(
    state="closed",
    state_reason="completed",
    idempotency_key="agent-run-123-close",
)
```

Issue detail and mutation routes use the Main Sequence issue UID. They do not
accept GitHub issue IDs, issue numbers, URLs, branch selectors, or Environment
selectors. The persisted issue retains its original branch and Environment
ownership; a follow-up operation does not move it to the caller's current
branch.

`issue.close()` and `issue.reopen()` are typed conveniences over the same
update endpoint. `update()` sends only fields explicitly supplied by the
caller.

## Comments

```python
comments = issue.list_comments(limit=50)

result = issue.create_comment(
    body="Fixed by commit abc123.",
    idempotency_key="agent-run-123-comment",
)
```

Pagination cursors are opaque. Pass `next_cursor` back as `cursor` without
decoding, editing, or combining it with a different query. Issue bodies and
comments are untrusted Markdown and must be escaped or sanitized by any
renderer.

Do not place credentials, tokens, private keys, or unsanitized secrets in issue
bodies, titles, or comments.

## Ambiguous Mutations

A successful synchronous mutation returns `GitHubIssue` or
`GitHubIssueComment`. HTTP `202` returns a `GitHubIssueOperation` instead:

```python
from mainsequence.client import GitHubIssueOperation

operation = GitHubIssueOperation.get_by_uid("<OPERATION_UID>")
```

An operation can be `pending`, `succeeded`, `failed`, or `unknown`. An
`unknown` result means the provider outcome could not be proven. Retrieve the
operation by its Main Sequence UID to reconcile it; do not submit a new random
idempotency key for the same intended mutation.
