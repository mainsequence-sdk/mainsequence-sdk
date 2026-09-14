# GitHub Issues

The SDK exposes GitHub issues through Main Sequence backend resources. It does
not call GitHub directly, read GitHub credentials, or let application code
select a repository, provider installation, branch, or Organization
Environment.

## Branch Context

Issue creation and collection listing are anchored to the Git branch executing
the current process. Obtain the branch from the SDK's process-frozen
CodeRepository context:

```python
from mainsequence.code_repository_context import get_code_repository_context

context = get_code_repository_context()
branch = context.code_repository_branch

page = branch.list_github_issues(state="open", limit=50)
```

The first context lookup resolves the actual Git repository, attached branch,
and commit through the canonical backend Git-context endpoint. That response
also supplies the `CodeRepositoryBranch` and its Organization Environment. The
complete result is locked and cached for the lifetime of the process.

`list_github_issues()` and `create_github_issue()` verify that the receiving
branch instance has the same UID as the process-frozen branch. An arbitrary or
stale `CodeRepositoryBranch` cannot redirect the operation. The methods expose
no branch or Environment argument.

The nested backend endpoint derives Environment ownership from the validated
branch. The SDK therefore does not transmit an Environment selector. This is
still the same single resolution path: branch and Environment were resolved
together and cached before the issue request.

An unregistered local branch remains usable for unrelated development. GitHub
issue listing or creation fails before making its request because those
operations require a registered current branch and its derived Environment.

## Create An Issue

```python
result = branch.create_github_issue(
    title="Deployment failure",
    body="Sanitized reproduction.",
    idempotency_key="agent-run-123-create",
)
```

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
