from __future__ import annotations

import datetime
import inspect

import pytest

import mainsequence.client.github_issues as github_issues
import mainsequence.code_repository_context as code_repository_context
from mainsequence.client.exceptions import ConflictError
from mainsequence.client.github_issues import (
    GitHubIssue,
    GitHubIssueComment,
    GitHubIssueOperation,
)
from mainsequence.client.models_foundry import CodeRepositoryBranch

CODE_REPOSITORY_UID = "1d0530c0-65d1-4db0-856b-dc29d8260a09"
BRANCH_UID = "5a28020a-0f1b-47ee-aab8-334286234bea"
OTHER_BRANCH_UID = "9bcaaf64-3af8-4a55-a9e1-cc7b0898fd48"
ENVIRONMENT_UID = "a5e95092-a77a-45a6-835c-46d327e8b5e7"
ISSUE_UID = "31c15ea4-d785-4df7-9f93-d4d8a420ab6e"
COMMENT_UID = "79fd4d2e-3dd6-4da8-9e5a-d6137240e092"
OPERATION_UID = "e0d34d4a-d30a-4f2f-b46f-c2e496eb7bc9"
COMMIT_SHA = "a" * 40


class Response:
    def __init__(self, status_code: int, data):
        self.status_code = status_code
        self._data = data
        self.content = b"{}"
        self.text = ""
        self.headers = {}

    def json(self):
        return self._data


def _issue_payload(*, include_detail: bool = True):
    payload = {
        "uid": ISSUE_UID,
        "code_repository_uid": CODE_REPOSITORY_UID,
        "origin_code_repository_branch_uid": BRANCH_UID,
        "organization_environment_uid": ENVIRONMENT_UID,
        "repository_branch": "main",
        "branch_head_commit_sha": COMMIT_SHA,
        "branch_context_visible": True,
        "github_repository_binding_uid": "0d381b55-255d-4387-aabb-3bb4f0a31d18",
        "github_repository_id": 123,
        "github_issue_id": 456,
        "github_issue_node_id": "I_kwDOExample",
        "issue_number": 17,
        "html_url": "https://github.com/mainsequence-sdk/example/issues/17",
        "title": "Deployment failure",
        "state": "open",
        "state_reason": None,
        "created_at": "2026-09-14T10:00:00Z",
        "updated_at": "2026-09-14T10:01:00Z",
        "closed_at": None,
        "last_synced_at": "2026-09-14T10:01:01Z",
    }
    if include_detail:
        payload.update(
            {
                "body": "Sanitized reproduction.",
                "author_login": "mainsequence-app",
            }
        )
    return payload


def _comment_payload():
    return {
        "uid": COMMENT_UID,
        "github_issue_uid": ISSUE_UID,
        "github_comment_id": 789,
        "github_comment_node_id": "IC_kwDOExample",
        "html_url": "https://github.com/mainsequence-sdk/example/issues/17#issuecomment-789",
        "body": "Fixed by commit abc123.",
        "author_login": "mainsequence-app",
        "created_at": "2026-09-14T10:02:00Z",
        "updated_at": "2026-09-14T10:02:00Z",
    }


def _operation_payload():
    return {
        "uid": OPERATION_UID,
        "operation_type": "create_issue",
        "status": "unknown",
        "github_issue_uid": None,
        "github_issue_id": 456,
        "issue_number": 17,
        "github_comment_uid": None,
        "github_comment_id": None,
        "retryable": False,
        "error_code": "",
        "created_at": "2026-09-14T10:00:00Z",
        "updated_at": "2026-09-14T10:00:01Z",
        "completed_at": None,
    }


@pytest.fixture(autouse=True)
def _reset_context(monkeypatch):
    code_repository_context._reset_code_repository_context()
    monkeypatch.setattr(
        github_issues.client_utils,
        "API_ENDPOINT",
        "https://backend.example/api/v1",
    )
    yield
    code_repository_context._reset_code_repository_context()


def _branch(uid=BRANCH_UID):
    return CodeRepositoryBranch.model_construct(uid=uid)


def test_branch_issue_list_uses_target_branch_and_no_environment_selector(monkeypatch):
    requests = []

    def fake_make_request(**kwargs):
        requests.append(kwargs)
        return Response(
            200,
            {"next_cursor": "opaque-cursor", "results": [_issue_payload(include_detail=False)]},
        )

    monkeypatch.setattr(github_issues, "make_request", fake_make_request)
    updated_since = datetime.datetime(2026, 9, 14, 10, tzinfo=datetime.UTC)

    first = _branch(OTHER_BRANCH_UID).list_github_issues(
        state="open",
        updated_since=updated_since,
        cursor="input-cursor",
        limit=25,
    )
    second = _branch(OTHER_BRANCH_UID).list_github_issues(limit=50)

    assert first.next_cursor == "opaque-cursor"
    assert first.results[0].uid == ISSUE_UID
    assert second.results[0].issue_number == 17
    assert requests[0]["url"] == (
        "https://backend.example/api/v1/code-repository-branches/"
        f"{OTHER_BRANCH_UID}/github-issues/"
    )
    assert requests[0]["payload"]["params"] == {
        "limit": 25,
        "cursor": "input-cursor",
        "updated_since": "2026-09-14T10:00:00+00:00",
        "state": "open",
    }
    assert "organization_environment_uid" not in requests[0]["payload"]["params"]


def test_list_issues_does_not_resolve_process_branch(monkeypatch):
    captured = {}

    def fail_context_resolution(*args, **kwargs):
        pytest.fail("targeted issue listing must not resolve the process branch")

    monkeypatch.setattr(
        code_repository_context,
        "resolve_code_repository_branch_uid",
        fail_context_resolution,
    )
    monkeypatch.setattr(
        code_repository_context,
        "resolve_organization_environment_uid",
        fail_context_resolution,
    )
    monkeypatch.setattr(
        github_issues,
        "make_request",
        lambda **kwargs: captured.update(kwargs)
        or Response(200, {"next_cursor": None, "results": []}),
    )

    result = _branch(OTHER_BRANCH_UID).list_github_issues()

    assert result.results == []
    assert captured["url"].endswith(f"/{OTHER_BRANCH_UID}/github-issues/")


def test_create_issue_uses_target_branch_without_process_branch_resolution(monkeypatch):
    captured = {}

    def fail_context_resolution(*args, **kwargs):
        pytest.fail("targeted issue creation must not resolve the process branch")

    monkeypatch.setattr(
        code_repository_context,
        "resolve_code_repository_branch_uid",
        fail_context_resolution,
    )
    monkeypatch.setattr(
        code_repository_context,
        "resolve_organization_environment_uid",
        fail_context_resolution,
    )
    monkeypatch.setattr(
        github_issues,
        "make_request",
        lambda **kwargs: captured.update(kwargs) or Response(201, _issue_payload()),
    )

    result = _branch(OTHER_BRANCH_UID).create_github_issue(
        title="Deployment failure",
        idempotency_key="cross-branch-create",
    )

    assert isinstance(result, GitHubIssue)
    assert captured["url"] == (
        "https://backend.example/api/v1/code-repository-branches/"
        f"{OTHER_BRANCH_UID}/github-issues/"
    )


def test_create_issue_preserves_body_and_parses_synchronous_response(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        github_issues,
        "make_request",
        lambda **kwargs: captured.update(kwargs) or Response(201, _issue_payload()),
    )
    body = "  Exact body\nwith trailing whitespace.  "

    result = _branch().create_github_issue(
        title="Deployment failure",
        body=body,
        idempotency_key="agent-run-123:create",
        timeout=8,
    )

    assert isinstance(result, GitHubIssue)
    assert captured["r_type"] == "POST"
    assert captured["payload"]["json"] == {
        "title": "Deployment failure",
        "body": body,
        "idempotency_key": "agent-run-123:create",
    }
    assert captured["time_out"] == 8


def test_accepted_mutation_returns_operation_without_retry(monkeypatch):
    calls = []

    def fake_make_request(**kwargs):
        calls.append(kwargs)
        return Response(202, _operation_payload())

    monkeypatch.setattr(github_issues, "make_request", fake_make_request)

    result = _branch().create_github_issue(
        title="Deployment failure",
        idempotency_key="agent-run-123-create",
    )

    assert isinstance(result, GitHubIssueOperation)
    assert result.status == "unknown"
    assert len(calls) == 1


def test_replayed_issue_create_http_200_returns_issue(monkeypatch):
    monkeypatch.setattr(
        github_issues,
        "make_request",
        lambda **kwargs: Response(200, _issue_payload()),
    )

    result = _branch().create_github_issue(
        title="Deployment failure",
        idempotency_key="agent-run-123-create",
    )

    assert isinstance(result, GitHubIssue)


def test_issue_detail_and_operation_use_only_main_sequence_public_uids(monkeypatch):
    requests = []

    def fake_make_request(**kwargs):
        requests.append(kwargs)
        if "github-issue-operations" in kwargs["url"]:
            return Response(200, _operation_payload())
        return Response(200, _issue_payload())

    monkeypatch.setattr(github_issues, "make_request", fake_make_request)

    issue = GitHubIssue.get_by_uid(ISSUE_UID)
    operation = GitHubIssueOperation.get_by_uid(OPERATION_UID)

    assert issue.uid == ISSUE_UID
    assert operation.uid == OPERATION_UID
    assert requests[0]["url"].endswith(f"/github-issues/{ISSUE_UID}/")
    assert requests[1]["url"].endswith(f"/github-issue-operations/{OPERATION_UID}/")
    assert requests[0]["payload"] == {}
    with pytest.raises(ValueError, match="valid public UUID"):
        GitHubIssue.get_by_uid(456)  # type: ignore[arg-type]


def test_issue_update_sends_only_explicit_fields_including_null_reason(monkeypatch):
    issue = GitHubIssue.model_validate(_issue_payload())
    captured = {}
    response_payload = {**_issue_payload(), "body": "", "state_reason": None}
    monkeypatch.setattr(
        github_issues,
        "make_request",
        lambda **kwargs: captured.update(kwargs) or Response(200, response_payload),
    )

    result = issue.update(
        body="",
        state_reason=None,
        idempotency_key="agent-run-123-update",
    )

    assert isinstance(result, GitHubIssue)
    assert captured["payload"]["json"] == {
        "body": "",
        "state_reason": None,
        "idempotency_key": "agent-run-123-update",
    }


def test_comments_preserve_cursor_and_body_and_parse_async_result(monkeypatch):
    issue = GitHubIssue.model_validate(_issue_payload())
    requests = []

    def fake_make_request(**kwargs):
        requests.append(kwargs)
        if kwargs["r_type"] == "GET":
            return Response(
                200,
                {"next_cursor": "next-opaque", "results": [_comment_payload()]},
            )
        return Response(202, {**_operation_payload(), "operation_type": "create_comment"})

    monkeypatch.setattr(github_issues, "make_request", fake_make_request)
    page = issue.list_comments(cursor="opaque-input", limit=10)
    body = "  exact comment\n"
    result = issue.create_comment(
        body=body,
        idempotency_key="agent-run-123-comment",
    )

    assert isinstance(page.results[0], GitHubIssueComment)
    assert page.next_cursor == "next-opaque"
    assert requests[0]["payload"]["params"] == {
        "limit": 10,
        "cursor": "opaque-input",
    }
    assert requests[1]["payload"]["json"]["body"] == body
    assert isinstance(result, GitHubIssueOperation)
    assert len(requests) == 2


def test_synchronous_comment_create_returns_comment(monkeypatch):
    issue = GitHubIssue.model_validate(_issue_payload())
    monkeypatch.setattr(
        github_issues,
        "make_request",
        lambda **kwargs: Response(201, _comment_payload()),
    )

    result = issue.create_comment(
        body="Fixed by commit abc123.",
        idempotency_key="agent-run-123-comment",
    )

    assert isinstance(result, GitHubIssueComment)
    assert result.uid == COMMENT_UID


@pytest.mark.parametrize(
    ("operation", "message"),
    [
        (
            lambda branch: branch.list_github_issues(
                updated_since=datetime.datetime(2026, 9, 14, 10)
            ),
            "explicit timezone",
        ),
        (lambda branch: branch.list_github_issues(limit=0), "between 1 and 100"),
        (lambda branch: branch.list_github_issues(cursor=""), "non-empty opaque"),
        (
            lambda branch: branch.create_github_issue(
                title="Failure",
                idempotency_key="contains spaces",
            ),
            "may contain only",
        ),
    ],
)
def test_request_validation_matches_backend_contract(monkeypatch, operation, message):
    monkeypatch.setattr(
        github_issues,
        "make_request",
        lambda **kwargs: pytest.fail("invalid input must fail before HTTP"),
    )

    with pytest.raises((TypeError, ValueError), match=message):
        operation(_branch())


def test_backend_error_code_remains_available_on_api_error(monkeypatch):
    monkeypatch.setattr(
        github_issues,
        "make_request",
        lambda **kwargs: Response(
            409,
            {
                "code": "github_issue_detached",
                "detail": "The issue binding is detached.",
            },
        ),
    )

    with pytest.raises(ConflictError) as raised:
        GitHubIssue.get_by_uid(ISSUE_UID)

    assert raised.value.payload["code"] == "github_issue_detached"


def test_github_issue_models_do_not_expose_nonexistent_generic_crud():
    for model in (GitHubIssue, GitHubIssueOperation, GitHubIssueComment):
        assert not hasattr(model, "filter")
        assert not hasattr(model, "create")
        assert not hasattr(model, "delete")


def test_public_methods_expose_no_branch_environment_or_provider_selector():
    forbidden = {
        "branch_uid",
        "code_repository_branch_uid",
        "organization_environment_uid",
        "repository_owner",
        "repository_name",
        "github_id",
        "installation_id",
        "token",
        "url",
    }
    public_methods = (
        CodeRepositoryBranch.list_github_issues,
        CodeRepositoryBranch.create_github_issue,
        GitHubIssue.get_by_uid,
        GitHubIssue.update,
        GitHubIssue.list_comments,
        GitHubIssue.create_comment,
        GitHubIssueOperation.get_by_uid,
    )

    for method in public_methods:
        assert forbidden.isdisjoint(inspect.signature(method).parameters)
