from __future__ import annotations

import datetime
import re
from typing import Any, ClassVar, Literal
from uuid import UUID

from pydantic import Field

from . import utils as client_utils
from .base import BasePydanticModel
from .exceptions import raise_for_response
from .utils import make_request
from .value_sets import OpenValueSet

GitHubIssueState = Literal["open", "closed"]
GitHubIssueStateReason = Literal["completed", "not_planned", "reopened"]
GitHubIssueOperationType = OpenValueSet[
    Literal["create_issue", "update_issue", "create_comment"]
]
GitHubIssueOperationStatus = OpenValueSet[
    Literal["pending", "succeeded", "failed", "unknown"]
]

_IDEMPOTENCY_KEY_PATTERN = re.compile(r"^[A-Za-z0-9._:-]+$")


class _UnsetType:
    __slots__ = ()


_UNSET = _UnsetType()


def _api_url(path: str) -> str:
    return f"{client_utils.API_ENDPOINT.rstrip('/')}/{path.lstrip('/')}"


def _public_uid(value: str | UUID, *, field_name: str) -> str:
    try:
        return str(UUID(str(value)))
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError(f"{field_name} must be a valid public UUID.") from exc


def _idempotency_key(value: str) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= 128:
        raise ValueError("idempotency_key must contain between 1 and 128 characters.")
    if _IDEMPOTENCY_KEY_PATTERN.fullmatch(value) is None:
        raise ValueError(
            "idempotency_key may contain only letters, numbers, period, underscore, colon, "
            "and hyphen."
        )
    return value


def _timestamp(value: datetime.datetime, *, field_name: str) -> str:
    if not isinstance(value, datetime.datetime):
        raise TypeError(f"{field_name} must be a datetime.")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include an explicit timezone.")
    return value.isoformat()


def _page_params(
    *,
    cursor: str | None,
    limit: int,
    updated_since: datetime.datetime | None,
) -> dict[str, Any]:
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 100:
        raise ValueError("limit must be an integer between 1 and 100.")
    params: dict[str, Any] = {"limit": limit}
    if cursor is not None:
        if not isinstance(cursor, str) or not cursor or len(cursor) > 512:
            raise ValueError("cursor must be a non-empty opaque string of at most 512 characters.")
        params["cursor"] = cursor
    if updated_since is not None:
        params["updated_since"] = _timestamp(updated_since, field_name="updated_since")
    return params


def _request_json(
    *,
    method: str,
    url: str,
    payload: dict[str, Any] | None,
    timeout: int | float | tuple[float, float] | None,
    expected_statuses: tuple[int, ...],
) -> tuple[int, dict[str, Any]]:
    request_payload = payload or {}
    response = make_request(
        s=client_utils.session,
        loaders=client_utils.loaders,
        r_type=method,
        url=url,
        payload=request_payload,
        time_out=timeout,
    )
    if response.status_code not in expected_statuses:
        raise_for_response(response, payload=request_payload or None)
    data = response.json()
    if not isinstance(data, dict):
        raise TypeError("GitHub issue endpoints must return a JSON object.")
    return response.status_code, data


class _GitHubIssueProjection(BasePydanticModel):
    uid: str
    code_repository_uid: str
    origin_code_repository_branch_uid: str
    organization_environment_uid: str
    repository_branch: str
    branch_head_commit_sha: str
    branch_context_visible: bool
    github_repository_binding_uid: str | None
    github_repository_id: int
    github_issue_id: int
    github_issue_node_id: str
    issue_number: int
    html_url: str
    title: str
    state: OpenValueSet[GitHubIssueState]
    state_reason: str | None
    created_at: datetime.datetime
    updated_at: datetime.datetime
    closed_at: datetime.datetime | None
    last_synced_at: datetime.datetime


class GitHubIssueListItem(_GitHubIssueProjection):
    """Body-less issue projection returned by a branch collection."""


class GitHubIssuePage(BasePydanticModel):
    """One branch-scoped page of GitHub issues."""

    next_cursor: str | None
    results: list[GitHubIssueListItem] = Field(default_factory=list)


class GitHubIssueComment(BasePydanticModel):
    """One provider-backed comment addressed by its Main Sequence UID."""

    uid: str
    github_issue_uid: str
    github_comment_id: int
    github_comment_node_id: str
    html_url: str
    body: str
    author_login: str | None
    created_at: datetime.datetime
    updated_at: datetime.datetime


class GitHubIssueCommentPage(BasePydanticModel):
    """One opaque-cursor page of comments for a Main Sequence issue."""

    next_cursor: str | None
    results: list[GitHubIssueComment] = Field(default_factory=list)


class GitHubIssueOperation(BasePydanticModel):
    """Durable backend reconciliation state for an ambiguous provider mutation."""

    ENDPOINT: ClassVar[str] = "github-issue-operations"

    uid: str
    operation_type: GitHubIssueOperationType
    status: GitHubIssueOperationStatus
    github_issue_uid: str | None
    github_issue_id: int | None
    issue_number: int | None
    github_comment_uid: str | None
    github_comment_id: int | None
    retryable: bool
    error_code: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    completed_at: datetime.datetime | None

    @classmethod
    def get_by_uid(
        cls,
        uid: str | UUID,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> GitHubIssueOperation:
        """Retrieve and reconcile one operation using only its Main Sequence UID."""

        operation_uid = _public_uid(uid, field_name="operation_uid")
        _, data = _request_json(
            method="GET",
            url=_api_url(f"{cls.ENDPOINT}/{operation_uid}/"),
            payload=None,
            timeout=timeout,
            expected_statuses=(200,),
        )
        return cls.model_validate(data)


GitHubIssueCommentMutationResult = GitHubIssueComment | GitHubIssueOperation


class GitHubIssue(_GitHubIssueProjection):
    """Complete provider-backed issue projection and its UID-addressed operations."""

    ENDPOINT: ClassVar[str] = "github-issues"

    body: str
    author_login: str | None

    @classmethod
    def get_by_uid(
        cls,
        uid: str | UUID,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> GitHubIssue:
        """Retrieve one issue using only its Main Sequence UID."""

        issue_uid = _public_uid(uid, field_name="issue_uid")
        _, data = _request_json(
            method="GET",
            url=_api_url(f"{cls.ENDPOINT}/{issue_uid}/"),
            payload=None,
            timeout=timeout,
            expected_statuses=(200,),
        )
        return cls.model_validate(data)

    def update(
        self,
        *,
        idempotency_key: str,
        title: str | _UnsetType = _UNSET,
        body: str | _UnsetType = _UNSET,
        state: GitHubIssueState | _UnsetType = _UNSET,
        state_reason: GitHubIssueStateReason | None | _UnsetType = _UNSET,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> GitHubIssueMutationResult:
        """Update only explicitly supplied fields on this issue."""

        mutation: dict[str, Any] = {}
        for field_name, value in {
            "title": title,
            "body": body,
            "state": state,
            "state_reason": state_reason,
        }.items():
            if value is not _UNSET:
                mutation[field_name] = value
        if not mutation:
            raise ValueError("At least one issue field must be supplied.")
        _validate_issue_mutation(mutation)
        mutation["idempotency_key"] = _idempotency_key(idempotency_key)
        issue_uid = _public_uid(self.uid, field_name="issue_uid")
        status_code, data = _request_json(
            method="PATCH",
            url=_api_url(f"{self.ENDPOINT}/{issue_uid}/"),
            payload={"json": mutation},
            timeout=timeout,
            expected_statuses=(200, 201, 202),
        )
        return _issue_mutation_result(status_code, data)

    def close(
        self,
        *,
        idempotency_key: str,
        state_reason: Literal["completed", "not_planned"] = "completed",
        timeout: int | float | tuple[float, float] | None = None,
    ) -> GitHubIssueMutationResult:
        """Close this issue through the canonical update operation."""

        return self.update(
            state="closed",
            state_reason=state_reason,
            idempotency_key=idempotency_key,
            timeout=timeout,
        )

    def reopen(
        self,
        *,
        idempotency_key: str,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> GitHubIssueMutationResult:
        """Reopen this issue through the canonical update operation."""

        return self.update(
            state="open",
            state_reason="reopened",
            idempotency_key=idempotency_key,
            timeout=timeout,
        )

    def list_comments(
        self,
        *,
        updated_since: datetime.datetime | None = None,
        cursor: str | None = None,
        limit: int = 50,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> GitHubIssueCommentPage:
        """List comments using their opaque backend cursor."""

        params = _page_params(
            cursor=cursor,
            limit=limit,
            updated_since=updated_since,
        )
        issue_uid = _public_uid(self.uid, field_name="issue_uid")
        _, data = _request_json(
            method="GET",
            url=_api_url(f"{self.ENDPOINT}/{issue_uid}/comments/"),
            payload={"params": params},
            timeout=timeout,
            expected_statuses=(200,),
        )
        return GitHubIssueCommentPage.model_validate(data)

    def create_comment(
        self,
        *,
        body: str,
        idempotency_key: str,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> GitHubIssueCommentMutationResult:
        """Create a comment without altering its body or generating an idempotency key."""

        if not isinstance(body, str) or not 1 <= len(body) <= 65_536:
            raise ValueError("body must contain between 1 and 65536 characters.")
        issue_uid = _public_uid(self.uid, field_name="issue_uid")
        request_body = {
            "body": body,
            "idempotency_key": _idempotency_key(idempotency_key),
        }
        status_code, data = _request_json(
            method="POST",
            url=_api_url(f"{self.ENDPOINT}/{issue_uid}/comments/"),
            payload={"json": request_body},
            timeout=timeout,
            expected_statuses=(200, 201, 202),
        )
        if status_code == 202:
            return GitHubIssueOperation.model_validate(data)
        return GitHubIssueComment.model_validate(data)


GitHubIssueMutationResult = GitHubIssue | GitHubIssueOperation


def _validate_issue_mutation(mutation: dict[str, Any]) -> None:
    title = mutation.get("title", _UNSET)
    if title is not _UNSET:
        if not isinstance(title, str) or not title.strip() or len(title.strip()) > 256:
            raise ValueError("title must contain between 1 and 256 non-whitespace characters.")
    body = mutation.get("body", _UNSET)
    if body is not _UNSET and not isinstance(body, str):
        raise TypeError("body must be a string.")
    state = mutation.get("state", _UNSET)
    if state is not _UNSET and state not in {"open", "closed"}:
        raise ValueError("state must be 'open' or 'closed'.")
    reason = mutation.get("state_reason", _UNSET)
    if reason is not _UNSET and reason not in {None, "completed", "not_planned", "reopened"}:
        raise ValueError("state_reason is not supported.")
    if reason in {"completed", "not_planned"} and state != "closed":
        raise ValueError("Closing reasons require state='closed'.")
    if reason == "reopened" and state != "open":
        raise ValueError("state_reason='reopened' requires state='open'.")


def _issue_mutation_result(
    status_code: int,
    data: dict[str, Any],
) -> GitHubIssueMutationResult:
    if status_code == 202:
        return GitHubIssueOperation.model_validate(data)
    return GitHubIssue.model_validate(data)


def _list_github_issues_for_branch(
    *,
    branch_uid: str,
    state: GitHubIssueState | None,
    updated_since: datetime.datetime | None,
    cursor: str | None,
    limit: int,
    timeout: int | float | tuple[float, float] | None,
) -> GitHubIssuePage:
    params = _page_params(
        cursor=cursor,
        limit=limit,
        updated_since=updated_since,
    )
    if state is not None:
        if state not in {"open", "closed"}:
            raise ValueError("state must be 'open' or 'closed'.")
        params["state"] = state
    _, data = _request_json(
        method="GET",
        url=_api_url(
            f"code-repository-branches/{_public_uid(branch_uid, field_name='branch_uid')}/github-issues/"
        ),
        payload={"params": params},
        timeout=timeout,
        expected_statuses=(200,),
    )
    return GitHubIssuePage.model_validate(data)


def _create_github_issue_for_branch(
    *,
    branch_uid: str,
    title: str,
    body: str,
    idempotency_key: str,
    timeout: int | float | tuple[float, float] | None,
) -> GitHubIssueMutationResult:
    mutation: dict[str, Any] = {"title": title, "body": body}
    _validate_issue_mutation(mutation)
    mutation["idempotency_key"] = _idempotency_key(idempotency_key)
    status_code, data = _request_json(
        method="POST",
        url=_api_url(
            f"code-repository-branches/{_public_uid(branch_uid, field_name='branch_uid')}/github-issues/"
        ),
        payload={"json": mutation},
        timeout=timeout,
        expected_statuses=(200, 201, 202),
    )
    return _issue_mutation_result(status_code, data)


__all__ = [
    "GitHubIssue",
    "GitHubIssueComment",
    "GitHubIssueCommentMutationResult",
    "GitHubIssueCommentPage",
    "GitHubIssueListItem",
    "GitHubIssueMutationResult",
    "GitHubIssueOperation",
    "GitHubIssueOperationStatus",
    "GitHubIssueOperationType",
    "GitHubIssuePage",
    "GitHubIssueState",
    "GitHubIssueStateReason",
]
