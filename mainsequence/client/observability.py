from __future__ import annotations

import datetime
from typing import Any, ClassVar, Literal
from urllib.parse import parse_qsl, unquote, urlparse, urlunparse
from uuid import UUID

from pydantic import ConfigDict, Field

from .base import BasePydanticModel
from .exceptions import ApiError, raise_for_response
from .utils import make_request
from .value_sets import OpenValueSet

PublicLogLevel = Literal[
    "debug",
    "info",
    "notice",
    "warning",
    "error",
    "critical",
    "alert",
    "emergency",
]
LogSearchOutcome = Literal[
    "succeeded",
    "failed",
    "cancelled",
    "canceled",
    "denied",
    "timeout",
    "unknown",
]
LogTime = datetime.datetime | str


class ObservabilityLinks(BasePydanticModel):
    """Backend-owned capability links for one product-level runtime owner."""

    application_logs_url: str | None = None
    resource_usage_url: str | None = None
    deployment_runs_url: str | None = None
    sessions_url: str | None = None


class OwnerLogRow(BasePydanticModel):
    """One sanitized runtime log row.

    The backend's normalized fields are declared while additive provider-neutral
    enrichment remains available to callers.
    """

    model_config = ConfigDict(extra="allow")

    occurred_at: datetime.datetime | None = None
    time: int | float | None = None
    timestamp: str | None = None
    severity: str | None = None
    level: OpenValueSet[PublicLogLevel] | None = None
    source: str | None = None
    event: str | None = None
    event_id: str | None = None
    message: str | None = None
    component: str | None = None
    request_id: str | None = None
    trace_id: str | None = None
    span_id: str | None = None
    parent_span_id: str | None = None
    operation_uid: str | None = None
    user_uid: str | None = None
    agent_session_uid: str | None = None
    agent_run_uid: str | None = None
    turn_uid: str | None = None
    model_call_uid: str | None = None
    tool_call_uid: str | None = None
    handoff_uid: str | None = None
    a2a_method: str | None = None
    a2a_request_id: str | None = None
    a2a_context_id: str | None = None
    a2a_task_id: str | None = None
    a2a_message_id: str | None = None
    http_method: str | None = None
    route: str | None = None
    status_code: int | None = None
    status_class: str | None = None
    duration_ms: int | float | None = None
    first_byte_ms: int | float | None = None
    request_size_bytes: int | None = None
    response_size_bytes: int | None = None
    is_streaming: bool | None = None
    client_disconnected: bool | None = None
    outcome: str | None = None
    error_code: str | None = None
    error_type: str | None = None
    retryable: bool | None = None
    agent_attempt: int | None = None
    data: Any = None


class OwnerLogPage(BasePydanticModel):
    organization_environment_uid: str
    start: int
    end: int
    start_time: datetime.datetime | None = None
    end_time: datetime.datetime | None = None
    next_cursor: str | None = None
    truncated: bool
    rows: list[OwnerLogRow] = Field(default_factory=list)


class EnvironmentLogSearchRow(OwnerLogRow):
    """One sanitized row returned by an Environment-scoped collection search."""

    owner_type: OpenValueSet[
        Literal[
            "deployment_run",
            "job_run",
            "resource_release",
            "agent",
            "agent_session",
        ]
    ]
    owner_uid: str
    occurred_at: datetime.datetime
    level: OpenValueSet[PublicLogLevel]


class EnvironmentLogSearchPage(BasePydanticModel):
    """One bounded page from an Environment-scoped collection log search."""

    organization_environment_uid: str
    start_time: datetime.datetime
    end_time: datetime.datetime
    limit: int = Field(ge=1, le=500)
    returned_count: int = Field(ge=0)
    cumulative_returned_count: int = Field(ge=0)
    result_limit: int = Field(ge=0)
    next_cursor: str | None = None
    truncated: bool
    truncation_reason: (
        OpenValueSet[Literal["page_limit", "result_limit", "candidate_limit"]] | None
    ) = None
    rows: list[EnvironmentLogSearchRow] = Field(default_factory=list)


class ResourceUsageSummary(BasePydanticModel):
    cpu_cores_current: float | None = None
    cpu_cores_peak: float | None = None
    memory_gib_current: float | None = None
    memory_gib_peak: float | None = None
    disk_gib_current: float | None = None
    disk_gib_peak: float | None = None


class ResourceUsagePoint(BasePydanticModel):
    time: int
    cpu_cores: float
    memory_gib: float
    disk_gib: float | None = None


class ResourceUsagePage(BasePydanticModel):
    start: int
    end: int
    step_seconds: int
    summary: ResourceUsageSummary
    rows: list[ResourceUsagePoint] = Field(default_factory=list)


def _serialize_log_time(name: str, value: LogTime) -> str:
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{name} must include an explicit timezone.")
        return value.isoformat()
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise ValueError(f"{name} must be a non-empty RFC 3339 timestamp.")


def _owner_log_query_params(
    *,
    start_time: LogTime | None,
    end_time: LogTime | None,
    level: PublicLogLevel | None,
    start: int | float | None,
    end: int | float | None,
    severity: str | None,
    **filters: Any,
) -> dict[str, Any]:
    if start_time is not None and start is not None:
        raise ValueError("Use either start_time or its deprecated start alias, not both.")
    if end_time is not None and end is not None:
        raise ValueError("Use either end_time or its deprecated end alias, not both.")
    if level is not None and severity is not None:
        raise ValueError("Use either level or its deprecated severity alias, not both.")
    if (start_time is None) != (end_time is None):
        raise ValueError("start_time and end_time must be supplied together.")

    params: dict[str, Any] = {
        "start_time": (
            _serialize_log_time("start_time", start_time) if start_time is not None else None
        ),
        "end_time": _serialize_log_time("end_time", end_time) if end_time is not None else None,
        "level": level,
        "start": start,
        "end": end,
        "severity": severity,
        **filters,
    }
    return {key: value for key, value in params.items() if value is not None}


class EnvironmentLogSearchMixin:
    """Authenticated transport for an ADR-060 collection log-search endpoint."""

    @classmethod
    def _search_environment_logs(
        cls,
        *,
        organization_environment_uid: str | UUID,
        start_time: LogTime,
        end_time: LogTime,
        cursor: str | None = None,
        limit: int | None = None,
        level: PublicLogLevel | None = None,
        event: str | None = None,
        request_id: str | None = None,
        outcome: LogSearchOutcome | None = None,
        timeout: int | float | tuple[float, float] | None = None,
        **family_filters: Any,
    ) -> EnvironmentLogSearchPage:
        environment_uid = str(organization_environment_uid).strip()
        if not environment_uid:
            raise ValueError("organization_environment_uid must be non-empty.")

        params: dict[str, Any] = {
            "organization_environment_uid": environment_uid,
            "start_time": _serialize_log_time("start_time", start_time),
            "end_time": _serialize_log_time("end_time", end_time),
            "cursor": cursor,
            "limit": limit,
            "level": level,
            "event": event,
            "request_id": request_id,
            "outcome": outcome,
            **family_filters,
        }
        params = {
            key: str(value) if isinstance(value, UUID) else value
            for key, value in params.items()
            if value is not None
        }
        payload = {"params": params}
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="GET",
            url=f"{cls.get_object_url()}/logs/",
            payload=payload,
            time_out=timeout,
        )
        if response.status_code != 200:
            raise_for_response(response, payload=payload)
        data = response.json()
        if not isinstance(data, dict):
            raise TypeError("Environment log-search responses must be JSON objects.")
        return EnvironmentLogSearchPage.model_validate(data)


class _OwnerObservabilityTransportMixin:
    _OBSERVABILITY_ENVIRONMENT_QUERY: ClassVar[str] = "organization_environment_uid"
    _OBSERVABILITY_ENVIRONMENT_OPTIONAL_FIELDS: ClassVar[frozenset[str]] = frozenset()

    def _observability_capability_url(self, field_name: str) -> str:
        links = getattr(self, "observability", None)
        if links is None:
            raise ApiError(f"{type(self).__name__} does not advertise observability capabilities.")
        raw_url = getattr(links, field_name, None)
        if not isinstance(raw_url, str) or not raw_url.strip():
            raise ApiError(f"{type(self).__name__} does not provide the {field_name} capability.")
        return raw_url.strip()

    def _validated_observability_request(
        self,
        field_name: str,
        *,
        params: dict[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        raw_url = self._observability_capability_url(field_name)
        backend = urlparse(type(self).ROOT_URL)
        candidate = urlparse(raw_url)

        if candidate.username or candidate.password or candidate.fragment:
            raise ApiError("The backend returned an invalid observability capability URL.")
        if candidate.scheme or candidate.netloc:
            if candidate.scheme != backend.scheme or candidate.netloc != backend.netloc:
                raise ApiError("The backend returned a cross-origin observability capability URL.")
            request_url = urlunparse(candidate._replace(query="", fragment=""))
        else:
            if not raw_url.startswith("/") or raw_url.startswith("//"):
                raise ApiError("The backend returned an invalid observability capability path.")
            request_url = urlunparse(
                (
                    backend.scheme,
                    backend.netloc,
                    candidate.path,
                    candidate.params,
                    "",
                    "",
                )
            )

        decoded_path_parts = unquote(candidate.path).split("/")
        if ".." in decoded_path_parts or not candidate.path.startswith("/api/v1/"):
            raise ApiError("The backend returned an invalid observability capability path.")

        query_items = parse_qsl(candidate.query, keep_blank_values=True)
        query: dict[str, Any] = {}
        for key, value in query_items:
            if key in query:
                raise ApiError("The backend returned a duplicate observability query parameter.")
            query[key] = value
        environment_uid = str(query.get(self._OBSERVABILITY_ENVIRONMENT_QUERY) or "").strip()
        environment_optional_fields = type(self)._OBSERVABILITY_ENVIRONMENT_OPTIONAL_FIELDS
        if not environment_uid and field_name not in environment_optional_fields:
            raise ApiError(
                "The backend observability capability is missing organization_environment_uid."
            )
        if self._OBSERVABILITY_ENVIRONMENT_QUERY in params:
            raise ValueError("Observability callers cannot override organization_environment_uid.")
        query.update({key: value for key, value in params.items() if value is not None})
        return request_url, query

    def _request_observability(
        self,
        field_name: str,
        *,
        params: dict[str, Any],
        timeout: int | float | tuple[float, float] | None,
    ) -> dict[str, Any]:
        request_url, request_params = self._validated_observability_request(
            field_name,
            params=params,
        )
        payload = {"params": request_params}
        response = make_request(
            s=type(self).build_session(),
            loaders=type(self).LOADERS,
            r_type="GET",
            url=request_url,
            payload=payload,
            time_out=timeout,
        )
        if response.status_code != 200:
            raise_for_response(response, payload=payload)
        data = response.json()
        if not isinstance(data, dict):
            raise TypeError("Owner observability responses must be JSON objects.")
        return data

    def _get_owner_logs(
        self,
        *,
        start_time: LogTime | None = None,
        end_time: LogTime | None = None,
        start: int | float | None = None,
        end: int | float | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        level: PublicLogLevel | None = None,
        severity: str | None = None,
        request_id: str | None = None,
        event: str | None = None,
        outcome: LogSearchOutcome | None = None,
        agent_session_uid: str | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> OwnerLogPage:
        params = _owner_log_query_params(
            start_time=start_time,
            end_time=end_time,
            level=level,
            start=start,
            end=end,
            severity=severity,
            cursor=cursor,
            limit=limit,
            request_id=request_id,
            event=event,
            outcome=outcome,
            agent_session_uid=agent_session_uid,
        )
        return OwnerLogPage.model_validate(
            self._request_observability(
                "application_logs_url",
                params=params,
                timeout=timeout,
            )
        )


class OwnerLogMixin(_OwnerObservabilityTransportMixin):
    def get_logs(
        self,
        *,
        start_time: LogTime | None = None,
        end_time: LogTime | None = None,
        start: int | float | None = None,
        end: int | float | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        level: PublicLogLevel | None = None,
        severity: str | None = None,
        request_id: str | None = None,
        event: str | None = None,
        outcome: LogSearchOutcome | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> OwnerLogPage:
        return self._get_owner_logs(
            start_time=start_time,
            end_time=end_time,
            start=start,
            end=end,
            cursor=cursor,
            limit=limit,
            level=level,
            severity=severity,
            request_id=request_id,
            event=event,
            outcome=outcome,
            timeout=timeout,
        )


class OwnerResourceUsageMixin(_OwnerObservabilityTransportMixin):
    def get_resource_usage(
        self,
        *,
        start: int | float | None = None,
        end: int | float | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> ResourceUsagePage:
        return ResourceUsagePage.model_validate(
            self._request_observability(
                "resource_usage_url",
                params={"start": start, "end": end},
                timeout=timeout,
            )
        )


__all__ = [
    "EnvironmentLogSearchMixin",
    "EnvironmentLogSearchPage",
    "EnvironmentLogSearchRow",
    "LogSearchOutcome",
    "LogTime",
    "ObservabilityLinks",
    "OwnerLogMixin",
    "OwnerLogPage",
    "OwnerLogRow",
    "OwnerResourceUsageMixin",
    "PublicLogLevel",
    "ResourceUsagePage",
    "ResourceUsagePoint",
    "ResourceUsageSummary",
]
