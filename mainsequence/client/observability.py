from __future__ import annotations

from typing import Any, ClassVar
from urllib.parse import parse_qsl, unquote, urlparse, urlunparse

from pydantic import ConfigDict, Field

from .base import BasePydanticModel
from .exceptions import ApiError, raise_for_response
from .utils import make_request


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

    time: int | float | None = None
    timestamp: str | None = None
    severity: str | None = None
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
    next_cursor: str | None = None
    truncated: bool
    rows: list[OwnerLogRow] = Field(default_factory=list)


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
        start: int | float | None = None,
        end: int | float | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        severity: str | None = None,
        request_id: str | None = None,
        event: str | None = None,
        outcome: str | None = None,
        agent_session_uid: str | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> OwnerLogPage:
        params = {
            "start": start,
            "end": end,
            "cursor": cursor,
            "limit": limit,
            "severity": severity,
            "request_id": request_id,
            "event": event,
            "outcome": outcome,
            "agent_session_uid": agent_session_uid,
        }
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
        start: int | float | None = None,
        end: int | float | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        severity: str | None = None,
        request_id: str | None = None,
        event: str | None = None,
        outcome: str | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> OwnerLogPage:
        return self._get_owner_logs(
            start=start,
            end=end,
            cursor=cursor,
            limit=limit,
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
    "ObservabilityLinks",
    "OwnerLogMixin",
    "OwnerLogPage",
    "OwnerLogRow",
    "OwnerResourceUsageMixin",
    "ResourceUsagePage",
    "ResourceUsagePoint",
    "ResourceUsageSummary",
]
