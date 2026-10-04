"""Recorded model calls through Main Sequence's authenticated API."""

from __future__ import annotations

from copy import deepcopy
from typing import Any
from uuid import UUID

import requests
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from . import utils
from .exceptions import raise_for_response


class InferenceResult(BaseModel):
    model_config = ConfigDict(extra="allow")
    conversation_uid: str
    request_uid: str
    sequence: int
    provider: str
    model: str
    status: str
    error_code: str
    content_available: bool
    usage: dict[str, Any] = Field(default_factory=dict)
    input: dict[str, Any] | None = None
    output: dict[str, Any] | None = None
    effective_controls: dict[str, Any] = Field(default_factory=dict)
    provider_error: dict[str, Any] | None = None


class InferenceExecutionError(RuntimeError):
    """A recorded failure; inspect ``result.request_uid`` without re-executing."""

    def __init__(self, result: InferenceResult, status_code: int):
        self.result = result
        self.status_code = status_code
        super().__init__(result.error_code or result.status)


class InferenceTransportError(RuntimeError):
    """Outcome is uncertain. Replay the same body and idempotency key only."""

    def __init__(self, idempotency_key: str | None):
        self.idempotency_key = idempotency_key
        super().__init__(
            "The request outcome is unknown. Inspect history or replay the identical request with the same idempotency key."
        )


class InferenceClient:
    """Full caller-supplied context, with durable history and no Agent dependency.

    Runtime credentials derive their Environment on the server. Human callers
    provide ``organization_environment_uid`` explicitly. Provider secrets never
    pass through this client. Optional provider options retain native JSON values.
    """

    def __init__(self, *, organization_environment_uid: str | UUID | None = None):
        self.organization_environment_uid = (
            str(UUID(str(organization_environment_uid)))
            if organization_environment_uid is not None
            else None
        )

    def _request(self, method, path, *, body=None, key=None, params=None, timeout=100):
        query = dict(params or {})
        if self.organization_environment_uid is not None:
            query["organization_environment_uid"] = self.organization_environment_uid
        url = f"{utils.API_ENDPOINT.rstrip('/')}/{path}"
        # Inference opts out of safe-read retries as well: an uncertain outcome
        # must preserve the caller's explicit idempotency key.
        with utils.build_session(loaders=utils.loaders, retries=0) as session:
            headers = dict(utils.loaders.refresh_headers(force=False, session=session))
            if key is not None:
                headers["Idempotency-Key"] = key
            try:
                response = session.request(
                    method,
                    url,
                    headers=headers,
                    params=query,
                    json=body,
                    timeout=timeout,
                    allow_redirects=False,
                )
                if response.status_code == 401:
                    headers.update(utils.loaders.refresh_headers(force=True, session=session))
                    response = session.request(
                        method,
                        url,
                        headers=headers,
                        params=query,
                        json=body,
                        timeout=timeout,
                        allow_redirects=False,
                    )
            except requests.RequestException as exc:
                raise InferenceTransportError(key) from exc
        if method == "POST":
            try:
                payload = response.json()
            except ValueError as exc:
                raise_for_response(response)
                raise InferenceTransportError(key) from exc
            if isinstance(payload, dict) and "request_uid" in payload:
                try:
                    result = InferenceResult.model_validate(payload)
                except ValidationError as exc:
                    raise InferenceTransportError(key) from exc
                if response.status_code >= 400:
                    raise InferenceExecutionError(result, response.status_code)
                return result
            raise_for_response(response)
            raise InferenceTransportError(key)
        raise_for_response(response)
        return response.json()

    def complete(
        self,
        *,
        provider: str,
        model: str,
        messages: list[dict[str, str]],
        idempotency_key: str,
        conversation_uid: str | UUID | None = None,
        thinking_level: str | None = None,
        provider_options: dict[str, Any] | None = None,
        provider_storage: dict[str, Any] | None = None,
        response_schema: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        timeout: float = 100,
    ) -> InferenceResult:
        """Execute once or replay an existing identity; never generate a retry key."""
        import re

        if not isinstance(idempotency_key, str) or not re.fullmatch(
            r"[A-Za-z0-9._:-]{1,128}", idempotency_key
        ):
            raise ValueError(
                "idempotency_key must contain 1–128 letters, digits, dots, underscores, colons or hyphens."
            )
        body = {"provider": provider, "model": model, "messages": deepcopy(messages)}
        if conversation_uid is not None:
            body["conversation_uid"] = str(UUID(str(conversation_uid)))
        for name, value in (
            ("thinking_level", thinking_level),
            ("provider_options", provider_options),
            ("provider_storage", provider_storage),
            ("response_schema", response_schema),
            ("metadata", metadata),
        ):
            if value is not None:
                body[name] = deepcopy(value)
        return self._request(
            "POST", "inference/completions/", body=body, key=idempotency_key, timeout=timeout
        )

    def conversations(self, *, limit: int = 25, offset: int = 0) -> dict[str, Any]:
        return self._request(
            "GET", "inference/conversations/", params={"limit": limit, "offset": offset}
        )

    def conversation(self, uid: str | UUID) -> dict[str, Any]:
        return self._request("GET", f"inference/conversations/{UUID(str(uid))}/")

    def history(self, uid: str | UUID, *, limit: int = 25, offset: int = 0) -> dict[str, Any]:
        return self._request(
            "GET",
            f"inference/conversations/{UUID(str(uid))}/history/",
            params={"limit": limit, "offset": offset},
        )

    def request(self, uid: str | UUID) -> InferenceResult:
        return InferenceResult.model_validate(
            self._request("GET", f"inference/requests/{UUID(str(uid))}/")
        )

    def insights(self, uid: str | UUID) -> dict[str, Any]:
        return self._request("GET", f"inference/conversations/{UUID(str(uid))}/insights/")

    def delete_content(self, uid: str | UUID) -> dict[str, Any]:
        return self._request("DELETE", f"inference/conversations/{UUID(str(uid))}/content/")
