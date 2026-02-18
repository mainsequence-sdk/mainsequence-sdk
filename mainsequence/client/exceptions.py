# exceptions.py
from __future__ import annotations

import json
from typing import Any

try:
    from requests import Response
except Exception:
    Response = Any  # type: ignore

from .utils import DoesNotExist


def _safe_json(r: Response) -> Any | None:
    try:
        return r.json()
    except Exception:
        return None


def _extract_message(data: Any | None, fallback_text: str) -> str:
    if isinstance(data, dict):
        for key in ("detail", "error", "message", "non_field_errors"):
            v = data.get(key)
            if v:
                if isinstance(v, str):
                    return v
                return json.dumps(v, ensure_ascii=False)
        if "errors" in data and data["errors"]:
            return json.dumps(data["errors"], ensure_ascii=False)
    if fallback_text:
        return fallback_text
    return "Request failed"


class ApiError(Exception):
    def __init__(self, message: str, *, response: Response | None = None, payload: Any = None):
        super().__init__(message)
        self.response = response
        self.payload = payload
        self.status_code = getattr(response, "status_code", None)
        self.url = getattr(response, "url", None)
        self.method = getattr(getattr(response, "request", None), "method", None)
        self.headers = getattr(response, "headers", None)

    def __str__(self) -> str:
        prefix = ""
        if self.status_code is not None:
            prefix += f"{self.status_code} "
        if self.method:
            prefix += f"{self.method} "
        if self.url:
            prefix += f"{self.url}: "
        return prefix + super().__str__()


class AuthenticationError(ApiError): ...
class PermissionDeniedError(ApiError): ...
class ConflictError(ApiError): ...
class RateLimitError(ApiError):
    def __init__(self, message: str, *, response: Response | None = None, payload: Any = None, retry_after: str | None = None):
        super().__init__(message, response=response, payload=payload)
        self.retry_after = retry_after

class ServerError(ApiError): ...
class BadRequestError(ApiError): ...
class TransportError(ApiError): ...


class NotFoundError(DoesNotExist, ApiError):
    """404 that is still catchable as DoesNotExist."""


def raise_for_response(r: Response, *, payload: Any = None) -> None:
    if r is None:
        raise ApiError("No response object", response=None, payload=payload)

    status = getattr(r, "status_code", None)
    if status is None:
        raise ApiError("Response missing status_code", response=r, payload=payload)

    if 200 <= status < 300:
        return

    # your make_request uses a synthetic Response with .code == "expired"
    if getattr(r, "code", None) == "expired":
        raise TransportError("Request failed after retries (expired)", response=r, payload=payload)

    data = _safe_json(r)
    msg = _extract_message(data, getattr(r, "text", "") or "")

    if status == 400:
        raise BadRequestError(msg, response=r, payload=data or payload)
    if status == 401:
        raise AuthenticationError(msg, response=r, payload=data or payload)
    if status == 403:
        raise PermissionDeniedError(msg, response=r, payload=data or payload)
    if status == 404:
        raise NotFoundError(msg, response=r, payload=data or payload)
    if status == 409:
        raise ConflictError(msg, response=r, payload=data or payload)
    if status == 429:
        retry_after = None
        try:
            retry_after = r.headers.get("Retry-After")
        except Exception:
            pass
        raise RateLimitError(msg, response=r, payload=data or payload, retry_after=retry_after)
    if 500 <= status <= 599:
        raise ServerError(msg, response=r, payload=data or payload)

    raise ApiError(msg, response=r, payload=data or payload)
