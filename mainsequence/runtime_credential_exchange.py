"""
The runtime credential exchange, shared by the auth provider and the logger.

A runtime started with ``MAINSEQUENCE_AUTH_MODE=runtime_credential`` exchanges its
credential at ``POST /api/v1/runtime-credentials/token/`` for a short-lived access
token. The request carries ``credential_id`` and exactly one proof:

* ``workload_identity_token`` when ``MAINSEQUENCE_RUNTIME_IDENTITY_TOKEN_FILE`` is
  set: the projected token read from that file for every exchange, because the
  file is rotated. The bootstrap secret is then never read or sent, and a missing,
  unreadable or empty file is an error, never a fallback to the secret.
* ``credential_secret`` otherwise: ``MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET``.

The token exists only inside one exchange request. It is not kept on any object,
written to the environment or a file, logged, or included in an error message.

The logger exchanges a runtime credential while it is being configured, so this
module imports nothing from the SDK.
"""

from __future__ import annotations

import os
import time
from collections.abc import Callable
from typing import Any

from urllib3.exceptions import InvalidHeader
from urllib3.util import Retry

RUNTIME_CREDENTIAL_ID_ENV = "MAINSEQUENCE_RUNTIME_CREDENTIAL_ID"
RUNTIME_CREDENTIAL_SECRET_ENV = "MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET"
RUNTIME_IDENTITY_TOKEN_FILE_ENV = "MAINSEQUENCE_RUNTIME_IDENTITY_TOKEN_FILE"

# 429: the exchange is throttled. 503: verification is temporarily unavailable.
# Neither exchanged anything, so the same request may be sent again.
RETRYABLE_EXCHANGE_STATUSES = frozenset({429, 503})
EXCHANGE_RETRIES = 3
EXCHANGE_BACKOFF_FACTOR = 0.5

_RETRY_AFTER = Retry(total=0)


class RuntimeIdentityTokenError(Exception):
    """The runtime identity token file cannot be used. The message never holds its content."""


def _environment_value(name: str) -> str:
    return (os.getenv(name) or "").strip()


def identity_token_file_from_environment() -> str | None:
    """Return the path in ``MAINSEQUENCE_RUNTIME_IDENTITY_TOKEN_FILE``, or None when unset."""
    return _environment_value(RUNTIME_IDENTITY_TOKEN_FILE_ENV) or None


def runtime_credential_configured() -> bool:
    """
    Say whether the environment holds a runtime credential to exchange.

    That is a credential ID and one proof: the identity token file, or else the
    bootstrap secret. Only the presence of the setting is checked. The token file
    is read by the exchange itself, which reports a missing or empty file.
    """
    if not _environment_value(RUNTIME_CREDENTIAL_ID_ENV):
        return False
    if identity_token_file_from_environment():
        return True
    return bool(_environment_value(RUNTIME_CREDENTIAL_SECRET_ENV))


def _read_identity_token(path: str) -> str:
    source = f"The runtime identity token file {path} ({RUNTIME_IDENTITY_TOKEN_FILE_ENV})"
    try:
        with open(path, "rb") as token_file:
            raw = token_file.read()
    except FileNotFoundError:
        raise RuntimeIdentityTokenError(f"{source} does not exist.") from None
    except OSError as exc:
        raise RuntimeIdentityTokenError(
            f"{source} could not be read ({type(exc).__name__})."
        ) from None
    try:
        token = raw.decode("utf-8").strip()
    except UnicodeDecodeError:
        # Raised below, outside this handler: the decode error holds the bytes.
        token = None
    if token is None:
        raise RuntimeIdentityTokenError(f"{source} does not hold a token.")
    if not token:
        raise RuntimeIdentityTokenError(f"{source} is empty.")
    return token


def _request_body(
    credential_id: str,
    credential_secret: str | None,
    identity_token_file: str | None,
) -> dict[str, str]:
    if identity_token_file:
        return {
            "credential_id": credential_id,
            "workload_identity_token": _read_identity_token(identity_token_file),
        }
    return {"credential_id": credential_id, "credential_secret": credential_secret or ""}


def _retry_delay(response: Any, attempt: int) -> float:
    delay = min(EXCHANGE_BACKOFF_FACTOR * 2**attempt, Retry.DEFAULT_BACKOFF_MAX)
    retry_after = (getattr(response, "headers", None) or {}).get("Retry-After")
    if retry_after is not None:
        try:
            delay = max(delay, _RETRY_AFTER.parse_retry_after(str(retry_after)))
        except InvalidHeader:
            pass
    return delay


def exchange_runtime_credential(
    send: Callable[[dict[str, str]], Any],
    *,
    credential_id: str,
    credential_secret: str | None,
    identity_token_file: str | None,
    deadline: float,
    on_retry: Callable[[int, float], None] | None = None,
) -> Any:
    """
    Send one runtime credential exchange and return the last response.

    Parameters
    ----------
    send:
        Posts the JSON body it is given to the exchange route and returns the
        response. It is called once per attempt.
    credential_id:
        The runtime credential ID.
    credential_secret:
        The bootstrap secret. Ignored when ``identity_token_file`` is set.
    identity_token_file:
        The projected token file. It is read again for every attempt.
    deadline:
        ``time.monotonic()`` value after which no retry starts.
    on_retry:
        Called with the status and the delay before each retry.

    A ``429`` or ``503`` answer is retried up to ``EXCHANGE_RETRIES`` times. The
    wait is the exponential backoff or the answer's ``Retry-After``, whichever is
    longer; a wait that would not end before ``deadline`` ends the retries. Any
    other answer, ``401`` included, is returned at once and the proof is never
    switched. ``RuntimeIdentityTokenError`` is raised when the token file cannot
    be used.
    """
    for attempt in range(EXCHANGE_RETRIES + 1):
        response = send(_request_body(credential_id, credential_secret, identity_token_file))
        status = response.status_code
        if status not in RETRYABLE_EXCHANGE_STATUSES or attempt == EXCHANGE_RETRIES:
            return response
        delay = _retry_delay(response, attempt)
        if delay >= deadline - time.monotonic():
            return response
        if on_retry is not None:
            on_retry(status, delay)
        close = getattr(response, "close", None)
        if callable(close):
            close()
        time.sleep(delay)
    raise AssertionError("Unreachable retry state")
