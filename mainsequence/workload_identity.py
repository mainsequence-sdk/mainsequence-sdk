"""
Subject tokens for workload identity federation.

A program that runs outside Main Sequence (a service in your own cloud, a CI job,
a scheduler) can sign in with the identity its cloud already gives it instead of
an API token. With ``MAINSEQUENCE_AUTH_MODE=workload_identity`` the SDK obtains a
token from that identity provider, the *subject token*, and exchanges it at
``POST /api/v1/workload-identity/token/`` for a short-lived Main Sequence access
token, as an RFC 8693 token exchange.

A subject token source is any callable that takes no argument and returns the
subject token as a string. It is called once per exchange, so a source may
return a new token every time. This module provides three, selected by
``MAINSEQUENCE_WORKLOAD_IDENTITY_SOURCE``:

``environment``
    The token in ``MAINSEQUENCE_WORKLOAD_IDENTITY_TOKEN``.
``file``
    The token in the file ``MAINSEQUENCE_WORKLOAD_IDENTITY_TOKEN_FILE`` names,
    read for every exchange because such files are rotated.
``azure_managed_identity``
    A token the Azure managed identity of the host issues for the audience in
    ``MAINSEQUENCE_WORKLOAD_IDENTITY_AUDIENCE``; a user-assigned identity is
    chosen by ``MAINSEQUENCE_WORKLOAD_IDENTITY_AZURE_CLIENT_ID``.

A subject token is never kept on an object, written to the environment or a
file, logged, or included in an error message.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass

import requests

from mainsequence._transport import DEFAULT_TIMEOUT, remaining_timeout

WORKLOAD_IDENTITY_AUTH_MODE = "workload_identity"
WORKLOAD_IDENTITY_SOURCE_ENV = "MAINSEQUENCE_WORKLOAD_IDENTITY_SOURCE"
WORKLOAD_IDENTITY_TOKEN_ENV = "MAINSEQUENCE_WORKLOAD_IDENTITY_TOKEN"
WORKLOAD_IDENTITY_TOKEN_FILE_ENV = "MAINSEQUENCE_WORKLOAD_IDENTITY_TOKEN_FILE"
WORKLOAD_IDENTITY_AUDIENCE_ENV = "MAINSEQUENCE_WORKLOAD_IDENTITY_AUDIENCE"
WORKLOAD_IDENTITY_AZURE_CLIENT_ID_ENV = "MAINSEQUENCE_WORKLOAD_IDENTITY_AZURE_CLIENT_ID"

SOURCE_ENVIRONMENT = "environment"
SOURCE_FILE = "file"
SOURCE_AZURE_MANAGED_IDENTITY = "azure_managed_identity"
SOURCES = (SOURCE_ENVIRONMENT, SOURCE_FILE, SOURCE_AZURE_MANAGED_IDENTITY)

# RFC 8693 token exchange.
TOKEN_EXCHANGE_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:token-exchange"
JWT_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:jwt"

# The managed identity endpoint of Azure Container Apps, App Service and Functions,
# announced by these two variables, and the instance metadata service of Azure
# virtual machines and scale sets, used when they are absent.
AZURE_IDENTITY_ENDPOINT_ENV = "IDENTITY_ENDPOINT"
AZURE_IDENTITY_HEADER_ENV = "IDENTITY_HEADER"
AZURE_IDENTITY_ENDPOINT_API_VERSION = "2019-08-01"
AZURE_IMDS_TOKEN_URL = "http://169.254.169.254/metadata/identity/oauth2/token"
AZURE_IMDS_API_VERSION = "2018-02-01"

SubjectTokenSource = Callable[[], str]


class SubjectTokenError(Exception):
    """No subject token could be obtained. The message never holds a token."""


def _environment_value(name: str) -> str:
    return (os.getenv(name) or "").strip()


def _non_empty(token: str, source: str) -> str:
    token = token.strip()
    if not token:
        raise SubjectTokenError(f"{source} is empty.")
    return token


@dataclass(frozen=True)
class EnvironmentSubjectToken:
    """The subject token in an environment variable, read for every exchange."""

    variable: str = WORKLOAD_IDENTITY_TOKEN_ENV

    def __call__(self) -> str:
        value = os.getenv(self.variable)
        if value is None:
            raise SubjectTokenError(f"{self.variable} is not set.")
        return _non_empty(value, self.variable)


@dataclass(frozen=True)
class FileSubjectToken:
    """The subject token in a file, read for every exchange."""

    path: str

    def __call__(self) -> str:
        source = f"The workload identity token file {self.path}"
        try:
            with open(self.path, "rb") as token_file:
                raw = token_file.read()
        except FileNotFoundError:
            raise SubjectTokenError(f"{source} does not exist.") from None
        except OSError as exc:
            raise SubjectTokenError(f"{source} could not be read ({type(exc).__name__}).") from None
        try:
            token = raw.decode("utf-8")
        except UnicodeDecodeError:
            # Raised below, outside this handler: the decode error holds the bytes.
            token = None
        if token is None:
            raise SubjectTokenError(f"{source} does not hold a token.")
        return _non_empty(token, source)


@dataclass(frozen=True)
class AzureManagedIdentitySubjectToken:
    """
    A token the host's Azure managed identity issues for ``audience``.

    On Azure Container Apps, App Service and Functions the platform sets
    ``IDENTITY_ENDPOINT`` and ``IDENTITY_HEADER`` and the token is requested
    there; elsewhere, such as on a virtual machine, from the instance metadata
    service. ``client_id`` selects a user-assigned managed identity; without it
    the system-assigned identity answers. ``audience`` is the Application ID URI
    the Main Sequence credential names, for example ``api://main-sequence``.

    One request is made per exchange. Azure caches managed identity tokens, so
    it is cheap. The request has no retry of its own: a failure fails the
    exchange, and the next SDK request tries again.
    """

    audience: str
    client_id: str | None = None
    timeout: tuple[float, float] = DEFAULT_TIMEOUT

    def _request(self) -> tuple[str, dict[str, str], dict[str, str]]:
        endpoint = _environment_value(AZURE_IDENTITY_ENDPOINT_ENV)
        identity_header = _environment_value(AZURE_IDENTITY_HEADER_ENV)
        if endpoint and identity_header:
            params = {"resource": self.audience, "api-version": AZURE_IDENTITY_ENDPOINT_API_VERSION}
            headers = {"X-IDENTITY-HEADER": identity_header}
        else:
            endpoint = AZURE_IMDS_TOKEN_URL
            params = {"resource": self.audience, "api-version": AZURE_IMDS_API_VERSION}
            headers = {"Metadata": "true"}
        if self.client_id:
            params["client_id"] = self.client_id
        return endpoint, params, headers

    def __call__(self) -> str:
        if not (self.audience or "").strip():
            raise SubjectTokenError(
                f"{WORKLOAD_IDENTITY_AUDIENCE_ENV} is required for the "
                f"{SOURCE_AZURE_MANAGED_IDENTITY} source: it is the audience the "
                "Main Sequence credential names."
            )
        endpoint, params, headers = self._request()
        try:
            response = requests.get(
                endpoint,
                params=params,
                headers=headers,
                timeout=remaining_timeout(self.timeout),
                allow_redirects=False,
            )
        except requests.RequestException as exc:
            # The exception text is not echoed: it can carry the request headers.
            raise SubjectTokenError(
                f"The Azure managed identity endpoint could not be reached ({type(exc).__name__})."
            ) from None
        if response.status_code != 200:
            raise SubjectTokenError(
                f"The Azure managed identity endpoint answered HTTP {response.status_code} "
                f"for the audience {self.audience}."
            )
        try:
            token = str(response.json().get("access_token") or "")
        except (ValueError, AttributeError):
            token = ""
        return _non_empty(token, "The Azure managed identity endpoint's token")


def subject_token_source_from_environment() -> SubjectTokenSource:
    """
    Return the subject token source ``MAINSEQUENCE_WORKLOAD_IDENTITY_SOURCE`` selects.

    Only the configuration is checked here; the token itself is obtained by
    calling the source, once per exchange.
    """
    source = _environment_value(WORKLOAD_IDENTITY_SOURCE_ENV).lower()
    if source == SOURCE_ENVIRONMENT:
        return EnvironmentSubjectToken()
    if source == SOURCE_FILE:
        path = _environment_value(WORKLOAD_IDENTITY_TOKEN_FILE_ENV)
        if not path:
            raise SubjectTokenError(
                f"{WORKLOAD_IDENTITY_TOKEN_FILE_ENV} is required when "
                f"{WORKLOAD_IDENTITY_SOURCE_ENV}={SOURCE_FILE}."
            )
        return FileSubjectToken(path)
    if source == SOURCE_AZURE_MANAGED_IDENTITY:
        audience = _environment_value(WORKLOAD_IDENTITY_AUDIENCE_ENV)
        if not audience:
            raise SubjectTokenError(
                f"{WORKLOAD_IDENTITY_AUDIENCE_ENV} is required when "
                f"{WORKLOAD_IDENTITY_SOURCE_ENV}={SOURCE_AZURE_MANAGED_IDENTITY}."
            )
        client_id = _environment_value(WORKLOAD_IDENTITY_AZURE_CLIENT_ID_ENV) or None
        return AzureManagedIdentitySubjectToken(audience=audience, client_id=client_id)
    expected = ", ".join(SOURCES)
    if not source:
        raise SubjectTokenError(
            f"{WORKLOAD_IDENTITY_SOURCE_ENV} is required when "
            f"MAINSEQUENCE_AUTH_MODE={WORKLOAD_IDENTITY_AUTH_MODE}: one of {expected}."
        )
    raise SubjectTokenError(
        f"{WORKLOAD_IDENTITY_SOURCE_ENV}={source} is not a subject token source: "
        f"use one of {expected}."
    )


def token_exchange_body(subject_token: str) -> dict[str, str]:
    """Return the RFC 8693 form body that exchanges ``subject_token``."""
    return {
        "grant_type": TOKEN_EXCHANGE_GRANT_TYPE,
        "subject_token": subject_token,
        "subject_token_type": JWT_TOKEN_TYPE,
    }
