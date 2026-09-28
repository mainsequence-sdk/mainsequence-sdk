"""Platform DataSource resources and workload-scoped connection material."""

from __future__ import annotations

import os
from typing import Annotated, Any, ClassVar, Literal
from uuid import UUID

import requests
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    StrictBool,
    StrictInt,
    StringConstraints,
    ValidationError,
)

from .base import BaseObjectOrm, BasePydanticModel
from .utils import AuthError, RuntimeCredentialAuthProvider
from .value_sets import OpenValueSet

NonemptyString = Annotated[str, StringConstraints(strict=True, strip_whitespace=True, min_length=1)]
StorageAccessMode = Literal["read_write", "read_only", "disabled"]


class DataSourceRuntimeError(RuntimeError):
    """A connection lookup failure containing only a safe machine-readable code."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


class DataSourceConnection(BaseModel):
    """Connection values; credentials require explicit SecretStr access."""

    model_config = ConfigDict(extra="ignore", frozen=True, hide_input_in_errors=True)

    host: NonemptyString
    port: StrictInt = Field(ge=1, le=65535)
    database_name: NonemptyString
    database_user: NonemptyString
    password: SecretStr | None = Field(default=None, repr=False, exclude=True)
    ssl_mode: NonemptyString
    default_schema: NonemptyString
    default_charset: str | None = None
    connection_timezone: str | None = None
    tls_ca_certificate: SecretStr | None = Field(default=None, repr=False, exclude=True)
    tls_client_certificate: SecretStr | None = Field(default=None, repr=False, exclude=True)
    tls_client_key: SecretStr | None = Field(default=None, repr=False, exclude=True)


class RuntimeDataSource(BaseModel):
    """Current platform connection response, bound to a source and workload scope.

    Capabilities are returned as boolean facts. The consuming application decides
    which capabilities and access mode its own operation requires.
    """

    model_config = ConfigDict(extra="ignore", frozen=True, hide_input_in_errors=True)

    uid: UUID = Field(alias="data_source_uid")
    organization_uid: UUID | None = None
    environment_uid: UUID = Field(alias="organization_environment_uid")
    class_type: NonemptyString
    status: NonemptyString
    storage_access_mode: StorageAccessMode
    capabilities: dict[str, StrictBool]
    connection: DataSourceConnection
    display_name: str | None = None
    environment_name: str | None = Field(default=None, alias="organization_environment_name")
    extra_arguments: dict[str, Any] | None = Field(default=None, repr=False, exclude=True)


def _scope_uid(value: UUID | str) -> UUID:
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        raise DataSourceRuntimeError("invalid_data_source_scope") from None


def _parse_runtime_source(
    payload: Any, *, uid: UUID
) -> RuntimeDataSource:
    try:
        source = RuntimeDataSource.model_validate(payload)
    except ValidationError:
        raise DataSourceRuntimeError("invalid_runtime_data_source_response") from None
    if source.uid != uid:
        raise DataSourceRuntimeError("data_source_identity_mismatch")
    return source


class DataSource(BasePydanticModel, BaseObjectOrm):
    """Thin adapter for the platform DataSource directory.

    Generic lookup/filter operations use the current SDK session. This resource
    does not open databases, execute SQL, or manage local storage.
    """

    ENDPOINT: ClassVar[str] = "data-sources"
    COLLECTION_CREATE_SUPPORTED: ClassVar[bool] = False

    uid: str | None = None
    data_source_uid: str | None = None
    organization_uid: str | None = None
    organization_environment_uid: str | None = None
    organization_environment_name: str | None = None
    display_name: str | None = None
    class_type: str | None = None
    status: str | None = None
    storage_access_mode: OpenValueSet[StorageAccessMode] | None = None

    @property
    def environment_uid(self) -> str | None:
        """Environment identity using the SDK's neutral public vocabulary."""
        return self.organization_environment_uid

    @property
    def environment_name(self) -> str | None:
        return self.organization_environment_name

    @classmethod
    def get_runtime_connection(
        cls,
        uid: UUID | str,
        *,
        timeout: float | tuple[float, float] = (5.0, 5.0),
    ) -> RuntimeDataSource:
        """Fetch current workload-granted material without caching the response.

        Requires runtime-credential authentication. The backend authorizes the
        connection request; the SDK validates response shape and source identity.
        One rejected access token triggers one forced SDK refresh; other failures
        do not retry. Redirects are not followed. No credentials enter exceptions.
        """
        source_uid = _scope_uid(uid)
        if (os.getenv("MAINSEQUENCE_AUTH_MODE") or "").strip() != "runtime_credential":
            raise DataSourceRuntimeError("runtime_credential_not_configured")
        url = f"{cls.get_object_url()}/{source_uid}/runtime-connection/"
        try:
            provider = cls.LOADERS._provider()
            if not isinstance(provider, RuntimeCredentialAuthProvider):
                raise DataSourceRuntimeError("runtime_credential_not_configured")
            # A separate session avoids shared header mutation and transport-level
            # retries. Authentication and token lifetime stay with the SDK provider.
            with requests.Session() as session:
                for attempt in range(2):
                    if attempt:
                        provider.refresh(force=True)
                    headers = provider.get_headers()
                    response = session.get(
                        url,
                        headers={**headers, "Accept": "application/json"},
                        timeout=timeout,
                        allow_redirects=False,
                    )
                    if response.status_code != 401 or attempt:
                        break
                    response.close()
                try:
                    code = {
                        401: "runtime_credential_rejected",
                        403: "data_source_runtime_access_denied",
                        404: "data_source_not_available",
                    }.get(response.status_code)
                    if response.status_code != 200:
                        raise DataSourceRuntimeError(code or "data_source_unavailable")
                    try:
                        payload = response.json()
                    except ValueError:
                        raise DataSourceRuntimeError(
                            "invalid_runtime_data_source_response"
                        ) from None
                finally:
                    response.close()
        except AuthError:
            raise DataSourceRuntimeError("runtime_credential_rejected") from None
        except requests.RequestException:
            raise DataSourceRuntimeError("data_source_unavailable") from None
        except (ValueError, TypeError, KeyError, AttributeError):
            raise DataSourceRuntimeError("invalid_runtime_data_source_response") from None
        return _parse_runtime_source(
            payload,
            uid=source_uid,
        )


__all__ = ["DataSource", "DataSourceConnection", "DataSourceRuntimeError", "RuntimeDataSource"]
