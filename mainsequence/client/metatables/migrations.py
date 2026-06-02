from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

from pydantic import ConfigDict, Field

from ..base import BasePydanticModel

ALEMBIC_MIGRATION_V1 = "metatable-migration.v1"
AlembicMigrationVersion = Literal["metatable-migration.v1"]
AlembicMigrationDirection = Literal["upgrade", "downgrade"]
AlembicMigrationApplyStatus = Literal["validated", "applied"]
AlembicMigrationStatementBoundary = dict[str, Any] | str


class AlembicMigrationError(BasePydanticModel):
    code: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="allow")


class AlembicMigrationOperation(BasePydanticModel):
    version: AlembicMigrationVersion = ALEMBIC_MIGRATION_V1
    alembic_version_meta_table_uid: str
    data_source_uid: str
    package: str = ""
    migration_namespace: str = ""
    revision: str
    down_revision: str | None = None
    direction: AlembicMigrationDirection = "upgrade"
    expected_current_revision: str | None = None
    manifest: dict[str, Any] = Field(default_factory=dict)
    sql: str
    statement_boundaries: list[AlembicMigrationStatementBoundary] = Field(default_factory=list)
    dry_run: bool = False

    model_config = ConfigDict(populate_by_name=True)


class AlembicMigrationStatusRequest(BasePydanticModel):
    alembic_version_meta_table_uid: str
    data_source_uid: str
    package: str = ""
    migration_namespace: str = ""

    model_config = ConfigDict(populate_by_name=True)


class AlembicMigrationApplyResponse(BasePydanticModel):
    ok: bool
    version: AlembicMigrationVersion = ALEMBIC_MIGRATION_V1
    status: AlembicMigrationApplyStatus | None = None
    dry_run: bool = False
    alembic_version_meta_table_uid: str
    alembic_version_table: str
    data_source_uid: str
    package: str = ""
    migration_namespace: str = ""
    revision: str
    direction: AlembicMigrationDirection
    previous_revision: str | None = None
    previous_revisions: list[str] = Field(default_factory=list)
    current_revision: str | None = None
    current_revisions: list[str] = Field(default_factory=list)
    executed_statement_count: int = Field(default=0, ge=0)
    error: AlembicMigrationError | None = None

    model_config = ConfigDict(populate_by_name=True, extra="allow")


class AlembicMigrationStatusResponse(BasePydanticModel):
    ok: bool
    version: AlembicMigrationVersion = ALEMBIC_MIGRATION_V1
    alembic_version_meta_table_uid: str
    alembic_version_table: str
    data_source_uid: str
    package: str = ""
    migration_namespace: str = ""
    current_revision: str | None = None
    current_revisions: list[str] = Field(default_factory=list)
    error: AlembicMigrationError | None = None

    model_config = ConfigDict(populate_by_name=True, extra="allow")


def _apply_migration(
    cls: type[Any],
    operation: AlembicMigrationOperation | Mapping[str, Any],
    *,
    timeout: int | float | tuple[float, float] | None = None,
) -> AlembicMigrationApplyResponse:
    payload = (
        operation
        if isinstance(operation, AlembicMigrationOperation)
        else AlembicMigrationOperation(**operation)
    )
    response_payload = cls._post_action(
        "apply-migration",
        payload,
        timeout=timeout,
        expected_statuses=(200,),
    )
    return AlembicMigrationApplyResponse(**response_payload)


def _get_migration_status(
    cls: type[Any],
    request: AlembicMigrationStatusRequest | Mapping[str, Any],
    *,
    timeout: int | float | tuple[float, float] | None = None,
) -> AlembicMigrationStatusResponse:
    payload = (
        request
        if isinstance(request, AlembicMigrationStatusRequest)
        else AlembicMigrationStatusRequest(**request)
    )
    response_payload = cls._post_action(
        "migration-status",
        payload,
        timeout=timeout,
        expected_statuses=(200,),
    )
    response_payload.setdefault(
        "alembic_version_meta_table_uid",
        payload.alembic_version_meta_table_uid,
    )
    response_payload.setdefault("data_source_uid", payload.data_source_uid)
    response_payload.setdefault("package", payload.package)
    response_payload.setdefault("migration_namespace", payload.migration_namespace)
    return AlembicMigrationStatusResponse(**response_payload)


def _bind_meta_table_migration_methods(meta_table_cls: type[Any]) -> None:
    meta_table_cls.apply_migration = classmethod(_apply_migration)
    meta_table_cls.get_migration_status = classmethod(_get_migration_status)


__all__ = [
    "ALEMBIC_MIGRATION_V1",
    "AlembicMigrationApplyResponse",
    "AlembicMigrationApplyStatus",
    "AlembicMigrationDirection",
    "AlembicMigrationError",
    "AlembicMigrationOperation",
    "AlembicMigrationStatementBoundary",
    "AlembicMigrationStatusRequest",
    "AlembicMigrationStatusResponse",
    "AlembicMigrationVersion",
]
