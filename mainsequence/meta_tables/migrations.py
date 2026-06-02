from __future__ import annotations

import io
from collections.abc import Sequence
from typing import Any, ClassVar

from pydantic import Field

from mainsequence.client.base import BasePydanticModel
from mainsequence.client.metatables import (
    AlembicMigrationDirection,
    AlembicMigrationStatementBoundary,
    MetaTable,
    MetaTableContract,
    MetaTablePhysicalContract,
    MetaTableRegistrationRequest,
)
from mainsequence.meta_tables.hashing import build_meta_table_storage_hash

DEFAULT_ALEMBIC_VERSION_IDENTIFIER = "alembic_version"
DEFAULT_ALEMBIC_VERSION_NAMESPACE = "mainsequence.migrations"
DEFAULT_ALEMBIC_VERSION_TABLE_NAME = "alembic_version"
DEFAULT_ALEMBIC_VERSION_SCHEMA = "public"
DEFAULT_ALEMBIC_VERSION_COLUMN_NAME = "version_num"


class PackagedAlembicMigrationArtifact(BasePydanticModel):
    """Alembic-rendered SQL artifact ready to embed in an apply request."""

    manifest: dict[str, Any]
    sql: str
    statement_boundaries: list[AlembicMigrationStatementBoundary] = Field(default_factory=list)


class AlembicVersionMetaTable:
    """MetaTable catalog binding for Alembic's version table.

    Alembic owns this physical table and its schema. The SDK registers a stable
    catalog pointer with the minimal known Alembic revision column. The backend
    uses the binding to locate the version table, not to validate Alembic DDL.
    """

    __alembic_version_table_name__: ClassVar[str] = DEFAULT_ALEMBIC_VERSION_TABLE_NAME
    __alembic_version_schema__: ClassVar[str] = DEFAULT_ALEMBIC_VERSION_SCHEMA
    __alembic_version_column_name__: ClassVar[str] = DEFAULT_ALEMBIC_VERSION_COLUMN_NAME
    __metatable_namespace__: ClassVar[str | None] = DEFAULT_ALEMBIC_VERSION_NAMESPACE
    __metatable_identifier__: ClassVar[str | None] = DEFAULT_ALEMBIC_VERSION_IDENTIFIER
    __metatable_description__: ClassVar[str | None] = "Alembic revision state table."

    @classmethod
    def build_registration_request(
        cls,
        *,
        data_source_uid: str,
        identifier: str | None = None,
        namespace: str | None = None,
        description: str | None = None,
        labels: Sequence[str] | None = None,
        protect_from_deletion: bool = False,
        open_for_everyone: bool = False,
        introspect: bool = False,
        table_name: str | None = None,
        schema: str | None = None,
    ) -> MetaTableRegistrationRequest:
        resolved_identifier = identifier or cls.__metatable_identifier__
        resolved_namespace = namespace or cls.__metatable_namespace__
        resolved_description = (
            description if description is not None else cls.__metatable_description__
        )
        resolved_table_name = (
            table_name
            or getattr(cls, "__alembic_version_table_name__", None)
            or DEFAULT_ALEMBIC_VERSION_TABLE_NAME
        )
        resolved_schema = (
            schema
            or getattr(cls, "__alembic_version_schema__", None)
            or DEFAULT_ALEMBIC_VERSION_SCHEMA
        )
        resolved_column_name = (
            getattr(cls, "__alembic_version_column_name__", None)
            or DEFAULT_ALEMBIC_VERSION_COLUMN_NAME
        )
        if not resolved_identifier:
            raise ValueError("AlembicVersionMetaTable requires a MetaTable identifier.")
        if not resolved_namespace:
            raise ValueError("AlembicVersionMetaTable requires a MetaTable namespace.")

        storage_hash = build_meta_table_storage_hash(
            namespace=resolved_namespace,
            identifier=resolved_identifier,
            schema=resolved_schema,
        )

        return MetaTableRegistrationRequest(
            data_source_uid=str(data_source_uid),
            management_mode="external_registered",
            storage_hash=storage_hash,
            identifier=resolved_identifier,
            namespace=resolved_namespace,
            description=resolved_description,
            protect_from_deletion=protect_from_deletion,
            open_for_everyone=open_for_everyone,
            labels=list(labels or []),
            introspect=introspect,
            table_contract=MetaTableContract(
                physical=MetaTablePhysicalContract(table_name=resolved_table_name),
                columns=[
                    {
                        "name": resolved_column_name,
                        "data_type": "string",
                        "backend_type": "VARCHAR",
                        "nullable": False,
                        "primary_key": True,
                    }
                ],
                constraints=[],
                indexes=[],
                foreign_keys=[],
                authoring={
                    "owner": "alembic",
                    "schema": resolved_schema,
                    "version_table": resolved_table_name,
                },
            ),
        )

    @classmethod
    def register(
        cls,
        *,
        data_source_uid: str,
        timeout: int | float | tuple[float, float] | None = None,
        **kwargs: Any,
    ) -> MetaTable:
        request = cls.build_registration_request(
            data_source_uid=data_source_uid,
            **kwargs,
        )
        return MetaTable.register(request, timeout=timeout)


def render_packaged_alembic_migration(
    *,
    package: str,
    migration_namespace: str,
    revision: str,
    down_revision: str | None = None,
    direction: AlembicMigrationDirection = "upgrade",
    current_revision: str | None = None,
    script_location: str | None = None,
    alembic_version_table: str = "public.alembic_version",
    sqlalchemy_url: str = "postgresql://",
    statement_boundaries: Sequence[AlembicMigrationStatementBoundary] | None = None,
) -> PackagedAlembicMigrationArtifact:
    """
    Render an installed package's Alembic migration SQL without applying it.

    This is the programmatic equivalent of ``alembic upgrade <from>:<to> --sql``
    for a packaged Alembic environment. The returned artifact contains only the
    manifest, rendered SQL, and optional diagnostic statement boundaries;
    callers still build the backend apply request separately.
    """

    if direction not in {"upgrade", "downgrade"}:
        raise ValueError("direction must be 'upgrade' or 'downgrade'.")

    resolved_script_location = script_location or f"{package}:migrations"
    revision_range = _alembic_revision_range(
        direction=direction,
        current_revision=current_revision,
        down_revision=down_revision,
        revision=revision,
    )
    rendered_sql = _render_alembic_sql(
        script_location=resolved_script_location,
        sqlalchemy_url=sqlalchemy_url,
        revision_range=revision_range,
        direction=direction,
        alembic_version_table=alembic_version_table,
    )
    manifest = {
        "package": package,
        "migration_namespace": migration_namespace,
        "revision": revision,
        "down_revision": down_revision,
        "direction": direction,
        "alembic_version_table": alembic_version_table,
    }

    return PackagedAlembicMigrationArtifact(
        manifest=manifest,
        sql=rendered_sql,
        statement_boundaries=list(statement_boundaries or []),
    )


def _render_alembic_sql(
    *,
    script_location: str,
    sqlalchemy_url: str,
    revision_range: str,
    direction: AlembicMigrationDirection,
    alembic_version_table: str,
) -> str:
    try:
        from alembic import command
        from alembic.config import Config
    except ImportError as exc:
        raise RuntimeError("Alembic is required to render packaged MetaTable migrations.") from exc

    sql_buffer = io.StringIO()
    message_buffer = io.StringIO()
    config = Config()
    config.stdout = message_buffer
    config.output_buffer = sql_buffer
    config.set_main_option("script_location", script_location)
    config.set_main_option("sqlalchemy.url", sqlalchemy_url)
    config.attributes["alembic_version_table"] = alembic_version_table

    if direction == "upgrade":
        command.upgrade(config, revision_range, sql=True)
    else:
        command.downgrade(config, revision_range, sql=True)

    return sql_buffer.getvalue()


def _alembic_revision_range(
    *,
    direction: AlembicMigrationDirection,
    current_revision: str | None,
    down_revision: str | None,
    revision: str,
) -> str:
    if direction == "upgrade":
        source_revision = current_revision if current_revision is not None else down_revision
        return f"{source_revision or 'base'}:{revision}"

    if current_revision is None:
        raise ValueError("current_revision is required when rendering a downgrade.")
    return f"{current_revision}:{revision}"


__all__ = [
    "AlembicVersionMetaTable",
    "DEFAULT_ALEMBIC_VERSION_COLUMN_NAME",
    "DEFAULT_ALEMBIC_VERSION_IDENTIFIER",
    "DEFAULT_ALEMBIC_VERSION_NAMESPACE",
    "DEFAULT_ALEMBIC_VERSION_SCHEMA",
    "DEFAULT_ALEMBIC_VERSION_TABLE_NAME",
    "PackagedAlembicMigrationArtifact",
    "render_packaged_alembic_migration",
]
