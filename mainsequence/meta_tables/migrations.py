from __future__ import annotations

import importlib
import io
import pathlib
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
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
from mainsequence.meta_tables.sqlalchemy_contracts import (
    PlatformManagedMetaTable,
    _resolve_model_data_source_uid,
    platform_managed_migration_registration_context,
    resolve_metatable_identifier,
)

DEFAULT_ALEMBIC_VERSION_IDENTIFIER = "alembic_version"
DEFAULT_ALEMBIC_VERSION_NAMESPACE = "mainsequence.migrations"
DEFAULT_ALEMBIC_VERSION_TABLE_NAME = "alembic_version"
DEFAULT_ALEMBIC_VERSION_SCHEMA = "public"
DEFAULT_ALEMBIC_VERSION_COLUMN_NAME = "version_num"
DEFAULT_ALEMBIC_PROVIDER_REFERENCE = "mainsequence_migrations:migration"


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
    __metatable__: ClassVar[MetaTable | None] = None
    __metatable_uid__: ClassVar[str | None] = None
    __metatable_data_source_uid__: ClassVar[str | None] = None

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
        timeout: int | float | tuple[float, float] | None = None,
        **kwargs: Any,
    ) -> MetaTable:
        resolved_data_source_uid = _resolve_model_data_source_uid(cls)
        request = cls.build_registration_request(
            data_source_uid=resolved_data_source_uid,
            **kwargs,
        )
        meta_table = MetaTable.register(request, timeout=timeout)
        cls._bind_meta_table(meta_table)
        return meta_table

    @classmethod
    def _bind_meta_table(cls, meta_table: Any) -> Any:
        cls.__metatable_uid__ = str(meta_table.uid)
        cls.__metatable__ = meta_table
        cls.__metatable_data_source_uid__ = str(meta_table.data_source_uid)
        return meta_table

    @classmethod
    def get_meta_table(cls) -> MetaTable | None:
        return cls.__metatable__

    @classmethod
    def get_meta_table_uid(cls) -> str | None:
        return cls.__metatable_uid__

    @classmethod
    def get_data_source_uid(cls) -> str | None:
        return cls.__metatable_data_source_uid__

    @classmethod
    def alembic_table_path(cls) -> str:
        schema = cls.__alembic_version_schema__
        table_name = cls.__alembic_version_table_name__
        return f"{schema}.{table_name}" if schema else table_name


@dataclass(slots=True)
class AlembicMetaTableMigration:
    """Single Alembic-backed migration provider for MetaTable projects."""

    package: str
    migration_namespace: str
    script_location: str
    target_metadata: Any
    alembic_registry: type[AlembicVersionMetaTable]
    metatable_models: Sequence[type[Any]] = field(default_factory=tuple)
    after_register_metatables: Callable[[list[Any]], Any] | None = None
    include_name_hook: Any | None = None
    include_object_hook: Any | None = None

    def __post_init__(self) -> None:
        if not self.package:
            raise ValueError("AlembicMetaTableMigration requires package.")
        if not self.migration_namespace:
            raise ValueError("AlembicMetaTableMigration requires migration_namespace.")
        if not self.script_location:
            raise ValueError("AlembicMetaTableMigration requires script_location.")
        if not issubclass(self.alembic_registry, AlembicVersionMetaTable):
            raise TypeError("alembic_registry must be an AlembicVersionMetaTable subclass.")
        if self.after_register_metatables is not None and not callable(
            self.after_register_metatables
        ):
            raise TypeError("after_register_metatables must be callable when provided.")

    @property
    def alembic_version_table(self) -> str:
        return self.alembic_registry.alembic_table_path()

    @property
    def version_table(self) -> str:
        return self.alembic_registry.__alembic_version_table_name__

    @property
    def version_table_schema(self) -> str | None:
        return self.alembic_registry.__alembic_version_schema__

    def include_name(self, name: str | None, type_: str, parent_names: dict[str, Any]) -> bool:
        if self.include_name_hook is not None:
            return bool(self.include_name_hook(name, type_, parent_names))
        if type_ != "table":
            return True
        target_tables = set(_metadata_table_names(self.target_metadata))
        if not target_tables:
            return True
        schema = parent_names.get("schema_name") if parent_names else None
        qualified_name = f"{schema}.{name}" if schema else str(name)
        return str(name) in target_tables or qualified_name in target_tables

    def include_object(
        self,
        object_: Any,
        name: str | None,
        type_: str,
        reflected: bool,
        compare_to: Any,
    ) -> bool:
        if self.include_object_hook is not None:
            return bool(self.include_object_hook(object_, name, type_, reflected, compare_to))
        return self.include_name(name, type_, {"schema_name": getattr(object_, "schema", None)})

    def register_alembic_registry(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> MetaTable:
        return self.alembic_registry.register(timeout=timeout)

    def ensure_alembic_registry(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> MetaTable:
        meta_table = self.alembic_registry.get_meta_table()
        if meta_table is not None:
            return meta_table
        return self.register_alembic_registry(timeout=timeout)

    def sync_metatable_catalog(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> list[Any]:
        registered: list[Any] = []
        with platform_managed_migration_registration_context():
            for model in self.metatable_models:
                self.resolve_or_register_metatable_model(model, timeout=timeout)
                registered.append(model.register(timeout=timeout))
        if self.after_register_metatables is not None:
            self.after_register_metatables(registered)
        return registered

    def resolve_or_register_metatable_models(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
        on_register_metatable: Callable[[type[Any], str], Any] | None = None,
    ) -> list[Any]:
        """Resolve provider-scoped models before SQL rendering.

        Platform-managed models are migration-first: missing catalog rows are
        created through the existing backend registration path inside the
        migration workflow, then bound to the returned physical table names.
        """

        resolved: list[Any] = []
        with platform_managed_migration_registration_context():
            for model in self.metatable_models:
                meta_table = self.resolve_or_register_metatable_model(
                    model,
                    timeout=timeout,
                    on_register_metatable=on_register_metatable,
                )
                if meta_table is not None:
                    resolved.append(meta_table)
        return resolved

    def resolve_or_register_metatable_model(
        self,
        model: type[Any],
        *,
        timeout: int | float | tuple[float, float] | None = None,
        on_register_metatable: Callable[[type[Any], str], Any] | None = None,
    ) -> Any | None:
        identifier = resolve_metatable_identifier(model)
        existing_meta_table = _get_metatable_by_identifier(identifier, timeout=timeout)
        if existing_meta_table is not None:
            _bind_model_to_existing_metatable(model, existing_meta_table)
            return existing_meta_table
        if _is_platform_managed_metatable_model(model):
            if on_register_metatable is not None:
                on_register_metatable(model, identifier)
            return model.register(timeout=timeout)
        return None


def load_alembic_metatable_migration_provider(
    provider: str | None = None,
    *,
    cwd: str | pathlib.Path | None = None,
) -> AlembicMetaTableMigration:
    """Load the selected Alembic MetaTable migration provider."""

    provider_refs = [provider] if provider else _conventional_provider_refs(cwd=cwd)
    last_error: Exception | None = None
    for provider_ref in provider_refs:
        if not provider_ref:
            continue
        try:
            loaded = _load_provider_reference(provider_ref, cwd=cwd)
        except (AttributeError, ImportError, ModuleNotFoundError) as exc:
            last_error = exc
            continue
        if not isinstance(loaded, AlembicMetaTableMigration):
            raise TypeError(
                f"Provider {provider_ref!r} must resolve to AlembicMetaTableMigration; "
                f"got {type(loaded).__name__}."
            )
        return loaded

    message = (
        "Could not find Alembic MetaTable migration provider. Define "
        "mainsequence_migrations.py:migration or pass --provider module.path:migration."
    )
    if last_error is not None:
        message = f"{message} Last error: {last_error}"
    raise RuntimeError(message)


def render_packaged_alembic_migration_for_provider(
    migration: AlembicMetaTableMigration,
    *,
    revision: str,
    direction: AlembicMigrationDirection = "upgrade",
    current_revision: str | None = None,
    sqlalchemy_url: str = "postgresql://",
    statement_boundaries: Sequence[AlembicMigrationStatementBoundary] | None = None,
) -> PackagedAlembicMigrationArtifact:
    resolved_revision, down_revision = resolve_alembic_revision_metadata(
        script_location=migration.script_location,
        revision=revision,
    )
    return render_packaged_alembic_migration(
        package=migration.package,
        migration_namespace=migration.migration_namespace,
        revision=resolved_revision,
        down_revision=down_revision,
        direction=direction,
        current_revision=current_revision,
        script_location=migration.script_location,
        alembic_version_table=migration.alembic_version_table,
        sqlalchemy_url=sqlalchemy_url,
        statement_boundaries=statement_boundaries,
    )


def resolve_alembic_revision_metadata(
    *,
    script_location: str,
    revision: str,
) -> tuple[str, str | None]:
    try:
        from alembic.config import Config
        from alembic.script import ScriptDirectory
    except ImportError as exc:
        raise RuntimeError("Alembic is required to resolve migration revisions.") from exc

    config = Config()
    config.set_main_option("script_location", script_location)
    script = ScriptDirectory.from_config(config)
    resolved = script.get_revision(revision)
    if resolved is None:
        raise ValueError(f"Alembic revision {revision!r} was not found.")
    return str(resolved.revision), _normalize_down_revision(resolved.down_revision)


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


def _conventional_provider_refs(*, cwd: str | pathlib.Path | None = None) -> list[str]:
    refs = [DEFAULT_ALEMBIC_PROVIDER_REFERENCE]
    package_name = _project_package_name(cwd=cwd)
    if package_name:
        refs.append(f"{package_name}.migrations:migration")
    return refs


def _load_provider_reference(
    provider_ref: str,
    *,
    cwd: str | pathlib.Path | None = None,
) -> Any:
    module_name, attr_name = _parse_provider_reference(provider_ref)
    cwd_path = pathlib.Path(cwd or pathlib.Path.cwd()).resolve()
    added_path = False
    if str(cwd_path) not in sys.path:
        sys.path.insert(0, str(cwd_path))
        added_path = True
    try:
        module = importlib.import_module(module_name)
        return getattr(module, attr_name)
    finally:
        if added_path:
            try:
                sys.path.remove(str(cwd_path))
            except ValueError:
                pass


def _parse_provider_reference(provider_ref: str) -> tuple[str, str]:
    if ":" not in provider_ref:
        raise ValueError("Provider must have shape module.path:migration.")
    module_name, attr_name = provider_ref.split(":", 1)
    if not module_name or not attr_name:
        raise ValueError("Provider must have shape module.path:migration.")
    return module_name, attr_name


def _project_package_name(*, cwd: str | pathlib.Path | None = None) -> str | None:
    pyproject_path = pathlib.Path(cwd or pathlib.Path.cwd()).resolve() / "pyproject.toml"
    if not pyproject_path.exists():
        return None
    try:
        import tomllib
    except ModuleNotFoundError:  # pragma: no cover - Python < 3.11 fallback.
        return None
    try:
        pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    project_name = pyproject.get("project", {}).get("name")
    if not project_name:
        return None
    package_name = str(project_name).replace("-", "_")
    package_name = "".join(ch for ch in package_name if ch.isalnum() or ch == "_")
    return package_name or None


def _metadata_table_names(target_metadata: Any) -> list[str]:
    tables = getattr(target_metadata, "tables", None)
    if tables is None:
        return []
    return [str(name) for name in tables.keys()]


def _normalize_down_revision(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, Sequence):
        return ",".join(str(item) for item in value)
    return str(value)


def _get_metatable_by_identifier(
    identifier: str,
    *,
    timeout: int | float | tuple[float, float] | None = None,
) -> MetaTable | None:
    matches = MetaTable.filter(identifier=identifier, timeout=timeout)
    if not matches:
        return None
    if len(matches) > 1:
        raise ValueError(
            f"MetaTable identifier {identifier!r} is not globally unique; "
            f"found {len(matches)} catalog rows."
        )
    return matches[0]


def _bind_model_to_existing_metatable(model: Any, meta_table: MetaTable) -> None:
    bind = getattr(model, "_bind_meta_table", None)
    if not callable(bind):
        model_name = getattr(model, "__qualname__", repr(model))
        raise TypeError(
            f"Migration MetaTable model {model_name} cannot bind an existing MetaTable row."
        )
    bind(meta_table)


def _is_platform_managed_metatable_model(model: Any) -> bool:
    return isinstance(model, type) and issubclass(model, PlatformManagedMetaTable)


__all__ = [
    "AlembicMetaTableMigration",
    "AlembicVersionMetaTable",
    "DEFAULT_ALEMBIC_PROVIDER_REFERENCE",
    "DEFAULT_ALEMBIC_VERSION_COLUMN_NAME",
    "DEFAULT_ALEMBIC_VERSION_IDENTIFIER",
    "DEFAULT_ALEMBIC_VERSION_NAMESPACE",
    "DEFAULT_ALEMBIC_VERSION_SCHEMA",
    "DEFAULT_ALEMBIC_VERSION_TABLE_NAME",
    "PackagedAlembicMigrationArtifact",
    "load_alembic_metatable_migration_provider",
    "render_packaged_alembic_migration_for_provider",
    "render_packaged_alembic_migration",
    "resolve_alembic_revision_metadata",
]
