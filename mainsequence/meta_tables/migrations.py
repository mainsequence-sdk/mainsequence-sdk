from __future__ import annotations

import importlib
import json
import pathlib
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, ClassVar, Literal

from mainsequence.client.metatables import (
    AlembicManagementRequest,
    AlembicProviderResetRequest,
    AlembicProviderResetResponse,
    DynamicTableDataSource,
    ManagedMetaTableFinalizeRequest,
    ManagedMetaTableFinalizeResponse,
    ManagedMetaTableFinalizeTableResult,
    MetaTable,
    MetaTableContract,
    MetaTablePhysicalContract,
    MetaTableRegistrationRequest,
    SchemaManagementRequest,
    TimeIndexMetaTable,
)
from mainsequence.meta_tables.hashing import build_meta_table_storage_hash
from mainsequence.meta_tables.sqlalchemy_contracts import (
    PlatformManagedMetaTable,
    PlatformTimeIndexMetaTable,
    _ensure_time_index_unique_grain_index,
    _normalize_table_default_schema,
    _resolve_model_data_source_uid,
    _resolve_table,
    _resolve_time_index_name,
    _resolve_time_index_names,
    _table_name,
    _validate_time_index_contract,
    platform_managed_migration_registration_context,
)

DEFAULT_ALEMBIC_VERSION_IDENTIFIER = "alembic_version"
DEFAULT_ALEMBIC_VERSION_NAMESPACE = "mainsequence.migrations"
DEFAULT_ALEMBIC_VERSION_TABLE_NAME = "alembic_version"
DEFAULT_ALEMBIC_VERSION_SCHEMA = "public"
DEFAULT_ALEMBIC_VERSION_COLUMN_NAME = "version_num"
DEFAULT_ALEMBIC_PROVIDER_REFERENCE = "mainsequence_migrations:migration"


@dataclass(slots=True)
class PreparedAlembicMetaTableMigration:
    data_source_uid: str
    meta_table_uids: list[str]
    reserved_tables: list[MetaTable] = field(default_factory=list)
    owner_role_name: str | None = None


@dataclass(frozen=True, slots=True)
class AlembicMetaTableCatalogRefreshContext:
    package: str
    migration_namespace: str
    registered_metatables: list[MetaTable]
    reserved_policy: Literal["reconcile"] | None = None


class AlembicProviderPhysicalStateError(RuntimeError):
    """Raised when backend finalization leaves provider MetaTables reserved."""

    def __init__(
        self,
        *,
        migration_provider_key: str,
        response: ManagedMetaTableFinalizeResponse,
    ) -> None:
        self.migration_provider_key = migration_provider_key
        self.response = response
        self.missing = [
            table
            for table in response.tables
            if table.provisioning_status != "active" or not table.physical_table_exists
        ]
        detail = ", ".join(
            (
                f"{table.identifier or table.meta_table_uid}"
                f"({table.physical_table_name or 'no-physical-table'})"
                f":status={table.provisioning_status}"
                f":physical_table_exists={table.physical_table_exists}"
                f":finalized={table.finalized}"
                + (
                    f":error={json.dumps(table.error, sort_keys=True, default=str)}"
                    if table.error not in (None, "", {})
                    else ""
                )
            )
            for table in self.missing
        )
        super().__init__(
            "Alembic provider physical state is not active after finalization "
            f"provider={migration_provider_key} "
            f"active={response.active_count} reserved={response.reserved_count} "
            f"failed={response.failed_count} missing=[{detail}]"
        )


def _normalize_optional_alembic_location(value: Any, field_name: str) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        raise ValueError(f"AlembicMetaTableMigration {field_name} cannot be empty.")
    return text


def _normalize_alembic_version_locations(value: str | Sequence[str] | None) -> tuple[str, ...]:
    if value is None:
        return ()
    raw_locations: Sequence[str]
    if isinstance(value, str):
        raw_locations = [value]
    else:
        raw_locations = value

    locations: list[str] = []
    seen: set[str] = set()
    for raw_location in raw_locations:
        location = _normalize_optional_alembic_location(raw_location, "version_locations")
        if location is None or location in seen:
            continue
        seen.add(location)
        locations.append(location)
    if not locations:
        raise ValueError("AlembicMetaTableMigration version_locations cannot be empty.")
    return tuple(locations)


def _configure_alembic_version_locations(
    config: Any,
    version_locations: str | Sequence[str] | None,
) -> tuple[str, ...]:
    locations = _normalize_alembic_version_locations(version_locations)
    if not locations:
        return ()
    config.set_main_option("version_locations", "\n".join(locations))
    config.set_main_option("version_path_separator", "newline")
    return locations


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
        data_source: DynamicTableDataSource | None = None,
        data_source_uid: str | None = None,
        identifier: str | None = None,
        namespace: str | None = None,
        description: str | None = None,
        labels: Sequence[str] | None = None,
        protect_from_deletion: bool = False,
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
        resolved_data_source_uid = _resolve_model_data_source_uid(
            cls,
            data_source=data_source,
            data_source_uid=data_source_uid,
        )

        return MetaTableRegistrationRequest(
            data_source_uid=resolved_data_source_uid,
            management_mode="external_registered",
            storage_hash=storage_hash,
            identifier=resolved_identifier,
            namespace=resolved_namespace,
            description=resolved_description,
            protect_from_deletion=protect_from_deletion,
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
        data_source: DynamicTableDataSource | None = None,
        data_source_uid: str | None = None,
        timeout: int | float | tuple[float, float] | None = None,
        on_registered: Callable[[type[Any], Any], Any] | None = None,
        **kwargs: Any,
    ) -> MetaTable:
        request = cls.build_registration_request(
            data_source=data_source,
            data_source_uid=data_source_uid,
            **kwargs,
        )
        meta_table = MetaTable.register(request, timeout=timeout)
        if on_registered is not None:
            on_registered(cls, meta_table)
        cls._bind_meta_table(meta_table)
        return meta_table

    @classmethod
    def _bind_meta_table(cls, meta_table: MetaTable) -> MetaTable:
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
    version_locations: str | Sequence[str] | None = None
    version_path: str | None = None
    metatable_models: Sequence[type[Any]] = field(default_factory=tuple)
    after_register_metatables: Callable[[AlembicMetaTableCatalogRefreshContext], Any] | None = None
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
        self.resolved_version_locations()
        self.resolved_version_path()
        _normalize_provider_default_schemas(self.metatable_models)
        _ensure_provider_time_index_grain_indexes(self.metatable_models)

    @property
    def alembic_version_table(self) -> str:
        return self.alembic_registry.alembic_table_path()

    @property
    def migration_provider_key(self) -> str:
        return f"{self.package}:{self.migration_namespace}"

    @property
    def version_table(self) -> str:
        return self.alembic_registry.__alembic_version_table_name__

    @property
    def version_table_schema(self) -> str | None:
        return self.alembic_registry.__alembic_version_schema__

    def resolved_version_locations(self) -> tuple[str, ...]:
        return _normalize_alembic_version_locations(self.version_locations)

    def resolved_version_path(self) -> str | None:
        explicit = _normalize_optional_alembic_location(self.version_path, "version_path")
        if explicit is not None:
            return explicit
        locations = self.resolved_version_locations()
        if len(locations) == 1:
            return locations[0]
        if len(locations) > 1:
            raise ValueError(
                "AlembicMetaTableMigration with multiple version_locations requires "
                "an explicit version_path for revision generation."
            )
        return None

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

    def _schema_management_request(
        self,
        *,
        alembic_version_meta_table_uid: str | None = None,
        revision: str | None = None,
    ) -> SchemaManagementRequest:
        return SchemaManagementRequest(
            mode="alembic_managed",
            alembic=AlembicManagementRequest(
                package=self.package,
                migration_namespace=self.migration_namespace,
                provider_key=self.migration_provider_key,
                alembic_version_meta_table_uid=alembic_version_meta_table_uid,
                revision=revision,
            ),
        )

    def register_alembic_registry(
        self,
        *,
        data_source: DynamicTableDataSource | None = None,
        data_source_uid: str | None = None,
        timeout: int | float | tuple[float, float] | None = None,
        on_metatable_registered: Callable[[type[Any], Any], Any] | None = None,
    ) -> MetaTable:
        return self.alembic_registry.register(
            data_source=data_source,
            data_source_uid=data_source_uid,
            timeout=timeout,
            on_registered=on_metatable_registered,
        )

    def ensure_alembic_registry(
        self,
        *,
        data_source: DynamicTableDataSource | None = None,
        data_source_uid: str | None = None,
        timeout: int | float | tuple[float, float] | None = None,
        on_metatable_registered: Callable[[type[Any], Any], Any] | None = None,
    ) -> MetaTable:
        return self.register_alembic_registry(
            data_source=data_source,
            data_source_uid=data_source_uid,
            timeout=timeout,
            on_metatable_registered=on_metatable_registered,
        )

    def sync_metatable_catalog(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
        create_table: bool = False,
        on_metatable_registered: Callable[[type[Any], Any], Any] | None = None,
        reserved_policy: Literal["reconcile"] | None = None,
    ) -> list[MetaTable]:
        registered: list[MetaTable] = []
        data_source_uid = self._resolve_provider_data_source_uid()
        schema_management = self._schema_management_request(
            alembic_version_meta_table_uid=self.alembic_registry.get_meta_table_uid(),
        )
        with platform_managed_migration_registration_context():
            for model in self.metatable_models:
                request = model.build_registration_request(
                    data_source_uid=data_source_uid,
                    provisioning={"create_table": create_table, "if_not_exists": True},
                    enforce_storage_hash_name=False,
                )
                request.schema_management = schema_management
                meta_table_cls = _metatable_resource_class_for_model(model)
                meta_table = meta_table_cls.register(request, timeout=timeout)
                if on_metatable_registered is not None:
                    on_metatable_registered(model, meta_table)
                _bind_model_to_existing_metatable(model, meta_table)
                registered.append(meta_table)
        if self.after_register_metatables is not None:
            self.after_register_metatables(
                AlembicMetaTableCatalogRefreshContext(
                    package=self.package,
                    migration_namespace=self.migration_namespace,
                    registered_metatables=registered,
                    reserved_policy=reserved_policy,
                )
            )
        return registered

    def refresh_metatable_catalog(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
        on_metatable_registered: Callable[[type[Any], Any], Any] | None = None,
    ) -> list[Any]:
        return self.sync_metatable_catalog(
            timeout=timeout,
            create_table=False,
            on_metatable_registered=on_metatable_registered,
            reserved_policy="reconcile",
        )

    def finalize_metatable_catalog(
        self,
        *,
        prepared: PreparedAlembicMetaTableMigration | None = None,
        alembic_revision: str | None = None,
        timeout: int | float | tuple[float, float] | None = None,
        on_metatable_finalized: Callable[[type[Any], Any], Any] | None = None,
        on_metatable_finalize_status: Callable[[str], Any] | None = None,
    ) -> ManagedMetaTableFinalizeResponse:
        prepared = prepared or self.prepare_for_alembic(timeout=timeout)
        request = ManagedMetaTableFinalizeRequest(
            meta_table_uids=prepared.meta_table_uids,
            migration_package=self.package,
            migration_namespace=self.migration_namespace,
            migration_provider_key=self.migration_provider_key,
            alembic_version_meta_table_uid=self.alembic_registry.get_meta_table_uid(),
            alembic_revision=alembic_revision,
        )
        response = MetaTable.finalize_managed(
            request,
            timeout=timeout,
            on_status=on_metatable_finalize_status,
        )
        if on_metatable_finalize_status is not None:
            on_metatable_finalize_status(
                "Finalize-managed response "
                f"ok={response.ok} finalized={response.finalized_count} "
                f"active={response.active_count} reserved={response.reserved_count} "
                f"failed={response.failed_count}."
            )
        finalized_by_table_name = {item.physical_table_name or "": item for item in response.tables}
        finalized_by_uid = {item.meta_table_uid: item for item in response.tables}
        if on_metatable_finalize_status is not None:
            for item in response.tables:
                if _finalize_table_failed(item):
                    on_metatable_finalize_status(_finalize_failure_message(item))
        finalized_for_models: list[MetaTable] = []
        for model in self.metatable_models:
            table_name = _migration_table_name(model)
            item = finalized_by_table_name.get(table_name) or finalized_by_uid.get(
                str(getattr(model, "get_meta_table_uid", lambda: "")())
            )
            if item is None:
                continue
            finalized_meta_table = _metatable_from_finalize_result(model, item)
            _bind_model_to_existing_metatable(model, finalized_meta_table)
            finalized_for_models.append(finalized_meta_table)
            if on_metatable_finalized is not None:
                on_metatable_finalized(model, item)

        if not response.ok:
            raise AlembicProviderPhysicalStateError(
                migration_provider_key=self.migration_provider_key,
                response=response,
            )

        if self.after_register_metatables is not None:
            self.after_register_metatables(
                AlembicMetaTableCatalogRefreshContext(
                    package=self.package,
                    migration_namespace=self.migration_namespace,
                    registered_metatables=finalized_for_models,
                    reserved_policy="reconcile",
                )
            )
        return response

    def reset_alembic_provider(
        self,
        *,
        confirm_reset: bool,
        drop_physical_tables: bool = True,
        clear_alembic_version_table: bool = True,
        include_reserved: bool = True,
        timeout: int | float | tuple[float, float] | None = None,
        on_reset_status: Callable[[str], Any] | None = None,
    ) -> AlembicProviderResetResponse:
        request = AlembicProviderResetRequest(
            migration_package=self.package,
            migration_namespace=self.migration_namespace,
            data_source_uid=self._resolve_provider_data_source_uid(),
            confirm_reset=confirm_reset,
            drop_physical_tables=drop_physical_tables,
            clear_alembic_version_table=clear_alembic_version_table,
            include_reserved=include_reserved,
        )
        return MetaTable.alembic_provider_reset(
            request,
            timeout=timeout,
            on_status=on_reset_status,
        )

    def prepare_for_alembic(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
        on_metatable_reservation_request: Callable[
            [Sequence[type[Any]], Sequence[Mapping[str, Any]]], Any
        ]
        | None = None,
        on_metatable_reservation_status: Callable[[str], Any] | None = None,
        on_metatable_reserved: Callable[[type[Any], Any], Any] | None = None,
    ) -> PreparedAlembicMetaTableMigration:
        data_source_uid = self._resolve_provider_data_source_uid()
        reserved_by_model: dict[type[Any], MetaTable] = {}
        reserved_tables: list[MetaTable] = []
        owner_role_name: str | None = None

        ordered_models = list(dict.fromkeys(self.metatable_models))
        target_table_names = {model: _migration_table_name(model) for model in ordered_models}
        with platform_managed_migration_registration_context():
            existing_by_table_name = _get_metatables_by_model_table_name(
                ordered_models,
                timeout=timeout,
            )
            if on_metatable_reservation_status is not None:
                on_metatable_reservation_status(
                    f"Found existing MetaTables by table name count={len(existing_by_table_name)}."
                )
            pending_by_resource: dict[
                type[MetaTable],
                list[tuple[type[Any], dict[str, Any]]],
            ] = {}
            for model in ordered_models:
                table_name = target_table_names[model]
                existing_meta_table = existing_by_table_name.get(table_name)
                if existing_meta_table is not None:
                    if _meta_table_provisioning_status(existing_meta_table) == "active":
                        _bind_model_to_existing_metatable(model, existing_meta_table)
                        reserved_by_model[model] = existing_meta_table
                        continue
                    if on_metatable_reservation_status is not None:
                        on_metatable_reservation_status(
                            "Restaging existing reserved MetaTable "
                            f"table_name={table_name} with current provider contract."
                        )
                else:
                    bound_meta_table = _bound_meta_table_for_model(model)
                    if bound_meta_table is not None:
                        reserved_by_model[model] = bound_meta_table
                        continue

                request = model.build_registration_request(
                    data_source_uid=data_source_uid,
                    identifier=table_name,
                    enforce_storage_hash_name=False,
                )

                resource_cls = _metatable_resource_class_for_model(model)
                pending_by_resource.setdefault(resource_cls, []).append(
                    (
                        model,
                        _collection_create_row_from_registration_request(
                            request,
                            migration_package=self.package,
                            migration_namespace=self.migration_namespace,
                            migration_provider_key=self.migration_provider_key,
                            alembic_version_meta_table_uid=(
                                self.alembic_registry.get_meta_table_uid()
                            ),
                            alembic_revision=None,
                        ),
                    )
                )

            for resource_cls, pending_items in pending_by_resource.items():
                pending_models = [model for model, _row in pending_items]
                pending_rows = [row for _model, row in pending_items]
                if on_metatable_reservation_request is not None:
                    on_metatable_reservation_request(pending_models, pending_rows)
                response_tables = resource_cls.bulk_create(
                    pending_rows,
                    timeout=timeout,
                    on_status=on_metatable_reservation_status,
                )
                if len(response_tables) != len(pending_models):
                    raise RuntimeError(
                        f"{resource_cls.__name__}.bulk_create response row count mismatch; "
                        f"requested {len(pending_models)}, got {len(response_tables)}."
                    )
                for model, reserved_meta_table in zip(
                    pending_models,
                    response_tables,
                    strict=True,
                ):
                    if on_metatable_reserved is not None:
                        on_metatable_reserved(model, reserved_meta_table)
                    _bind_model_to_existing_metatable(model, reserved_meta_table)
                    reserved_by_model[model] = reserved_meta_table
                    reserved_tables.append(reserved_meta_table)
                    item_owner_role_name = getattr(reserved_meta_table, "owner_role_name", None)
                    if item_owner_role_name not in (None, ""):
                        owner_role_name = str(item_owner_role_name)

        meta_table_uids = []
        for model in ordered_models:
            meta_table = reserved_by_model.get(model) or _bound_meta_table_for_model(model)
            if meta_table is None:
                model_name = getattr(model, "__qualname__", repr(model))
                raise RuntimeError(f"MetaTable model {model_name} was not bound after reservation.")
            uid = _meta_table_uid(meta_table)
            if uid in (None, ""):
                model_name = getattr(model, "__qualname__", repr(model))
                raise RuntimeError(f"MetaTable model {model_name} was bound without uid.")
            meta_table_uids.append(str(uid))
        return PreparedAlembicMetaTableMigration(
            data_source_uid=data_source_uid,
            meta_table_uids=list(dict.fromkeys(meta_table_uids)),
            reserved_tables=reserved_tables,
            owner_role_name=owner_role_name,
        )

    def _resolve_provider_data_source_uid(self) -> str:
        data_source_uids: list[str] = []
        registry_uid = self.alembic_registry.get_data_source_uid()
        if registry_uid not in (None, ""):
            data_source_uids.append(str(registry_uid))
        for model in self.metatable_models:
            data_source_uids.append(_resolve_model_data_source_uid(model))

        unique_data_source_uids = list(dict.fromkeys(data_source_uids))
        if not unique_data_source_uids:
            raise ValueError(
                "AlembicMetaTableMigration requires a data source uid on the "
                "alembic registry or at least one provider MetaTable model."
            )
        if len(unique_data_source_uids) > 1:
            raise ValueError(
                "AlembicMetaTableMigration models must share one data source uid; got "
                f"{unique_data_source_uids!r}."
            )
        return unique_data_source_uids[0]

    def resolve_or_register_metatable_models(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
        on_metatable_resolution: Callable[[type[Any], str, str, Any | None], Any] | None = None,
    ) -> list[Any]:
        """Reserve and bind provider-scoped models before Alembic runs."""

        prepared = self.prepare_for_alembic(timeout=timeout)
        reserved_by_table_name = {
            _meta_table_physical_table_name(item) or "": item for item in prepared.reserved_tables
        }
        if on_metatable_resolution is not None:
            for model in self.metatable_models:
                table_name = _migration_table_name(model)
                on_metatable_resolution(
                    model,
                    table_name,
                    "reserved",
                    reserved_by_table_name.get(table_name) or _bound_meta_table_for_model(model),
                )
        return prepared.reserved_tables

    def resolve_or_register_metatable_model(
        self,
        model: type[Any],
        *,
        timeout: int | float | tuple[float, float] | None = None,
        on_metatable_resolution: Callable[[type[Any], str, str, Any | None], Any] | None = None,
    ) -> Any | None:
        if not _is_platform_managed_metatable_model(model):
            return None
        prepared = AlembicMetaTableMigration(
            package=self.package,
            migration_namespace=self.migration_namespace,
            script_location=self.script_location,
            target_metadata=self.target_metadata,
            alembic_registry=self.alembic_registry,
            metatable_models=[model],
        ).prepare_for_alembic(timeout=timeout)
        meta_table = prepared.reserved_tables[0] if prepared.reserved_tables else None
        if on_metatable_resolution is not None:
            on_metatable_resolution(
                model,
                _migration_table_name(model),
                "reserved",
                meta_table,
            )
        return meta_table


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


def alembic_config_for_provider(
    migration: AlembicMetaTableMigration,
    *,
    sqlalchemy_url: str,
    owner_role_name: str | None = None,
    stdout: Any | None = None,
    output_buffer: Any | None = None,
) -> Any:
    try:
        from alembic.config import Config
    except ImportError as exc:
        raise RuntimeError("Alembic is required for MetaTable migrations.") from exc

    config = Config()
    if stdout is not None:
        config.stdout = stdout
    if output_buffer is not None:
        config.output_buffer = output_buffer
    config.set_main_option("script_location", migration.script_location)
    version_locations = _configure_alembic_version_locations(config, migration.version_locations)
    config.set_main_option("sqlalchemy.url", sqlalchemy_url.replace("%", "%%"))
    config.set_main_option("sqlalchemy.echo", "true")
    config.set_main_option("version_table", migration.version_table)
    if migration.version_table_schema:
        config.set_main_option("version_table_schema", migration.version_table_schema)
    if owner_role_name not in (None, ""):
        config.set_main_option("mainsequence.owner_role_name", str(owner_role_name))
        config.attributes["mainsequence_migration_owner_role_name"] = str(owner_role_name)
    config.attributes["mainsequence_migration_sqlalchemy_url"] = sqlalchemy_url
    config.attributes["mainsequence_migration_provider"] = migration
    config.attributes["target_metadata"] = migration.target_metadata
    config.attributes["alembic_version_table"] = migration.alembic_version_table
    config.attributes["version_table"] = migration.version_table
    config.attributes["version_table_schema"] = migration.version_table_schema
    config.attributes["version_locations"] = version_locations
    config.attributes["version_path"] = migration.resolved_version_path()
    return config


def apply_mainsequence_migration_role(connection: Any, config: Any) -> None:
    owner_role_name = None
    attributes = getattr(config, "attributes", None)
    if isinstance(attributes, Mapping):
        owner_role_name = attributes.get("mainsequence_migration_owner_role_name")
    if owner_role_name in (None, "") and hasattr(config, "get_main_option"):
        owner_role_name = config.get_main_option("mainsequence.owner_role_name")
    if owner_role_name in (None, ""):
        return

    try:
        from sqlalchemy import text
    except ImportError as exc:
        raise RuntimeError("SQLAlchemy is required to apply the migration owner role.") from exc

    escaped_role = str(owner_role_name).replace('"', '""')
    was_in_transaction = _connection_in_transaction(connection)
    connection.execute(text(f'SET ROLE "{escaped_role}"'))
    if not was_in_transaction and _connection_in_transaction(connection):
        commit = getattr(connection, "commit", None)
        if callable(commit):
            commit()


def _connection_in_transaction(connection: Any) -> bool:
    in_transaction = getattr(connection, "in_transaction", None)
    if not callable(in_transaction):
        return False
    try:
        return bool(in_transaction())
    except Exception:
        return False


def resolve_alembic_revision_metadata(
    *,
    script_location: str,
    revision: str,
    version_locations: str | Sequence[str] | None = None,
) -> tuple[str, str | None]:
    try:
        from alembic.config import Config
        from alembic.script import ScriptDirectory
    except ImportError as exc:
        raise RuntimeError("Alembic is required to resolve migration revisions.") from exc

    config = Config()
    config.set_main_option("script_location", script_location)
    _configure_alembic_version_locations(config, version_locations)
    script = ScriptDirectory.from_config(config)
    resolved = script.get_revision(revision)
    if resolved is None:
        raise ValueError(f"Alembic revision {revision!r} was not found.")
    return str(resolved.revision), _normalize_down_revision(resolved.down_revision)


def _collection_create_row_from_registration_request(
    request: Any,
    *,
    migration_package: str,
    migration_namespace: str,
    migration_provider_key: str | None,
    alembic_version_meta_table_uid: str | None,
    alembic_revision: str | None,
) -> dict[str, Any]:
    data_source_uid = getattr(request, "data_source_uid", None)
    if data_source_uid in (None, ""):
        raise ValueError(
            "Alembic MetaTable collection-create requires the model registration "
            "request data_source_uid; data-source ownership is table-scoped, "
            "not a provider-request default."
        )
    storage_hash = getattr(request, "storage_hash", None)
    if storage_hash in (None, ""):
        raise ValueError("Alembic MetaTable collection-create requires storage_hash.")
    physical_table_name = _request_contract_physical_table_name(request)
    if physical_table_name in (None, ""):
        raise ValueError("Alembic MetaTable collection-create requires physical_table_name.")

    row: dict[str, Any] = {
        "data_source_uid": str(data_source_uid),
        "storage_hash": str(storage_hash),
        "identifier": getattr(request, "identifier", None),
        "namespace": getattr(request, "namespace", None),
        "description": getattr(request, "description", None),
        "labels": list(getattr(request, "labels", None) or []),
        "management_mode": "platform_managed",
        "provisioning_status": "reserved",
        "is_alembic_managed": True,
        "migration_package": migration_package,
        "migration_namespace": migration_namespace,
        "migration_provider_key": migration_provider_key,
        "alembic_version_meta_table_uid": alembic_version_meta_table_uid,
        "alembic_revision": alembic_revision,
        "physical_table_name": str(physical_table_name),
        "protect_from_deletion": bool(getattr(request, "protect_from_deletion", False)),
        "table_contract": request.table_contract,
    }
    time_index_name = getattr(request, "time_index_name", None)
    if time_index_name not in (None, ""):
        row["time_index_name"] = str(time_index_name)
    partition_strategy = getattr(request, "partition_strategy", None)
    if partition_strategy not in (None, ""):
        row["partition_strategy"] = str(partition_strategy)
    return row


def _request_contract_physical_table_name(request: Any) -> str | None:
    table_contract = getattr(request, "table_contract", None)
    if isinstance(table_contract, MetaTableContract):
        return table_contract.physical.table_name
    if isinstance(table_contract, Mapping):
        physical = table_contract.get("physical")
        if isinstance(physical, Mapping):
            value = physical.get("table_name")
            return str(value) if value not in (None, "") else None
    return None


def _bound_meta_table_for_model(model: type[Any]) -> MetaTable | None:
    return model.get_meta_table()


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
    names: list[str] = []
    for key, table in tables.items():
        for candidate in (
            key,
            getattr(table, "fullname", None),
            getattr(table, "name", None),
        ):
            if candidate not in (None, ""):
                names.append(str(candidate))
    return list(dict.fromkeys(names))


def _normalize_provider_default_schemas(models: Sequence[type[Any]]) -> None:
    for model in models:
        if _is_platform_managed_metatable_model(model):
            _normalize_table_default_schema(_resolve_table(model))


def _ensure_provider_time_index_grain_indexes(models: Sequence[type[Any]]) -> None:
    for model in models:
        if not _is_platform_time_index_metatable_model(model):
            continue
        table = _resolve_table(model)
        time_index_name = _resolve_time_index_name(model)
        index_names = _resolve_time_index_names(model, time_index_name=time_index_name)
        _validate_time_index_contract(
            columns=list(table.columns),
            time_index_name=time_index_name,
            index_names=index_names,
        )
        _ensure_time_index_unique_grain_index(table=table, index_names=index_names)


def _normalize_down_revision(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, Sequence):
        return ",".join(str(item) for item in value)
    return str(value)


def _migration_table_name(model: type[Any]) -> str:
    """Return the authored SQLAlchemy table name used for migration preparation."""

    table_name = _table_name(_resolve_table(model))
    if table_name in (None, ""):
        model_name = getattr(model, "__qualname__", repr(model))
        raise ValueError(
            f"Alembic MetaTable migrations require a SQLAlchemy table name for {model_name}."
        )
    return str(table_name)


def _get_metatables_by_model_table_name(
    models: Sequence[type[Any]],
    *,
    timeout: int | float | tuple[float, float] | None = None,
) -> dict[str, MetaTable]:
    table_names_by_resource: dict[type[MetaTable], list[str]] = {}
    for model in models:
        table_names_by_resource.setdefault(
            _metatable_resource_class_for_model(model),
            [],
        ).append(_migration_table_name(model))

    resolved: dict[str, Any] = {}
    for meta_table_cls, table_names in table_names_by_resource.items():
        matches = _get_metatables_by_table_name(
            table_names,
            timeout=timeout,
            meta_table_cls=meta_table_cls,
        )
        duplicate_table_names = set(resolved).intersection(matches)
        if duplicate_table_names:
            duplicate_list = ", ".join(sorted(duplicate_table_names))
            raise ValueError(
                "MetaTable physical table names are not unique across provider models: "
                f"{duplicate_list}."
            )
        resolved.update(matches)
    return resolved


def _get_metatables_by_table_name(
    table_names: Sequence[str],
    *,
    timeout: int | float | tuple[float, float] | None = None,
    meta_table_cls: type[MetaTable] = MetaTable,
) -> dict[str, MetaTable]:
    unique_table_names = list(dict.fromkeys(str(table_name) for table_name in table_names))
    if not unique_table_names:
        return {}

    matches = meta_table_cls.filter_by_body(
        timeout=timeout,
        physical_table_name__in=unique_table_names,
        limit=max(len(unique_table_names), 1),
    )
    if not matches:
        return {}

    matched_by_table_name: dict[str, MetaTable] = {}
    for meta_table in matches:
        table_name = _meta_table_physical_table_name(meta_table)
        if table_name is None:
            if len(unique_table_names) == 1:
                table_name = unique_table_names[0]
            else:
                raise ValueError(
                    f"Backend returned a {meta_table_cls.__name__} row without "
                    "physical_table_name while resolving migration provider models "
                    "by table name."
                )
        if table_name in matched_by_table_name:
            raise ValueError(
                f"MetaTable physical table name {table_name!r} is not unique; "
                "found multiple catalog rows."
            )
        matched_by_table_name[table_name] = meta_table
    return matched_by_table_name


def _meta_table_identifier(meta_table: MetaTable) -> str | None:
    identifier = meta_table.identifier
    if identifier in (None, ""):
        return None
    return str(identifier)


def _meta_table_physical_table_name(meta_table: MetaTable) -> str | None:
    physical_table_name = meta_table.physical_table_name
    if physical_table_name in (None, ""):
        return None
    return str(physical_table_name)


def _meta_table_provisioning_status(meta_table: MetaTable) -> str | None:
    provisioning_status = meta_table.provisioning_status
    if provisioning_status in (None, ""):
        return None
    return str(provisioning_status)


def _meta_table_uid(meta_table: MetaTable) -> str | None:
    uid = meta_table.uid
    if uid in (None, ""):
        return None
    return str(uid)


def _finalize_table_failed(item: ManagedMetaTableFinalizeTableResult) -> bool:
    return (
        item.provisioning_status != "active"
        or item.physical_table_exists is False
        or item.error not in (None, "", {})
    )


def _finalize_failure_message(item: ManagedMetaTableFinalizeTableResult) -> str:
    identifier = item.identifier or item.meta_table_uid or "<unknown>"
    physical_table_name = item.physical_table_name or "no-physical-table"
    parts = [
        "Finalize-managed table failed",
        f"identifier={identifier}",
        f"physical_table={physical_table_name}",
        f"provisioning_status={item.provisioning_status}",
        f"physical_table_exists={item.physical_table_exists}",
        f"finalized={item.finalized}",
    ]
    if item.error not in (None, "", {}):
        parts.append(f"error={json.dumps(item.error, sort_keys=True, default=str)}")
    return " ".join(parts)


def _metatable_resource_class_for_model(model: type[Any]) -> type[MetaTable]:
    if _is_platform_time_index_metatable_model(model):
        return TimeIndexMetaTable
    return MetaTable


def _bind_model_to_existing_metatable(model: Any, meta_table: MetaTable) -> None:
    bind = getattr(model, "_bind_meta_table", None)
    if not callable(bind):
        model_name = getattr(model, "__qualname__", repr(model))
        raise TypeError(
            f"Migration MetaTable model {model_name} cannot bind an existing MetaTable row."
        )
    bind(meta_table)


def _metatable_from_finalize_result(
    model: type[Any],
    item: ManagedMetaTableFinalizeTableResult,
) -> MetaTable:
    resource_cls = _metatable_resource_class_for_model(model)
    return resource_cls.model_construct(
        uid=item.meta_table_uid,
        data_source_uid=_resolve_model_data_source_uid(model),
        storage_hash=item.storage_hash,
        identifier=item.identifier,
        management_mode="platform_managed",
        provisioning_status=item.provisioning_status,
        schema_management={},
        schema_management_mode=item.schema_management_mode or "backend_managed",
        migration_provider_key=item.migration_provider_key,
        alembic_version_meta_table_uid=item.alembic_version_meta_table_uid,
        physical_table_name=item.physical_table_name,
        table_kind=item.table_kind,
        time_indexed=item.time_indexed,
    )


def _is_platform_managed_metatable_model(model: Any) -> bool:
    return isinstance(model, type) and issubclass(model, PlatformManagedMetaTable)


def _is_platform_time_index_metatable_model(model: Any) -> bool:
    return isinstance(model, type) and issubclass(model, PlatformTimeIndexMetaTable)


__all__ = [
    "AlembicMetaTableCatalogRefreshContext",
    "AlembicMetaTableMigration",
    "AlembicVersionMetaTable",
    "DEFAULT_ALEMBIC_PROVIDER_REFERENCE",
    "DEFAULT_ALEMBIC_VERSION_COLUMN_NAME",
    "DEFAULT_ALEMBIC_VERSION_IDENTIFIER",
    "DEFAULT_ALEMBIC_VERSION_NAMESPACE",
    "DEFAULT_ALEMBIC_VERSION_SCHEMA",
    "DEFAULT_ALEMBIC_VERSION_TABLE_NAME",
    "PreparedAlembicMetaTableMigration",
    "alembic_config_for_provider",
    "apply_mainsequence_migration_role",
    "load_alembic_metatable_migration_provider",
    "resolve_alembic_revision_metadata",
]
