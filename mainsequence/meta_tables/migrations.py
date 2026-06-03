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
    ManagedMetaTableReservationRequest,
    ManagedMetaTableReservationTable,
    MetaTable,
    MetaTableContract,
    MetaTablePhysicalContract,
    MetaTableRegistrationRequest,
    SchemaManagementRequest,
    TimeIndexMetaData,
)
from mainsequence.meta_tables.hashing import build_meta_table_storage_hash
from mainsequence.meta_tables.sqlalchemy_contracts import (
    PlatformManagedMetaTable,
    PlatformTimeIndexMetaData,
    _metatable_foreign_key_target_models,
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


@dataclass(slots=True)
class PreparedAlembicMetaTableMigration:
    data_source_uid: str
    meta_table_uids: list[str]
    reserved_tables: list[Any] = field(default_factory=list)
    owner_role_name: str | None = None


@dataclass(frozen=True, slots=True)
class AlembicMetaTableCatalogRefreshContext:
    package: str
    migration_namespace: str
    registered_metatables: list[Any]
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
    ) -> list[Any]:
        registered: list[Any] = []
        data_source_uid = self._resolve_provider_data_source_uid()
        schema_management = self._schema_management_request(
            alembic_version_meta_table_uid=self.alembic_registry.get_meta_table_uid(),
        )
        with platform_managed_migration_registration_context():
            for model in self.metatable_models:
                request = model.build_registration_request(
                    data_source_uid=data_source_uid,
                    provisioning={"create_table": create_table, "if_not_exists": True},
                    _target_meta_tables=_bound_target_meta_tables(model),
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
        finalized_by_identifier = {
            _meta_table_identifier(item) or "": item for item in response.tables
        }
        finalized_by_uid = {_meta_table_uid(item) or "": item for item in response.tables}
        if on_metatable_finalize_status is not None:
            for item in response.tables:
                if _finalize_table_failed(item):
                    on_metatable_finalize_status(_finalize_failure_message(item))
        finalized_for_models: list[Any] = []
        for model in self.metatable_models:
            identifier = resolve_metatable_identifier(model)
            item = finalized_by_identifier.get(identifier) or finalized_by_uid.get(
                str(getattr(model, "get_meta_table_uid", lambda: "")())
            )
            if item is None:
                continue
            _bind_model_to_existing_metatable(model, item)
            finalized_for_models.append(item)
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
        stage_existing_schema_management: bool = True,
        require_existing_contract_match: bool = True,
        on_metatable_reservation_request: Callable[
            [Sequence[type[Any]], Sequence[ManagedMetaTableReservationTable]], Any
        ]
        | None = None,
        on_metatable_reservation_status: Callable[[str], Any] | None = None,
        on_metatable_reserved: Callable[[type[Any], Any], Any] | None = None,
    ) -> PreparedAlembicMetaTableMigration:
        data_source_uid = self._resolve_provider_data_source_uid()
        reserved_by_model: dict[type[Any], Any] = {}
        reserved_tables: list[Any] = []
        owner_role_name: str | None = None

        ordered_models = _reservation_order(self.metatable_models)
        target_identifiers = {
            model: resolve_metatable_identifier(model) for model in ordered_models
        }
        schema_management = self._schema_management_request(
            alembic_version_meta_table_uid=self.alembic_registry.get_meta_table_uid(),
        )
        with platform_managed_migration_registration_context():
            existing_by_identifier = _get_metatables_by_model_identifier(
                ordered_models,
                timeout=timeout,
            )
            if on_metatable_reservation_status is not None:
                on_metatable_reservation_status(
                    f"Found existing MetaTables by identifier count={len(existing_by_identifier)}."
                )
            pending_models: list[type[Any]] = []
            pending_tables: list[ManagedMetaTableReservationTable] = []
            for model in ordered_models:
                identifier = target_identifiers[model]
                existing_meta_table = existing_by_identifier.get(identifier)
                if existing_meta_table is not None:
                    table_contract = _meta_table_contract(existing_meta_table)
                    if isinstance(table_contract, Mapping):
                        _bind_backend_contract_names(model, table_contract)
                    _bind_model_to_existing_metatable(model, existing_meta_table)
                    reserved_by_model[model] = existing_meta_table
                else:
                    bound_meta_table = _bound_meta_table_for_model(model)
                    if bound_meta_table is not None and _meta_table_uid(bound_meta_table) not in (
                        None,
                        "",
                    ):
                        table_contract = _meta_table_contract(bound_meta_table)
                        if isinstance(table_contract, Mapping):
                            _bind_backend_contract_names(model, table_contract)
                        reserved_by_model[model] = bound_meta_table

                request = model.build_registration_request(
                    data_source_uid=data_source_uid,
                    _target_meta_tables=reserved_by_model,
                    _target_identifiers=target_identifiers,
                    enforce_storage_hash_name=False,
                )
                request.schema_management = schema_management

                bound_meta_table = reserved_by_model.get(model)
                if bound_meta_table is not None:
                    readiness_failure = _alembic_readiness_failure(
                        bound_meta_table,
                        request=request,
                        migration=self,
                        registry_meta_table_uid=self.alembic_registry.get_meta_table_uid(),
                        stage_existing_schema_management=stage_existing_schema_management,
                        require_existing_contract_match=require_existing_contract_match,
                    )
                    if readiness_failure is None:
                        reserved_tables.append(bound_meta_table)
                        continue
                    if on_metatable_reservation_status is not None:
                        on_metatable_reservation_status(
                            _alembic_readiness_failure_message(
                                model,
                                bound_meta_table,
                                readiness_failure,
                            )
                        )

                pending_models.append(model)
                pending_tables.append(_reservation_table_from_registration_request(request))

            if pending_tables:
                reservation_request = ManagedMetaTableReservationRequest(
                    tables=pending_tables,
                )
                if on_metatable_reservation_request is not None:
                    on_metatable_reservation_request(
                        pending_models,
                        reservation_request.tables,
                    )
                response = MetaTable.reserve_managed(
                    reservation_request,
                    timeout=timeout,
                    on_status=on_metatable_reservation_status,
                )
                if len(response.tables) != len(pending_models):
                    raise RuntimeError(
                        "MetaTable reservation response row count mismatch; "
                        f"requested {len(pending_models)}, got {len(response.tables)}."
                    )
                for model, item in zip(pending_models, response.tables, strict=True):
                    if on_metatable_reserved is not None:
                        on_metatable_reserved(model, item)
                    _bind_model_to_existing_metatable(model, item)
                    _bind_backend_contract_names(model, item.table_contract)
                    reserved_by_model[model] = item
                    reserved_tables.append(item)
                    item_owner_role_name = getattr(item, "owner_role_name", None)
                    if item_owner_role_name not in (None, ""):
                        owner_role_name = str(item_owner_role_name)

        meta_table_uids = []
        for model in ordered_models:
            meta_table = reserved_by_model.get(model) or _bound_meta_table_for_model(model)
            uid = _meta_table_uid(meta_table)
            if uid not in (None, ""):
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
        reserved_by_identifier = {
            _meta_table_identifier(item) or "": item for item in prepared.reserved_tables
        }
        if on_metatable_resolution is not None:
            for model in self.metatable_models:
                identifier = resolve_metatable_identifier(model)
                on_metatable_resolution(
                    model,
                    identifier,
                    "reserved",
                    reserved_by_identifier.get(identifier) or _bound_meta_table_for_model(model),
                )
        return prepared.reserved_tables

    def resolve_or_register_metatable_model(
        self,
        model: type[Any],
        *,
        timeout: int | float | tuple[float, float] | None = None,
        existing_meta_tables_by_identifier: Mapping[str, Any] | None = None,
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
                resolve_metatable_identifier(model),
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


def _reservation_order(models: Sequence[type[Any]]) -> list[type[Any]]:
    ordered: list[type[Any]] = []
    visiting: set[type[Any]] = set()
    visited: set[type[Any]] = set()

    def visit(model: type[Any]) -> None:
        if model in visited:
            return
        if model in visiting:
            model_name = getattr(model, "__qualname__", repr(model))
            raise ValueError(f"MetaTable reservation cycle detected at {model_name}.")
        visiting.add(model)
        for target_model in _metatable_foreign_key_target_models(model):
            visit(target_model)
        visiting.remove(model)
        visited.add(model)
        if _is_platform_managed_metatable_model(model):
            ordered.append(model)

    for model in models:
        visit(model)
    return ordered


def _reservation_table_from_registration_request(
    request: Any,
) -> ManagedMetaTableReservationTable:
    data_source_uid = getattr(request, "data_source_uid", None)
    if data_source_uid in (None, ""):
        raise ValueError(
            "Managed MetaTable reservation requires the model registration "
            "request data_source_uid; data-source ownership is table-scoped, "
            "not a reservation-request default."
        )
    return ManagedMetaTableReservationTable(
        identifier=str(request.identifier),
        namespace=request.namespace,
        data_source_uid=str(data_source_uid),
        storage_hash=getattr(request, "storage_hash", None),
        physical_table_name=_request_contract_physical_table_name(request),
        description=getattr(request, "description", None),
        labels=list(getattr(request, "labels", None) or []),
        protect_from_deletion=bool(getattr(request, "protect_from_deletion", False)),
        schema_management=getattr(request, "schema_management", None),
        table_contract=request.table_contract,
        time_index_name=getattr(request, "time_index_name", None),
        partition_strategy=getattr(request, "partition_strategy", None),
    )


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


def _bound_target_meta_tables(model: type[Any]) -> dict[type[Any], Any]:
    targets: dict[type[Any], Any] = {}
    for target_model in _metatable_foreign_key_target_models(model):
        bound = _bound_meta_table_for_model(target_model)
        if bound is not None:
            targets[target_model] = bound
    return targets


def _bound_meta_table_for_model(model: type[Any]) -> Any | None:
    get_meta_table = getattr(model, "get_meta_table", None)
    if callable(get_meta_table):
        meta_table = get_meta_table()
        if meta_table is not None:
            return meta_table
    uid = getattr(model, "get_meta_table_uid", lambda: None)()
    if uid not in (None, ""):
        return model
    return None


def _bind_backend_contract_names(model: type[Any], table_contract: Mapping[str, Any]) -> None:
    table = getattr(model, "__table__", None)
    if table is None:
        return
    _bind_backend_index_names(table, table_contract.get("indexes") or [])
    _bind_backend_foreign_key_names(table, table_contract.get("foreign_keys") or [])


def _bind_backend_index_names(table: Any, index_contracts: Sequence[Any]) -> None:
    indexes = list(getattr(table, "indexes", []) or [])
    by_signature = _group_by_signature(indexes, _sqlalchemy_index_signature)
    fallback_indexes = sorted(indexes, key=lambda index: getattr(index, "name", None) or "")
    fallback_index = 0
    for contract in index_contracts:
        contract_dict = _contract_dict(contract)
        name = contract_dict.get("name")
        if name in (None, ""):
            continue
        signature = _contract_index_signature(contract_dict)
        candidates = by_signature.get(signature) or []
        index = candidates.pop(0) if candidates else None
        if index is None and fallback_index < len(fallback_indexes):
            index = fallback_indexes[fallback_index]
            fallback_index += 1
        if index is not None:
            index.name = str(name)


def _bind_backend_foreign_key_names(table: Any, foreign_key_contracts: Sequence[Any]) -> None:
    constraints = list(getattr(table, "foreign_key_constraints", []) or [])
    by_signature = _group_by_signature(constraints, _sqlalchemy_foreign_key_signature)
    fallback_constraints = sorted(
        constraints,
        key=lambda constraint: getattr(constraint, "name", None) or "",
    )
    fallback_index = 0
    for contract in foreign_key_contracts:
        contract_dict = _contract_dict(contract)
        name = contract_dict.get("name")
        if name in (None, ""):
            continue
        signature = _contract_foreign_key_signature(contract_dict)
        candidates = by_signature.get(signature) or []
        constraint = candidates.pop(0) if candidates else None
        if constraint is None and fallback_index < len(fallback_constraints):
            constraint = fallback_constraints[fallback_index]
            fallback_index += 1
        if constraint is not None:
            constraint.name = str(name)


def _group_by_signature(
    items: Sequence[Any], signature_fn: Callable[[Any], Any]
) -> dict[Any, list[Any]]:
    grouped: dict[Any, list[Any]] = {}
    for item in items:
        grouped.setdefault(signature_fn(item), []).append(item)
    return grouped


def _contract_dict(contract: Any) -> dict[str, Any]:
    if hasattr(contract, "model_dump"):
        return contract.model_dump(mode="json", exclude_none=True)
    return dict(contract or {})


def _sqlalchemy_index_signature(index: Any) -> tuple[Any, ...]:
    columns = tuple(
        str(column.name)
        for column in list(getattr(index, "columns", []) or [])
        if getattr(column, "name", None) not in (None, "")
    )
    expression = None if columns else str(index)
    return (
        columns,
        bool(getattr(index, "unique", False)),
        _sqlalchemy_index_method(index),
        expression,
    )


def _contract_index_signature(contract: Mapping[str, Any]) -> tuple[Any, ...]:
    columns = tuple(str(column) for column in contract.get("columns") or [])
    expression = contract.get("expression")
    return (
        columns,
        bool(contract.get("unique", False)),
        contract.get("method"),
        None if columns else expression,
    )


def _sqlalchemy_index_method(index: Any) -> str | None:
    dialect_options = getattr(index, "dialect_options", None)
    if isinstance(dialect_options, Mapping):
        postgresql_options = dialect_options.get("postgresql")
        if isinstance(postgresql_options, Mapping):
            method = postgresql_options.get("using")
            if method:
                return str(method)
    return None


def _sqlalchemy_foreign_key_signature(constraint: Any) -> tuple[Any, ...]:
    elements = list(getattr(constraint, "elements", []) or [])
    on_delete = getattr(elements[0], "ondelete", None) if elements else None
    on_delete = on_delete or getattr(constraint, "ondelete", None) or "restrict"
    return (
        tuple(str(element.parent.name) for element in elements),
        tuple(str(element.column.name) for element in elements),
        str(on_delete).lower(),
    )


def _contract_foreign_key_signature(contract: Mapping[str, Any]) -> tuple[Any, ...]:
    return (
        tuple(str(column) for column in contract.get("source_columns") or []),
        tuple(str(column) for column in contract.get("target_columns") or []),
        str(contract.get("on_delete") or "restrict").lower(),
    )


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
    meta_table_cls: type[Any] = MetaTable,
) -> MetaTable | None:
    matches = _get_metatables_by_identifier(
        [identifier],
        timeout=timeout,
        meta_table_cls=meta_table_cls,
    )
    return matches.get(identifier)


def _get_metatable_by_model_identifier(
    model: type[Any],
    *,
    timeout: int | float | tuple[float, float] | None = None,
) -> Any | None:
    identifier = resolve_metatable_identifier(model)
    return _get_metatable_by_identifier(
        identifier,
        timeout=timeout,
        meta_table_cls=_metatable_resource_class_for_model(model),
    )


def _get_metatables_by_model_identifier(
    models: Sequence[type[Any]],
    *,
    timeout: int | float | tuple[float, float] | None = None,
) -> dict[str, Any]:
    identifiers_by_resource: dict[type[Any], list[str]] = {}
    for model in models:
        identifiers_by_resource.setdefault(
            _metatable_resource_class_for_model(model),
            [],
        ).append(resolve_metatable_identifier(model))

    resolved: dict[str, Any] = {}
    for meta_table_cls, identifiers in identifiers_by_resource.items():
        matches = _get_metatables_by_identifier(
            identifiers,
            timeout=timeout,
            meta_table_cls=meta_table_cls,
        )
        duplicate_identifiers = set(resolved).intersection(matches)
        if duplicate_identifiers:
            duplicate_list = ", ".join(sorted(duplicate_identifiers))
            raise ValueError(
                "MetaTable identifiers are not globally unique across provider models: "
                f"{duplicate_list}."
            )
        resolved.update(matches)
    return resolved


def _get_metatables_by_identifier(
    identifiers: Sequence[str],
    *,
    timeout: int | float | tuple[float, float] | None = None,
    meta_table_cls: type[Any] = MetaTable,
) -> dict[str, Any]:
    unique_identifiers = list(dict.fromkeys(str(identifier) for identifier in identifiers))
    if not unique_identifiers:
        return {}

    matches = meta_table_cls.filter_by_body(
        timeout=timeout,
        identifiers=unique_identifiers,
        limit=max(len(unique_identifiers), 1),
    )
    if not matches:
        return {}

    matched_by_identifier: dict[str, Any] = {}
    for meta_table in matches:
        identifier = _meta_table_identifier(meta_table)
        if identifier is None:
            if len(unique_identifiers) == 1:
                identifier = unique_identifiers[0]
            else:
                raise ValueError(
                    f"Backend returned a {meta_table_cls.__name__} row without identifier "
                    "while resolving migration provider models by body identifiers."
                )
        if identifier in matched_by_identifier:
            raise ValueError(
                f"MetaTable identifier {identifier!r} is not globally unique; "
                "found multiple catalog rows."
            )
        matched_by_identifier[identifier] = meta_table
    return matched_by_identifier


def _meta_table_identifier(meta_table: Any) -> str | None:
    if isinstance(meta_table, Mapping):
        identifier = meta_table.get("identifier")
    else:
        identifier = getattr(meta_table, "identifier", None)
    if identifier in (None, ""):
        return None
    return str(identifier)


def _meta_table_uid(meta_table: Any) -> str | None:
    if meta_table is None:
        return None
    if isinstance(meta_table, Mapping):
        uid = meta_table.get("meta_table_uid") or meta_table.get("uid")
    else:
        uid = getattr(meta_table, "meta_table_uid", None) or getattr(meta_table, "uid", None)
    if uid in (None, ""):
        return None
    return str(uid)


def _meta_table_contract(meta_table: Any) -> Any:
    if isinstance(meta_table, Mapping):
        return meta_table.get("table_contract")
    return getattr(meta_table, "table_contract", None)


def _finalize_table_failed(item: Any) -> bool:
    provisioning_status = _meta_table_attr(item, "provisioning_status")
    physical_table_exists = _meta_table_attr(item, "physical_table_exists")
    finalized = _meta_table_attr(item, "finalized")
    error = _meta_table_attr(item, "error")
    return (
        provisioning_status != "active"
        or physical_table_exists is False
        or finalized is False
        or error not in (None, "", {})
    )


def _finalize_failure_message(item: Any) -> str:
    identifier = _meta_table_identifier(item) or _meta_table_uid(item) or "<unknown>"
    physical_table_name = _meta_table_attr(item, "physical_table_name") or "no-physical-table"
    provisioning_status = _meta_table_attr(item, "provisioning_status")
    physical_table_exists = _meta_table_attr(item, "physical_table_exists")
    finalized = _meta_table_attr(item, "finalized")
    error = _meta_table_attr(item, "error")
    parts = [
        "Finalize-managed table failed",
        f"identifier={identifier}",
        f"physical_table={physical_table_name}",
        f"provisioning_status={provisioning_status}",
        f"physical_table_exists={physical_table_exists}",
        f"finalized={finalized}",
    ]
    if error not in (None, "", {}):
        parts.append(f"error={json.dumps(error, sort_keys=True, default=str)}")
    return " ".join(parts)


def _meta_table_attr(meta_table: Any, name: str) -> Any:
    if isinstance(meta_table, Mapping):
        return meta_table.get(name)
    return getattr(meta_table, name, None)


def _schema_management_payload(meta_table: Any) -> Mapping[str, Any]:
    payload = _meta_table_attr(meta_table, "schema_management")
    return payload if isinstance(payload, Mapping) else {}


def _meta_table_schema_management_mode(meta_table: Any) -> str | None:
    mode = _meta_table_attr(meta_table, "schema_management_mode")
    if mode in (None, ""):
        schema_management = _schema_management_payload(meta_table)
        mode = schema_management.get("mode")
    return str(mode) if mode not in (None, "") else None


def _meta_table_migration_provider_key(meta_table: Any) -> str | None:
    provider_key = _meta_table_attr(meta_table, "migration_provider_key")
    if provider_key in (None, ""):
        alembic = _schema_management_payload(meta_table).get("alembic")
        if isinstance(alembic, Mapping):
            provider_key = alembic.get("provider_key")
    return str(provider_key) if provider_key not in (None, "") else None


def _meta_table_alembic_version_uid(meta_table: Any) -> str | None:
    uid = _meta_table_attr(meta_table, "alembic_version_meta_table_uid")
    if uid in (None, ""):
        alembic = _schema_management_payload(meta_table).get("alembic")
        if isinstance(alembic, Mapping):
            uid = alembic.get("alembic_version_meta_table_uid")
    return str(uid) if uid not in (None, "") else None


def _existing_metatable_is_ready_for_alembic(
    meta_table: Any,
    *,
    request: Any,
    migration: AlembicMetaTableMigration,
    registry_meta_table_uid: str | None,
    stage_existing_schema_management: bool,
    require_existing_contract_match: bool,
) -> bool:
    return (
        _alembic_readiness_failure(
            meta_table,
            request=request,
            migration=migration,
            registry_meta_table_uid=registry_meta_table_uid,
            stage_existing_schema_management=stage_existing_schema_management,
            require_existing_contract_match=require_existing_contract_match,
        )
        is None
    )


def _alembic_readiness_failure(
    meta_table: Any,
    *,
    request: Any,
    migration: AlembicMetaTableMigration,
    registry_meta_table_uid: str | None,
    stage_existing_schema_management: bool,
    require_existing_contract_match: bool,
) -> tuple[str, dict[str, Any]] | None:
    if _meta_table_uid(meta_table) in (None, ""):
        return "missing_uid", {}
    if _meta_table_attr(meta_table, "physical_table_name") in (None, ""):
        return "missing_physical_table", {}
    existing_contract = _meta_table_contract(meta_table)
    if require_existing_contract_match and not isinstance(existing_contract, Mapping):
        return "missing_contract", {}
    if require_existing_contract_match and not _contracts_equivalent(
        existing_contract,
        getattr(request, "table_contract", None),
    ):
        return "contract_mismatch", {}
    if not stage_existing_schema_management:
        return None
    existing_mode = _meta_table_schema_management_mode(meta_table)
    if existing_mode != "alembic_managed":
        return "not_alembic_managed", {
            "existing_mode": existing_mode,
            "expected_mode": "alembic_managed",
        }
    existing_provider_key = _meta_table_migration_provider_key(meta_table)
    if existing_provider_key not in (None, migration.migration_provider_key):
        return "provider_key_mismatch", {
            "existing_provider_key": existing_provider_key,
            "expected_provider_key": migration.migration_provider_key,
        }
    existing_registry_uid = _meta_table_alembic_version_uid(meta_table)
    if registry_meta_table_uid not in (None, "") and existing_registry_uid not in (
        None,
        str(registry_meta_table_uid),
    ):
        return "alembic_registry_uid_mismatch", {
            "existing_registry_uid": existing_registry_uid,
            "expected_registry_uid": str(registry_meta_table_uid),
        }
    return None


def _alembic_readiness_failure_message(
    model: type[Any],
    meta_table: Any,
    failure: tuple[str, dict[str, Any]],
) -> str:
    reason, details = failure
    model_name = getattr(model, "__name__", repr(model))
    identifier = _meta_table_identifier(meta_table) or resolve_metatable_identifier(model)
    uid = _meta_table_uid(meta_table)
    physical_table_name = _meta_table_attr(meta_table, "physical_table_name")
    parts = [
        "Existing MetaTable not ready for Alembic fast path; reserving",
        f"identifier={identifier}",
        f"model={model_name}",
        f"reason={reason}",
    ]
    if uid not in (None, ""):
        parts.append(f"uid={uid}")
    if physical_table_name not in (None, ""):
        parts.append(f"physical_table={physical_table_name}")
    for key, value in details.items():
        parts.append(f"{key}={value}")
    return " ".join(parts)


def _contracts_equivalent(left: Any, right: Any) -> bool:
    return _contract_fingerprint(left) == _contract_fingerprint(right)


def _contract_fingerprint(contract: Any) -> str:
    return json.dumps(
        _strip_client_metadata(_jsonable_contract(contract)),
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _jsonable_contract(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", exclude_none=True)
    if isinstance(value, Mapping):
        return {str(key): _jsonable_contract(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable_contract(item) for item in value]
    if isinstance(value, tuple):
        return [_jsonable_contract(item) for item in value]
    return value


def _strip_client_metadata(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _strip_client_metadata(item)
            for key, item in value.items()
            if key != "orm_class"
        }
    if isinstance(value, list):
        return [_strip_client_metadata(item) for item in value]
    return value


def _metatable_resource_class_for_model(model: type[Any]) -> type[Any]:
    if _is_platform_time_index_metatable_model(model):
        return TimeIndexMetaData
    return MetaTable


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


def _is_platform_time_index_metatable_model(model: Any) -> bool:
    return isinstance(model, type) and issubclass(model, PlatformTimeIndexMetaData)


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
