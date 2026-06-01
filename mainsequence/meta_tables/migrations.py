from __future__ import annotations

import datetime
import hashlib
import json
import uuid
from collections.abc import Mapping
from importlib import resources
from typing import Any, ClassVar

import yaml
from pydantic import ConfigDict, Field, model_validator
from sqlalchemy import JSON, DateTime, Index, Integer, String, Text, Uuid
from sqlalchemy.orm import Mapped, mapped_column

from mainsequence.client.base import BasePydanticModel
from mainsequence.client.dtype_codec import TIMESTAMP_TZ
from mainsequence.client.models_metatables import (
    METATABLE_MIGRATION_V1,
    MetaTable,
    MetaTableCompiledSQLOperation,
    MetaTableMigrationAffectedTable,
    MetaTableMigrationApplyResponse,
    MetaTableMigrationDirection,
    MetaTableMigrationOperation,
    MetaTableMigrationSchemaOperation,
    MetaTableMigrationStatusRequest,
    MetaTableMigrationStatusResponse,
)
from mainsequence.client.utils import serialize_to_json

from .compiled_sql.v1 import build_operation
from .sqlalchemy_contracts import (
    MigrationManagedMetaTable,
    PlatformManagedMetaTable,
    PlatformTimeIndexMetaData,
    table_contract_from_sqlalchemy_model,
)

MIGRATION_MANIFEST_V1 = "metatable-migration-manifest.v1"
DEFAULT_MIGRATION_REGISTRY_IDENTIFIER = "metatable_migrations"
DEFAULT_MIGRATION_REGISTRY_NAMESPACE = "mainsequence.migrations"
_MIGRATION_ROW_NAMESPACE = uuid.uuid5(
    uuid.NAMESPACE_URL,
    "mainsequence.meta_tables.migrations.registry-row",
)


class MigrationMetaTable(PlatformManagedMetaTable):
    """
    SQLAlchemy mixin for client-defined MetaTable migration registries.

    Subclass this together with a SQLAlchemy declarative base, or use
    ``create_default_migration_registry_model(...)``.
    """

    __abstract__ = True

    __metatable_namespace__: ClassVar[str | None] = DEFAULT_MIGRATION_REGISTRY_NAMESPACE
    __metatable_identifier__: ClassVar[str | None] = DEFAULT_MIGRATION_REGISTRY_IDENTIFIER
    __metatable_description__: ClassVar[str | None] = (
        "Client-defined registry of packaged MetaTable schema migration artifacts."
    )
    __metatable_extra_hash_components__: ClassVar[Mapping[str, Any] | None] = {
        "storage_name": DEFAULT_MIGRATION_REGISTRY_IDENTIFIER,
    }

    uid: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    package: Mapped[str] = mapped_column(String(255), nullable=False)
    migration_namespace: Mapped[str] = mapped_column(String(255), nullable=False)
    revision: Mapped[str] = mapped_column(String(128), nullable=False)
    down_revision: Mapped[str | None] = mapped_column(String(128), nullable=True)
    direction: Mapped[str] = mapped_column(String(16), nullable=False)
    version: Mapped[str] = mapped_column(String(64), nullable=False)
    target_data_source_uid: Mapped[str] = mapped_column(String(64), nullable=False)
    expected_current_revision: Mapped[str | None] = mapped_column(String(128), nullable=True)
    manifest: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    manifest_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    operations: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False)
    sql: Mapped[str] = mapped_column(Text, nullable=False)
    sql_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    statement_boundaries: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False)
    affected_tables: Mapped[list[dict[str, Any]]] = mapped_column(JSON, nullable=False)
    old_contracts: Mapped[dict[str, dict[str, Any]]] = mapped_column(JSON, nullable=False)
    old_contract_hashes: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)
    new_contract_hashes: Mapped[dict[str, str]] = mapped_column(JSON, nullable=False)
    new_contracts: Mapped[dict[str, dict[str, Any]]] = mapped_column(JSON, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(512), nullable=False)
    lock_key: Mapped[str] = mapped_column(String(512), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    previous_revision: Mapped[str | None] = mapped_column(String(128), nullable=True)
    applied_revision: Mapped[str | None] = mapped_column(String(128), nullable=True)
    executed_statement_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    affected_table_uids: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    introspection_snapshots: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    error: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    started_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    finished_at: Mapped[datetime.datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )


class MetaTableMigrationManifest(BasePydanticModel):
    version: str = MIGRATION_MANIFEST_V1
    package: str | None = None
    migration_namespace: str
    revision: str
    down_revision: str | None = None
    direction: MetaTableMigrationDirection = "upgrade"
    expected_current_revision: str | None = None
    sql_path: str | None = None
    operations: list[MetaTableMigrationSchemaOperation] = Field(default_factory=list)
    statement_boundaries: list[dict[str, Any]] = Field(default_factory=list)
    affected_tables: list[MetaTableMigrationAffectedTable] = Field(default_factory=list)
    old_contracts: dict[str, dict[str, Any]] = Field(default_factory=dict)
    old_contract_hashes: dict[str, str] = Field(default_factory=dict)
    new_contract_hashes: dict[str, str] = Field(default_factory=dict)
    new_contracts: dict[str, dict[str, Any]] = Field(default_factory=dict)
    extension_fields: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(extra="allow")

    @model_validator(mode="after")
    def _validate_manifest(self) -> MetaTableMigrationManifest:
        if self.version != MIGRATION_MANIFEST_V1:
            raise ValueError(f"Unsupported MetaTable migration manifest version: {self.version!r}")
        if self.direction not in {"upgrade", "downgrade"}:
            raise ValueError("direction must be 'upgrade' or 'downgrade'.")
        if self.sql_path is not None and not self.sql_path.strip():
            raise ValueError("sql_path must be non-empty when provided.")
        if not self.sql_path and not self.operations:
            raise ValueError("MetaTable migrations require operations or sql_path.")
        return self


class PackagedMetaTableMigration(BasePydanticModel):
    package: str
    manifest_path: str
    manifest: MetaTableMigrationManifest
    manifest_text: str
    manifest_sha256: str
    sql: str
    sql_sha256: str
    operations_sha256: str

    @property
    def revision(self) -> str:
        return self.manifest.revision

    @property
    def direction(self) -> MetaTableMigrationDirection:
        return self.manifest.direction


class MetaTableMigrationRegistryRow(BasePydanticModel):
    uid: str
    package: str
    migration_namespace: str
    revision: str
    down_revision: str | None = None
    direction: MetaTableMigrationDirection = "upgrade"
    version: str = METATABLE_MIGRATION_V1
    target_data_source_uid: str
    expected_current_revision: str | None = None
    manifest: dict[str, Any]
    manifest_sha256: str
    sql: str
    sql_sha256: str
    operations: list[dict[str, Any]] = Field(default_factory=list)
    statement_boundaries: list[dict[str, Any]] = Field(default_factory=list)
    affected_tables: list[dict[str, Any]] = Field(default_factory=list)
    old_contracts: dict[str, dict[str, Any]] = Field(default_factory=dict)
    old_contract_hashes: dict[str, str] = Field(default_factory=dict)
    new_contract_hashes: dict[str, str] = Field(default_factory=dict)
    new_contracts: dict[str, dict[str, Any]] = Field(default_factory=dict)
    idempotency_key: str
    lock_key: str
    status: str = "pending"
    previous_revision: str | None = None
    applied_revision: str | None = None
    executed_statement_count: int | None = None
    affected_table_uids: list[str] = Field(default_factory=list)
    introspection_snapshots: dict[str, Any] = Field(default_factory=dict)
    error: dict[str, Any] | None = None
    created_at: datetime.datetime | None = None
    started_at: datetime.datetime | None = None
    finished_at: datetime.datetime | None = None

    model_config = ConfigDict(extra="allow")


def create_default_migration_registry_model(
    base: type[Any],
    *,
    namespace: str = DEFAULT_MIGRATION_REGISTRY_NAMESPACE,
    identifier: str = DEFAULT_MIGRATION_REGISTRY_IDENTIFIER,
    schema: str = "public",
    class_name: str = "MetaTableMigrationRegistry",
) -> type[MigrationMetaTable]:
    """
    Build a default SQLAlchemy declarative migration registry model.

    ``base`` must be the project's SQLAlchemy declarative base.
    """

    attrs: dict[str, Any] = {
        "__module__": __name__,
        "__abstract__": False,
        "__table_args__": (
            Index(
                None,
                "target_data_source_uid",
                "package",
                "migration_namespace",
                "revision",
                "direction",
                unique=True,
            ),
            {"schema": schema},
        ),
        "__metatable_namespace__": namespace,
        "__metatable_identifier__": identifier,
        "__metatable_description__": (
            "Client-defined registry of packaged MetaTable schema migration artifacts."
        ),
        "__metatable_extra_hash_components__": {"storage_name": identifier},
    }
    return type(class_name, (MigrationMetaTable, base), attrs)


def load_packaged_migration(
    package: str,
    manifest_path: str,
    *,
    old_contract_models: Mapping[str, Any] | None = None,
    new_contract_models: Mapping[str, Any] | None = None,
    target_meta_tables: Mapping[Any, Any] | None = None,
) -> PackagedMetaTableMigration:
    manifest_bytes = _read_package_bytes(package, manifest_path)
    manifest_text = manifest_bytes.decode("utf-8")
    manifest_payload = _parse_manifest(manifest_text, manifest_path=manifest_path)
    manifest_payload.setdefault("package", package)
    manifest = MetaTableMigrationManifest(**manifest_payload)

    sql = ""
    if manifest.sql_path:
        sql_bytes = _read_package_bytes(package, manifest.sql_path)
        sql = sql_bytes.decode("utf-8")

    _validate_migration_managed_models(old_contract_models or {})
    _validate_migration_managed_models(new_contract_models or {})

    old_contracts = dict(manifest.old_contracts)
    old_contracts.update(
        contracts_from_models(
            old_contract_models or {},
            target_meta_tables=target_meta_tables,
        )
    )
    old_hashes = dict(manifest.old_contract_hashes)
    old_hashes.update(
        {identifier: contract_hash(contract) for identifier, contract in old_contracts.items()}
    )
    new_contracts = dict(manifest.new_contracts)
    new_contracts.update(
        contracts_from_models(
            new_contract_models or {},
            target_meta_tables=target_meta_tables,
        )
    )
    new_hashes = dict(manifest.new_contract_hashes)
    new_hashes.update(
        {identifier: contract_hash(contract) for identifier, contract in new_contracts.items()}
    )
    manifest.old_contracts = old_contracts
    manifest.new_contract_hashes = new_hashes
    manifest.old_contract_hashes = old_hashes
    manifest.new_contracts = new_contracts

    return PackagedMetaTableMigration(
        package=package,
        manifest_path=manifest_path,
        manifest=manifest,
        manifest_text=manifest_text,
        manifest_sha256=sha256_text(manifest_text),
        sql=sql,
        sql_sha256=sha256_text(sql),
        operations_sha256=sha256_payload(
            [
                _strip_client_metadata(operation.model_dump(mode="json", exclude_none=True))
                for operation in manifest.operations
            ]
        ),
    )


def build_migration_registry_row(
    packaged_migration: PackagedMetaTableMigration,
    *,
    data_source_uid: str,
    uid: str | None = None,
    status: str = "pending",
    extension_fields: Mapping[str, Any] | None = None,
) -> MetaTableMigrationRegistryRow:
    manifest = packaged_migration.manifest
    row_uid = uid or migration_row_uid(
        data_source_uid=data_source_uid,
        package=packaged_migration.package,
        migration_namespace=manifest.migration_namespace,
        revision=manifest.revision,
        direction=manifest.direction,
    )
    payload: dict[str, Any] = {
        "uid": row_uid,
        "package": packaged_migration.package,
        "migration_namespace": manifest.migration_namespace,
        "revision": manifest.revision,
        "down_revision": manifest.down_revision,
        "direction": manifest.direction,
        "version": METATABLE_MIGRATION_V1,
        "target_data_source_uid": str(data_source_uid),
        "expected_current_revision": manifest.expected_current_revision,
        "manifest": _strip_client_metadata(manifest.model_dump(mode="json", exclude_none=True)),
        "manifest_sha256": packaged_migration.manifest_sha256,
        "operations": [
            _strip_client_metadata(operation.model_dump(mode="json", exclude_none=True))
            for operation in manifest.operations
        ],
        "sql": packaged_migration.sql,
        "sql_sha256": packaged_migration.sql_sha256,
        "statement_boundaries": list(manifest.statement_boundaries),
        "affected_tables": [
            _strip_client_metadata(table.model_dump(mode="json", by_alias=True, exclude_none=True))
            for table in manifest.affected_tables
        ],
        "old_contracts": dict(manifest.old_contracts),
        "old_contract_hashes": dict(manifest.old_contract_hashes),
        "new_contract_hashes": dict(manifest.new_contract_hashes),
        "new_contracts": dict(manifest.new_contracts),
        "idempotency_key": migration_idempotency_key(
            row_uid=row_uid,
            artifact_sha256=migration_artifact_hash(
                manifest_sha256=packaged_migration.manifest_sha256,
                operations_sha256=packaged_migration.operations_sha256,
                sql_sha256=packaged_migration.sql_sha256,
            ),
        ),
        "lock_key": migration_lock_key(
            data_source_uid=data_source_uid,
            package=packaged_migration.package,
            migration_namespace=manifest.migration_namespace,
        ),
        "status": status,
    }
    if extension_fields:
        payload.update(dict(extension_fields))
    return MetaTableMigrationRegistryRow(**payload)


def build_registry_upsert_operation(
    migration_meta_table: MetaTable,
    row: MetaTableMigrationRegistryRow | Mapping[str, Any],
) -> MetaTableCompiledSQLOperation:
    row_payload = _row_payload(row)
    table_name = _registry_physical_table_name(migration_meta_table)
    columns = list(row_payload.keys())
    sql = (
        f"INSERT INTO {table_name} ({', '.join(_quote_identifier(column) for column in columns)}) "
        f"VALUES ({', '.join(f'%({column})s' for column in columns)}) "
        'ON CONFLICT ("uid") DO UPDATE SET '
        + ", ".join(
            f"{_quote_identifier(column)} = EXCLUDED.{_quote_identifier(column)}"
            for column in columns
            if column != "uid"
        )
    )
    return build_operation(
        operation="upsert",
        sql=sql,
        parameters=row_payload,
        parameter_types=_temporal_parameter_types(row_payload),
        scope={
            "tables": [
                {
                    "meta_table_uid": _require_meta_table_uid(migration_meta_table),
                    "alias": "migration_registry",
                    "access": "write",
                }
            ]
        },
    )


def sync_packaged_migration(
    migration_registry: MetaTable | type[MigrationMetaTable],
    packaged_migration: PackagedMetaTableMigration,
    *,
    data_source_uid: str,
    timeout: int | float | tuple[float, float] | None = None,
    extension_fields: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    migration_meta_table = _resolve_migration_registry(
        migration_registry,
        timeout=timeout,
    )
    row = build_migration_registry_row(
        packaged_migration,
        data_source_uid=data_source_uid,
        extension_fields=extension_fields,
    )
    operation = build_registry_upsert_operation(migration_meta_table, row)
    result = MetaTable.execute_operation(operation, timeout=timeout)
    return {
        "meta_table": migration_meta_table,
        "row": row,
        "operation": operation,
        "result": result,
    }


def build_migration_operation(
    migration_meta_table: MetaTable,
    row: MetaTableMigrationRegistryRow | Mapping[str, Any],
    *,
    dry_run: bool = False,
) -> MetaTableMigrationOperation:
    row_payload = _row_payload(row)
    return MetaTableMigrationOperation(
        migration_meta_table_uid=_require_meta_table_uid(migration_meta_table),
        migration_row_uid=str(row_payload["uid"]),
        data_source_uid=str(row_payload["target_data_source_uid"]),
        package=str(row_payload["package"]),
        migration_namespace=str(row_payload["migration_namespace"]),
        revision=str(row_payload["revision"]),
        down_revision=row_payload.get("down_revision"),
        direction=row_payload.get("direction", "upgrade"),
        expected_current_revision=row_payload.get("expected_current_revision"),
        manifest_sha256=str(row_payload["manifest_sha256"]),
        sql_sha256=str(row_payload["sql_sha256"]),
        idempotency_key=str(row_payload["idempotency_key"]),
        lock_key=str(row_payload["lock_key"]),
        dry_run=dry_run,
    )


def apply_migration(
    migration_meta_table: MetaTable,
    row: MetaTableMigrationRegistryRow | Mapping[str, Any],
    *,
    dry_run: bool = False,
    timeout: int | float | tuple[float, float] | None = None,
) -> MetaTableMigrationApplyResponse:
    operation = build_migration_operation(
        migration_meta_table,
        row,
        dry_run=dry_run,
    )
    return MetaTable.apply_migration(operation, timeout=timeout)


def get_migration_status(
    migration_meta_table: MetaTable,
    *,
    package: str,
    migration_namespace: str,
    data_source_uid: str | None = None,
    timeout: int | float | tuple[float, float] | None = None,
) -> MetaTableMigrationStatusResponse:
    request = MetaTableMigrationStatusRequest(
        migration_meta_table_uid=_require_meta_table_uid(migration_meta_table),
        data_source_uid=data_source_uid,
        package=package,
        migration_namespace=migration_namespace,
    )
    return MetaTable.get_migration_status(request, timeout=timeout)


def contracts_from_models(
    models: Mapping[str, Any],
    *,
    target_meta_tables: Mapping[Any, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    return {
        str(identifier): _contract_payload(model, target_meta_tables=target_meta_tables)
        for identifier, model in dict(models).items()
    }


def contract_hashes_from_models(
    models: Mapping[str, Any],
    *,
    target_meta_tables: Mapping[Any, Any] | None = None,
) -> dict[str, str]:
    return {
        identifier: contract_hash(contract)
        for identifier, contract in contracts_from_models(
            models,
            target_meta_tables=target_meta_tables,
        ).items()
    }


def validate_migration_managed_models(models: Mapping[str, Any]) -> None:
    _validate_migration_managed_models(models)


def contract_hash(contract: Mapping[str, Any] | Any) -> str:
    if hasattr(contract, "model_dump"):
        payload = contract.model_dump(mode="json", by_alias=True, exclude_none=True)
    else:
        payload = dict(contract)
    encoded = json.dumps(
        _strip_client_metadata(serialize_to_json(payload)),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def sha256_payload(value: Any) -> str:
    jsonable = serialize_to_json({"value": value})["value"]
    encoded = json.dumps(
        _strip_client_metadata(jsonable),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def migration_row_uid(
    *,
    data_source_uid: str,
    package: str,
    migration_namespace: str,
    revision: str,
    direction: str,
) -> str:
    key = ":".join(
        [
            str(data_source_uid),
            str(package),
            str(migration_namespace),
            str(revision),
            str(direction),
        ]
    )
    return str(uuid.uuid5(_MIGRATION_ROW_NAMESPACE, key))


def migration_lock_key(
    *,
    data_source_uid: str,
    package: str,
    migration_namespace: str,
) -> str:
    return f"{data_source_uid}:{package}:{migration_namespace}"


def migration_artifact_hash(
    *,
    manifest_sha256: str,
    operations_sha256: str,
    sql_sha256: str,
) -> str:
    return sha256_payload(
        {
            "manifest_sha256": manifest_sha256,
            "operations_sha256": operations_sha256,
            "sql_sha256": sql_sha256,
        }
    )


def migration_idempotency_key(
    *,
    row_uid: str,
    artifact_sha256: str | None = None,
    sql_sha256: str | None = None,
) -> str:
    artifact = artifact_sha256 or sql_sha256
    if artifact in (None, ""):
        raise ValueError("artifact_sha256 is required.")
    return f"metatable-migration:{row_uid}:{artifact}"


def _contract_payload(
    model: Any,
    *,
    target_meta_tables: Mapping[Any, Any] | None = None,
) -> dict[str, Any]:
    if isinstance(model, type) and issubclass(model, PlatformTimeIndexMetaData):
        request = model.build_registration_request(
            data_source_uid="00000000-0000-4000-8000-000000000000",
            _target_meta_tables=target_meta_tables,
        )
        return _strip_client_metadata(serialize_to_json(request.table_contract))

    contract = table_contract_from_sqlalchemy_model(
        model,
        target_meta_tables=target_meta_tables,
        include_physical_table_name=False,
        require_metatable_foreign_keys=False,
    )
    return _strip_client_metadata(
        contract.model_dump(mode="json", by_alias=True, exclude_none=True)
    )


def _validate_migration_managed_models(models: Mapping[str, Any]) -> None:
    for identifier, model in dict(models).items():
        if not isinstance(model, type) or not issubclass(model, MigrationManagedMetaTable):
            model_name = getattr(model, "__qualname__", repr(model))
            raise ValueError(
                "In-place MetaTable migration contract models must inherit "
                "MigrationManagedMetaTable so storage identity is stable by identifier. "
                f"{identifier!r} uses {model_name}."
            )


def _read_package_bytes(package: str, path: str) -> bytes:
    return resources.files(package).joinpath(path).read_bytes()


def _parse_manifest(manifest_text: str, *, manifest_path: str) -> dict[str, Any]:
    if manifest_path.endswith((".yaml", ".yml")):
        payload = yaml.safe_load(manifest_text) or {}
    else:
        payload = json.loads(manifest_text)
    if not isinstance(payload, Mapping):
        raise ValueError("MetaTable migration manifest must decode to an object.")
    return dict(payload)


def _strip_client_metadata(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            key: _strip_client_metadata(item) for key, item in value.items() if key != "orm_class"
        }
    if isinstance(value, list):
        return [_strip_client_metadata(item) for item in value]
    if isinstance(value, tuple):
        return [_strip_client_metadata(item) for item in value]
    return value


def _resolve_migration_registry(
    migration_registry: MetaTable | type[MigrationMetaTable],
    *,
    timeout: int | float | tuple[float, float] | None = None,
) -> MetaTable:
    if isinstance(migration_registry, MetaTable):
        return migration_registry
    if isinstance(migration_registry, type) and issubclass(migration_registry, MigrationMetaTable):
        return migration_registry.register(timeout=timeout)
    raise TypeError("migration_registry must be a MetaTable or MigrationMetaTable subclass.")


def _row_payload(row: MetaTableMigrationRegistryRow | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(row, MetaTableMigrationRegistryRow):
        payload = row.model_dump(mode="json", by_alias=True, exclude_none=True)
    else:
        payload = dict(row)
    return _strip_client_metadata(serialize_to_json(payload))


def _temporal_parameter_types(row_payload: Mapping[str, Any]) -> dict[str, str] | None:
    temporal_fields = {
        field: TIMESTAMP_TZ
        for field in ("created_at", "started_at", "finished_at")
        if field in row_payload and row_payload[field] is not None
    }
    return temporal_fields or None


def _registry_physical_table_name(migration_meta_table: MetaTable) -> str:
    table_name = getattr(migration_meta_table, "physical_table_name", None)
    if not table_name:
        raise ValueError("migration_meta_table.physical_table_name is required.")
    return ".".join(_quote_identifier(part) for part in str(table_name).split("."))


def _require_meta_table_uid(meta_table: MetaTable) -> str:
    if getattr(meta_table, "uid", None) in (None, ""):
        raise ValueError("MetaTable uid is required.")
    return str(meta_table.uid)


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


__all__ = [
    "DEFAULT_MIGRATION_REGISTRY_IDENTIFIER",
    "DEFAULT_MIGRATION_REGISTRY_NAMESPACE",
    "METATABLE_MIGRATION_V1",
    "MIGRATION_MANIFEST_V1",
    "MigrationMetaTable",
    "MetaTableMigrationManifest",
    "MetaTableMigrationRegistryRow",
    "PackagedMetaTableMigration",
    "apply_migration",
    "build_migration_operation",
    "build_migration_registry_row",
    "build_registry_upsert_operation",
    "contract_hash",
    "contract_hashes_from_models",
    "contracts_from_models",
    "create_default_migration_registry_model",
    "get_migration_status",
    "load_packaged_migration",
    "migration_idempotency_key",
    "migration_artifact_hash",
    "migration_lock_key",
    "migration_row_uid",
    "sha256_payload",
    "sha256_text",
    "sync_packaged_migration",
    "validate_migration_managed_models",
]
