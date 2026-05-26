from __future__ import annotations

import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from mainsequence.client.utils import DataFrequency

from ..configuration_models import BaseConfiguration


class RecordDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    column_name: str = Field(
        ...,
        max_length=63,
        title="Column Name",
        description=(
            "Physical output column name persisted in the DataNode table. "
            "This must match the column name produced by update() or insert_records(), "
            "and it must stay within PostgreSQL's 63-character identifier limit."
        ),
        examples=["close"],
    )
    dtype: str = Field(
        ...,
        title="Pandas DType",
        description=(
            "Expected pandas dtype for the output column. This should match the dtype "
            "produced in the DataFrame that is persisted, for example 'float64', "
            "'Int64', 'string', or 'datetime64[ns, UTC]'."
        ),
        examples=["float64"],
    )
    label: str | None = Field(
        default=None,
        description="Optional human-readable label used for discovery and UI display.",
        examples=["Close"],
        json_schema_extra={"hash_excluded": True},
    )
    description: str | None = Field(
        default=None,
        description="Optional long-form explanation of what the column represents.",
        examples=["Simulated close price."],
        json_schema_extra={"hash_excluded": True},
    )


SourceColumnRef = RecordDefinition | str
TargetColumnRef = Any


def _source_column_name(source_column: SourceColumnRef) -> str:
    if isinstance(source_column, RecordDefinition):
        return source_column.column_name
    if isinstance(source_column, str) and source_column:
        return source_column
    raise TypeError(
        "SourceTableForeignKey.source_columns entries must be RecordDefinition "
        "objects, or serialized column-name strings when rebuilding configuration."
    )


def _target_column_name(target_column: TargetColumnRef) -> str:
    if isinstance(target_column, str) and target_column:
        return target_column

    key = getattr(target_column, "key", None)
    if key not in (None, ""):
        return str(key)

    name = getattr(target_column, "name", None)
    if name not in (None, ""):
        return str(name)

    expression = getattr(target_column, "expression", None)
    expression_name = getattr(expression, "name", None)
    if expression_name not in (None, ""):
        return str(expression_name)

    property_ = getattr(target_column, "property", None)
    columns = list(getattr(property_, "columns", []) or [])
    if columns:
        column_name = getattr(columns[0], "name", None)
        if column_name not in (None, ""):
            return str(column_name)

    raise TypeError(
        "SourceTableForeignKey.target_columns entries must be MetaTable column "
        "references such as Asset.uid, or serialized column-name strings when "
        "rebuilding configuration."
    )


def _target_table_name(target: Any) -> str:
    table = getattr(target, "__table__", target)
    table_name = getattr(table, "name", None)
    if table_name in (None, ""):
        raise TypeError(
            "SourceTableForeignKey.target must be a MetaTable object, a MetaTable "
            "uid string, or a SQLAlchemy/PlatformManagedMetaTable model with a table name."
        )
    return str(table_name)


def _target_meta_table_uid(
    target: Any,
    *,
    data_source_uid: str | None = None,
    timeout: int | float | tuple[float, float] | None = None,
) -> str:
    if isinstance(target, UUID):
        return str(target)
    if isinstance(target, str) and target:
        return target

    uid = getattr(target, "uid", None)
    if isinstance(uid, UUID):
        return str(uid)
    if isinstance(uid, str) and uid:
        return uid

    if data_source_uid is None:
        from mainsequence.client.models_tdag import get_session_data_source

        data_source = get_session_data_source()
        data_source_uid = getattr(data_source, "uid", None)

    if data_source_uid in (None, ""):
        raise ValueError(
            "Could not resolve a data source uid while resolving a "
            "SourceTableForeignKey target. Pass a registered MetaTable object "
            "or run inside a configured Main Sequence project/session."
        )

    from mainsequence.client.models_metatables import MetaTable

    physical_table_name = _target_table_name(target)
    matches = MetaTable.filter(
        timeout=timeout,
        data_source__uid=str(data_source_uid),
        physical_table_name=physical_table_name,
        management_mode="platform_managed",
    )
    if not matches:
        raise ValueError(
            "Could not resolve registered MetaTable target for "
            f"{physical_table_name!r}. Register the MetaTable first or pass a "
            "MetaTable object with a uid."
        )
    if len(matches) > 1:
        raise ValueError(
            "Multiple registered MetaTables matched "
            f"{physical_table_name!r}; pass a MetaTable object with a uid."
        )

    resolved_uid = getattr(matches[0], "uid", None)
    if resolved_uid in (None, ""):
        raise ValueError(f"Resolved MetaTable target {physical_table_name!r} has no uid.")
    return str(resolved_uid)


class SourceTableForeignKey(BaseModel):
    """
    Hashable authoring declaration for a DataNode source-table FK to a MetaTable.

    This is not the backend serializer contract. It accepts user-facing object
    references and resolves them to ``SourceTableForeignKeyContract`` only at the
    source-table initialization boundary.
    """

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    target: Any = Field(
        ...,
        description=(
            "Target MetaTable reference. Prefer a registered MetaTable object or "
            "a PlatformManagedMetaTable/SQLAlchemy model class."
        ),
    )
    source_columns: list[SourceColumnRef] = Field(
        ...,
        min_length=1,
        description="DataNode record references used as FK source columns.",
    )
    target_columns: list[TargetColumnRef] = Field(
        ...,
        min_length=1,
        description="Target MetaTable column references such as Asset.uid.",
    )
    on_delete: str = Field(
        default="restrict",
        description="Foreign-key delete action.",
    )

    def source_column_names(self) -> list[str]:
        return [_source_column_name(column) for column in self.source_columns]

    def target_column_names(self) -> list[str]:
        return [_target_column_name(column) for column in self.target_columns]

    def target_meta_table_uid(
        self,
        *,
        data_source_uid: str | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> str:
        return _target_meta_table_uid(
            self.target,
            data_source_uid=data_source_uid,
            timeout=timeout,
        )

    def hash_payload(
        self,
        *,
        data_source_uid: str | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> dict[str, Any]:
        return {
            "source_columns": self.source_column_names(),
            "target_meta_table_uid": self.target_meta_table_uid(
                data_source_uid=data_source_uid,
                timeout=timeout,
            ),
            "target_columns": self.target_column_names(),
            "on_delete": self.on_delete.lower(),
        }

    def to_contract(
        self,
        *,
        records: list[RecordDefinition] | None,
        data_source_uid: str | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ):
        if not records:
            raise ValueError("DataNode foreign_keys require DataNodeConfiguration.records.")

        record_names = [record.column_name for record in records]
        duplicate_record_names = sorted(
            {column_name for column_name in record_names if record_names.count(column_name) > 1}
        )
        if duplicate_record_names:
            raise ValueError(f"Duplicate DataNode record column names: {duplicate_record_names}")

        source_column_names = self.source_column_names()
        missing_source_columns = [
            column_name for column_name in source_column_names if column_name not in record_names
        ]
        if missing_source_columns:
            raise ValueError(
                "SourceTableForeignKey source columns must be present in "
                f"DataNodeConfiguration.records. Missing: {missing_source_columns}"
            )

        from mainsequence.client.models_tdag import SourceTableForeignKeyContract

        return SourceTableForeignKeyContract(
            source_columns=source_column_names,
            target_meta_table_uid=self.target_meta_table_uid(
                data_source_uid=data_source_uid,
                timeout=timeout,
            ),
            target_columns=self.target_column_names(),
            on_delete=self.on_delete.lower(),
        )


class DataNodeMetaData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    identifier: str | None = Field(
        default=None,
        title="Portable Identifier",
        description=(
            "Optional published identifier used for discovery and migration. "
            "This metadata is hash-excluded so the identifier can be repointed "
            "to a different backing table without rotating hashes."
        ),
        examples=["daily_prices"],
        json_schema_extra={"hash_excluded": True},
    )
    description: str | None = Field(
        default=None,
        title="Description",
        description="Optional human-readable dataset description.",
        examples=["Global daily prices dataset."],
        json_schema_extra={"hash_excluded": True},
    )
    data_frequency_id: DataFrequency | None = Field(
        default=None,
        title="Data Frequency",
        description=(
            "Optional published frequency metadata. This remains hash-excluded "
            "to preserve the current get_table_metadata() behavior where table "
            "metadata does not participate in hashing."
        ),
        examples=["one_d"],
        json_schema_extra={"hash_excluded": True},
    )


class DataNodeConfiguration(BaseConfiguration):
    """
    Base class for DataNode build configuration.

    Fields participate in the update hash by default. Mark a field with
    ``json_schema_extra={"update_only": True}`` to exclude it from the
    storage hash, or ``json_schema_extra={"hash_excluded": True}`` to
    exclude descriptive metadata from both hashes.
    """
    offset_start: datetime.datetime | None = Field(
        default=None,
        description=(
            "Optional first-run fallback start date. This affects updater bootstrap "
            "behavior but should not rotate the storage hash."
        ),
        json_schema_extra={"update_only": True},
    )
    records: list[RecordDefinition] | None = Field(
        default=None,
        description="Optional structural output column definitions for the DataNode.",
    )
    foreign_keys: list[SourceTableForeignKey] | None = Field(
        default=None,
        description=(
            "Optional source-table foreign keys from this DataNode to registered "
            "MetaTables. Foreign keys participate in storage hashing."
        ),
    )
    node_metadata: DataNodeMetaData | None = Field(
        default=None,
        description=(
            "Optional published table metadata. This field is hash-excluded so "
            "identifier/description/frequency can be managed without changing hashes."
        ),
        json_schema_extra={"hash_excluded": True},
    )
