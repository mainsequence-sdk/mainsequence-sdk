from __future__ import annotations

import datetime

from pydantic import BaseModel, ConfigDict, Field


class BaseConfiguration(BaseModel):
    """
    Shared runtime configuration fields for MetaTable-backed DataNode objects.

    Subclasses can add hashed build fields.
    """

    model_config = ConfigDict(extra="forbid")


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


class DataNodeConfiguration(BaseConfiguration):
    """
    Base class for DataNode build configuration.

    Fields participate in the update hash by default. Mark a field with
    ``json_schema_extra={"hash_excluded": True}`` only when it must not affect
    update identity.
    """

    offset_start: datetime.datetime | None = Field(
        default=None,
        description=(
            "Optional first-run fallback start date. This affects updater bootstrap "
            "behavior and participates in update hashing."
        ),
    )
    records: list[RecordDefinition] | None = Field(
        default=None,
        description="Optional structural output column definitions for the DataNode.",
    )
    node_metadata: DataNodeMetaData | None = Field(
        default=None,
        description=(
            "Optional published table metadata. This field is hash-excluded so "
            "identifier/description/frequency can be managed without changing hashes."
        ),
        json_schema_extra={"hash_excluded": True},
    )
