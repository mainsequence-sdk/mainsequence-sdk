from __future__ import annotations

import datetime

from pydantic import BaseModel, ConfigDict, Field

import mainsequence.client as ms_client

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
        json_schema_extra={"runtime_only": True},
    )
    description: str | None = Field(
        default=None,
        description="Optional long-form explanation of what the column represents.",
        examples=["Simulated close price."],
        json_schema_extra={"runtime_only": True},
    )


class DataNodeMetaData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    identifier: str | None = Field(
        default=None,
        title="Portable Identifier",
        description=(
            "Optional published identifier used for discovery and migration. "
            "This is runtime-only metadata so the identifier can be repointed "
            "to a different backing table without rotating hashes."
        ),
        examples=["daily_prices"],
        json_schema_extra={"runtime_only": True},
    )
    description: str | None = Field(
        default=None,
        title="Description",
        description="Optional human-readable dataset description.",
        examples=["Global daily prices dataset."],
        json_schema_extra={"runtime_only": True},
    )
    data_frequency_id: ms_client.DataFrequency | None = Field(
        default=None,
        title="Data Frequency",
        description=(
            "Optional published frequency metadata. This remains runtime-only "
            "to preserve the current get_table_metadata() behavior where table "
            "metadata does not participate in hashing."
        ),
        examples=["one_d"],
        json_schema_extra={"runtime_only": True},
    )


class DataNodeConfiguration(BaseConfiguration):
    """
    Base class for DataNode build configuration.

    Fields participate in the update hash by default. Mark a field with
    ``json_schema_extra={"update_only": True}`` to exclude it from the
    storage hash, or ``json_schema_extra={"runtime_only": True}`` to
    exclude it from both hashes.
    """
    offset_start: datetime.datetime | None = Field(
        default=None,
        description=(
            "Optional first-run fallback start date. This affects updater bootstrap "
            "behavior but should not rotate the storage hash."
        ),
        json_schema_extra={"update_only": True},
    )
    asset_list: list[ms_client.AssetMixin] | None = Field(
        default=None,
        description="Optional asset scope selector for updater partitioning.",
        json_schema_extra={"update_only": True},
    )
    records: list[RecordDefinition] | None = Field(
        default=None,
        description="Optional structural output column definitions for the DataNode.",
    )
    node_metadata: DataNodeMetaData | None = Field(
        default=None,
        description=(
            "Optional published table metadata. This field is runtime-only so "
            "identifier/description/frequency can be managed without changing hashes."
        ),
        json_schema_extra={"runtime_only": True},
    )
