from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .tabular import CORE_TABULAR_FRAME_CONTRACT, TabularFrameFieldType


class AdapterResponseMappingTimeSeriesHint(BaseModel):
    """Optional metadata for interpreting mapped rows as a time series."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    time_field: str | None = Field(default=None, alias="timeField")
    value_field: str | None = Field(default=None, alias="valueField")
    entity_field: str | None = Field(default=None, alias="entityField")
    x_field: str | None = Field(default=None, alias="xField")
    y_field: str | None = Field(default=None, alias="yField")
    series_kind: str | None = Field(default=None, alias="seriesKind")
    sorted: bool | None = None
    time_unit: str | None = Field(default=None, alias="timeUnit")
    timezone: str | None = None


class AdapterResponseMapping(BaseModel):
    """Metadata describing how provider-native JSON may map to a Command Center contract.

    A response mapping is descriptive metadata. It does not make provider-native
    JSON a runtime core.tabular_frame@v1 payload by itself.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    id: str
    label: str
    contract: str = CORE_TABULAR_FRAME_CONTRACT
    status_code: str = Field(default="200", alias="statusCode")
    content_type: str = Field(default="application/json", alias="contentType")
    rows_path: str | None = Field(default=None, alias="rowsPath")
    field_types: dict[str, TabularFrameFieldType | str] | None = Field(
        default=None,
        alias="fieldTypes",
    )
    time_series: AdapterResponseMappingTimeSeriesHint | dict[str, Any] | None = Field(
        default=None,
        alias="timeSeries",
    )


def make_tabular_response_mapping(
    *,
    id: str,
    label: str,
    rows_path: str | None = None,
    field_types: dict[str, TabularFrameFieldType | str] | None = None,
    status_code: str = "200",
    content_type: str = "application/json",
    time_series: AdapterResponseMappingTimeSeriesHint | dict[str, Any] | None = None,
) -> AdapterResponseMapping:
    """Create response-mapping metadata for core.tabular_frame@v1."""

    return AdapterResponseMapping(
        id=id,
        label=label,
        contract=CORE_TABULAR_FRAME_CONTRACT,
        status_code=status_code,
        content_type=content_type,
        rows_path=rows_path,
        field_types=field_types,
        time_series=time_series,
    )


__all__ = [
    "AdapterResponseMapping",
    "AdapterResponseMappingTimeSeriesHint",
    "make_tabular_response_mapping",
]
