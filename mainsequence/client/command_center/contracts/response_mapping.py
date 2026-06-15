from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

from .tabular import CORE_TABULAR_FRAME_CONTRACT, TabularFrameFieldType


class AdapterResponseMappingTimeSeriesHint(BaseModel):
    """Optional metadata for interpreting mapped rows as a time series."""

    model_config = ConfigDict(extra="forbid")

    timeField: str | None = None
    valueField: str | None = None
    entityField: str | None = None
    xField: str | None = None
    yField: str | None = None
    seriesKind: str | None = None
    sorted: bool | None = None
    timeUnit: str | None = None
    timezone: str | None = None


class AdapterResponseMapping(BaseModel):
    """Metadata describing how provider-native JSON may map to a Command Center contract.

    A response mapping is descriptive metadata. It does not make provider-native
    JSON a runtime core.tabular_frame@v1 payload by itself.
    """

    model_config = ConfigDict(extra="forbid")

    id: str
    label: str | None = None
    contract: str = CORE_TABULAR_FRAME_CONTRACT
    statusCode: str | int = "200"
    contentType: str = "application/json"
    rowsPath: str | None = None
    fieldTypes: dict[str, TabularFrameFieldType] | None = None
    timeSeries: AdapterResponseMappingTimeSeriesHint | None = None
    description: str | None = None
    examples: list[dict[str, Any]] | None = None


def make_tabular_response_mapping(
    *,
    id: str,
    label: str | None = None,
    rows_path: str | None = None,
    field_types: dict[str, TabularFrameFieldType] | None = None,
    status_code: str | int = "200",
    content_type: str = "application/json",
    time_series: AdapterResponseMappingTimeSeriesHint | dict[str, Any] | None = None,
) -> AdapterResponseMapping:
    """Create response-mapping metadata for core.tabular_frame@v1."""

    normalized_time_series = None
    if time_series is not None:
        normalized_time_series = (
            time_series
            if isinstance(time_series, AdapterResponseMappingTimeSeriesHint)
            else AdapterResponseMappingTimeSeriesHint(**time_series)
        )

    return AdapterResponseMapping(
        id=id,
        label=label,
        contract=CORE_TABULAR_FRAME_CONTRACT,
        statusCode=status_code,
        contentType=content_type,
        rowsPath=rows_path,
        fieldTypes=field_types,
        timeSeries=normalized_time_series,
    )


__all__ = [
    "AdapterResponseMapping",
    "AdapterResponseMappingTimeSeriesHint",
    "make_tabular_response_mapping",
]
