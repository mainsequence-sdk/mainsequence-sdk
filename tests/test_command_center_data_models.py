from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from mainsequence.client.command_center.data_models import (
    CORE_TABULAR_FRAME_CONTRACT,
    TabularFrameFieldResponse,
    TabularFrameMetaResponse,
    TabularFrameResponse,
    TabularFrameSourceResponse,
    TabularTimeSeriesMetaResponse,
)


def test_tabular_frame_response_serializes_canonical_shape():
    frame = TabularFrameResponse(
        status="ready",
        columns=["timestamp", "price"],
        rows=[{"timestamp": 1710000000000, "price": 101.4}],
        fields=[
            TabularFrameFieldResponse(key="timestamp", type="datetime", provenance="backend"),
            TabularFrameFieldResponse(key="price", type="number", nullable=False),
        ],
        meta=TabularFrameMetaResponse(
            timeSeries=TabularTimeSeriesMetaResponse(
                shape="long",
                timeField="timestamp",
                sorted=True,
                valueField="price",
            ),
            provider="tutorial",
        ),
        source=TabularFrameSourceResponse(
            kind="api",
            id="prices",
            label="Prices API",
            updatedAtMs=datetime(2024, 3, 10, tzinfo=UTC),
            context={"limit": 100},
        ),
    )

    payload = frame.model_dump(mode="json", exclude_none=True)

    assert CORE_TABULAR_FRAME_CONTRACT == "core.tabular_frame@v1"
    assert payload == {
        "status": "ready",
        "columns": ["timestamp", "price"],
        "rows": [{"timestamp": 1710000000000, "price": 101.4}],
        "fields": [
            {"key": "timestamp", "type": "datetime", "provenance": "backend"},
            {"key": "price", "type": "number", "nullable": False},
        ],
        "meta": {
            "timeSeries": {
                "shape": "long",
                "timeField": "timestamp",
                "timeUnit": "ms",
                "timezone": "UTC",
                "sorted": True,
                "valueField": "price",
            },
            "provider": "tutorial",
        },
        "source": {
            "kind": "api",
            "id": "prices",
            "label": "Prices API",
            "updatedAtMs": 1710028800000,
            "context": {"limit": 100},
        },
    }


def test_tabular_frame_rejects_source_specific_top_level_fields():
    with pytest.raises(ValidationError) as exc_info:
        TabularFrameResponse(
            status="ready",
            columns=["x"],
            rows=[{"x": 1}],
            limit=100,
        )

    assert "Extra inputs are not permitted" in str(exc_info.value)


def test_source_specific_details_belong_in_source_context():
    frame = TabularFrameResponse(
        status="ready",
        columns=["x"],
        rows=[{"x": 1}],
        source=TabularFrameSourceResponse(
            kind="connection-query",
            context={
                "limit": 100,
                "rangeStartMs": 1710000000000,
                "rangeEndMs": 1710086400000,
            },
        ),
    )

    assert frame.source is not None
    assert frame.source.context == {
        "limit": 100,
        "rangeStartMs": 1710000000000,
        "rangeEndMs": 1710086400000,
    }
