from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ...sdk.contracts.tabular import CORE_TABULAR_FRAME_CONTRACT
from .tabular_transform import (
    TABULAR_TRANSFORM_DATASET_OUTPUT_ID,
    TABULAR_TRANSFORM_LIVE_UPDATES_INPUT_ID,
    TABULAR_TRANSFORM_SEED_INPUT_ID,
    TABULAR_TRANSFORM_UPDATES_OUTPUT_ID,
)


def make_widget_output_ref(
    *,
    widget_uid: str,
    output: str = "dataset",
    contract: str = CORE_TABULAR_FRAME_CONTRACT,
) -> dict[str, Any]:
    """Reference a widget output by stable widget uid and output name."""

    return {
        "widgetUid": widget_uid,
        "output": output,
        "contract": contract,
    }


def make_widget_input_binding(
    *,
    input_name: str,
    source_widget_uid: str,
    source_output: str = "dataset",
    contract: str = CORE_TABULAR_FRAME_CONTRACT,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a generic serializable widget input binding declaration."""

    binding: dict[str, Any] = {
        "input": input_name,
        "source": make_widget_output_ref(
            widget_uid=source_widget_uid,
            output=source_output,
            contract=contract,
        ),
    }
    if extra:
        binding.update(dict(extra))
    return binding


def make_dataset_binding(
    *,
    source_widget_id: str,
    source_output_id: str = "dataset",
) -> dict[str, Any]:
    """Build the current workspace binding shape for a dataset output."""

    return {
        "sourceWidgetId": source_widget_id,
        "sourceOutputId": source_output_id,
    }


def bind_tabular_seed_data(
    *,
    source_widget_id: str,
    source_output_id: str = "dataset",
) -> dict[str, Any]:
    """Bind a tabular dataset source to a consumer input named seedData."""

    return make_dataset_binding(
        source_widget_id=source_widget_id,
        source_output_id=source_output_id,
    )


def bind_tabular_transform_seed(
    *,
    source_widget_uid: str,
    source_output: str = TABULAR_TRANSFORM_DATASET_OUTPUT_ID,
) -> dict[str, Any]:
    """Bind retained tabular data to a transform's canonical seed input."""

    return make_widget_input_binding(
        input_name=TABULAR_TRANSFORM_SEED_INPUT_ID,
        source_widget_uid=source_widget_uid,
        source_output=source_output,
    )


def bind_tabular_transform_live_updates(
    *,
    source_widget_uid: str,
    source_output: str = TABULAR_TRANSFORM_UPDATES_OUTPUT_ID,
) -> dict[str, Any]:
    """Bind incremental tabular data to a transform's canonical live input."""

    return make_widget_input_binding(
        input_name=TABULAR_TRANSFORM_LIVE_UPDATES_INPUT_ID,
        source_widget_uid=source_widget_uid,
        source_output=source_output,
    )


__all__ = [
    "bind_tabular_seed_data",
    "bind_tabular_transform_live_updates",
    "bind_tabular_transform_seed",
    "make_dataset_binding",
    "make_widget_input_binding",
    "make_widget_output_ref",
]
