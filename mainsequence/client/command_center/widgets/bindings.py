from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ..contracts.tabular import CORE_TABULAR_FRAME_CONTRACT


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
    """Build a serializable widget input binding declaration."""

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


def bind_tabular_seed_data(
    *,
    source_widget_uid: str,
    source_output: str = "dataset",
) -> dict[str, Any]:
    """Bind a tabular dataset source to a consumer input named seedData."""

    return make_widget_input_binding(
        input_name="seedData",
        source_widget_uid=source_widget_uid,
        source_output=source_output,
        contract=CORE_TABULAR_FRAME_CONTRACT,
    )


__all__ = [
    "bind_tabular_seed_data",
    "make_widget_input_binding",
    "make_widget_output_ref",
]
