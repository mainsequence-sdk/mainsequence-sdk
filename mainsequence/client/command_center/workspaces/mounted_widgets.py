from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from uuid import uuid4


def make_mounted_widget_payload(
    *,
    widget_type_id: str,
    widget_uid: str | None = None,
    title: str | None = None,
    props: Mapping[str, Any] | None = None,
    layout: Mapping[str, Any] | None = None,
    bindings: Mapping[str, Any] | list[Mapping[str, Any]] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a generic mounted-widget payload draft.

    The helper is schema-light on purpose: projects can pass the exact props,
    layout, and binding shape required by the active registered widget type.
    """

    payload: dict[str, Any] = {
        "uid": widget_uid or str(uuid4()),
        "widgetTypeId": widget_type_id,
        "props": dict(props or {}),
    }
    if title is not None:
        payload["title"] = title
    if layout is not None:
        payload["layout"] = dict(layout)
    if bindings is not None:
        payload["bindings"] = bindings if isinstance(bindings, list) else dict(bindings)
    if metadata is not None:
        payload["metadata"] = dict(metadata)
    return payload


__all__ = ["make_mounted_widget_payload"]
