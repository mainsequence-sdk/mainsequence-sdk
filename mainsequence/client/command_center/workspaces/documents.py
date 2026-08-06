from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def make_workspace_document_payload(
    *,
    title: str,
    widgets: Sequence[Mapping[str, Any]] | None = None,
    layout: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a generic workspace document payload draft."""

    payload: dict[str, Any] = {
        "title": title,
        "widgets": [dict(widget) for widget in (widgets or [])],
    }
    if layout is not None:
        payload["layout"] = dict(layout)
    if metadata is not None:
        payload["metadata"] = dict(metadata)
    return payload


def append_workspace_widget(
    workspace_document: Mapping[str, Any],
    widget: Mapping[str, Any],
) -> dict[str, Any]:
    """Return a copy of a workspace document with one widget appended."""

    document = dict(workspace_document)
    widgets = list(document.get("widgets") or [])
    widgets.append(dict(widget))
    document["widgets"] = widgets
    return document


__all__ = [
    "append_workspace_widget",
    "make_workspace_document_payload",
]
