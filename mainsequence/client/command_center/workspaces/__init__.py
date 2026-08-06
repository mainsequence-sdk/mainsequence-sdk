"""Compatibility wrapper for :mod:`mainsequence.command_center.workspaces`."""

from __future__ import annotations

from typing import Any

from mainsequence.command_center import workspaces as _canonical

__all__ = _canonical.__all__


def __getattr__(name: str) -> Any:
    return getattr(_canonical, name)
