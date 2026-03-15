from __future__ import annotations

from importlib import import_module

__all__ = [
    "render_logged_user_details",
    "render_logged_user_username",
    "sidebar_asset_single_select",
    "sidebar_date_settings",
    "sidebar_logged_user_details",
    "sidebar_logged_user_username",
]

_ATTR_TO_MODULE = {
    "render_logged_user_details": ".logged_user",
    "render_logged_user_username": ".logged_user",
    "sidebar_logged_user_details": ".logged_user",
    "sidebar_logged_user_username": ".logged_user",
    "sidebar_asset_single_select": ".asset_select",
    "sidebar_date_settings": ".date_settings",
}


def __getattr__(name: str):
    module_name = _ATTR_TO_MODULE.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = import_module(module_name, __name__)
    return getattr(module, name)
