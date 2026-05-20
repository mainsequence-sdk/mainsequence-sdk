from __future__ import annotations

from importlib import import_module

__all__ = [
    "RenderResult",
    "StreamlitModelFormFactory",
    "schedule_editor",
]

_ATTR_TO_MODULE = {
    "RenderResult": ".streamlit_form_factory",
    "StreamlitModelFormFactory": ".streamlit_form_factory",
    "schedule_editor": ".streamlit_form_factory",
}


def __getattr__(name: str):
    module_name = _ATTR_TO_MODULE.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = import_module(module_name, __name__)
    return getattr(module, name)
