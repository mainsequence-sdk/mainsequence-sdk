from __future__ import annotations

from typing import ClassVar

from ..base import BaseObjectOrm


class CommandCenterBaseObjectOrm(BaseObjectOrm):
    """Base client for canonical Command Center API resources."""

    COMMAND_CENTER_PREFIX: ClassVar[str] = "api/v1/command_center"
    ENDPOINT: ClassVar[str]

    @classmethod
    def _command_center_root(cls) -> str:
        root = str(getattr(cls, "ROOT_URL", BaseObjectOrm.ROOT_URL)).rstrip("/")
        if root.endswith("/orm/api"):
            root = root[: -len("/orm/api")]
        return root

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        endpoint = custom_endpoint_name or getattr(cls, "ENDPOINT", None)
        if not endpoint:
            raise ValueError(f"{cls.__name__} must define ENDPOINT.")

        return (
            f"{cls._command_center_root().rstrip('/')}/"
            f"{cls.COMMAND_CENTER_PREFIX.strip('/')}/"
            f"{endpoint.strip('/')}"
        )


__all__ = ["CommandCenterBaseObjectOrm"]
