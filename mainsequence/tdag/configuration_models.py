from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class BaseConfiguration(BaseModel):
    """
    Shared runtime configuration fields for TDAG-backed objects.

    Subclasses can add hashed build fields.
    """

    model_config = ConfigDict(extra="forbid")
