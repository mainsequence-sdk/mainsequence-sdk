from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class BaseConfiguration(BaseModel):
    """
    Shared runtime configuration fields for TDAG-backed objects.

    Subclasses can add hashed build fields, while shared runtime-only metadata
    such as publication controls live here.
    """

    model_config = ConfigDict(extra="forbid")

    open_to_public: bool = Field(
        default=False,
        description=(
            "Optional publication flag for exposing the node publicly. This is "
            "runtime-only operational metadata and does not affect hashes."
        ),
        json_schema_extra={"runtime_only": True},
    )
