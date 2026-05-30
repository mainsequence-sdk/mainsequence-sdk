from __future__ import annotations

import datetime

from pydantic import BaseModel, ConfigDict, Field


class BaseConfiguration(BaseModel):
    """
    Shared runtime configuration fields for MetaTable-backed DataNode objects.

    Subclasses can add hashed build fields.
    """

    model_config = ConfigDict(extra="forbid")


class DataNodeConfiguration(BaseConfiguration):
    """
    Base class for DataNode build configuration.

    Fields participate in the update hash by default. Mark a field with
    ``json_schema_extra={"hash_excluded": True}`` only when it must not affect
    update identity.
    """

    offset_start: datetime.datetime | None = Field(
        default=None,
        description=(
            "Optional first-run fallback start date. This affects updater bootstrap "
            "behavior and participates in update hashing."
        ),
    )
