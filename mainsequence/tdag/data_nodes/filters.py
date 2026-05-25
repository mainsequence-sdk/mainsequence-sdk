"""Time-series specific filter and join request models."""

from __future__ import annotations

from pydantic import Field, model_validator

from ..filters import (
    BaseSearchRequest,
    Condition,
    F,
    FilterExpr,
    Group,
    JoinSpec,
    JoinType,
    Logic,
    Op,
    and_,
    or_,
)


class _JoinKeyValue(str):
    @property
    def value(self) -> str:
        return str(self)


class JoinKey:
    """Compatibility constants for common data-node join keys.

    Data-node joins are no longer limited to this constant. `JoinSpec.on` may
    contain any configured time-first index vector, for example
    ["time_index", "account_uid", "entity_uid"].
    """

    time_index = _JoinKeyValue("time_index")


class SearchRequest(BaseSearchRequest):
    """Data-node search request wrapper."""

    node_unique_identifier: str | None = None
    joins: list[JoinSpec] = Field(default_factory=list)

    @model_validator(mode="after")
    def _xor_target_and_unique_join_names(self):
        a = bool(self.storage_hash and self.storage_hash.strip())
        b = bool(self.node_unique_identifier and self.node_unique_identifier.strip())
        if a == b:
            raise ValueError("Exactly one of storage_hash or node_unique_identifier must be provided.")
        super()._validate_join_names()
        for join in self.joins:
            join_storage_hash = bool(join.storage_hash and join.storage_hash.strip())
            join_node_identifier = bool(
                join.node_unique_identifier and join.node_unique_identifier.strip()
            )
            if join_storage_hash == join_node_identifier:
                raise ValueError(
                    "JoinSpec requires exactly one of storage_hash or node_unique_identifier."
                )
            join_on = [str(key) for key in (join.on or [])]
            if not join_on:
                raise ValueError("JoinSpec.on must include a time-first index vector.")
            if any(not key.strip() for key in join_on):
                raise ValueError("JoinSpec.on cannot contain empty index names.")
            if len(join_on) != len(set(join_on)):
                raise ValueError("JoinSpec.on cannot contain duplicate index names.")
            join.on = join_on
        return self


__all__ = [
    "Condition",
    "F",
    "FilterExpr",
    "Group",
    "JoinKey",
    "JoinSpec",
    "JoinType",
    "Logic",
    "Op",
    "SearchRequest",
    "and_",
    "or_",
]
