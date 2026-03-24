"""Time-series specific filter and join request models."""

from __future__ import annotations

from enum import Enum

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


class JoinKey(str, Enum):
    """Allowed data-node join keys."""

    time_index = "time_index"
    unique_identifier = "unique_identifier"


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
        expected_on = {JoinKey.time_index.value, JoinKey.unique_identifier.value}
        for join in self.joins:
            join_storage_hash = bool(join.storage_hash and join.storage_hash.strip())
            join_node_identifier = bool(
                join.node_unique_identifier and join.node_unique_identifier.strip()
            )
            if join_storage_hash == join_node_identifier:
                raise ValueError(
                    "JoinSpec requires exactly one of storage_hash or node_unique_identifier."
                )
            if set(join.on or []) != expected_on:
                raise ValueError("JoinSpec.on must be exactly ['time_index', 'unique_identifier'].")
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
