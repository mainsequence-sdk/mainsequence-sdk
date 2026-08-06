from __future__ import annotations

from typing import Any, Literal

from pydantic import Field

from ..data_models import ContractBaseModel


class EntitySummaryEntity(ContractBaseModel):
    id: int | str
    uid: str | None = None
    type: str
    title: str


class EntitySummaryBadge(ContractBaseModel):
    key: str
    label: str
    tone: str


class EntitySummaryField(ContractBaseModel):
    key: str
    label: str
    value: Any
    kind: str
    meta: str | None = None
    icon: str | None = None
    tone: str | None = None
    info: str | None = None
    href: str | None = None
    iframe: bool | None = None
    edit: dict[str, Any] | None = None


class EntitySummaryStat(ContractBaseModel):
    key: str
    label: str
    display: str
    value: Any
    kind: str
    info: str | None = None
    edit: dict[str, Any] | None = None


class EntitySummaryHelperLink(ContractBaseModel):
    uid: str
    label: str
    is_current: bool = False


class EntitySummaryHelper(ContractBaseModel):
    key: str
    label: str
    kind: Literal["links"] = "links"
    links: list[EntitySummaryHelperLink] = Field(default_factory=list)


class EntitySummaryLabelManagement(ContractBaseModel):
    labels: list[str] = Field(default_factory=list)
    add_label_url: str | None = None
    remove_label_url: str | None = None


class EntitySummary(ContractBaseModel):
    entity: EntitySummaryEntity
    badges: list[EntitySummaryBadge] = Field(default_factory=list)
    inline_fields: list[EntitySummaryField] = Field(default_factory=list)
    highlight_fields: list[EntitySummaryField] = Field(default_factory=list)
    stats: list[EntitySummaryStat] = Field(default_factory=list)
    helpers: list[EntitySummaryHelper] | None = None
    label_management: EntitySummaryLabelManagement | None = None
    extensions: dict[str, Any] = Field(default_factory=dict)
    summary_warning: str | None = None


__all__ = [
    "EntitySummary",
    "EntitySummaryBadge",
    "EntitySummaryEntity",
    "EntitySummaryField",
    "EntitySummaryHelper",
    "EntitySummaryHelperLink",
    "EntitySummaryLabelManagement",
    "EntitySummaryStat",
]
