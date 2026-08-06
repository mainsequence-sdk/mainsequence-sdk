from __future__ import annotations

from typing import Annotated, Literal

from pydantic import Field, field_validator

from ..data_models import ContractBaseModel

ResourceCollectionFilterValue = str | int | float | bool


class ResourceCollectionSearchControl(ContractBaseModel):
    placeholder: str = Field(min_length=1)
    fields: list[str] = Field(min_length=1)

    @field_validator("fields")
    @classmethod
    def _validate_fields(cls, value: list[str]) -> list[str]:
        if any(not field.strip() for field in value):
            raise ValueError("Search fields cannot be blank.")
        if len(set(value)) != len(value):
            raise ValueError("Search fields must not contain duplicates.")
        return value


class ResourceCollectionFilterOption(ContractBaseModel):
    value: ResourceCollectionFilterValue
    label: str = Field(min_length=1)


class _ResourceCollectionFilterControl(ContractBaseModel):
    key: str = Field(min_length=1)
    label: str = Field(min_length=1)


class ResourceCollectionTextFilterControl(_ResourceCollectionFilterControl):
    type: Literal["text"] = "text"


class ResourceCollectionBooleanFilterControl(_ResourceCollectionFilterControl):
    type: Literal["boolean"] = "boolean"


class ResourceCollectionSelectFilterControl(_ResourceCollectionFilterControl):
    type: Literal["select"] = "select"
    options: list[ResourceCollectionFilterOption] = Field(min_length=1)


ResourceCollectionFilterControl = Annotated[
    ResourceCollectionTextFilterControl
    | ResourceCollectionBooleanFilterControl
    | ResourceCollectionSelectFilterControl,
    Field(discriminator="type"),
]


class ResourceCollectionControls(ContractBaseModel):
    search: ResourceCollectionSearchControl | None
    filters: list[ResourceCollectionFilterControl]
    ordering: list[str]

    @field_validator("filters")
    @classmethod
    def _validate_filters(
        cls,
        value: list[ResourceCollectionFilterControl],
    ) -> list[ResourceCollectionFilterControl]:
        keys = [item.key for item in value]
        if len(set(keys)) != len(keys):
            raise ValueError("Collection filter keys must not contain duplicates.")
        return value

    @field_validator("ordering")
    @classmethod
    def _validate_ordering(cls, value: list[str]) -> list[str]:
        if any(not field.strip() for field in value):
            raise ValueError("Ordering fields cannot be blank.")
        if len(set(value)) != len(value):
            raise ValueError("Ordering fields must not contain duplicates.")
        return value


__all__ = [
    "ResourceCollectionBooleanFilterControl",
    "ResourceCollectionControls",
    "ResourceCollectionFilterControl",
    "ResourceCollectionFilterOption",
    "ResourceCollectionFilterValue",
    "ResourceCollectionSearchControl",
    "ResourceCollectionSelectFilterControl",
    "ResourceCollectionTextFilterControl",
]
