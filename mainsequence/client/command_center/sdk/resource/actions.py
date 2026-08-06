from __future__ import annotations

from typing import Annotated, Any, Literal
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import Field, SerializerFunctionWrapHandler, field_validator, model_serializer

from ..data_models import ContractBaseModel

ResourceBulkSelectionMode = Literal["explicit", "all_matching"]
ResourceActionTone = Literal["default", "primary", "warning", "danger"]

_NON_SEMANTIC_FILTER_KEYS = {
    "light",
    "limit",
    "offset",
    "ordering",
    "page",
    "page_size",
    "search",
    "sort",
}


def validate_resource_action_path(value: str) -> str:
    """Validate the same-origin relative action paths consumed by Command Center."""

    normalized = value.strip()
    parsed = urlsplit(normalized)
    if (
        not normalized.startswith("/")
        or normalized.startswith("//")
        or not normalized.endswith("/")
        or "\\" in normalized
        or parsed.scheme
        or parsed.netloc
        or parsed.query
        or parsed.fragment
        or parsed.path != normalized
    ):
        raise ValueError("Action endpoints must be safe relative paths ending in '/'.")
    return normalized


class ResourceBulkActionQuery(ContractBaseModel):
    search: str = ""
    filters: dict[str, Any] = Field(default_factory=dict)

    @field_validator("search")
    @classmethod
    def _normalize_search(cls, value: str) -> str:
        return value.strip()

    @field_validator("filters")
    @classmethod
    def _reject_presentation_filters(cls, value: dict[str, Any]) -> dict[str, Any]:
        invalid = sorted(set(value) & _NON_SEMANTIC_FILTER_KEYS)
        if invalid:
            raise ValueError(
                "Bulk-action filters cannot contain pagination or presentation keys: "
                + ", ".join(invalid)
            )
        return value


class ResourceExplicitSelection(ContractBaseModel):
    mode: Literal["explicit"]
    uids: list[UUID] = Field(min_length=1)

    @field_validator("uids")
    @classmethod
    def _deduplicate_uids(cls, value: list[UUID]) -> list[UUID]:
        return list(dict.fromkeys(value))


class ResourceAllMatchingSelection(ContractBaseModel):
    mode: Literal["all_matching"]
    query: ResourceBulkActionQuery


ResourceBulkSelection = Annotated[
    ResourceExplicitSelection | ResourceAllMatchingSelection,
    Field(discriminator="mode"),
]


class ResourceBulkActionConfirmation(ContractBaseModel):
    title: str
    word: str
    button_label: str
    warning: str


class ResourceBulkActionOption(ContractBaseModel):
    key: str = Field(min_length=1)
    type: Literal["boolean"]
    default: bool
    label: str
    description: str


class ResourceBulkActionDefinition(ContractBaseModel):
    id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    endpoint: str
    method: Literal["POST"]
    tone: ResourceActionTone | None = None
    selection_modes: list[ResourceBulkSelectionMode] = Field(min_length=1)
    confirmation: ResourceBulkActionConfirmation | None = None
    options: list[ResourceBulkActionOption]
    preflight_endpoint: str | None = None

    @model_serializer(mode="wrap")
    def _omit_absent_optional_fields(
        self,
        handler: SerializerFunctionWrapHandler,
    ):
        """Match DRF's omission semantics for optional discovery fields."""

        payload = handler(self)
        for field_name in ("tone", "confirmation", "preflight_endpoint"):
            if payload.get(field_name) is None:
                payload.pop(field_name, None)
        return payload

    @field_validator("endpoint", "preflight_endpoint")
    @classmethod
    def _validate_endpoint(cls, value: str | None) -> str | None:
        return None if value is None else validate_resource_action_path(value)

    @field_validator("selection_modes")
    @classmethod
    def _validate_selection_modes(
        cls,
        value: list[ResourceBulkSelectionMode],
    ) -> list[ResourceBulkSelectionMode]:
        if len(set(value)) != len(value):
            raise ValueError("Selection modes must not contain duplicates.")
        return value

    @field_validator("options")
    @classmethod
    def _validate_options(
        cls,
        value: list[ResourceBulkActionOption],
    ) -> list[ResourceBulkActionOption]:
        keys = [option.key for option in value]
        if len(set(keys)) != len(keys):
            raise ValueError("Bulk-action option keys must not contain duplicates.")
        return value


class ResourceBulkActionDiscoveryResponse(ContractBaseModel):
    actions: list[ResourceBulkActionDefinition]

    @field_validator("actions")
    @classmethod
    def _validate_actions(
        cls,
        value: list[ResourceBulkActionDefinition],
    ) -> list[ResourceBulkActionDefinition]:
        action_ids = [action.id for action in value]
        if len(set(action_ids)) != len(value):
            raise ValueError("Bulk-action IDs must not contain duplicates.")
        return value


class ResourceBulkActionRequest[OptionsT](ContractBaseModel):
    selection: ResourceBulkSelection
    options: OptionsT


class ResourceBulkActionEmptyOptions(ContractBaseModel):
    """Strict empty options object for actions without user choices."""


class ResourceBulkActionPreflightResponse(ContractBaseModel):
    allowed: bool
    detail: str | None = None
    blockers: list[str] | None = None
    warnings: list[str] = Field(default_factory=list)
    matched_count: int = Field(ge=0)

    @model_serializer(mode="wrap")
    def _omit_absent_optional_fields(
        self,
        handler: SerializerFunctionWrapHandler,
    ):
        payload = handler(self)
        for field_name in ("detail", "blockers"):
            if payload.get(field_name) is None:
                payload.pop(field_name, None)
        return payload


def validate_resource_bulk_action_filters[FiltersT: ContractBaseModel](
    query: ResourceBulkActionQuery,
    filters_model: type[FiltersT],
) -> FiltersT:
    """Validate all-matching filters through the resource's list-filter model."""

    if not issubclass(filters_model, ContractBaseModel):
        raise TypeError("Bulk-action filter models must inherit ContractBaseModel.")
    return filters_model.model_validate(query.filters)


def require_supported_selection(
    action: ResourceBulkActionDefinition,
    selection: ResourceBulkSelection,
) -> None:
    if selection.mode not in action.selection_modes:
        raise ValueError(f'Bulk action "{action.id}" does not support {selection.mode} selection.')


def resolve_bulk_action_options(
    action: ResourceBulkActionDefinition,
    supplied: dict[str, Any],
) -> dict[str, bool]:
    definitions = {option.key: option for option in action.options}
    unknown = sorted(set(supplied) - set(definitions))
    if unknown:
        raise ValueError(
            f'Bulk action "{action.id}" does not advertise options: {", ".join(unknown)}.'
        )

    resolved: dict[str, bool] = {}
    for key, definition in definitions.items():
        value = supplied.get(key, definition.default)
        if not isinstance(value, bool):
            raise TypeError(f'Bulk-action option "{key}" must be boolean.')
        resolved[key] = value
    return resolved


__all__ = [
    "ResourceActionTone",
    "ResourceAllMatchingSelection",
    "ResourceBulkActionConfirmation",
    "ResourceBulkActionDefinition",
    "ResourceBulkActionDiscoveryResponse",
    "ResourceBulkActionEmptyOptions",
    "ResourceBulkActionOption",
    "ResourceBulkActionPreflightResponse",
    "ResourceBulkActionQuery",
    "ResourceBulkActionRequest",
    "ResourceBulkSelection",
    "ResourceBulkSelectionMode",
    "ResourceExplicitSelection",
    "require_supported_selection",
    "resolve_bulk_action_options",
    "validate_resource_bulk_action_filters",
    "validate_resource_action_path",
]
