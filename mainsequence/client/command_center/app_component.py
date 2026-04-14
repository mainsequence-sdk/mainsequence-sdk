from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class FormFieldKind(str, Enum):
    STRING = "string"
    NUMBER = "number"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "date-time"
    PERCENT = "percent"
    ENUM = "enum"
    JSON = "json"


class FormChoice(BaseModel):
    value: Any
    label: str


class FormFieldDefinition(BaseModel):
    token: str = Field(
        ...,
        description="Stable global field token used for draft state and downstream bindings.",
        examples=["custom::funding_rate", "fenics_product::LoTrigger"],
    )
    name: str = Field(..., description="Backend field identifier.")
    label: str = Field(..., description="User-facing field label.")
    kind: FormFieldKind
    editable: bool = Field(
        ...,
        description="Whether the user can edit this field in the rendered form.",
    )
    required: bool = False
    value: Any = Field(
        default=None,
        description="Current initial value for this fetched form session.",
    )
    default_value: Any = Field(
        default=None,
        description="Reset/default baseline value.",
    )
    description: str = ""
    formatter: str | None = Field(
        default=None,
        description="Optional frontend formatting hint such as percent, currency, bp.",
    )
    choices: list[FormChoice] | None = Field(
        default=None,
        description="Optional allowed choices. Use for enums, dropdowns, radio groups.",
    )
    extra: dict[str, Any] | None = Field(
        default=None,
        description="Optional backend-specific metadata for future extensions.",
    )


class FormSectionDefinition(BaseModel):
    id: str = Field(..., description="Stable section id.")
    title: str
    description: str | None = None
    fields: list[FormFieldDefinition] = Field(default_factory=list)
    extra: dict[str, Any] | None = None


class EditableFormMeta(BaseModel):
    model_config = ConfigDict(extra="allow")

    record_status_options: list[str] | None = None
    grid_axes: list[dict[str, Any]] | None = None


class EditableFormDefinition(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "x-ui-role": "editable-form",
            "x-ui-widget": "definition-v1",
        }
    )

    form_id: str | None = Field(
        default=None,
        description="Stable identity for reset/reload semantics.",
    )
    title: str = Field(..., description="Read-only form header title.")
    description: str | None = Field(
        default=None,
        description="Read-only form header description.",
    )
    sections: list[FormSectionDefinition] = Field(default_factory=list)
    meta: EditableFormMeta | None = None


class NotificationTone(str, Enum):
    SUCCESS = "success"
    PRIMARY = "primary"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class NotificationDefinition(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "x-ui-role": "notification",
            "x-ui-widget": "banner-v1",
            "examples": [
                {
                    "title": "Action completed",
                    "message": "The operation finished successfully.",
                    "tone": "success",
                    "details": "Optional follow-up detail for the user.",
                }
            ],
        }
    )

    title: str | None = Field(
        default=None,
        description="Optional short banner heading.",
        examples=["Action completed"],
    )
    message: str = Field(
        ...,
        description="Primary user-facing notification message.",
        examples=["The operation finished successfully."],
    )
    tone: NotificationTone = Field(
        ...,
        description="Visual tone for the banner.",
        examples=["success"],
    )
    details: str | None = Field(
        default=None,
        description="Optional secondary explanatory text.",
        examples=["Optional follow-up detail for the user."],
    )


__all__ = [
    "EditableFormDefinition",
    "EditableFormMeta",
    "FormChoice",
    "FormFieldDefinition",
    "FormFieldKind",
    "FormSectionDefinition",
    "NotificationDefinition",
    "NotificationTone",
]
