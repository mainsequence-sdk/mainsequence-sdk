from mainsequence.client.command_center import (
    EditableFormDefinition,
    FormFieldDefinition,
    FormFieldKind,
    NotificationDefinition,
    NotificationTone,
)


def test_form_field_definition_accepts_choices_and_kind():
    field = FormFieldDefinition.model_validate(
        {
            "token": "custom::funding_rate",
            "name": "funding_rate",
            "label": "Funding Rate",
            "kind": "percent",
            "editable": True,
            "required": True,
            "value": 0.0125,
            "default_value": 0.0,
            "formatter": "percent",
            "choices": [
                {"value": "daily", "label": "Daily"},
                {"value": "annualized", "label": "Annualized"},
            ],
        }
    )

    assert field.kind == FormFieldKind.PERCENT
    assert field.choices is not None
    assert field.choices[0].value == "daily"


def test_editable_form_definition_builds_sections():
    form = EditableFormDefinition.model_validate(
        {
            "form_id": "rates-desk",
            "title": "Rates Desk Form",
            "description": "Editable pricing inputs.",
            "sections": [
                {
                    "id": "pricing",
                    "title": "Pricing",
                    "fields": [
                        {
                            "token": "custom::funding_rate",
                            "name": "funding_rate",
                            "label": "Funding Rate",
                            "kind": "percent",
                            "editable": True,
                        }
                    ],
                }
            ],
            "meta": {
                "record_status_options": ["draft", "published"],
                "grid_axes": [{"axis": "tenor"}],
                "extra_backend_flag": True,
            },
        }
    )

    assert form.form_id == "rates-desk"
    assert len(form.sections) == 1
    assert form.sections[0].fields[0].token == "custom::funding_rate"
    assert form.meta is not None
    assert form.meta.record_status_options == ["draft", "published"]
    assert form.meta.grid_axes == [{"axis": "tenor"}]


def test_notification_definition_validates_payload():
    notification = NotificationDefinition.model_validate(
        {
            "title": "Action completed",
            "message": "The operation finished successfully.",
            "tone": "success",
            "details": "Optional follow-up detail for the user.",
        }
    )

    assert notification.title == "Action completed"
    assert notification.message == "The operation finished successfully."
    assert notification.tone == NotificationTone.SUCCESS
    assert notification.details == "Optional follow-up detail for the user."


def test_notification_definition_schema_emits_ui_metadata():
    schema = NotificationDefinition.model_json_schema()

    assert schema["x-ui-role"] == "notification"
    assert schema["x-ui-widget"] == "banner-v1"
    assert schema["properties"]["tone"]["$ref"] == "#/$defs/NotificationTone"
