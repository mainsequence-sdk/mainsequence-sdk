from mainsequence.client.command_center import (
    EditableFormDefinition,
    FormFieldDefinition,
    FormFieldKind,
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
