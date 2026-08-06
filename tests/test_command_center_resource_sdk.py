from __future__ import annotations

import importlib
import json
import subprocess
import sys
from typing import Literal
from urllib.parse import parse_qs, urlsplit
from uuid import UUID

import pytest
from pydantic import ValidationError

from mainsequence.client.command_center.sdk.data_models import ContractBaseModel
from mainsequence.client.command_center.sdk.resource import (
    CanonicalResourceCollection,
    EntitySummary,
    ResourceBulkActionDefinition,
    ResourceBulkActionDiscoveryResponse,
    ResourceBulkActionEmptyOptions,
    ResourceBulkActionPreflightResponse,
    ResourceBulkActionQuery,
    ResourceBulkActionRequest,
    ResourceCollectionControls,
    ResourceExplicitSelection,
    ResourceLimitOffset,
    build_canonical_resource_collection,
    validate_resource_bulk_action_filters,
)

FIRST_UID = "11111111-1111-4111-8111-111111111111"
SECOND_UID = "22222222-2222-4222-8222-222222222222"


class ExampleRow(ContractBaseModel):
    uid: str
    name: str


class DeleteOptions(ContractBaseModel):
    delete_from_provider: bool = False


class ProjectFilters(ContractBaseModel):
    project_uid: UUID | None = None
    kind: Literal["service", "job"] | None = None


class StructuredPreflight(ContractBaseModel):
    allowed: bool
    matched_count: int
    data_sources: list[dict[str, object]]


def _controls() -> ResourceCollectionControls:
    return ResourceCollectionControls.model_validate(
        {
            "search": {
                "placeholder": "Search by name or UID",
                "fields": ["name", "uid"],
            },
            "filters": [
                {
                    "key": "kind",
                    "label": "Kind",
                    "type": "select",
                    "options": [{"value": "service", "label": "Service"}],
                }
            ],
            "ordering": ["name"],
        }
    )


def _action() -> ResourceBulkActionDefinition:
    return ResourceBulkActionDefinition.model_validate(
        {
            "id": "delete",
            "label": "Delete resources",
            "endpoint": "/api/resources/bulk-delete/",
            "method": "POST",
            "tone": "danger",
            "selection_modes": ["explicit", "all_matching"],
            "confirmation": {
                "title": "Delete resources",
                "word": "DELETE RESOURCES",
                "button_label": "Delete resources",
                "warning": "This operation cannot be undone.",
            },
            "options": [
                {
                    "key": "delete_from_provider",
                    "type": "boolean",
                    "default": False,
                    "label": "Delete from provider",
                    "description": "Also remove the provider resource.",
                }
            ],
            "preflight_endpoint": "/api/resources/bulk-delete/preflight/",
        }
    )


def _minimal_action() -> ResourceBulkActionDefinition:
    return ResourceBulkActionDefinition(
        id="delete",
        label="Delete resources",
        endpoint="/api/resources/bulk-delete/",
        method="POST",
        selection_modes=["explicit"],
        options=[],
    )


def test_canonical_collection_builds_authoritative_limit_offset_links():
    page = build_canonical_resource_collection(
        request_url=(
            "https://api.example.test/api/resources/?search=risk&kind=service&limit=25&offset=25"
        ),
        results=[ExampleRow(uid=FIRST_UID, name="Risk")],
        count=51,
        pagination=ResourceLimitOffset(limit=25, offset=25),
        controls=_controls(),
        actions=[_action()],
    )

    assert isinstance(page, CanonicalResourceCollection)
    assert page.count == 51
    assert page.results[0].uid == FIRST_UID
    assert page.next is not None
    assert parse_qs(urlsplit(page.next).query) == {
        "search": ["risk"],
        "kind": ["service"],
        "limit": ["25"],
        "offset": ["50"],
    }
    assert page.previous is not None
    assert parse_qs(urlsplit(page.previous).query) == {
        "search": ["risk"],
        "kind": ["service"],
        "limit": ["25"],
    }


def test_collection_controls_reject_duplicate_and_unrenderable_definitions():
    with pytest.raises(ValidationError):
        ResourceCollectionControls.model_validate(
            {
                "search": {"placeholder": "Search", "fields": ["name", "name"]},
                "filters": [],
                "ordering": [],
            }
        )

    with pytest.raises(ValidationError):
        ResourceCollectionControls.model_validate(
            {
                "search": None,
                "filters": [{"key": "kind", "label": "Kind", "type": "select"}],
                "ordering": [],
            }
        )


def test_action_contract_rejects_unsafe_paths_and_duplicate_metadata():
    payload = _action().model_dump(mode="json", exclude_none=True)
    payload["endpoint"] = "https://attacker.example/delete/"

    with pytest.raises(ValidationError):
        ResourceBulkActionDefinition.model_validate(payload)

    payload = _action().model_dump(mode="json", exclude_none=True)
    payload["selection_modes"] = ["explicit", "explicit"]

    with pytest.raises(ValidationError):
        ResourceBulkActionDefinition.model_validate(payload)


def test_action_serialization_omits_absent_optional_fields_inside_collections():
    payload = build_canonical_resource_collection(
        request_url="https://api.example.test/api/resources/?limit=25",
        results=[],
        count=0,
        pagination=ResourceLimitOffset(limit=25),
        controls=_controls(),
        actions=[_minimal_action()],
    ).model_dump(mode="json")

    assert payload["next"] is None
    assert payload["previous"] is None
    assert payload["actions"] == [
        {
            "id": "delete",
            "label": "Delete resources",
            "endpoint": "/api/resources/bulk-delete/",
            "method": "POST",
            "selection_modes": ["explicit"],
            "options": [],
        }
    ]

def test_bulk_action_request_is_strict_and_normalizes_explicit_uids():
    request = ResourceBulkActionRequest[DeleteOptions].model_validate(
        {
            "selection": {
                "mode": "explicit",
                "uids": [FIRST_UID, FIRST_UID, SECOND_UID],
            },
            "options": {"delete_from_provider": True},
        }
    )

    assert request.selection == ResourceExplicitSelection(
        mode="explicit",
        uids=[UUID(FIRST_UID), UUID(SECOND_UID)],
    )
    assert request.options.delete_from_provider is True

    with pytest.raises(ValidationError):
        ResourceBulkActionRequest[DeleteOptions].model_validate(
            {
                "selected_uids": [FIRST_UID],
                "selection": {"mode": "explicit", "uids": [FIRST_UID]},
                "options": {},
            }
        )

    with pytest.raises(ValidationError):
        ResourceBulkActionRequest[DeleteOptions].model_validate(
            {
                "selection": {"uids": [FIRST_UID]},
                "options": {},
            }
        )


def test_all_matching_query_rejects_pagination_and_presentation_state():
    with pytest.raises(ValidationError):
        ResourceBulkActionQuery(
            search="risk",
            filters={"project_uid": FIRST_UID, "offset": 25},
        )


def test_all_matching_filters_reuse_a_strict_resource_list_filter_model():
    query = ResourceBulkActionQuery(
        search="  risk  ",
        filters={"project_uid": FIRST_UID, "kind": "service"},
    )

    filters = validate_resource_bulk_action_filters(query, ProjectFilters)

    assert query.search == "risk"
    assert filters == ProjectFilters(
        project_uid=UUID(FIRST_UID),
        kind="service",
    )

    with pytest.raises(ValidationError):
        validate_resource_bulk_action_filters(
            ResourceBulkActionQuery(filters={"unsupported": True}),
            ProjectFilters,
        )


def test_empty_bulk_action_options_reject_undeclared_action_flags():
    request_model = ResourceBulkActionRequest[ResourceBulkActionEmptyOptions]

    request = request_model.model_validate(
        {
            "selection": {"mode": "explicit", "uids": [FIRST_UID]},
            "options": {},
        }
    )
    assert request.options == ResourceBulkActionEmptyOptions()

    with pytest.raises(ValidationError):
        request_model.model_validate(
            {
                "selection": {"mode": "explicit", "uids": [FIRST_UID]},
                "options": {"legacy_confirm": True},
            }
        )


def test_preflight_contract_uses_the_canonical_advisory_response():
    preflight = ResourceBulkActionPreflightResponse(
        allowed=False,
        detail="Deletion is blocked.",
        blockers=["One protected resource blocks deletion."],
        warnings=["One dependent resource will also be deleted."],
        matched_count=2,
    )

    assert preflight.model_dump() == {
        "allowed": False,
        "detail": "Deletion is blocked.",
        "blockers": ["One protected resource blocks deletion."],
        "warnings": ["One dependent resource will also be deleted."],
        "matched_count": 2,
    }

    allowed = ResourceBulkActionPreflightResponse(
        allowed=True,
        matched_count=1,
    ).model_dump(mode="json")
    assert "detail" not in allowed
    assert "blockers" not in allowed


def test_summary_matches_the_canonical_drf_wire_shape():
    summary = EntitySummary.model_validate(
        {
            "entity": {
                "id": FIRST_UID,
                "uid": FIRST_UID,
                "type": "Project",
                "title": "Risk Project",
            },
            "badges": [{"key": "status", "label": "Ready", "tone": "success"}],
            "inline_fields": [],
            "highlight_fields": [],
            "stats": [],
            "label_management": {
                "labels": ["risk"],
                "add_label_url": f"/api/projects/{FIRST_UID}/add-label/",
                "remove_label_url": f"/api/projects/{FIRST_UID}/remove-label/",
            },
        }
    )

    payload = summary.model_dump(mode="json", exclude_none=True)
    assert payload["entity"]["uid"] == FIRST_UID
    assert "labels" not in payload
    assert "labelable" not in payload

    with pytest.raises(ValidationError):
        EntitySummary.model_validate({**payload, "labels": ["legacy"]})


def test_workspace_and_sdk_contracts_have_exclusive_canonical_module_ownership():
    from mainsequence.client import command_center
    from mainsequence.client.command_center.workspaces.models import Workspace

    assert ContractBaseModel.__module__ == "mainsequence.client.command_center.sdk.data_models"
    assert Workspace.__module__ == "mainsequence.client.command_center.workspaces.models"

    for removed_module in (
        "mainsequence.client.command_center.app_component",
        "mainsequence.client.command_center.contracts",
        "mainsequence.client.command_center.data_models",
        "mainsequence.client.command_center.providers",
        "mainsequence.client.command_center.widgets",
        "mainsequence.client.command_center.workspace",
        "mainsequence.client.command_center.workspace_snapshot",
    ):
        removed_name = removed_module.rsplit(".", 1)[-1]
        assert not hasattr(command_center, removed_name)
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module(removed_module)


def test_top_level_command_center_namespace_is_removed():
    for removed_module in (
        "mainsequence.command_center",
        "mainsequence.command_center.sdk.data_models",
        "mainsequence.command_center.workspaces.models",
    ):
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module(removed_module)


def test_resource_contract_import_does_not_eagerly_load_command_center_clients():
    check = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys; "
                "import mainsequence.client.command_center.sdk.resource; "
                "forbidden = {"
                "'mainsequence.client.command_center.connections', "
                "'mainsequence.client.command_center.workspaces', "
                "'mainsequence.client.command_center.workspaces.snapshot', "
                "'fastapi'"
                "}; "
                "loaded = sorted(forbidden.intersection(sys.modules)); "
                "assert not loaded, loaded"
            ),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert check.returncode == 0, check.stderr


def test_fastapi_helpers_publish_and_validate_the_canonical_collection_contract():
    fastapi = pytest.importorskip("fastapi")

    from mainsequence.client.command_center.sdk.fastapi import (
        apply_resource_discovery_headers,
        build_fastapi_bulk_action_discovery,
        build_fastapi_bulk_action_preflight_response,
        build_fastapi_resource_collection,
        resource_limit_offset,
    )

    app = fastapi.FastAPI()

    def list_resources(
        request,
        response,
        pagination: ResourceLimitOffset = fastapi.Depends(  # noqa: B008
            resource_limit_offset
        ),
    ):
        return build_fastapi_resource_collection(
            request=request,
            response=response,
            results=[ExampleRow(uid=FIRST_UID, name="Risk")],
            count=2,
            pagination=pagination,
            controls=_controls(),
            actions=[_minimal_action()],
        )

    list_resources.__annotations__["request"] = fastapi.Request
    list_resources.__annotations__["response"] = fastapi.Response
    app.get(
        "/resources/",
        response_model=CanonicalResourceCollection[ExampleRow],
    )(list_resources)

    def discover_resources(response):
        return build_fastapi_bulk_action_discovery(
            response=response,
            actions=[_minimal_action()],
        )

    discover_resources.__annotations__["response"] = fastapi.Response
    app.get(
        "/resources/bulk-actions/",
        response_model=ResourceBulkActionDiscoveryResponse,
    )(discover_resources)

    request_model = ResourceBulkActionRequest[ResourceBulkActionEmptyOptions]

    def preflight_resources(payload):
        return build_fastapi_bulk_action_preflight_response(
            ResourceBulkActionPreflightResponse(
                allowed=False,
                detail="Deletion is blocked.",
                blockers=["One protected resource blocks deletion."],
                matched_count=1,
            )
        )

    preflight_resources.__annotations__["payload"] = request_model
    app.post(
        "/resources/bulk-delete/preflight/",
        response_model=ResourceBulkActionPreflightResponse,
        responses={409: {"model": ResourceBulkActionPreflightResponse}},
    )(preflight_resources)

    def execute_resources(payload):
        return {"deleted_count": 1}

    execute_resources.__annotations__["payload"] = request_model
    app.post("/resources/bulk-delete/")(execute_resources)

    request = fastapi.Request(
        {
            "type": "http",
            "scheme": "https",
            "server": ("api.example.test", 443),
            "path": "/resources/",
            "raw_path": b"/resources/",
            "query_string": b"limit=1&offset=0&kind=service",
            "headers": [],
            "client": ("testclient", 50000),
            "root_path": "",
        }
    )
    response = fastapi.Response(headers={"Vary": "Accept-Encoding"})
    page = build_fastapi_resource_collection(
        request=request,
        response=response,
        results=[ExampleRow(uid=FIRST_UID, name="Risk")],
        count=2,
        pagination=resource_limit_offset(limit=1, offset=0),
        controls=_controls(),
        actions=[_minimal_action()],
    ).model_dump(mode="json")

    assert set(page) == {
        "count",
        "next",
        "previous",
        "results",
        "controls",
        "actions",
    }
    assert page["previous"] is None
    assert response.headers["Cache-Control"] == "private, no-store"
    assert response.headers["Vary"] == "Accept-Encoding, Authorization, Cookie"
    assert "preflight_endpoint" not in page["actions"][0]
    assert parse_qs(urlsplit(page["next"]).query) == {
        "limit": ["1"],
        "offset": ["1"],
        "kind": ["service"],
    }

    openapi = app.openapi()
    operation = openapi["paths"]["/resources/"]["get"]
    parameters = {parameter["name"]: parameter for parameter in operation["parameters"]}
    assert parameters["limit"]["schema"]["minimum"] == 1
    assert parameters["offset"]["schema"]["minimum"] == 0
    response_schema = operation["responses"]["200"]["content"]["application/json"]["schema"]
    assert "$ref" in response_schema
    component_name = response_schema["$ref"].rsplit("/", 1)[-1]
    assert set(openapi["components"]["schemas"][component_name]["required"]) == {
        "count",
        "next",
        "previous",
        "results",
        "controls",
        "actions",
    }

    action_schema = openapi["components"]["schemas"]["ResourceBulkActionDefinition"]
    assert "method" in action_schema["required"]
    option_schema = openapi["components"]["schemas"]["ResourceBulkActionOption"]
    assert "type" in option_schema["required"]

    preflight_request_schema = openapi["paths"][
        "/resources/bulk-delete/preflight/"
    ]["post"]["requestBody"]["content"]["application/json"]["schema"]
    execution_request_schema = openapi["paths"]["/resources/bulk-delete/"][
        "post"
    ]["requestBody"]["content"]["application/json"]["schema"]
    assert preflight_request_schema == execution_request_schema

    request_component_name = preflight_request_schema["$ref"].rsplit("/", 1)[-1]
    request_component = openapi["components"]["schemas"][request_component_name]
    assert set(request_component["required"]) == {"selection", "options"}
    for selection_ref in request_component["properties"]["selection"]["oneOf"]:
        selection_component_name = selection_ref["$ref"].rsplit("/", 1)[-1]
        selection_component = openapi["components"]["schemas"][
            selection_component_name
        ]
        assert "mode" in selection_component["required"]

    discovery_response = fastapi.Response()
    discovery = build_fastapi_bulk_action_discovery(
        response=discovery_response,
        actions=[_minimal_action()],
    )
    discovery_payload = fastapi.encoders.jsonable_encoder(discovery)
    assert discovery_payload["actions"][0].keys() == {
        "id",
        "label",
        "endpoint",
        "method",
        "selection_modes",
        "options",
    }
    assert discovery_response.headers["Cache-Control"] == "private, no-store"

    wildcard_vary_response = fastapi.Response(headers={"Vary": "*"})
    apply_resource_discovery_headers(wildcard_vary_response)
    assert wildcard_vary_response.headers["Vary"] == "*"

    blocked_response = build_fastapi_bulk_action_preflight_response(
        ResourceBulkActionPreflightResponse(
            allowed=False,
            detail="Deletion is blocked.",
            matched_count=1,
        )
    )
    assert blocked_response.status_code == 409
    assert json.loads(blocked_response.body) == {
        "allowed": False,
        "detail": "Deletion is blocked.",
        "warnings": [],
        "matched_count": 1,
    }

    structured_response = build_fastapi_bulk_action_preflight_response(
        StructuredPreflight(
            allowed=False,
            matched_count=1,
            data_sources=[
                {
                    "uid": FIRST_UID,
                    "blockers": [{"kind": "protected_relations", "count": 1}],
                }
            ],
        )
    )
    assert structured_response.status_code == 409
    assert json.loads(structured_response.body)["data_sources"][0]["blockers"] == [
        {"kind": "protected_relations", "count": 1}
    ]
