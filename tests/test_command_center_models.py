import mainsequence.client.base as base_mod
import mainsequence.client.utils as client_utils
from mainsequence.client.command_center import (
    RegisteredWidgetType,
    Workspace,
    WorkspaceLayoutKind,
    WorkspaceWidgetMutationResult,
)


def test_workspace_uses_command_center_endpoint():
    assert Workspace.get_object_url().endswith("/api/v1/command_center/workspaces")


def test_workspace_strips_orm_api_from_base_root(monkeypatch):
    monkeypatch.setattr(base_mod.BaseObjectOrm, "ROOT_URL", "https://backend.test/orm/api")
    assert Workspace.get_object_url() == "https://backend.test/api/v1/command_center/workspaces"


def test_workspace_parses_backend_alias_fields():
    workspace = Workspace.model_validate(
        {
            "id": 7,
            "title": "Rates Desk",
            "description": "Shared workspace for rates monitoring",
            "labels": ["rates", "monitoring"],
            "category": "Custom",
            "source": "user",
            "schemaVersion": 1,
            "requiredPermissions": ["dashboard:view"],
            "grid": {"columns": 96, "rowHeight": 18, "gap": 2},
            "layoutKind": "custom",
            "autoGrid": {},
            "companions": [],
            "controls": {"enabled": True},
            "widgets": [],
            "createdAt": "2026-04-04T10:00:00Z",
            "updatedAt": "2026-04-04T10:30:00Z",
        }
    )

    assert workspace.id == 7
    assert workspace.schema_version == 1
    assert workspace.required_permissions == ["dashboard:view"]
    assert workspace.layout_kind == WorkspaceLayoutKind.CUSTOM
    assert workspace.model_dump(by_alias=True)["layoutKind"] == "custom"


def test_registered_widget_type_uses_command_center_endpoint():
    assert RegisteredWidgetType.get_object_url().endswith("/api/v1/command_center/widget-types")


def test_registered_widget_type_parses_backend_fields():
    widget_type = RegisteredWidgetType.model_validate(
        {
            "widgetId": "main-sequence-data-node",
            "title": "Data Node",
            "description": "Renders a data node payload.",
            "category": "Main Sequence",
            "widgetVersion": "1.2.3",
            "kind": "custom",
            "source": "main-sequence",
            "tags": ["data", "node"],
            "requiredPermissions": ["dashboard:view"],
            "schema": {"type": "object"},
            "io": {"inputs": [], "outputs": []},
            "defaultPresentation": {"chrome": "card"},
            "defaultSize": {"w": 6, "h": 4},
            "responsive": {"sm": {"w": 12}},
            "usageGuidance": {"summary": "Use this to select a data node."},
            "capabilities": {"publishes": ["dataNodeId"]},
            "examples": [{"props": {"nodeId": 1}}],
            "isActive": True,
            "registryVersion": "2026.04.04",
            "checksum": "abc123",
            "lastSyncedAt": "2026-04-04T10:00:00Z",
            "createdAt": "2026-04-04T10:00:00Z",
            "updatedAt": "2026-04-04T10:30:00Z",
            "descriptor": {"ui": "card"},
            "x_registry_detail": {"sourceFile": "widgets/data-node.json"},
        }
    )

    assert widget_type.widget_id == "main-sequence-data-node"
    assert widget_type.is_active is True
    assert widget_type.required_permissions == ["dashboard:view"]
    assert widget_type.schema_payload == {"type": "object"}
    assert widget_type.widget_version == "1.2.3"
    assert widget_type.default_size == {"w": 6, "h": 4}
    assert widget_type.responsive == {"sm": {"w": 12}}
    assert widget_type.usage_guidance == {"summary": "Use this to select a data node."}
    assert widget_type.capabilities == {"publishes": ["dataNodeId"]}
    assert widget_type.examples == [{"props": {"nodeId": 1}}]
    assert widget_type.model_dump()["widget_id"] == "main-sequence-data-node"
    assert widget_type.model_dump()["is_active"] is True
    assert widget_type.model_dump()["schema_payload"] == {"type": "object"}
    assert widget_type.model_dump(by_alias=True)["widgetId"] == "main-sequence-data-node"
    assert widget_type.model_dump(by_alias=True)["widgetVersion"] == "1.2.3"
    assert widget_type.model_dump(by_alias=True)["isActive"] is True
    assert widget_type.model_dump(by_alias=True)["schema"] == {"type": "object"}
    assert widget_type.model_dump(by_alias=True)["defaultSize"] == {"w": 6, "h": 4}
    assert widget_type.model_dump(by_alias=True)["usageGuidance"] == {
        "summary": "Use this to select a data node."
    }
    assert widget_type.model_dump()["descriptor"] == {"ui": "card"}
    assert widget_type.model_dump()["x_registry_detail"] == {"sourceFile": "widgets/data-node.json"}


def test_registered_widget_type_filter_uses_snake_case(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "results": [
                    {
                        "widgetId": "main-sequence-data-node",
                        "title": "Data Node",
                        "description": "Renders a data node payload.",
                        "category": "Main Sequence",
                        "kind": "custom",
                        "source": "main-sequence",
                        "isActive": True,
                        "registryVersion": "2026.04.04",
                        "checksum": "abc123",
                        "lastSyncedAt": "2026-04-04T10:00:00Z",
                        "createdAt": "2026-04-04T10:00:00Z",
                        "updatedAt": "2026-04-04T10:30:00Z",
                    }
                ],
                "next": None,
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    results = RegisteredWidgetType.filter(
        widget_id="main-sequence-data-node",
        is_active=True,
        include_inactive=False,
        timeout=14,
    )

    assert len(results) == 1
    assert results[0].widget_id == "main-sequence-data-node"
    assert captured == {
        "r_type": "GET",
        "url": f"{RegisteredWidgetType.get_object_url()}/",
        "payload": {
            "params": {
                "widget_id": "main-sequence-data-node",
                "is_active": True,
                "include_inactive": False,
            }
        },
        "timeout": 14,
    }


def test_registered_widget_type_get_uses_widget_id_detail_lookup(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "widgetId": "main-sequence-data-node",
                "title": "Data Node",
                "description": "Renders a data node payload.",
                "category": "Main Sequence",
                "widgetVersion": "1.2.3",
                "kind": "custom",
                "source": "main-sequence",
                "defaultSize": {"w": 6, "h": 4},
                "responsive": {"sm": {"w": 12}},
                "usageGuidance": {"summary": "Use this to select a data node."},
                "capabilities": {"publishes": ["dataNodeId"]},
                "examples": [{"props": {"nodeId": 1}}],
                "isActive": True,
                "registryVersion": "2026.04.04",
                "checksum": "abc123",
                "lastSyncedAt": "2026-04-04T10:00:00Z",
                "createdAt": "2026-04-04T10:00:00Z",
                "updatedAt": "2026-04-04T10:30:00Z",
                "descriptor": {"ui": "card"},
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    result = RegisteredWidgetType.get(widget_id="main-sequence-data-node", timeout=9)

    assert result.widget_id == "main-sequence-data-node"
    assert result.widget_version == "1.2.3"
    assert result.default_size == {"w": 6, "h": 4}
    assert result.usage_guidance == {"summary": "Use this to select a data node."}
    assert result.model_dump()["descriptor"] == {"ui": "card"}
    assert captured == {
        "r_type": "GET",
        "url": f"{RegisteredWidgetType.get_object_url()}/main-sequence-data-node/",
        "payload": {"params": {}},
        "timeout": 9,
    }


def test_workspace_patch_widget_uses_widget_detail_endpoint(monkeypatch):
    captured = {}

    workspace = Workspace.model_validate(
        {
            "id": 7,
            "title": "Rates Desk",
            "description": "",
            "labels": [],
            "category": "Custom",
            "source": "user",
            "schemaVersion": 1,
            "requiredPermissions": None,
            "grid": {},
            "layoutKind": "custom",
            "autoGrid": {},
            "companions": [],
            "controls": {},
            "widgets": [],
            "createdAt": "2026-04-04T10:00:00Z",
            "updatedAt": "2026-04-04T10:30:00Z",
        }
    )

    class FakeResponse:
        status_code = 200
        content = b'{"workspaceId":7}'

        @staticmethod
        def json():
            return {
                "workspaceId": 7,
                "widgetInstanceId": "widget-existing",
                "parentWidgetId": "row-1",
                "widget": {
                    "id": "widget-existing",
                    "widgetId": "main-sequence-data-node",
                    "title": "Funding Curve Source",
                    "props": {"nodeId": 123},
                    "layout": {"cols": 12, "rows": 8},
                },
                "updatedAt": "2026-04-04T21:00:00Z",
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(client_utils, "make_request", _fake_make_request)

    result = workspace.patch_workspace_widget(
        "widget-existing",
        widget={
            "title": "Funding Curve Source v2",
            "props": {"nodeId": 123},
            "runtimeState": {"tab": "bindings"},
        },
        timeout=11,
    )

    assert isinstance(result, WorkspaceWidgetMutationResult)
    assert result.workspace_id == 7
    assert result.widget_instance_id == "widget-existing"
    assert result.parent_widget_id == "row-1"
    assert result.widget["widgetId"] == "main-sequence-data-node"
    assert captured == {
        "r_type": "PATCH",
        "url": f"{Workspace.get_object_url()}/7/widgets/widget-existing/",
        "payload": {
            "json": {
                "widget": {
                    "title": "Funding Curve Source v2",
                    "props": {"nodeId": 123},
                    "runtimeState": {"tab": "bindings"},
                }
            }
        },
        "timeout": 11,
    }


def test_workspace_move_widget_uses_move_action(monkeypatch):
    captured = {}

    workspace = Workspace.model_validate(
        {
            "id": 7,
            "title": "Rates Desk",
            "description": "",
            "labels": [],
            "category": "Custom",
            "source": "user",
            "schemaVersion": 1,
            "requiredPermissions": None,
            "grid": {},
            "layoutKind": "custom",
            "autoGrid": {},
            "companions": [],
            "controls": {},
            "widgets": [],
            "createdAt": "2026-04-04T10:00:00Z",
            "updatedAt": "2026-04-04T10:30:00Z",
        }
    )

    class FakeResponse:
        status_code = 200
        content = b'{"workspaceId":7}'

        @staticmethod
        def json():
            return {
                "workspaceId": 7,
                "widgetInstanceId": "widget-existing",
                "parentWidgetId": None,
                "widget": {
                    "id": "widget-existing",
                    "widgetId": "main-sequence-data-node",
                    "title": "Funding Curve Source",
                },
                "updatedAt": "2026-04-04T21:00:00Z",
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(client_utils, "make_request", _fake_make_request)

    result = workspace.move_workspace_widget(
        "widget-existing",
        parent_widget_id="row-1",
        index=0,
        timeout=13,
    )

    assert result.parent_widget_id is None
    assert captured == {
        "r_type": "POST",
        "url": f"{Workspace.get_object_url()}/7/widgets/widget-existing/move/",
        "payload": {"json": {"parentWidgetId": "row-1", "index": 0}},
        "timeout": 13,
    }


def test_workspace_delete_widget_supports_recursive_param(monkeypatch):
    captured = {}

    workspace = Workspace.model_validate(
        {
            "id": 7,
            "title": "Rates Desk",
            "description": "",
            "labels": [],
            "category": "Custom",
            "source": "user",
            "schemaVersion": 1,
            "requiredPermissions": None,
            "grid": {},
            "layoutKind": "custom",
            "autoGrid": {},
            "companions": [],
            "controls": {},
            "widgets": [],
            "createdAt": "2026-04-04T10:00:00Z",
            "updatedAt": "2026-04-04T10:30:00Z",
        }
    )

    class FakeResponse:
        status_code = 204
        content = b""

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(client_utils, "make_request", _fake_make_request)

    workspace.delete_workspace_widget("row-1", recursive=True, timeout=7)

    assert captured == {
        "r_type": "DELETE",
        "url": f"{Workspace.get_object_url()}/7/widgets/row-1/",
        "payload": {"params": {"recursive": "true"}},
        "timeout": 7,
    }
