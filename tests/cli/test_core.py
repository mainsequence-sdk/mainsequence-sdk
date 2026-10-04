from __future__ import annotations

import dataclasses
import datetime
import importlib
import json
import pathlib
from enum import Enum
from uuid import UUID

import pytest
from pydantic import BaseModel

from tests.cli.support import TEAM_UID, USER_UID, _cli_platform_skill_catalog


def test_retired_creation_commands_are_not_registered(cli_mod):
    for group, retired_name in (
        (cli_mod.code_repository_images_group, "create"),
        (cli_mod.code_repository_jobs_group, "create"),
        (cli_mod.code_repository_resources_group, "create_fastapi"),
    ):
        assert retired_name not in {command.name for command in group.registered_commands}


def test_root_version(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_package_version", lambda: "3.18.9")

    result = runner.invoke(cli_mod.app, ["--version"])
    assert result.exit_code == 0
    assert result.output.strip() == "mainsequence 3.18.9"


def test_settings_defaults_to_show(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_persistent_config",
        lambda: {
            "backend_url": "https://main-sequence.app",
            "mainsequence_path": "/tmp/mainsequence",
        },
    )
    result = runner.invoke(cli_mod.app, ["settings"])
    assert result.exit_code == 0
    assert "backend_url" in result.output
    assert "mainsequence_path" in result.output


def test_user_show(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "get_logged_user_details",
        lambda: {
            "uid": "user-uid-7",
            "username": "jose",
            "email": "jose@main-sequence.io",
            "organization": {"uid": "org-uid-2", "name": "Main Sequence"},
            "is_active": True,
            "is_verified": True,
            "mfa_enabled": False,
            "date_joined": "2026-01-01T10:00:00Z",
            "last_login": "2026-03-15T09:30:00Z",
        },
    )

    result = runner.invoke(cli_mod.app, ["user"])
    assert result.exit_code == 0
    assert "MainSequence User" in result.output
    assert "user-uid-7" in result.output
    assert "jose" in result.output
    assert "jose@main-sequence.io" in result.output
    assert "Main Sequence" in result.output


def test_user_show_json(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod,
        "get_logged_user_details",
        lambda: {
            "uid": "user-uid-7",
            "username": "jose",
            "email": "jose@main-sequence.io",
            "organization": {"uid": "org-uid-2", "name": "Main Sequence"},
        },
    )

    result = runner.invoke(cli_mod.app, ["user", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert "id" not in payload
    assert payload["uid"] == "user-uid-7"
    assert payload["username"] == "jose"
    assert payload["organization"]["name"] == "Main Sequence"


@pytest.mark.parametrize("json_output", [False, True])
def test_user_show_handles_nested_uuid_identity(cli_mod, runner, monkeypatch, json_output):
    organization_uid = "00000000-0000-4000-8000-000000000002"
    identity = {
        "uid": UUID(USER_UID),
        "username": "jose",
        "organization": {"uid": UUID(organization_uid), "name": "Main Sequence"},
        "active_team_uids": [UUID(TEAM_UID)],
    }
    monkeypatch.setattr(cli_mod, "get_logged_user_details", lambda: identity)

    result = runner.invoke(cli_mod.app, ["user"] + (["--json"] if json_output else []))

    assert result.exit_code == 0
    if json_output:
        assert json.loads(result.output) == {
            "uid": USER_UID,
            "username": "jose",
            "organization": {"uid": organization_uid, "name": "Main Sequence"},
            "active_team_uids": [TEAM_UID],
        }
    else:
        assert USER_UID in result.output
        assert "Main Sequence" in result.output


def test_to_jsonable_normalizes_uuid_in_nested_containers(cli_mod):
    @dataclasses.dataclass
    class Identity:
        uid: UUID

    class IdentityModel(BaseModel):
        uid: UUID

    class State(Enum):
        ACTIVE = "active"

    uid = UUID(USER_UID)
    joined = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    payload = {
        "uid": uid,
        "nested": {"uid": uid, "orm_class": object()},
        "list": [uid],
        "tuple": (uid,),
        "set": {uid},
        "dataclass": Identity(uid),
        "model": IdentityModel(uid=uid),
        "indexed": {uid: uid},
        "date_joined": joined,
        "path": pathlib.Path("skills"),
        "state": State.ACTIVE,
        "orm_class": object(),
    }

    assert json.loads(json.dumps(cli_mod._to_jsonable(payload))) == {
        "uid": USER_UID,
        "nested": {"uid": USER_UID},
        "list": [USER_UID],
        "tuple": [USER_UID],
        "set": [USER_UID],
        "dataclass": {"uid": USER_UID},
        "model": {"uid": USER_UID},
        "indexed": {USER_UID: USER_UID},
        "date_joined": joined.isoformat(),
        "path": "skills",
        "state": "active",
    }


def test_to_jsonable_does_not_hide_unsupported_type_errors(cli_mod):
    with pytest.raises(TypeError, match="not JSON serializable"):
        json.dumps(cli_mod._to_jsonable(object()))


@pytest.mark.parametrize("json_output", [False, True])
@pytest.mark.parametrize(
    ("error_type", "message", "expected_message"),
    [
        ("NotLoggedIn", "Invalid session", "Not logged in. Run: mainsequence login"),
        ("ApiError", "Current user unavailable", "Current user unavailable"),
    ],
)
def test_user_show_preserves_errors(
    cli_mod, runner, monkeypatch, json_output, error_type, message, expected_message
):
    def rejected():
        raise getattr(cli_mod, error_type)(message)

    monkeypatch.setattr(cli_mod, "get_logged_user_details", rejected)

    result = runner.invoke(cli_mod.app, ["user"] + (["--json"] if json_output else []))

    assert result.exit_code == 1
    assert expected_message in result.output


def test_organization_github_organizations(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_github_organizations",
        lambda: [
            {
                "uid": "github-org-uid-33",
                "display_name": "Main Sequence Projects",
                "login": "mainsequence-projects",
            },
            {
                "uid": "github-org-uid-34",
                "display_name": "Research Labs",
                "login": "research-labs",
            },
        ],
    )

    result = runner.invoke(cli_mod.app, ["organization", "github-organizations"])
    assert result.exit_code == 0
    assert "GitHub Organizations" in result.output
    assert "github-org-uid-33" in result.output
    assert "Main Sequence Projects" in result.output


def test_organization_github_organizations_json(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_github_organizations",
        lambda: [
            {
                "uid": "github-org-uid-33",
                "display_name": "Main Sequence Projects",
                "login": "mainsequence-projects",
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["organization", "github-organizations", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == [
        {
            "uid": "github-org-uid-33",
            "display_name": "Main Sequence Projects",
            "login": "mainsequence-projects",
        }
    ]


def test_organization_teams_list(cli_mod, runner, monkeypatch):
    team_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_organization_teams",
        lambda timeout=None, filters=None: [
            {
                "uid": team_uid,
                "name": "Research",
                "description": "Model validation",
                "member_count": 4,
                "is_active": True,
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["organization", "teams", "list"])
    assert result.exit_code == 0
    assert "Organization Teams" in result.output
    assert "Research" in result.output
    assert "Model validation" in result.output
    assert "Total organization teams: 1" in result.output


def test_organization_teams_create(cli_mod, runner, monkeypatch):
    captured = {}
    team_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _create(*, name, description="", timeout=None):
        captured["name"] = name
        captured["description"] = description
        captured["timeout"] = timeout
        return {
            "uid": team_uid,
            "name": name,
            "description": description,
            "member_count": 0,
            "is_active": True,
        }

    monkeypatch.setattr(cli_mod, "create_organization_team", _create)

    result = runner.invoke(
        cli_mod.app,
        ["organization", "teams", "create", "Research", "--description", "Model validation"],
    )
    assert result.exit_code == 0
    assert captured == {"name": "Research", "description": "Model validation", "timeout": None}
    assert "Organization team created: Research" in result.output


def test_organization_teams_edit(cli_mod, runner, monkeypatch):
    captured = {}
    team_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_organization_team",
        lambda team_uid_arg, timeout=None: {
            "uid": team_uid_arg,
            "name": "Research",
            "description": "Old description",
            "member_count": 4,
            "is_active": True,
        },
    )

    def _update(team_uid_arg, *, name=None, description=None, is_active=None, timeout=None):
        captured["team_uid"] = team_uid_arg
        captured["name"] = name
        captured["description"] = description
        captured["is_active"] = is_active
        captured["timeout"] = timeout
        return {
            "uid": team_uid_arg,
            "name": name or "Research",
            "description": description or "Old description",
            "member_count": 4,
            "is_active": is_active,
        }

    monkeypatch.setattr(cli_mod, "update_organization_team", _update)

    result = runner.invoke(
        cli_mod.app,
        [
            "organization",
            "teams",
            "edit",
            team_uid,
            "--name",
            "Research Core",
            "--inactive",
        ],
    )
    assert result.exit_code == 0
    assert captured == {
        "team_uid": team_uid,
        "name": "Research Core",
        "description": None,
        "is_active": False,
        "timeout": None,
    }
    assert f"Organization team updated: uid={team_uid}" in result.output


def test_organization_teams_delete(cli_mod, runner, monkeypatch):
    captured = {}
    team_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_organization_team",
        lambda team_uid_arg, timeout=None: {
            "uid": team_uid_arg,
            "name": "Research",
            "description": "Model validation",
            "member_count": 4,
            "is_active": True,
        },
    )
    monkeypatch.setattr(cli_mod, "_require_delete_verification", lambda **kwargs: None)

    def _delete(team_uid_arg, *, timeout=None):
        captured["team_uid"] = team_uid_arg
        captured["timeout"] = timeout
        return {
            "uid": team_uid_arg,
            "name": "Research",
            "description": "Model validation",
            "member_count": 4,
            "is_active": True,
        }

    monkeypatch.setattr(cli_mod, "delete_organization_team", _delete)

    result = runner.invoke(cli_mod.app, ["organization", "teams", "delete", team_uid])
    assert result.exit_code == 0
    assert captured == {"team_uid": team_uid, "timeout": None}
    assert f"Organization team deleted: uid={team_uid}" in result.output


def test_list_organization_teams_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class FakeTeam:
        def __init__(self, team_uid, name):
            self.uid = team_uid
            self.name = name

        def model_dump(self, mode="json"):
            return {"uid": self.uid, "name": self.name}

    def _run_sdk_model_operation(
        *, module_name, class_name, operation, code_repository_id_env=None
    ):
        captured["module_name"] = module_name
        captured["class_name"] = class_name

        class _ClientTeam:
            @classmethod
            def filter(cls, timeout=None, **kwargs):
                captured["timeout"] = timeout
                captured["filters"] = kwargs
                return [FakeTeam("3f1cc452-43ec-49cb-b2ba-87dbac164d29", "Research")]

        return operation(_ClientTeam)

    monkeypatch.setattr(api_mod, "_run_sdk_model_operation", _run_sdk_model_operation)

    out = api_mod.list_organization_teams(timeout=9, filters={"name__contains": "Res"})
    assert captured == {
        "module_name": "mainsequence.client.models_user",
        "class_name": "Team",
        "timeout": 9,
        "filters": {"name__contains": "Res"},
    }
    assert out == [{"uid": "3f1cc452-43ec-49cb-b2ba-87dbac164d29", "name": "Research"}]


def test_pydantic_cli_metadata_from_source():
    metadata_mod = importlib.import_module("mainsequence.cli.pydantic_cli")
    meta = metadata_mod.get_cli_field_metadata(
        "mainsequence.client.models_helpers.Job",
        "execution_path",
    )
    assert meta.label == "Execution path"
    assert "content root" in meta.description
    assert "scripts/test.py" in meta.examples


def test_model_filter_parser_uses_filterset_metadata():
    filters_mod = importlib.import_module("mainsequence.cli.model_filters")

    class FakeModel:
        FILTERSET_FIELDS = {
            "id": ["exact", "in"],
            "is_active": ["exact", "isnull"],
            "name": ["contains"],
        }
        FILTER_VALUE_NORMALIZERS = {
            "id": "id",
            "is_active__isnull": "bool",
            "name": "str",
        }

    rows = filters_mod.build_cli_model_filter_rows(FakeModel)
    assert ["id", "exact", "integer ID", "id"] in rows
    assert ["id__in", "in", "comma-separated integer IDs", "id"] in rows
    assert ["is_active__isnull", "isnull", "true/false", "bool"] in rows
    assert ["name__contains", "contains", "text", "str"] in rows

    parsed = filters_mod.parse_cli_model_filters(
        FakeModel,
        ["id__in=1,2,3", "name__contains=daily", "is_active__isnull=true"],
    )
    assert parsed == {
        "id__in": ["1", "2", "3"],
        "name__contains": "daily",
        "is_active__isnull": "true",
    }


def test_shared_compute_validation_supports_k8s_quantities(cli_mod):
    compute_mod = importlib.import_module("mainsequence.client.compute_validation")

    decimal_out = compute_mod.validate_and_normalize_compute_fields(
        cpu_request="500m",
        memory_request="1Gi",
        gpu_request="",
        gpu_type="",
        output_format="decimal",
    )
    assert decimal_out == {
        "cpu_request": "0.5",
        "memory_request": "1",
        "gpu_request": None,
        "gpu_type": None,
    }

    k8s_out = compute_mod.validate_and_normalize_compute_fields(
        cpu_request="500m",
        memory_request="1Gi",
        gpu_request="",
        gpu_type="",
        output_format="k8s",
    )
    assert k8s_out == {
        "cpu_request": "500m",
        "memory_request": "1Gi",
        "gpu_request": None,
        "gpu_type": None,
    }


def test_settings_show_ignores_session_overrides(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_persistent_config",
        lambda: {
            "backend_url": "https://main-sequence.app",
            "mainsequence_path": "/tmp/mainsequence",
        },
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "get_session_overrides",
        lambda: {
            "backend_url": "http://127.0.0.1:8000",
            "mainsequence_path": "/tmp/mainsequence-dev",
        },
    )
    result = runner.invoke(cli_mod.app, ["settings"])
    assert result.exit_code == 0
    assert "https://main-sequence.app" in result.output
    assert "/tmp/mainsequence" in result.output
    assert "127.0.0.1:8000" not in result.output


def test_settings_set_base(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_mainsequence_path",
        lambda path: {"mainsequence_path": path},
    )
    result = runner.invoke(cli_mod.app, ["settings", "set-base", "/tmp/ms-base"])
    assert result.exit_code == 0
    assert "CodeRepositories base folder set to" in result.output


def test_settings_set_base_json(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_mainsequence_path",
        lambda path: {"mainsequence_path": path},
    )
    result = runner.invoke(cli_mod.app, ["settings", "set-base", "/tmp/ms-base", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["mainsequence_path"] == "/tmp/ms-base"


def test_settings_set_backend(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_backend_url",
        lambda url: {"backend_url": url},
    )
    result = runner.invoke(cli_mod.app, ["settings", "set-backend", "https://example.test"])
    assert result.exit_code == 0
    assert "Backend URL set to" in result.output


def test_settings_set_backend_json(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_backend_url",
        lambda url: {"backend_url": url},
    )
    result = runner.invoke(
        cli_mod.app, ["settings", "set-backend", "https://example.test", "--json"]
    )
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["backend_url"] == "https://example.test"


def test_settings_reset(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(
        cli_mod.cfg,
        "DEFAULTS",
        {
            "backend_url": f"{cli_mod.cfg.STANDARD_BACKEND_URL}/",
            "mainsequence_path": "/tmp/mainsequence",
            "version": 1,
        },
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_config",
        lambda updates: captured.update(updates)
        or updates | {"updated_at": "2026-04-20T00:00:00Z"},
    )
    monkeypatch.setattr(
        cli_mod.cfg, "clear_session_overrides", lambda: captured.update(cleared=True)
    )

    result = runner.invoke(cli_mod.app, ["settings", "reset"])
    assert result.exit_code == 0
    assert captured["backend_url"] == cli_mod.cfg.STANDARD_BACKEND_URL
    assert captured["mainsequence_path"].endswith("/tmp/mainsequence")
    assert captured["cleared"] is True
    assert "Settings reset to standard defaults" in result.output


def test_settings_refresh_alias(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(
        cli_mod.cfg,
        "DEFAULTS",
        {
            "backend_url": f"{cli_mod.cfg.STANDARD_BACKEND_URL}/",
            "mainsequence_path": "/tmp/mainsequence",
            "version": 1,
        },
    )
    monkeypatch.setattr(
        cli_mod.cfg,
        "set_config",
        lambda updates: updates | {"updated_at": "2026-04-20T00:00:00Z"},
    )
    monkeypatch.setattr(cli_mod.cfg, "clear_session_overrides", lambda: None)

    result = runner.invoke(cli_mod.app, ["settings", "refresh"])
    assert result.exit_code == 0
    assert "Settings reset to standard defaults" in result.output


def test_config_normalize_backend_url(cli_mod):
    assert cli_mod.cfg.normalize_backend_url("127.0.0.1:800") == "http://127.0.0.1:800"
    assert cli_mod.cfg.normalize_backend_url("localhost:8000") == "http://localhost:8000"
    assert cli_mod.cfg.normalize_backend_url("main-sequence.app") == "https://main-sequence.app"
    assert cli_mod.cfg.normalize_backend_url("https://example.test/") == "https://example.test"


def test_config_normalize_mainsequence_path(cli_mod):
    assert cli_mod.cfg.normalize_mainsequence_path("mainsequence-dev").endswith("/mainsequence-dev")
    assert cli_mod.cfg.normalize_mainsequence_path("~/mainsequence-dev").endswith(
        "/mainsequence-dev"
    )


def test_config_session_overrides_do_not_persist(cli_mod, monkeypatch, tmp_path):
    config_json = tmp_path / "config.json"
    session_json = tmp_path / "session.json"
    cli_mod.cfg.write_json(
        config_json,
        {
            "backend_url": "https://prod.test",
            "mainsequence_path": str(tmp_path / "mainsequence"),
            "version": 1,
        },
    )

    monkeypatch.setattr(cli_mod.cfg, "CONFIG_JSON", config_json)
    monkeypatch.setattr(cli_mod.cfg, "_session_override_path", lambda: session_json)

    cli_mod.cfg.set_session_overrides(
        backend_url="127.0.0.1:8000",
        mainsequence_path="mainsequence-dev",
    )

    effective = cli_mod.cfg.get_config()
    persisted = cli_mod.cfg.read_json(config_json, {})

    assert effective["backend_url"] == "http://127.0.0.1:8000"
    assert effective["mainsequence_path"].endswith("/mainsequence-dev")
    assert persisted["backend_url"] == "https://prod.test"
    assert persisted["mainsequence_path"] == str(tmp_path / "mainsequence")


def test_sdk_latest(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "fetch_latest_sdk_version", lambda: "v1.2.3")
    result = runner.invoke(cli_mod.app, ["sdk", "latest"])
    assert result.exit_code == 0
    assert "Latest SDK (GitHub): v1.2.3" in result.output


def test_sdk_latest_json(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "fetch_latest_sdk_version", lambda: "v1.2.3")
    result = runner.invoke(cli_mod.app, ["sdk", "latest", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["latest"] == "v1.2.3"


@pytest.mark.parametrize(
    "arguments",
    [
        ["data-node", "list"],
        ["data_node", "list"],
        ["data-node-storage", "list"],
        ["data_node_storage", "list"],
        ["code-repository", "data-node-updates", "list"],
        ["code-repository", "list", "data_nodes_updates"],
        ["code-repository", "get-data-node-updates"],
    ],
)
def test_removed_data_node_commands_are_unknown(cli_mod, runner, arguments):
    result = runner.invoke(cli_mod.app, arguments)

    assert result.exit_code != 0
    assert "No such command" in result.output


def test_platform_skill_refresh_uses_separate_namespace(cli_mod, runner, monkeypatch, git_checkout):
    target = git_checkout("repository")
    sdk = target / ".agents" / "skills" / "mainsequence" / "SKILL.md"
    sdk.parent.mkdir(parents=True)
    sdk.write_text("SDK unchanged", encoding="utf-8")
    calls = []

    def fetch():
        calls.append("authenticated MCP")
        return _cli_platform_skill_catalog()

    monkeypatch.setattr(cli_mod, "fetch_platform_code_repository_skill_catalog", fetch)
    result = runner.invoke(
        cli_mod.app, ["code-repository", "update-platform-skills", "--path", str(target), "--json"]
    )
    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["namespace"] == "mainsequence_platform"
    assert payload["updated_count"] == 3
    assert calls == ["authenticated MCP"]
    assert sdk.read_text() == "SDK unchanged"
    assert (
        target / ".agents" / "skills" / "mainsequence_platform" / "a2a_communication" / "SKILL.md"
    ).is_file()
