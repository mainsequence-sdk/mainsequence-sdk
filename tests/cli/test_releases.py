from __future__ import annotations

import importlib
import os
import sys
import types

from tests.cli.support import _UNSUPPORTED_REPOSITORY_UID_ENV


def test_list_code_repository_resources_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")
    monkeypatch.delenv(_UNSUPPORTED_REPOSITORY_UID_ENV, raising=False)
    monkeypatch.setattr(api_mod, "resolve_code_repository_branch_uid", lambda value: str(value))

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_helpers = types.ModuleType("mainsequence.client.models_helpers")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeCodeRepositoryResource:
        ROOT_URL = "https://old.test/api/v1/code-repository-resources"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
            captured["filters"].append(kwargs)
            captured["env_code_repository_uid"] = os.environ.get(_UNSUPPORTED_REPOSITORY_UID_ENV)
            return [
                types.SimpleNamespace(
                    model_dump=lambda: {
                        "uid": "857bec7b-dd77-4272-aecd-13fc2138eacc",
                        "name": "pricing_api.py",
                        "resource_type": "script",
                        "path": "api/pricing/main.py",
                        "filesize": 2048,
                        "last_modified": "2026-03-15T10:30:00Z",
                        "repo_commit_sha": "abc123",
                    }
                ),
                types.SimpleNamespace(
                    model_dump=lambda: {
                        "uid": "12cc8937-2e90-45da-8326-9e09a46f32cb",
                        "name": "CodeRepository Agent Card",
                        "resource_type": "code_repository_agent_card",
                        "path": ".agents/agent_card.json",
                        "repo_commit_sha": "abc123",
                    }
                ),
            ]

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_helpers.CodeRepositoryResource = FakeCodeRepositoryResource
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    out = api_mod.list_code_repository_resources(
        code_repository_branch_uid="5a28020a-0f1b-47ee-aab8-334286234bea",
        repo_commit_sha="abc123",
        resource_type="fastapi",
        filters={"uid__in": ["857bec7b-dd77-4272-aecd-13fc2138eacc"]},
    )
    assert captured["filters"][0] == {
        "uid__in": ["857bec7b-dd77-4272-aecd-13fc2138eacc"],
        "code_repository_branch_uid": "5a28020a-0f1b-47ee-aab8-334286234bea",
        "repo_commit_sha": "abc123",
        "resource_type": "fastapi",
    }
    assert captured["env_code_repository_uid"] is None
    assert captured["jwt"] == ("acc", "ref")
    assert out[0]["name"] == "pricing_api.py"
    assert [resource["resource_type"] for resource in out] == [
        "script",
        "code_repository_agent_card",
    ]
    assert out[1]["path"] == ".agents/agent_card.json"
    assert os.environ.get(_UNSUPPORTED_REPOSITORY_UID_ENV) is None


def test_delete_resource_release_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_helpers = types.ModuleType("mainsequence.client.models_helpers")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeResourceRelease:
        ROOT_URL = "https://old.test/api/v1/resource-releases"

        @classmethod
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "timeout": timeout, "filters": filters}

            class _Release:
                uid = pk

                def model_dump(self, mode="python"):
                    return {
                        "uid": pk,
                        "release_kind": "fastapi",
                        "resource": 381,
                        "related_image": 94,
                    }

                def delete(self):
                    captured["deleted"] = pk

            return _Release()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_helpers.ResourceRelease = FakeResourceRelease
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    out = api_mod.delete_resource_release(
        release_uid="0ce33c15-e3b1-4677-a66e-70460b89198f",
        expected_release_kind="fastapi",
    )
    assert captured["get"] == {
        "pk": "0ce33c15-e3b1-4677-a66e-70460b89198f",
        "timeout": None,
        "filters": {},
    }
    assert captured["deleted"] == "0ce33c15-e3b1-4677-a66e-70460b89198f"
    assert captured["jwt"] == ("acc", "ref")
    assert out["uid"] == "0ce33c15-e3b1-4677-a66e-70460b89198f"
    assert out["release_kind"] == "fastapi"


def test_code_repository_code_repository_resource_list_defaults_to_remote_branch_head(
    cli_mod, runner, monkeypatch, tmp_path
):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")
    captured = {}

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_code_repository_branch_uid_for_command",
        lambda *args, **kwargs: "code-repository-branch-uid-123",
    )
    monkeypatch.setattr(
        cli_mod,
        "_get_remote_branch_head_commit",
        lambda code_repository_dir: ("origin/main", "abc123"),
    )

    def _list_code_repository_resources(
        code_repository_branch_uid, repo_commit_sha, resource_type=None, filters=None, timeout=None
    ):
        captured["code_repository_branch_uid"] = code_repository_branch_uid
        captured["repo_commit_sha"] = repo_commit_sha
        captured["resource_type"] = resource_type
        return [
            {
                "uid": "857bec7b-dd77-4272-aecd-13fc2138eacc",
                "name": "api.py",
                "resource_type": "script",
                "path": "api/pricing/main.py",
                "filesize": 2048,
                "last_modified": "2026-03-15T10:30:00Z",
            }
        ]

    monkeypatch.setattr(cli_mod, "list_code_repository_resources", _list_code_repository_resources)

    result = runner.invoke(cli_mod.app, ["code-repository", "resources", "list"])
    assert result.exit_code == 0
    assert captured["code_repository_branch_uid"] == "code-repository-branch-uid-123"
    assert captured["repo_commit_sha"] == "abc123"
    assert "Using repo_commit_sha=abc123 from origin/main." in result.output
    assert "CodeRepository Resources" in result.output
    assert "api.py" in result.output
    assert "Total code repository resources: 1" in result.output


def test_code_repository_code_repository_resource_list_passes_extra_filters(
    cli_mod, runner, monkeypatch, tmp_path
):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_code_repository_branch_uid_for_command",
        lambda *args, **kwargs: "code-repository-branch-uid-123",
    )
    monkeypatch.setattr(
        cli_mod,
        "_get_remote_branch_head_commit",
        lambda code_repository_dir: ("origin/main", "abc123"),
    )

    def _parse(model_ref, entries):
        captured["entries"] = list(entries or [])
        return {"uid__in": ["857bec7b-dd77-4272-aecd-13fc2138eacc"]}

    def _list_code_repository_resources(
        code_repository_branch_uid, repo_commit_sha, resource_type=None, filters=None, timeout=None
    ):
        captured["filters"] = filters
        return []

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)
    monkeypatch.setattr(cli_mod, "list_code_repository_resources", _list_code_repository_resources)

    result = runner.invoke(
        cli_mod.app,
        [
            "code-repository",
            "resources",
            "list",
            "--path",
            str(target),
            "--filter",
            "uid__in=857bec7b-dd77-4272-aecd-13fc2138eacc",
        ],
    )
    assert result.exit_code == 0
    assert captured["entries"] == ["uid__in=857bec7b-dd77-4272-aecd-13fc2138eacc"]
    assert captured["filters"] == {"uid__in": ["857bec7b-dd77-4272-aecd-13fc2138eacc"]}


def test_code_repository_resource_help_omits_retired_dashboard_commands(cli_mod, runner):
    result = runner.invoke(cli_mod.app, ["code-repository", "resources", "--help"])

    assert result.exit_code == 0
    assert "create_dashboard" not in result.output
    assert "delete_dashboard" not in result.output


def test_code_repository_code_repository_resource_delete_fastapi_requires_confirmation(
    cli_mod, runner, monkeypatch
):
    captured = {}
    release_uid = "2ec33c15-e3b1-4677-a66e-70460b89198f"

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_resource_release",
        lambda release_uid, expected_release_kind=None, timeout=None: {
            "uid": release_uid,
            "release_kind": expected_release_kind,
            "resource": 382,
            "related_image": 94,
        },
    )

    def _delete_resource_release(release_uid, expected_release_kind=None, timeout=None):
        captured["release_uid"] = release_uid
        captured["expected_release_kind"] = expected_release_kind
        return {
            "uid": release_uid,
            "release_kind": expected_release_kind,
            "resource": 382,
            "related_image": 94,
        }

    monkeypatch.setattr(cli_mod, "delete_resource_release", _delete_resource_release)

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "resources", "delete_fastapi", release_uid],
        input="y\n",
    )
    assert result.exit_code == 0
    assert captured["release_uid"] == release_uid
    assert captured["expected_release_kind"] == "fastapi"
    assert "Subdomain" not in result.output
    assert f"Delete FastAPI release {release_uid}?" in result.output
    assert f"CodeRepository resource release deleted: uid={release_uid}" in result.output
