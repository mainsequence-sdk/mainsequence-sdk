from __future__ import annotations

import importlib
import json
import os
import sys
import types

from tests.cli.support import _UNSUPPORTED_REPOSITORY_UID_ENV


def test_list_code_repository_images_uses_client_model(cli_mod, monkeypatch):
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
    fake_models = types.ModuleType("mainsequence.client.models_foundry")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeCodeRepositoryImage:
        ROOT_URL = "https://old.test/api/v1/code-repository-images"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
            captured["filters"].append(kwargs)
            captured["env_code_repository_uid"] = os.environ.get(_UNSUPPORTED_REPOSITORY_UID_ENV)
            if "related_code_repository_branch_uid" in kwargs:
                return [
                    types.SimpleNamespace(
                        model_dump=lambda: {
                            "uid": "8b62d1dd-e146-44af-957c-38c5f5b1d8d5",
                            "code_repository_commit_hash": "abc123",
                            "related_code_repository_branch_uid": 123,
                            "base_image": {"id": 22, "title": "Python 3.12"},
                            "build_error": False,
                            "is_ready": False,
                            "creation_date": "2026-04-07T09:00:00Z",
                        }
                    )
                ]
            return []

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.CodeRepositoryImage = FakeCodeRepositoryImage
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.list_code_repository_images(
        related_code_repository_branch_uid="5a28020a-0f1b-47ee-aab8-334286234bea",
        filters={"code_repository_commit_hash__in": ["abc123", "def456"]},
    )
    assert captured["filters"][0] == {
        "code_repository_commit_hash__in": ["abc123", "def456"],
        "related_code_repository_branch_uid": "5a28020a-0f1b-47ee-aab8-334286234bea",
    }
    assert captured["env_code_repository_uid"] is None
    assert captured["jwt"] == ("acc", "ref")
    assert out == [
        {
            "uid": "8b62d1dd-e146-44af-957c-38c5f5b1d8d5",
            "code_repository_commit_hash": "abc123",
            "related_code_repository_branch_uid": 123,
            "base_image": {"id": 22, "title": "Python 3.12"},
            "build_error": False,
            "is_ready": False,
            "creation_date": "2026-04-07T09:00:00Z",
        }
    ]
    assert os.environ.get(_UNSUPPORTED_REPOSITORY_UID_ENV) is None


def test_delete_code_repository_image_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_models = types.ModuleType("mainsequence.client.models_foundry")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeCodeRepositoryImage:
        ROOT_URL = "https://old.test/api/v1/code-repository-images"

        @classmethod
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "timeout": timeout, "filters": filters}

            class _Image:
                uid = pk

                def model_dump(self, mode="python"):
                    return {
                        "uid": pk,
                        "code_repository_commit_hash": "abc123",
                        "base_image": {"id": 22, "title": "Python 3.12"},
                        "is_ready": True,
                    }

                def delete(self):
                    captured["deleted"] = pk

            return _Image()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.CodeRepositoryImage = FakeCodeRepositoryImage
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    image_uid = "8b62d1dd-e146-44af-957c-38c5f5b1d8d5"
    out = api_mod.delete_code_repository_image(image_uid=image_uid)
    assert captured["get"] == {"pk": image_uid, "timeout": None, "filters": {}}
    assert captured["deleted"] == image_uid
    assert captured["jwt"] == ("acc", "ref")
    assert out["uid"] == image_uid
    assert out["code_repository_commit_hash"] == "abc123"


def test_code_repository_images_defaults_to_env_code_repository_id(
    cli_mod, runner, monkeypatch, git_checkout
):
    target = git_checkout("demo-123")
    (target / ".env").write_text("", encoding="utf-8")

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_code_repository_branch_uid_for_command",
        lambda *args, **kwargs: "code-repository-branch-uid-123",
    )
    monkeypatch.setattr(
        cli_mod,
        "list_code_repository_images",
        lambda related_code_repository_branch_uid, filters=None, timeout=None: [
            {
                "uid": "8b62d1dd-e146-44af-957c-38c5f5b1d8d5",
                "code_repository_commit_hash": "abc123",
                "base_image": {"id": 22, "title": "Python 3.12"},
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["code-repository", "images", "list"])
    assert result.exit_code == 0
    assert "CodeRepository Images" in result.output
    assert "abc123" in result.output
    assert "Python 3.12" in result.output
    assert "Total images: 1" in result.output


def test_code_repository_images_list_json(cli_mod, runner, monkeypatch, git_checkout):
    target = git_checkout("demo-123")
    (target / ".env").write_text("", encoding="utf-8")

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_code_repository_branch_uid_for_command",
        lambda *args, **kwargs: "code-repository-branch-uid-123",
    )
    monkeypatch.setattr(
        cli_mod,
        "list_code_repository_images",
        lambda related_code_repository_branch_uid, filters=None, timeout=None: [
            {
                "uid": "8b62d1dd-e146-44af-957c-38c5f5b1d8d5",
                "code_repository_commit_hash": "abc123",
                "base_image": {"id": 22, "title": "Python 3.12"},
                "creation_date": "2026-04-10T12:00:00Z",
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["code-repository", "images", "list", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload[0]["uid"] == "8b62d1dd-e146-44af-957c-38c5f5b1d8d5"
    assert payload[0]["code_repository_commit_hash"] == "abc123"
    assert payload[0]["creation_date"] == "2026-04-10T12:00:00Z"


def test_code_repository_images_list_rejects_reserved_filter(cli_mod, runner, monkeypatch):
    def _parse(model_ref, entries):
        return {"related_code_repository_branch_uid": ["other-branch-uid"]}

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)

    result = runner.invoke(
        cli_mod.app,
        [
            "code-repository",
            "images",
            "list",
            "code-repository-uid-123",
            "--filter",
            "related_code_repository_branch_uid=other-branch-uid",
        ],
    )
    assert result.exit_code == 1
    assert "cannot be overridden" in result.output


def test_code_repository_images_delete_requires_confirmation(cli_mod, runner, monkeypatch):
    captured = {}
    image_uid = "8b62d1dd-e146-44af-957c-38c5f5b1d8d5"

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_code_repository_image",
        lambda image_uid, timeout=None: {
            "uid": image_uid,
            "code_repository_commit_hash": "abc123",
            "base_image": {"id": 22, "title": "Python 3.12"},
            "is_ready": True,
        },
    )

    def _delete_code_repository_image(image_uid, timeout=None):
        captured["image_uid"] = image_uid
        return {
            "uid": image_uid,
            "code_repository_commit_hash": "abc123",
            "base_image": {"id": 22, "title": "Python 3.12"},
            "is_ready": True,
        }

    monkeypatch.setattr(cli_mod, "delete_code_repository_image", _delete_code_repository_image)

    result = runner.invoke(
        cli_mod.app, ["code-repository", "images", "delete", image_uid], input="y\n"
    )
    assert result.exit_code == 0
    assert captured["image_uid"] == image_uid
    assert "CodeRepository Image Delete Preview" in result.output
    assert f"Delete code repository image {image_uid}?" in result.output
    assert f"CodeRepository image deleted: uid={image_uid}" in result.output
