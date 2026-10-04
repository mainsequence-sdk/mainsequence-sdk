from __future__ import annotations

import importlib
import sys
import types

from tests.cli.support import USER_UID


def test_list_constants_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

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

    class FakeConstant:
        ROOT_URL = "https://old.test/api/v1/constants"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
            captured["filters"].append(kwargs)
            return [
                types.SimpleNamespace(
                    model_dump=lambda mode="python": {
                        "id": 7,
                        "name": "ASSETS__MASTER",
                        "value": {"source": "bbg"},
                    }
                )
            ]

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Constant = FakeConstant
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.list_constants(filters={"name__in": ["ASSETS__MASTER"]})
    assert captured["filters"][0] == {"name__in": ["ASSETS__MASTER"]}
    assert captured["jwt"] == ("acc", "ref")
    assert out == [{"id": 7, "name": "ASSETS__MASTER", "value": {"source": "bbg"}}]


def test_create_constant_uses_client_model(cli_mod, monkeypatch):
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

    class FakeConstant:
        ROOT_URL = "https://old.test/api/v1/constants"

        @classmethod
        def create(cls, *, name, value, timeout=None):
            captured["name"] = name
            captured["value"] = value
            captured["timeout"] = timeout
            return types.SimpleNamespace(
                model_dump=lambda mode="python": {
                    "id": 7,
                    "name": name,
                    "value": value,
                }
            )

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Constant = FakeConstant
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.create_constant(name="ASSETS__MASTER", value={"source": "bbg"}, timeout=15)
    assert captured["name"] == "ASSETS__MASTER"
    assert captured["value"] == {"source": "bbg"}
    assert captured["timeout"] == 15
    assert captured["jwt"] == ("acc", "ref")
    assert out["id"] == 7


def test_delete_constant_uses_client_model(cli_mod, monkeypatch):
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

    class FakeConstant:
        ROOT_URL = "https://old.test/api/v1/constants"

        @classmethod
        def get_by_uid(cls, uid, timeout=None):
            captured["get_by_uid"] = {"uid": uid, "timeout": timeout}

            class _Constant:
                def model_dump(self, mode="python"):
                    return {
                        "uid": uid,
                        "name": "ASSETS__MASTER",
                        "value": {"source": "bbg"},
                    }

                def delete(self, timeout=None):
                    captured["delete_timeout"] = timeout

            return _Constant()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Constant = FakeConstant
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.delete_constant("constant-uid-7", timeout=20)
    assert captured["get_by_uid"] == {"uid": "constant-uid-7", "timeout": 20}
    assert captured["delete_timeout"] == 20
    assert captured["jwt"] == ("acc", "ref")
    assert out["name"] == "ASSETS__MASTER"


def test_list_constant_users_can_edit_uses_client_model(cli_mod, monkeypatch):
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

    class FakeConstant:
        ROOT_URL = "https://old.test/api/v1/constants"

        @classmethod
        def get_by_uid(cls, uid, timeout=None):
            captured["get_by_uid"] = {"uid": uid, "timeout": timeout}

            class _Constant:
                def can_edit(self, timeout=None):
                    captured["can_edit_timeout"] = timeout
                    return types.SimpleNamespace(
                        model_dump=lambda mode="python": {
                            "object_uid": uid,
                            "object_type": "tdag.constant",
                            "access_level": "edit",
                            "users": [
                                {
                                    "id": 9,
                                    "username": "editor",
                                    "email": "editor@example.com",
                                    "first_name": "Edit",
                                    "last_name": "User",
                                }
                            ],
                            "teams": [],
                        }
                    )

            return _Constant()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Constant = FakeConstant
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.list_constant_users_can_edit("constant-uid-7", timeout=12)
    assert captured["get_by_uid"] == {"uid": "constant-uid-7", "timeout": 12}
    assert captured["can_edit_timeout"] == 12
    assert captured["jwt"] == ("acc", "ref")
    assert out["users"][0]["username"] == "editor"


def test_add_constant_user_to_edit_uses_client_model(cli_mod, monkeypatch):
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

    class FakeConstant:
        ROOT_URL = "https://old.test/api/v1/constants"

        @classmethod
        def get_by_uid(cls, uid, timeout=None):
            captured["get_by_uid"] = {"uid": uid, "timeout": timeout}

            class _Constant:
                def add_to_edit(self, user_uid, timeout=None):
                    captured["add_to_edit"] = {"user_uid": user_uid, "timeout": timeout}
                    return {
                        "ok": True,
                        "action": "add_to_edit",
                        "detail": "User now has explicit edit access.",
                        "object_uid": uid,
                        "object_type": "tdag.constant",
                        "user": {
                            "uid": user_uid,
                            "username": "editor",
                            "email": "editor@example.com",
                        },
                        "explicit_can_view": True,
                        "explicit_can_edit": True,
                        "explicit_can_view_user_uids": [user_uid],
                        "explicit_can_edit_user_uids": [user_uid],
                    }

            return _Constant()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Constant = FakeConstant
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.add_constant_user_to_edit("constant-uid-7", USER_UID, timeout=14)
    assert captured["get_by_uid"] == {"uid": "constant-uid-7", "timeout": 14}
    assert captured["add_to_edit"] == {"user_uid": USER_UID, "timeout": 14}
    assert captured["jwt"] == ("acc", "ref")
    assert out["action"] == "add_to_edit"


def test_list_secrets_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

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

    class FakeSecret:
        ROOT_URL = "https://old.test/api/v1/secrets"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
            captured["filters"].append(kwargs)
            return [
                types.SimpleNamespace(
                    model_dump=lambda mode="python": {
                        "id": 8,
                        "name": "API_KEY",
                    }
                )
            ]

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Secret = FakeSecret
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.list_secrets(filters={"name__in": ["API_KEY"]})
    assert captured["filters"][0] == {"name__in": ["API_KEY"]}
    assert captured["jwt"] == ("acc", "ref")
    assert out == [{"id": 8, "name": "API_KEY"}]


def test_list_secret_users_can_view_uses_client_model(cli_mod, monkeypatch):
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

    class FakeSecret:
        ROOT_URL = "https://old.test/api/v1/secrets"

        @classmethod
        def get_by_uid(cls, uid, timeout=None):
            captured["get_by_uid"] = {"uid": uid, "timeout": timeout}

            class _Secret:
                def can_view(self, timeout=None):
                    captured["can_view_timeout"] = timeout
                    return types.SimpleNamespace(
                        model_dump=lambda mode="python": {
                            "object_uid": uid,
                            "object_type": "tdag.secret",
                            "access_level": "view",
                            "users": [
                                {
                                    "id": 11,
                                    "username": "viewer",
                                    "email": "viewer@example.com",
                                    "first_name": "View",
                                    "last_name": "User",
                                }
                            ],
                            "teams": [
                                {"id": 4, "name": "Ops", "description": "", "member_count": 2}
                            ],
                        }
                    )

            return _Secret()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Secret = FakeSecret
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.list_secret_users_can_view("secret-uid-8", timeout=13)
    assert captured["get_by_uid"] == {"uid": "secret-uid-8", "timeout": 13}
    assert captured["can_view_timeout"] == 13
    assert captured["jwt"] == ("acc", "ref")
    assert out["users"][0]["username"] == "viewer"
    assert out["teams"][0]["name"] == "Ops"
    assert out["teams"][0]["member_count"] == 2


def test_add_secret_user_to_edit_uses_client_model(cli_mod, monkeypatch):
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

    class FakeSecret:
        ROOT_URL = "https://old.test/api/v1/secrets"

        @classmethod
        def get_by_uid(cls, uid, timeout=None):
            captured["get_by_uid"] = {"uid": uid, "timeout": timeout}

            class _Secret:
                def add_to_edit(self, user_uid, timeout=None):
                    captured["add_to_edit"] = {"user_uid": user_uid, "timeout": timeout}
                    return {
                        "ok": True,
                        "action": "add_to_edit",
                        "detail": "User now has explicit edit access.",
                        "object_uid": uid,
                        "object_type": "tdag.secret",
                        "user": {
                            "uid": user_uid,
                            "username": "editor",
                            "email": "editor@example.com",
                        },
                        "explicit_can_view": True,
                        "explicit_can_edit": True,
                        "explicit_can_view_user_uids": [user_uid],
                        "explicit_can_edit_user_uids": [user_uid],
                    }

            return _Secret()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Secret = FakeSecret
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.add_secret_user_to_edit("secret-uid-8", USER_UID, timeout=14)
    assert captured["get_by_uid"] == {"uid": "secret-uid-8", "timeout": 14}
    assert captured["add_to_edit"] == {"user_uid": USER_UID, "timeout": 14}
    assert captured["jwt"] == ("acc", "ref")
    assert out["action"] == "add_to_edit"


def test_create_secret_uses_client_model(cli_mod, monkeypatch):
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

    class FakeSecret:
        ROOT_URL = "https://old.test/api/v1/secrets"

        @classmethod
        def create(cls, *, name, value, timeout=None):
            captured["name"] = name
            captured["value"] = value
            captured["timeout"] = timeout
            return types.SimpleNamespace(
                model_dump=lambda mode="python": {
                    "id": 8,
                    "name": name,
                }
            )

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Secret = FakeSecret
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.create_secret(name="API_KEY", value="super-secret", timeout=10)
    assert captured["name"] == "API_KEY"
    assert captured["value"] == "super-secret"
    assert captured["timeout"] == 10
    assert captured["jwt"] == ("acc", "ref")
    assert out["id"] == 8


def test_delete_secret_uses_client_model(cli_mod, monkeypatch):
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

    class FakeSecret:
        ROOT_URL = "https://old.test/api/v1/secrets"

        @classmethod
        def get(cls, pk=None, timeout=None, **filters):
            captured["get"] = {"pk": pk, "timeout": timeout, "filters": filters}

            class _Secret:
                id = pk

                def model_dump(self, mode="python"):
                    return {
                        "id": pk,
                        "name": "API_KEY",
                    }

                def delete(self, timeout=None):
                    captured["delete_timeout"] = timeout

            return _Secret()

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_models.Secret = FakeSecret
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_foundry", fake_models)

    out = api_mod.delete_secret(8, timeout=20)
    assert captured["get"] == {"pk": 8, "timeout": 20, "filters": {}}
    assert captured["delete_timeout"] == 20
    assert captured["jwt"] == ("acc", "ref")
    assert out["name"] == "API_KEY"


def test_constants_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_constants",
        lambda filters=None, timeout=None: [
            {
                "uid": "11111111-1111-4111-8111-111111111111",
                "name": "ASSETS__MASTER",
                "value": {"source": "bbg"},
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["constants", "list"])
    assert result.exit_code == 0
    assert "Constants" in result.output
    assert "ASSETS__MASTER" in result.output
    assert "ASSETS" in result.output
    assert "Total constants: 1" in result.output


def test_constants_list_passes_cli_filters(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _parse(model_ref, entries):
        captured["entries"] = list(entries or [])
        return {"name__in": ["ASSETS__MASTER", "APP__MODE"]}

    def _list(timeout=None, filters=None):
        captured["filters"] = filters
        return []

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)
    monkeypatch.setattr(cli_mod, "list_constants", _list)

    result = runner.invoke(
        cli_mod.app,
        ["constants", "list", "--filter", "name__in=ASSETS__MASTER,APP__MODE"],
    )
    assert result.exit_code == 0
    assert captured["entries"] == ["name__in=ASSETS__MASTER,APP__MODE"]
    assert captured["filters"] == {"name__in": ["ASSETS__MASTER", "APP__MODE"]}


def test_constants_create_parses_json_value(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _create(*, name, value, timeout=None):
        captured["name"] = name
        captured["value"] = value
        captured["timeout"] = timeout
        return {"uid": "11111111-1111-4111-8111-111111111111", "name": name, "value": value}

    monkeypatch.setattr(cli_mod, "create_constant", _create)

    result = runner.invoke(
        cli_mod.app,
        ["constants", "create", "ASSETS__MASTER", '{"source":"bbg"}'],
    )
    assert result.exit_code == 0
    assert captured["name"] == "ASSETS__MASTER"
    assert captured["value"] == {"source": "bbg"}
    assert "Constant created: ASSETS__MASTER" in result.output
    assert "Created Constant" in result.output
    assert "ASSETS" in result.output


def test_constants_delete_requires_typed_verification(cli_mod, runner, monkeypatch):
    captured = {}
    constant_uid = "11111111-1111-4111-8111-111111111111"

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_constant",
        lambda constant_uid, timeout=None: {
            "uid": constant_uid,
            "name": "ASSETS__MASTER",
            "value": {"source": "bbg"},
        },
    )

    def _delete(constant_uid, timeout=None):
        captured["constant_uid"] = constant_uid
        captured["timeout"] = timeout
        return {
            "uid": constant_uid,
            "name": "ASSETS__MASTER",
            "value": {"source": "bbg"},
        }

    monkeypatch.setattr(cli_mod, "delete_constant", _delete)

    result = runner.invoke(
        cli_mod.app,
        ["constants", "delete", constant_uid],
        input="ASSETS__MASTER\n",
    )
    assert result.exit_code == 0
    assert "Constant Delete Preview" in result.output
    assert "Type constant name 'ASSETS__MASTER' to confirm deletion" in result.output
    assert captured["constant_uid"] == constant_uid
    assert f"Constant deleted: uid={constant_uid}" in result.output


def test_constants_can_edit(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_constant_users_can_edit",
        lambda constant_uid, timeout=None: {
            "object_uid": constant_uid,
            "object_type": "tdag.constant",
            "access_level": "edit",
            "users": [
                {
                    "id": 9,
                    "username": "editor",
                    "email": "editor@example.com",
                    "first_name": "Edit",
                    "last_name": "User",
                }
            ],
            "teams": [{"id": 3, "name": "Research", "description": "Core team", "member_count": 6}],
        },
    )

    result = runner.invoke(
        cli_mod.app,
        ["constants", "can_edit", "11111111-1111-4111-8111-111111111111"],
    )
    assert result.exit_code == 0
    assert "Constant Users Who Can Edit" in result.output
    assert "Constant Teams Who Can Edit" in result.output
    assert "editor" in result.output
    assert "editor@example.com" in result.output
    assert "Total users who can edit: 1" in result.output
    assert "Total teams who can edit: 1" in result.output


def test_constants_add_to_edit(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _add(constant_uid, user_uid, timeout=None):
        captured["constant_uid"] = constant_uid
        captured["user_uid"] = user_uid
        captured["timeout"] = timeout
        return {
            "ok": True,
            "action": "add_to_edit",
            "detail": "User now has explicit edit access.",
            "object_uid": constant_uid,
            "object_type": "tdag.constant",
            "user": {
                "uid": user_uid,
                "username": "editor",
                "email": "editor@example.com",
                "first_name": "Edit",
                "last_name": "User",
            },
            "explicit_can_view": True,
            "explicit_can_edit": True,
            "explicit_can_view_user_uids": [user_uid],
            "explicit_can_edit_user_uids": [user_uid],
        }

    monkeypatch.setattr(cli_mod, "add_constant_user_to_edit", _add)

    constant_uid = "11111111-1111-4111-8111-111111111111"
    result = runner.invoke(
        cli_mod.app,
        ["constants", "add_to_edit", constant_uid, USER_UID],
    )
    assert result.exit_code == 0
    assert captured == {
        "constant_uid": constant_uid,
        "user_uid": USER_UID,
        "timeout": None,
    }
    assert "Constant add_to_edit completed." in result.output
    assert "Constant Sharing Update" in result.output
    assert "editor@example.com" in result.output


def test_secrets_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_secrets",
        lambda filters=None, timeout=None: [
            {
                "uid": "498d499f-b74c-43f7-acf1-2e2955ad0e6b",
                "name": "API_KEY",
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["secrets", "list"])
    assert result.exit_code == 0
    assert "Secrets" in result.output
    assert "UID" in result.output
    assert "API_KEY" in result.output
    assert "Total secrets: 1" in result.output


def test_secrets_list_passes_cli_filters(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _parse(model_ref, entries):
        captured["entries"] = list(entries or [])
        return {"name__in": ["API_KEY", "DB_PASSWORD"]}

    def _list(timeout=None, filters=None):
        captured["filters"] = filters
        return []

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)
    monkeypatch.setattr(cli_mod, "list_secrets", _list)

    result = runner.invoke(
        cli_mod.app,
        ["secrets", "list", "--filter", "name__in=API_KEY,DB_PASSWORD"],
    )
    assert result.exit_code == 0
    assert captured["entries"] == ["name__in=API_KEY,DB_PASSWORD"]
    assert captured["filters"] == {"name__in": ["API_KEY", "DB_PASSWORD"]}


def test_secrets_create_hides_value_in_output(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _create(*, name, value, timeout=None):
        captured["name"] = name
        captured["value"] = value
        captured["timeout"] = timeout
        return {"uid": "498d499f-b74c-43f7-acf1-2e2955ad0e6b", "name": name}

    monkeypatch.setattr(cli_mod, "create_secret", _create)

    result = runner.invoke(
        cli_mod.app,
        ["secrets", "create", "API_KEY", "super-secret"],
    )
    assert result.exit_code == 0
    assert captured["name"] == "API_KEY"
    assert captured["value"] == "super-secret"
    assert "Secret created: API_KEY" in result.output
    assert "Created Secret" in result.output
    assert "super-secret" not in result.output


def test_secrets_delete_requires_typed_verification(cli_mod, runner, monkeypatch):
    captured = {}
    secret_uid = "498d499f-b74c-43f7-acf1-2e2955ad0e6b"

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_secret",
        lambda secret_uid, timeout=None: {
            "uid": secret_uid,
            "name": "API_KEY",
        },
    )

    def _delete(secret_uid, timeout=None):
        captured["secret_uid"] = secret_uid
        captured["timeout"] = timeout
        return {
            "uid": secret_uid,
            "name": "API_KEY",
        }

    monkeypatch.setattr(cli_mod, "delete_secret", _delete)

    result = runner.invoke(
        cli_mod.app,
        ["secrets", "delete", secret_uid],
        input="API_KEY\n",
    )
    assert result.exit_code == 0
    assert "Secret Delete Preview" in result.output
    assert "Type secret name 'API_KEY' to confirm deletion" in result.output
    assert captured["secret_uid"] == secret_uid
    assert f"Secret deleted: uid={secret_uid}" in result.output


def test_secrets_can_view(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_secret_users_can_view",
        lambda secret_uid, timeout=None: {
            "object_uid": secret_uid,
            "object_type": "tdag.secret",
            "access_level": "view",
            "users": [
                {
                    "id": 11,
                    "username": "viewer",
                    "email": "viewer@example.com",
                    "first_name": "View",
                    "last_name": "User",
                }
            ],
            "teams": [],
        },
    )

    result = runner.invoke(
        cli_mod.app,
        ["secrets", "can_view", "498d499f-b74c-43f7-acf1-2e2955ad0e6b"],
    )
    assert result.exit_code == 0
    assert "Secret Users Who Can View" in result.output
    assert "viewer@example.com" in result.output
    assert "Total users who can view: 1" in result.output
    assert "Total teams who can view: 0" in result.output


def test_secrets_add_to_edit(cli_mod, runner, monkeypatch):
    captured = {}
    secret_uid = "498d499f-b74c-43f7-acf1-2e2955ad0e6b"

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _add(secret_uid, user_uid, timeout=None):
        captured["secret_uid"] = secret_uid
        captured["user_uid"] = user_uid
        captured["timeout"] = timeout
        return {
            "ok": True,
            "action": "add_to_edit",
            "detail": "User now has explicit edit access.",
            "object_uid": secret_uid,
            "object_type": "tdag.secret",
            "user": {
                "uid": user_uid,
                "username": "editor",
                "email": "editor@example.com",
                "first_name": "Edit",
                "last_name": "User",
            },
            "explicit_can_view": True,
            "explicit_can_edit": True,
            "explicit_can_view_user_uids": [user_uid],
            "explicit_can_edit_user_uids": [user_uid],
        }

    monkeypatch.setattr(cli_mod, "add_secret_user_to_edit", _add)

    result = runner.invoke(
        cli_mod.app,
        ["secrets", "add_to_edit", secret_uid, USER_UID],
    )
    assert result.exit_code == 0
    assert captured == {
        "secret_uid": secret_uid,
        "user_uid": USER_UID,
        "timeout": None,
    }
    assert "Secret add_to_edit completed." in result.output
    assert "Secret Sharing Update" in result.output
    assert "editor@example.com" in result.output
