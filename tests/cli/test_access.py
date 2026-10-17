from __future__ import annotations

import importlib
import json
import sys
import types

import pytest

POLICY_UID = "6f0c2e8a-3b1d-4c5e-9f7a-2d4b6c8e0a12"


def _policy(**overrides):
    policy = {
        "uid": POLICY_UID,
        "id": "secrets.reader",
        "resource_type": "secrets",
        "name": "reader",
        "label": "Secret reader",
        "summary": "See the name.",
        "impact": "Shows it exists.",
        "not_included": "The value.",
        "actions": ["secrets.read", "secrets.read_access"],
        "attachable": True,
        "principal_kinds": ["people", "teams"],
        "stage": "active",
    }
    policy.update(overrides)
    return policy


def _sdk_policy(**overrides):
    payload = _policy(**overrides)
    return types.SimpleNamespace(
        id=payload["id"],
        model_dump=lambda mode="python": {**payload, "orm_class": "AccessPolicy"},
    )


def _install_fake_access_catalog(monkeypatch, catalog_cls, captured):
    """Stand in for the SDK client modules that `_run_sdk_model_operation` imports."""

    api_mod = importlib.import_module("mainsequence.cli.api")
    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_access_catalog = types.ModuleType("mainsequence.client.access_catalog")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"
    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_access_catalog.AccessCatalog = catalog_cls
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.access_catalog", fake_access_catalog)
    return api_mod


def test_access_policies_lists_every_policy(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _list(*, resource_type=None, timeout=None):
        captured["kwargs"] = {"resource_type": resource_type, "timeout": timeout}
        return [_policy()]

    monkeypatch.setattr(cli_mod, "list_access_policies", _list)

    result = runner.invoke(cli_mod.app, ["access", "policies"])

    assert result.exit_code == 0, result.output
    assert captured["kwargs"] == {"resource_type": None, "timeout": None}
    assert "Access Policies" in result.output
    assert "secrets.reader" in result.output
    assert "Secret reader" in result.output
    assert "Total access policies: 1" in result.output


def test_access_policies_type_option_lists_one_types_policies(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _list(*, resource_type=None, timeout=None):
        captured["kwargs"] = {"resource_type": resource_type, "timeout": timeout}
        return [_policy()]

    monkeypatch.setattr(cli_mod, "list_access_policies", _list)

    result = runner.invoke(
        cli_mod.app, ["access", "policies", "--type", "secrets", "--timeout", "9"]
    )

    assert result.exit_code == 0, result.output
    assert captured["kwargs"] == {"resource_type": "secrets", "timeout": 9}


def test_access_policies_reports_a_type_without_policies(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod, "list_access_policies", lambda *, resource_type=None, timeout=None: []
    )

    result = runner.invoke(cli_mod.app, ["access", "policies", "--type", "unknown-type"])

    assert result.exit_code == 0, result.output
    assert "No access policies for resource type unknown-type." in result.output
    assert "Total access policies: 0" in result.output


def test_access_policies_json_prints_the_policies(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_access_policies",
        lambda *, resource_type=None, timeout=None: [_policy()],
    )

    result = runner.invoke(cli_mod.app, ["access", "policies", "--json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == [_policy()]


def test_access_policies_marks_a_computed_policy(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    # Short cells keep the row inside Rich's 80-column fallback width, so no cell wraps.
    monkeypatch.setattr(
        cli_mod,
        "list_access_policies",
        lambda *, resource_type=None, timeout=None: [
            _policy(
                id="teams.member",
                label="Member",
                summary="Leave it.",
                attachable=False,
                principal_kinds=[],
            )
        ],
    )

    result = runner.invoke(cli_mod.app, ["access", "policies"])

    assert result.exit_code == 0, result.output
    assert "computed, never attached" in result.output


def test_access_policies_reports_a_failed_fetch(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _list(*, resource_type=None, timeout=None):
        raise cli_mod.ApiError("backend unavailable")

    monkeypatch.setattr(cli_mod, "list_access_policies", _list)

    result = runner.invoke(cli_mod.app, ["access", "policies"])

    assert result.exit_code == 1
    assert "Access policies fetch failed: backend unavailable" in result.output


def test_access_policies_requires_a_session(cli_mod, runner, monkeypatch):
    calls = []
    monkeypatch.setattr(cli_mod, "get_current_user_profile", lambda: {})
    monkeypatch.setattr(
        cli_mod,
        "list_access_policies",
        lambda *, resource_type=None, timeout=None: calls.append("list") or [],
    )

    result = runner.invoke(cli_mod.app, ["access", "policies"])

    assert result.exit_code == 1
    assert "Not logged in. Run: mainsequence login" in result.output
    assert calls == []


def test_access_policies_show_prints_the_policy_texts_and_actions(cli_mod, runner, monkeypatch):
    captured = {}
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _get(policy_ref, *, timeout=None):
        captured["call"] = (policy_ref, timeout)
        return _policy()

    monkeypatch.setattr(cli_mod, "get_access_policy", _get)

    result = runner.invoke(
        cli_mod.app, ["access", "policies", "show", "secrets.reader", "--timeout", "4"]
    )

    assert result.exit_code == 0, result.output
    assert captured["call"] == ("secrets.reader", 4)
    assert "Access Policy" in result.output
    assert "Secret reader" in result.output
    assert "See the name." in result.output
    assert "Shows it exists." in result.output
    assert "The value." in result.output
    assert "secrets.read, secrets.read_access" in result.output
    assert "people, teams" in result.output


def test_access_policies_show_json_prints_the_policy(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(cli_mod, "get_access_policy", lambda policy_ref, *, timeout=None: _policy())

    result = runner.invoke(cli_mod.app, ["access", "policies", "show", POLICY_UID, "--json"])

    assert result.exit_code == 0, result.output
    assert json.loads(result.output) == _policy()


def test_access_policies_show_reports_an_unknown_policy(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _get(policy_ref, *, timeout=None):
        raise cli_mod.ApiError(f"Access policy not found: {policy_ref}")

    monkeypatch.setattr(cli_mod, "get_access_policy", _get)

    result = runner.invoke(cli_mod.app, ["access", "policies", "show", "secrets.unknown"])

    assert result.exit_code == 1
    assert "Access policy not found: secrets.unknown" in result.output


def test_access_policies_show_reads_through_the_sdk_client_at_the_cli_backend(
    cli_mod, runner, monkeypatch
):
    import mainsequence.client.access_catalog as access_catalog
    from mainsequence.client import utils as client_utils

    api_mod = importlib.import_module("mainsequence.cli.api")
    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    # The operation points the SDK at the CLI's backend and leaves it there; undo it afterwards.
    for name in ("MAINSEQUENCE_ENDPOINT", "API_ENDPOINT", "AUTH_ENDPOINT"):
        monkeypatch.setattr(client_utils, name, getattr(client_utils, name))
    monkeypatch.setattr(access_catalog.AccessCatalog, "ROOT_URL", None, raising=False)

    sent = []

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return [
                _policy(uid="0d7c1b2e-5f3a-4e6b-8c9d-1a2b3c4d5e6f", id="secrets.owner"),
                _policy(),
            ]

    def fake_make_request(**kwargs):
        sent.append((kwargs["r_type"], kwargs["url"], kwargs["payload"]))
        return Response()

    monkeypatch.setattr(access_catalog, "make_request", fake_make_request)

    result = runner.invoke(cli_mod.app, ["access", "policies", "show", "secrets.reader", "--json"])

    assert result.exit_code == 0, result.output
    assert sent == [
        (
            "GET",
            "https://backend.test/api/v1/access/policies/",
            {"params": {"resource_type": "secrets"}},
        )
    ]
    assert json.loads(result.output) == _policy()


def test_list_access_policies_uses_the_access_catalog_client(cli_mod, monkeypatch):
    captured = {}

    class FakeAccessCatalog:
        def list_policies(self, *, resource_type=None, timeout=None):
            captured["list_policies"] = {"resource_type": resource_type, "timeout": timeout}
            return [_sdk_policy()]

    api_mod = _install_fake_access_catalog(monkeypatch, FakeAccessCatalog, captured)

    out = api_mod.list_access_policies(resource_type="secrets", timeout=5)

    assert captured["list_policies"] == {"resource_type": "secrets", "timeout": 5}
    assert captured["jwt"] == ("acc", "ref")
    assert out == [_policy()]


def test_get_access_policy_reads_a_uid_from_the_policy_route(cli_mod, monkeypatch):
    captured = {}

    class FakeAccessCatalog:
        def get_policy(self, uid, *, timeout=None):
            captured["get_policy"] = {"uid": uid, "timeout": timeout}
            return _sdk_policy()

        def list_policies(self, *, resource_type=None, timeout=None):
            raise AssertionError("a UID is read from the policy route")

    api_mod = _install_fake_access_catalog(monkeypatch, FakeAccessCatalog, captured)

    out = api_mod.get_access_policy(f" {POLICY_UID.upper()} ", timeout=6)

    assert captured["get_policy"] == {"uid": POLICY_UID, "timeout": 6}
    assert captured["jwt"] == ("acc", "ref")
    assert out == _policy()


def test_get_access_policy_finds_an_id_among_its_types_policies(cli_mod, monkeypatch):
    captured = {}

    class FakeAccessCatalog:
        def get_policy(self, uid, *, timeout=None):
            raise AssertionError("an id is looked up among its type's policies")

        def list_policies(self, *, resource_type=None, timeout=None):
            captured["list_policies"] = {"resource_type": resource_type, "timeout": timeout}
            return [
                _sdk_policy(id="secrets.owner", name="owner", label="Secret owner"),
                _sdk_policy(),
            ]

    api_mod = _install_fake_access_catalog(monkeypatch, FakeAccessCatalog, captured)

    out = api_mod.get_access_policy("secrets.reader")

    assert captured["list_policies"] == {"resource_type": "secrets", "timeout": None}
    assert out == _policy()


def test_get_access_policy_reports_an_id_its_type_does_not_have(cli_mod, monkeypatch):
    class FakeAccessCatalog:
        def list_policies(self, *, resource_type=None, timeout=None):
            return [_sdk_policy()]

    api_mod = _install_fake_access_catalog(monkeypatch, FakeAccessCatalog, {})

    with pytest.raises(api_mod.ApiError, match="Access policy not found: secrets.unknown"):
        api_mod.get_access_policy("secrets.unknown")


def test_get_access_policy_reports_an_unknown_uid(cli_mod, monkeypatch):
    class NotFoundError(Exception):
        pass

    class FakeAccessCatalog:
        def get_policy(self, uid, *, timeout=None):
            raise NotFoundError("404")

    api_mod = _install_fake_access_catalog(monkeypatch, FakeAccessCatalog, {})

    with pytest.raises(api_mod.ApiError, match=f"Access policy not found: {POLICY_UID}"):
        api_mod.get_access_policy(POLICY_UID)


@pytest.mark.parametrize("reference", ["reader", "secrets.", ".reader", "", "   "])
def test_get_access_policy_refuses_a_reference_that_is_neither_an_id_nor_a_uid(
    cli_mod, monkeypatch, reference
):
    class FakeAccessCatalog:
        def get_policy(self, uid, *, timeout=None):
            raise AssertionError("no request for a malformed reference")

        def list_policies(self, *, resource_type=None, timeout=None):
            raise AssertionError("no request for a malformed reference")

    api_mod = _install_fake_access_catalog(monkeypatch, FakeAccessCatalog, {})

    with pytest.raises(api_mod.ApiError, match="Not a policy id such as secrets.reader"):
        api_mod.get_access_policy(reference)
