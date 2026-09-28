from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest
import requests

from mainsequence.client import DataSource, base, models_data_sources, utils
from mainsequence.client.models_data_sources import DataSourceRuntimeError


def response(status=200, payload=None):
    result = requests.Response()
    result.status_code = status
    result._content = json.dumps(payload).encode()
    result._content_consumed = True
    return result


@pytest.fixture
def runtime(monkeypatch):
    ids = tuple(uuid4() for _ in range(3))
    payload = {
        "data_source_uid": str(ids[0]),
        "organization_uid": str(ids[1]),
        "organization_environment_uid": str(ids[2]),
        "class_type": "postgresql",
        "status": "AVAILABLE",
        "storage_access_mode": "read_write",
        "capabilities": {"supports_table_ddl": True, "supports_future_feature": False},
        "connection": {
            "host": "db.example.test",
            "port": 5432,
            "database_name": "analytics",
            "database_user": "workload",
            "password": "private-password",
            "ssl_mode": "verify-full",
            "default_schema": "public",
            "tls_ca_certificate": "private-ca",
            "tls_client_certificate": "private-cert",
            "tls_client_key": "private-key",
        },
        "display_name": "Analytics",
        "organization_environment_name": "Development",
        "extra_arguments": {"private": "private-extra"},
        "future_field": "ignored",
    }
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID", "workload-id")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET", "private-bootstrap")
    provider = utils.RuntimeCredentialAuthProvider(token_url="https://platform.test/token/")
    monkeypatch.setattr(DataSource, "LOADERS", utils.AuthLoaders(provider))
    monkeypatch.setattr(DataSource, "ROOT_URL", "https://platform.test/api/v1")
    calls = []

    def post(url, **kwargs):
        calls.append(("POST", url, kwargs))
        return response(
            payload={
                "access": f"token-{sum(c[0] == 'POST' for c in calls)}",
                "token_type": "Bearer",
                "expires_in": 3600,
            }
        )

    def get(self, url, **kwargs):
        calls.append(("GET", url, kwargs))
        return response(payload=payload)

    monkeypatch.setattr(utils.requests, "post", post)
    monkeypatch.setattr(models_data_sources.requests.Session, "get", get)

    def fetch():
        return DataSource.get_runtime_connection(
            ids[0], expected_organization_uid=ids[1], expected_environment_uid=ids[2]
        )

    return payload, calls, fetch, provider


def test_public_directory_lookup_uses_sdk_transport_and_tolerates_new_fields(monkeypatch):
    calls = []
    uid = str(uuid4())
    monkeypatch.setattr(DataSource, "ROOT_URL", "https://platform.test/api/v1")
    monkeypatch.setattr(DataSource, "build_session", classmethod(lambda cls: object()))

    def request(**kwargs):
        calls.append(kwargs)
        return response(payload={"uid": uid, "display_name": "Analytics", "new_field": 4})

    monkeypatch.setattr(base, "make_request", request)
    result = DataSource.get_by_uid(uid)
    assert result.uid == uid and result.display_name == "Analytics"
    assert not hasattr(result, "new_field")
    assert calls[0]["r_type"] == "GET"
    assert calls[0]["url"] == f"https://platform.test/api/v1/data-sources/{uid}/"
    from mainsequence.client.models_helpers import get_model_class

    assert get_model_class("DataSource") is DataSource


def test_runtime_fetch_reuses_auth_but_never_caches_connection_material(runtime):
    payload, calls, fetch, _ = runtime
    first = fetch()
    payload["connection"]["password"] = "rotated-password"
    second = fetch()
    assert first.connection.password.get_secret_value() == "private-password"
    assert second.connection.password.get_secret_value() == "rotated-password"
    assert first.display_name == "Analytics" and first.environment_name == "Development"
    assert first.capabilities["supports_future_feature"] is False
    assert [c[0] for c in calls] == ["POST", "GET", "GET"]
    assert all(c[2]["allow_redirects"] is False for c in calls)
    assert calls[1][2]["headers"]["Authorization"] == "Bearer token-1"
    assert calls[1][2]["timeout"] == (5.0, 5.0)
    visible = repr(first) + repr(first.connection) + first.model_dump_json()
    for value in ("private-password", "private-ca", "private-cert", "private-key", "private-extra"):
        assert value not in visible


def test_runtime_auth_refreshes_once_after_401(runtime, monkeypatch):
    payload, calls, fetch, _ = runtime

    def get(self, url, **kwargs):
        calls.append(("GET", url, kwargs))
        return response(
            401 if kwargs["headers"]["Authorization"] == "Bearer token-1" else 200, payload
        )

    monkeypatch.setattr(requests.Session, "get", get)
    assert str(fetch().uid) == payload["data_source_uid"]
    assert [c[0] for c in calls] == ["POST", "GET", "POST", "GET"]


@pytest.mark.parametrize(
    ("status", "code", "attempts"),
    [
        (401, "runtime_credential_rejected", 2),
        (403, "data_source_runtime_access_denied", 1),
        (404, "data_source_not_available", 1),
        (302, "data_source_unavailable", 1),
        (500, "data_source_unavailable", 1),
    ],
)
def test_denials_and_failures_are_bounded_and_do_not_expose_responses(
    runtime, monkeypatch, status, code, attempts
):
    _, calls, fetch, _ = runtime

    def get(self, url, **kwargs):
        calls.append(("GET", url, kwargs))
        return response(status, {"detail": "private-error"})

    monkeypatch.setattr(requests.Session, "get", get)
    with pytest.raises(DataSourceRuntimeError) as error:
        fetch()
    assert error.value.code == code
    assert "private-error" not in str(error.value)
    assert sum(c[0] == "GET" for c in calls) == attempts


def test_revocation_is_observed_on_next_lookup(runtime, monkeypatch):
    _, _, fetch, _ = runtime
    fetch()
    monkeypatch.setattr(requests.Session, "get", lambda *a, **kw: response(404))
    with pytest.raises(DataSourceRuntimeError, match="data_source_not_available"):
        fetch()


@pytest.mark.parametrize(
    "field", ["data_source_uid", "organization_uid", "organization_environment_uid"]
)
def test_scope_mismatch_is_rejected(runtime, field):
    payload, _, fetch, _ = runtime
    payload[field] = str(uuid4())
    with pytest.raises(DataSourceRuntimeError, match="data_source_scope_mismatch"):
        fetch()


@pytest.mark.parametrize(
    "change",
    [
        lambda p: p["connection"].update(port=True),
        lambda p: p["connection"].update(port=0),
        lambda p: p["connection"].update(host=""),
        lambda p: p["capabilities"].update(supports_table_ddl="true"),
        lambda p: p.update(storage_access_mode="unknown"),
        lambda p: p.pop("connection"),
    ],
)
def test_malformed_material_fails_without_exposing_payload(runtime, change):
    payload, _, fetch, _ = runtime
    change(payload)
    with pytest.raises(DataSourceRuntimeError) as error:
        fetch()
    assert str(error.value) == "invalid_runtime_data_source_response"


def test_invalid_json_has_safe_response_error(runtime, monkeypatch):
    _, _, fetch, _ = runtime
    invalid = response()
    invalid._content = b"private-invalid-json"
    monkeypatch.setattr(requests.Session, "get", lambda *a, **kw: invalid)
    with pytest.raises(DataSourceRuntimeError, match="invalid_runtime_data_source_response"):
        fetch()


def test_outage_has_no_retry_or_exception_detail(runtime, monkeypatch):
    _, calls, fetch, _ = runtime

    def get(*args, **kwargs):
        calls.append(("GET",))
        raise requests.ConnectionError("private-transport-detail")

    monkeypatch.setattr(requests.Session, "get", get)
    with pytest.raises(DataSourceRuntimeError) as error:
        fetch()
    assert str(error.value) == "data_source_unavailable"
    assert sum(c[0] == "GET" for c in calls) == 1


def test_user_auth_cannot_fetch_runtime_credentials(runtime, monkeypatch):
    _, calls, fetch, _ = runtime
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "jwt")
    with pytest.raises(DataSourceRuntimeError, match="runtime_credential_not_configured"):
        fetch()
    assert calls == []


def test_bad_input_scope_is_rejected_before_transport(runtime):
    _, calls, _, _ = runtime
    with pytest.raises(DataSourceRuntimeError, match="invalid_data_source_scope"):
        DataSource.get_runtime_connection(
            "../other", expected_organization_uid=uuid4(), expected_environment_uid=uuid4()
        )
    assert calls == []


def test_core_import_does_not_load_server_or_domain_dependencies():
    script = """
import importlib.abc
import sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'jwt', 'cryptography', 'metatables', 'pandas', 'sqlalchemy', 'alembic', 'duckdb'}:
            raise ImportError(fullname)
sys.meta_path.insert(0, Block())
from mainsequence.client import DataSource
assert not hasattr(DataSource, 'get_or_create_duck_db')
assert not hasattr(DataSource, 'insert_data_into_table')
assert not hasattr(DataSource, 'execute_query')
"""
    subprocess.run(
        [sys.executable, "-c", script], cwd=Path(__file__).resolve().parents[1], check=True
    )
