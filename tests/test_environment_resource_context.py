from __future__ import annotations

import datetime

import pytest

import mainsequence.client.base as base_module
import mainsequence.client.models_foundry as models_module
import mainsequence.project_context as project_context

ENVIRONMENT_UID = "58218213-5e4e-43de-a5bd-6757f4e1c8f6"


class _Response:
    def __init__(self, payload, *, status_code=200, content=b"{}"):
        self._payload = payload
        self.status_code = status_code
        self.content = content
        self.text = ""

    def json(self):
        return self._payload


@pytest.fixture(autouse=True)
def _resolved_environment(monkeypatch):
    monkeypatch.setattr(
        project_context,
        "resolve_organization_environment_uid",
        lambda operation: ENVIRONMENT_UID,
    )


def _secret_payload():
    return {
        "uid": "498d499f-b74c-43f7-acf1-2e2955ad0e6b",
        "name": "API_KEY",
        "organization_environment_uid": ENVIRONMENT_UID,
        "organization_environment_name": "Development",
    }


def test_environment_resource_reads_use_sdk_owned_scope(monkeypatch):
    requests = []

    def fake_make_request(**kwargs):
        requests.append(kwargs)
        if kwargs["url"].rstrip("/").endswith("secrets"):
            return _Response([_secret_payload()])
        return _Response(_secret_payload())

    monkeypatch.setattr(base_module, "make_request", fake_make_request)

    listed = models_module.Secret.filter(name="API_KEY")
    detail = models_module.Secret.get_by_uid(_secret_payload()["uid"])

    assert listed[0].organization_environment_uid == ENVIRONMENT_UID
    assert detail.organization_environment_name == "Development"
    assert requests[0]["payload"]["params"] == {
        "name": "API_KEY",
        "organization_environment_uid": ENVIRONMENT_UID,
    }
    assert requests[1]["payload"]["params"] == {
        "organization_environment_uid": ENVIRONMENT_UID,
    }


def test_environment_resource_create_delete_and_detail_actions_are_scoped(monkeypatch):
    requests = []

    def fake_make_request(**kwargs):
        requests.append(kwargs)
        if kwargs["r_type"] == "DELETE":
            return _Response(None, status_code=204, content=b"")
        if kwargs["url"].endswith("/can-view/"):
            return _Response({"ok": True})
        return _Response(_secret_payload(), status_code=201)

    monkeypatch.setattr(base_module, "make_request", fake_make_request)

    secret = models_module.Secret.create(name="API_KEY", value="secret")
    secret.delete()
    secret._request_detail_action(r_type="GET", action_name="can-view")

    assert requests[0]["payload"]["json"] == {
        "name": "API_KEY",
        "value": "secret",
        "organization_environment_uid": ENVIRONMENT_UID,
    }
    assert requests[1]["payload"]["params"] == {
        "organization_environment_uid": ENVIRONMENT_UID,
    }
    assert requests[2]["payload"]["params"] == {
        "organization_environment_uid": ENVIRONMENT_UID,
    }


def test_environment_resource_rejects_caller_scope_override():
    with pytest.raises(ValueError, match="cannot override SDK-resolved context"):
        models_module.Secret.filter(
            organization_environment_uid=(
                "11111111-1111-4111-8111-111111111111"
            )
        )


def test_artifact_upload_uses_sdk_owned_environment(monkeypatch, tmp_path):
    captured = {}
    artifact_path = tmp_path / "report.pdf"
    artifact_path.write_bytes(b"report")

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return _Response(
            {
                "uid": "44444444-4444-4444-8444-444444444444",
                "name": "report.pdf",
                "bucket_name": "default_bucket",
                "bucket_uid": "33333333-3333-4333-8333-333333333333",
                "content": "https://signed.example/report.pdf",
                "creation_date": "2026-01-01T00:00:00Z",
                "organization_environment_uid": ENVIRONMENT_UID,
                "organization_environment_name": "Development",
            },
            status_code=201,
        )

    monkeypatch.setattr(models_module, "make_request", fake_make_request)

    artifact = models_module.Artifact.upload_file(
        artifact_path,
        "report.pdf",
    )

    assert artifact.organization_environment_uid == ENVIRONMENT_UID
    assert captured["payload"]["json"] == {
        "name": "report.pdf",
        "bucket_name": "default_bucket",
        "organization_environment_uid": ENVIRONMENT_UID,
    }


def test_environment_resource_models_accept_canonical_read_only_projections():
    bucket = models_module.Bucket(
        uid="33333333-3333-4333-8333-333333333333",
        name="default_bucket",
        organization_environment_uid=ENVIRONMENT_UID,
        organization_environment_name="Development",
        physical_bucket_uid="55555555-5555-4555-8555-555555555555",
        backing_ready=True,
        backing_last_discovered_at=datetime.datetime(
            2026,
            8,
            23,
            tzinfo=datetime.UTC,
        ),
    )
    constant = models_module.Constant(
        uid="22222222-2222-4222-8222-222222222222",
        name="APP__MODE",
        value="production",
        organization_environment_uid=ENVIRONMENT_UID,
        organization_environment_name="Development",
    )

    assert bucket.backing_ready is True
    assert bucket.organization_environment_uid == ENVIRONMENT_UID
    assert constant.organization_environment_name == "Development"
