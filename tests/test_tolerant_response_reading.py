"""ADR 0032: the SDK reads backend responses tolerantly.

A response field this SDK release does not declare is dropped instead of failing
the call, so a backend change the API rules call compatible does not break an
installed SDK. The rule holds for parsing, for a listing, and for the body a
PATCH returns.
"""

from __future__ import annotations

from typing import ClassVar

import pytest

import mainsequence.client.base as base_mod
import mainsequence.client.models_user as models_user_mod
from mainsequence.client.base import BaseObjectOrm, BasePydanticModel

ORGANIZATION_UID = "org_01hxyzabcd1234"
NOTIFICATION_UID = "24001fc7-098c-40fa-b398-1d2352b7c224"


class DemoTolerantModel(BasePydanticModel, BaseObjectOrm):
    ENDPOINT: ClassVar[str] = "demo-tolerant"

    uid: str
    name: str | None = None

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        return "https://backend.test/demo-tolerant"

    @classmethod
    def build_session(cls):
        return object()


class FakeResponse:
    status_code = 200
    content = b"{}"

    def __init__(self, body):
        self._body = body

    def json(self):
        return self._body


def test_base_model_ignores_undeclared_fields():
    assert BasePydanticModel.model_config["extra"] == "ignore"
    assert DemoTolerantModel.model_config["extra"] == "ignore"


def test_undeclared_response_field_is_dropped():
    parsed = DemoTolerantModel.model_validate(
        {"uid": "demo-1", "name": "Demo", "future_field": "value"}
    )

    assert parsed.uid == "demo-1"
    assert parsed.name == "Demo"
    assert "future_field" not in parsed.__dict__


def test_one_undeclared_field_does_not_fail_a_listing(monkeypatch):
    rows = [
        {"uid": "demo-1", "name": "First"},
        {"uid": "demo-2", "name": "Second", "future_field": "value"},
    ]
    monkeypatch.setattr(
        base_mod,
        "make_request",
        lambda **kwargs: FakeResponse(rows),
    )

    listed = DemoTolerantModel.filter()

    assert [item.uid for item in listed] == ["demo-1", "demo-2"]
    assert "future_field" not in listed[1].__dict__


def test_patch_response_with_undeclared_field_updates_the_instance(monkeypatch):
    monkeypatch.setattr(
        base_mod,
        "make_request",
        lambda **kwargs: FakeResponse(
            {"uid": "demo-1", "name": "Patched", "future_field": "value"}
        ),
    )
    instance = DemoTolerantModel(uid="demo-1", name="Demo")

    patched = DemoTolerantModel.patch_by_uid("demo-1", _into=instance, name="Patched")

    assert patched is instance
    assert patched.name == "Patched"
    assert "future_field" not in patched.__dict__


@pytest.mark.parametrize(
    ("model", "payload", "undeclared_fields"),
    [
        pytest.param(
            models_user_mod.Organization,
            {
                "uid": ORGANIZATION_UID,
                "name": "Main Sequence",
                "organization_domain": "main-sequence.io",
                "is_org_admin": True,
                "stats": {"users": 4},
            },
            ("is_org_admin", "stats"),
            id="organization",
        ),
        pytest.param(
            models_user_mod.Notification,
            {
                "uid": NOTIFICATION_UID,
                "title": "Deployment complete",
                "description": "The deployment finished successfully.",
                "id": 101,
                "created_by_user": 4,
                "source_organization": 2,
                "target_team": None,
                "target_user": 7,
            },
            ("id", "created_by_user", "source_organization", "target_team", "target_user"),
            id="notification",
        ),
    ],
)
def test_fields_the_backend_serves_today_parse(model, payload, undeclared_fields):
    """The #120 cases: fields served today that 8.1.19 rejected."""
    parsed = model.model_validate(payload)

    assert parsed.uid == payload["uid"]
    for field_name in undeclared_fields:
        assert field_name not in model.model_fields
        assert field_name not in parsed.__dict__
