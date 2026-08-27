from __future__ import annotations

import inspect
from typing import ClassVar

import pytest
from pydantic import Field

import mainsequence.client.agent_runtime_models as agent_models
import mainsequence.client.observability as observability_models
from mainsequence.client.base import BaseObjectOrm, BasePydanticModel
from mainsequence.client.exceptions import ApiError
from mainsequence.client.observability import (
    ObservabilityLinks,
    OwnerLogMixin,
    OwnerResourceUsageMixin,
)

ENVIRONMENT_UID = "58218213-5e4e-43de-a5bd-6757f4e1c8f6"


class DemoOwner(
    OwnerLogMixin,
    OwnerResourceUsageMixin,
    BaseObjectOrm,
    BasePydanticModel,
):
    ROOT_URL: ClassVar[str] = "https://backend.test/api/v1"
    ENDPOINT: ClassVar[str] = "demo-owners"

    uid: str
    observability: ObservabilityLinks | None = Field(default=None)


def _owner(**link_overrides):
    links = {
        "application_logs_url": (
            f"/api/v1/demo-owners/owner-uid/logs/?organization_environment_uid={ENVIRONMENT_UID}"
        ),
        "resource_usage_url": (
            f"/api/v1/demo-owners/owner-uid/resource-usage/"
            f"?organization_environment_uid={ENVIRONMENT_UID}"
        ),
        "deployment_runs_url": None,
        "sessions_url": None,
        **link_overrides,
    }
    return DemoOwner(uid="owner-uid", observability=links)


def test_owner_logs_follow_authenticated_backend_capability_and_preserve_enrichment(
    monkeypatch,
):
    captured = {}

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {
                "organization_environment_uid": ENVIRONMENT_UID,
                "start": 100,
                "end": 200,
                "next_cursor": "opaque-cursor",
                "truncated": True,
                "rows": [
                    {
                        "time": 123000,
                        "severity": "INFO",
                        "message": "Completed",
                        "future_enrichment": {"provider": "normalized"},
                    }
                ],
            }

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return Response()

    monkeypatch.setattr(observability_models, "make_request", fake_make_request)

    page = _owner().get_logs(
        start=100,
        end=200,
        limit=25,
        severity="INFO",
        timeout=8,
    )

    assert captured["url"] == "https://backend.test/api/v1/demo-owners/owner-uid/logs/"
    assert captured["payload"]["params"] == {
        "organization_environment_uid": ENVIRONMENT_UID,
        "start": 100,
        "end": 200,
        "limit": 25,
        "severity": "INFO",
    }
    assert captured["time_out"] == 8
    assert page.next_cursor == "opaque-cursor"
    assert page.rows[0].model_extra == {"future_enrichment": {"provider": "normalized"}}


def test_owner_resource_usage_uses_distinct_strict_contract(monkeypatch):
    captured = {}

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {
                "start": 100,
                "end": 200,
                "step_seconds": 30,
                "summary": {
                    "cpu_cores_current": 0.5,
                    "cpu_cores_peak": 1.0,
                    "memory_gib_current": 2.0,
                    "memory_gib_peak": 3.0,
                    "disk_gib_current": None,
                    "disk_gib_peak": None,
                },
                "rows": [
                    {
                        "time": 123000,
                        "cpu_cores": 0.5,
                        "memory_gib": 2.0,
                        "disk_gib": None,
                    }
                ],
            }

    monkeypatch.setattr(
        observability_models,
        "make_request",
        lambda **kwargs: captured.update(kwargs) or Response(),
    )

    page = _owner().get_resource_usage(start=100, end=200)

    assert captured["payload"]["params"] == {
        "organization_environment_uid": ENVIRONMENT_UID,
        "start": 100,
        "end": 200,
    }
    assert page.summary.cpu_cores_peak == 1.0
    assert page.rows[0].memory_gib == 2.0


@pytest.mark.parametrize(
    "url",
    [
        "https://attacker.test/api/v1/demo-owners/owner-uid/logs/"
        f"?organization_environment_uid={ENVIRONMENT_UID}",
        f"//attacker.test/api/v1/logs/?organization_environment_uid={ENVIRONMENT_UID}",
        f"/api/v1/../admin/?organization_environment_uid={ENVIRONMENT_UID}",
        "/api/v1/demo-owners/owner-uid/logs/",
    ],
)
def test_owner_observability_rejects_untrusted_or_unscoped_links(url):
    owner = _owner(application_logs_url=url)

    with pytest.raises(ApiError):
        owner.get_logs()


def test_agent_logs_support_optional_session_filter_without_environment_override(
    monkeypatch,
):
    captured = {}
    agent = agent_models.Agent(
        uid="e0e75693-4110-464c-93e0-82c7fd9c9a23",
        name="Research Copilot",
        llm_thinking="medium",
        repository_branch=None,
        organization_environment_uid=ENVIRONMENT_UID,
        organization_environment_name="Development",
        observability={
            "application_logs_url": (
                "/api/v1/agents/e0e75693-4110-464c-93e0-82c7fd9c9a23/logs/"
                f"?organization_environment_uid={ENVIRONMENT_UID}"
            )
        },
    )

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {
                "organization_environment_uid": ENVIRONMENT_UID,
                "start": 100,
                "end": 200,
                "next_cursor": None,
                "truncated": False,
                "rows": [],
            }

    monkeypatch.setattr(agent_models.Agent, "ROOT_URL", "https://backend.test/api/v1")
    monkeypatch.setattr(
        observability_models,
        "make_request",
        lambda **kwargs: captured.update(kwargs) or Response(),
    )

    agent.get_logs(agent_session_uid="session-uid")

    assert captured["payload"]["params"]["agent_session_uid"] == "session-uid"
    assert "organization_environment_uid" not in inspect.signature(agent.get_logs).parameters
    assert (
        "agent_session_uid" not in inspect.signature(agent_models.AgentSession.get_logs).parameters
    )


def test_owner_observability_models_remain_strict_except_enriched_rows():
    assert observability_models.ObservabilityLinks.model_config["extra"] == "forbid"
    assert observability_models.OwnerLogPage.model_config["extra"] == "forbid"
    assert observability_models.ResourceUsagePage.model_config["extra"] == "forbid"
    assert observability_models.OwnerLogRow.model_config["extra"] == "allow"
