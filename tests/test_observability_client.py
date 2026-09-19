from __future__ import annotations

import datetime
import inspect
from typing import ClassVar

import pytest
from pydantic import Field

import mainsequence.client.agent_runtime_models as agent_models
import mainsequence.client.models_helpers as helper_models
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
                "start_time": "2026-09-13T10:00:00Z",
                "end_time": "2026-09-13T11:00:00Z",
                "next_cursor": "opaque-cursor",
                "truncated": True,
                "rows": [
                    {
                        "time": 123000,
                        "occurred_at": "2026-09-13T10:30:00Z",
                        "severity": "INFO",
                        "level": "info",
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
        start_time="2026-09-13T10:00:00Z",
        end_time="2026-09-13T11:00:00Z",
        limit=25,
        level="info",
        timeout=8,
    )

    assert captured["url"] == "https://backend.test/api/v1/demo-owners/owner-uid/logs/"
    assert captured["payload"]["params"] == {
        "organization_environment_uid": ENVIRONMENT_UID,
        "start_time": "2026-09-13T10:00:00Z",
        "end_time": "2026-09-13T11:00:00Z",
        "limit": 25,
        "level": "info",
    }
    assert captured["time_out"] == 8
    assert page.next_cursor == "opaque-cursor"
    assert page.start_time == datetime.datetime(2026, 9, 13, 10, tzinfo=datetime.UTC)
    assert page.rows[0].level == "info"
    assert page.rows[0].model_extra == {"future_enrichment": {"provider": "normalized"}}


def test_owner_logs_keep_deprecated_aliases_and_reject_ambiguous_filters(monkeypatch):
    captured = {}

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {
                "organization_environment_uid": ENVIRONMENT_UID,
                "start": 100,
                "end": 200,
                "start_time": "2026-09-13T10:00:00Z",
                "end_time": "2026-09-13T11:00:00Z",
                "next_cursor": None,
                "truncated": False,
                "rows": [],
            }

    monkeypatch.setattr(
        observability_models,
        "make_request",
        lambda **kwargs: captured.update(kwargs) or Response(),
    )

    _owner().get_logs(start=100, end=200, severity="ERROR")

    assert captured["payload"]["params"] == {
        "organization_environment_uid": ENVIRONMENT_UID,
        "start": 100,
        "end": 200,
        "severity": "ERROR",
    }
    with pytest.raises(ValueError, match="either start_time"):
        _owner().get_logs(start_time="2026-09-13T10:00:00Z", start=100)
    with pytest.raises(ValueError, match="supplied together"):
        _owner().get_logs(start_time="2026-09-13T10:00:00Z")
    with pytest.raises(ValueError, match="either level"):
        _owner().get_logs(level="error", severity="ERROR")


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


def test_job_run_logs_accept_backend_derived_environment_without_weakening_usage(
    monkeypatch,
):
    captured = {}
    job_run_uid = "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d"
    job_run = helper_models.JobRun.model_construct(
        uid=job_run_uid,
        observability=ObservabilityLinks(
            application_logs_url=f"/api/v1/job-runs/{job_run_uid}/logs/",
            resource_usage_url=f"/api/v1/job-runs/{job_run_uid}/resource-usage/",
        ),
    )

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {
                "organization_environment_uid": ENVIRONMENT_UID,
                "start": 100,
                "end": 200,
                "start_time": "1970-01-01T00:01:40Z",
                "end_time": "1970-01-01T00:03:20Z",
                "next_cursor": None,
                "truncated": False,
                "rows": [],
            }

    monkeypatch.setattr(helper_models.JobRun, "ROOT_URL", "https://backend.test/api/v1")
    monkeypatch.setattr(
        observability_models,
        "make_request",
        lambda **kwargs: captured.update(kwargs) or Response(),
    )

    page = job_run.get_logs(start=100, end=200, limit=25)

    assert captured["payload"]["params"] == {
        "start": 100,
        "end": 200,
        "limit": 25,
    }
    assert page.organization_environment_uid == ENVIRONMENT_UID
    with pytest.raises(ApiError, match="missing organization_environment_uid"):
        job_run.get_resource_usage()


def test_agent_logs_support_optional_session_filter_without_environment_override(
    monkeypatch,
):
    captured = {}
    agent = agent_models.Agent(
        uid="e0e75693-4110-464c-93e0-82c7fd9c9a23",
        name="Research Copilot",
        description="Research assistant.",
        agent_card={"name": "Research Copilot", "description": "Research assistant."},
        llm_thinking="medium",
        repository_branch=None,
        organization_environment_uid=ENVIRONMENT_UID,
        organization_environment_name="Development",
        runtime_update={
            "state": "current",
            "needs_redeploy": False,
            "remediation": None,
        },
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
                "start_time": "1970-01-01T00:01:40Z",
                "end_time": "1970-01-01T00:03:20Z",
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
    assert observability_models.EnvironmentLogSearchPage.model_config["extra"] == "forbid"
    assert observability_models.ResourceUsagePage.model_config["extra"] == "forbid"
    assert observability_models.OwnerLogRow.model_config["extra"] == "allow"
    assert observability_models.EnvironmentLogSearchRow.model_config["extra"] == "allow"


@pytest.mark.parametrize(
    ("model", "owner_type", "endpoint", "family_filters"),
    [
        (
            helper_models.DeploymentRun,
            "deployment_run",
            "deployment-runs",
            {
                "deployment_run_uid": "11111111-1111-4111-8111-111111111111",
                "target_type": "resource_release",
                "target_uid": "22222222-2222-4222-8222-222222222222",
                "step_uid": "33333333-3333-4333-8333-333333333333",
                "source": "orchestrator",
            },
        ),
        (
            helper_models.JobRun,
            "job_run",
            "job-runs",
            {
                "job_run_uid": "44444444-4444-4444-8444-444444444444",
                "job_uid": "55555555-5555-4555-8555-555555555555",
                "request_id": "request-1",
                "outcome": "failed",
            },
        ),
        (
            helper_models.ResourceRelease,
            "resource_release",
            "resource-releases",
            {
                "resource_release_uid": "66666666-6666-4666-8666-666666666666",
                "request_id": "request-2",
                "outcome": "failed",
            },
        ),
        (
            agent_models.Agent,
            "agent",
            "agents",
            {
                "agent_uid": "77777777-7777-4777-8777-777777777777",
                "agent_session_uid": "88888888-8888-4888-8888-888888888888",
                "request_id": "request-3",
                "outcome": "failed",
            },
        ),
        (
            agent_models.AgentSession,
            "agent_session",
            "agent-sessions",
            {
                "agent_session_uid": "99999999-9999-4999-8999-999999999999",
                "agent_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                "request_id": "request-4",
                "outcome": "failed",
            },
        ),
    ],
)
def test_environment_collection_log_clients_use_adr060_routes_and_filters(
    monkeypatch,
    model,
    owner_type,
    endpoint,
    family_filters,
):
    captured = {}
    start_time = datetime.datetime(2026, 9, 13, 10, tzinfo=datetime.UTC)
    end_time = datetime.datetime(2026, 9, 13, 11, tzinfo=datetime.UTC)

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {
                "organization_environment_uid": ENVIRONMENT_UID,
                "start_time": "2026-09-13T10:00:00Z",
                "end_time": "2026-09-13T11:00:00Z",
                "limit": 25,
                "returned_count": 1,
                "cumulative_returned_count": 1,
                "result_limit": 10_000,
                "next_cursor": "opaque-cursor",
                "truncated": True,
                "truncation_reason": "page_limit",
                "rows": [
                    {
                        "owner_type": owner_type,
                        "owner_uid": next(
                            value for key, value in family_filters.items() if key.endswith("_uid")
                        ),
                        "occurred_at": "2026-09-13T10:30:00Z",
                        "level": "error",
                        "message": "failed",
                        "family_projection": "preserved",
                    }
                ],
            }

    monkeypatch.setattr(model, "ROOT_URL", "https://backend.test/api/v1")
    monkeypatch.setattr(
        observability_models,
        "make_request",
        lambda **kwargs: captured.update(kwargs) or Response(),
    )

    page = model.search_logs(
        organization_environment_uid=ENVIRONMENT_UID,
        start_time=start_time,
        end_time=end_time,
        cursor="previous-cursor",
        limit=25,
        level="error",
        event="request.failed",
        timeout=8,
        **family_filters,
    )

    assert captured["r_type"] == "GET"
    assert captured["url"] == f"https://backend.test/api/v1/{endpoint}/logs/"
    assert captured["payload"] == {
        "params": {
            "organization_environment_uid": ENVIRONMENT_UID,
            "start_time": "2026-09-13T10:00:00+00:00",
            "end_time": "2026-09-13T11:00:00+00:00",
            "cursor": "previous-cursor",
            "limit": 25,
            "level": "error",
            "event": "request.failed",
            **family_filters,
        }
    }
    assert captured["time_out"] == 8
    assert page.returned_count == 1
    assert page.rows[0].owner_type == owner_type
    assert page.rows[0].model_extra == {"family_projection": "preserved"}


def test_environment_collection_log_clients_require_explicit_timezone():
    with pytest.raises(ValueError, match="explicit timezone"):
        helper_models.JobRun.search_logs(
            organization_environment_uid=ENVIRONMENT_UID,
            start_time=datetime.datetime(2026, 9, 13, 10),
            end_time=datetime.datetime(2026, 9, 13, 11, tzinfo=datetime.UTC),
        )
