import datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

import mainsequence.client.base as base_mod
import mainsequence.client.models_helpers as models_helpers_mod
from tests.client.support import (
    CODE_REPOSITORY_BRANCH_UID,
    _deployment_run_billing_payload,
    _deployment_run_cost_projections,
    _deployment_run_cost_summary_payload,
    _deployment_run_runtime_billing_payload,
    _resource_release_pipeline_payload,
)


@pytest.mark.parametrize(
    "pricing_state",
    ["priced", "partial", "pending", "unavailable", "failed"],
)
def test_deployment_run_billing_parses_every_backend_pricing_state(pricing_state):
    total_cost = "1.250000" if pricing_state in {"priced", "partial"} else None

    billing = models_helpers_mod.DeploymentRunBilling.model_validate(
        _deployment_run_billing_payload(
            pricing_state=pricing_state,
            total_cost=total_cost,
        )
    )

    assert billing.pricing_state == pricing_state
    assert billing.total_cost == (Decimal(total_cost) if total_cost is not None else None)
    assert billing.components.image_build == billing.total_cost


def test_deployment_run_billing_contract_is_complete_and_read_tolerantly():
    payload = _deployment_run_billing_payload()
    for field_name in (
        "scope",
        "total_cost",
        "currency",
        "pricing_state",
        "components",
        "priced_rows",
        "unpriced_rows",
        "reused_image_count",
    ):
        incomplete = dict(payload)
        incomplete.pop(field_name)
        with pytest.raises(ValidationError, match=field_name):
            models_helpers_mod.DeploymentRunBilling.model_validate(incomplete)

    # Tolerant reading (ADR 0032): a billing field or component this release does
    # not declare is dropped instead of failing the whole response.
    assert not hasattr(
        models_helpers_mod.DeploymentRunBilling.model_validate(
            payload | {"future_billing_field": "value"}
        ),
        "future_billing_field",
    )
    assert not hasattr(
        models_helpers_mod.DeploymentRunBilling.model_validate(
            payload | {"components": payload["components"] | {"future_component": "0.000000"}}
        ).components,
        "future_component",
    )

    with pytest.raises(ValidationError, match="greater_than_equal"):
        models_helpers_mod.DeploymentRunBilling.model_validate(payload | {"unpriced_rows": -1})


@pytest.mark.parametrize(
    "pricing_state",
    ["priced", "partial", "pending", "unavailable", "failed"],
)
def test_deployment_run_runtime_billing_parses_every_backend_pricing_state(
    pricing_state,
):
    total_cost = "1.250000" if pricing_state in {"priced", "partial"} else None
    base_cost = "0.750000" if total_cost is not None else None

    billing = models_helpers_mod.DeploymentRunRuntimeBilling.model_validate(
        _deployment_run_runtime_billing_payload(
            pricing_state=pricing_state,
            total_cost=total_cost,
            base_cost=base_cost,
            is_complete=pricing_state == "priced",
        )
    )

    assert billing.pricing_state == pricing_state
    assert billing.total_cost == (Decimal(total_cost) if total_cost is not None else None)
    assert billing.base_cost == (Decimal(base_cost) if base_cost is not None else None)
    assert billing.is_complete is (pricing_state == "priced")


@pytest.mark.parametrize("total_cost", ["2.000000", None])
def test_deployment_run_cost_summary_parses_nullable_decimal(total_cost):
    summary = models_helpers_mod.DeploymentRunCostSummary.model_validate(
        _deployment_run_cost_summary_payload(
            total_cost=total_cost,
            is_complete=total_cost is not None,
        )
    )

    assert summary.total_cost == (Decimal(total_cost) if total_cost is not None else None)
    assert summary.is_complete is (total_cost is not None)


@pytest.mark.parametrize(
    ("model", "payload", "required_fields"),
    [
        (
            models_helpers_mod.DeploymentRunRuntimeBilling,
            _deployment_run_runtime_billing_payload(),
            (
                "scope",
                "total_cost",
                "base_cost",
                "currency",
                "pricing_state",
                "priced_rows",
                "unpriced_rows",
                "is_complete",
            ),
        ),
        (
            models_helpers_mod.DeploymentRunCostSummary,
            _deployment_run_cost_summary_payload(),
            ("total_cost", "currency", "is_complete"),
        ),
    ],
)
def test_deployment_run_additive_billing_contracts_are_complete_and_read_tolerantly(
    model,
    payload,
    required_fields,
):
    for field_name in required_fields:
        incomplete = dict(payload)
        incomplete.pop(field_name)
        with pytest.raises(ValidationError, match=field_name):
            model.model_validate(incomplete)

    # Tolerant reading (ADR 0032): an undeclared billing field is dropped.
    assert not hasattr(
        model.model_validate(payload | {"future_billing_field": "value"}),
        "future_billing_field",
    )


@pytest.mark.parametrize("field_name", ["priced_rows", "unpriced_rows"])
def test_deployment_run_runtime_billing_rejects_negative_counters(field_name):
    with pytest.raises(ValidationError, match="greater_than_equal"):
        models_helpers_mod.DeploymentRunRuntimeBilling.model_validate(
            _deployment_run_runtime_billing_payload() | {field_name: -1}
        )


def test_deployment_run_collection_and_detail_use_unified_resource_release_contract(monkeypatch):
    captured = []
    run_uid = "11111111-1111-4111-8111-111111111111"
    release_uid = "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed"
    response_payload = {
        "uid": run_uid,
        "target_type": "resource_release",
        "target": {"uid": release_uid, "name": "analytics-123", "kind": "fastapi"},
        "code_repository_branch_uid": CODE_REPOSITORY_BRANCH_UID,
        "operation": "build_and_deploy",
        "source": "repository_event",
        "commit_sha": "a" * 40,
        "configuration_revision": 4,
        "state": "succeeded",
        "outcome": "deployed",
        "pipeline": _resource_release_pipeline_payload(),
        "created_at": "2026-07-19T12:00:00Z",
        "started_at": "2026-07-19T12:00:01Z",
        "finished_at": "2026-07-19T12:00:05Z",
        "revision_context": {},
        "trigger_context": {},
        "artifact_context": {},
        "cleanup_context": {},
        "result": {},
        "builder_image": "",
        "builder_runtime": "",
        "logs": {
            "state": "available",
            "url": f"/api/v1/deployment-runs/{run_uid}/logs/",
            "retention_expires_at": None,
        },
        "error": None,
        **_deployment_run_cost_projections(),
    }

    collection_payload = {
        key: value
        for key, value in response_payload.items()
        if key
        not in {
            "revision_context",
            "trigger_context",
            "artifact_context",
            "cleanup_context",
            "result",
            "builder_image",
            "builder_runtime",
        }
    }

    class FakeResponse:
        status_code = 200

        def __init__(self, payload):
            self.payload = payload

        def json(self):
            return self.payload

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured.append({"r_type": r_type, "url": url, "payload": payload})
        if url.rstrip("/") == models_helpers_mod.DeploymentRun.get_object_url():
            return FakeResponse({"results": [dict(collection_payload)], "next": None})
        return FakeResponse(dict(response_payload))

    monkeypatch.setattr(
        models_helpers_mod.DeploymentRun,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    runs = models_helpers_mod.DeploymentRun.filter(
        target_type="resource_release",
        target_uid=release_uid,
    )
    detail = models_helpers_mod.DeploymentRun.get(pk=run_uid)

    assert len(runs) == 1
    assert runs[0].target.uid == release_uid
    assert detail.state == "succeeded"
    assert detail.outcome == "deployed"
    assert len(runs[0].pipeline.steps) == 6
    assert detail.pipeline.current_step_key is None
    assert detail.builder_image == ""
    assert detail.builder_runtime == ""
    assert detail.logs.state == "available"
    assert detail.error is None
    assert runs[0].billing.pricing_state == "priced"
    assert detail.billing.components.image_registry_storage == Decimal("0.000000")
    assert runs[0].runtime_billing.is_complete is True
    assert detail.runtime_billing.base_cost == Decimal("0.000000")
    assert runs[0].cost_summary.total_cost == Decimal("0.000000")
    assert detail.cost_summary.currency == "USD"
    assert captured[0]["payload"] == {
        "params": {
            "code_repository_branch_uid": CODE_REPOSITORY_BRANCH_UID,
            "target_type": "resource_release",
            "target_uid": release_uid,
        }
    }
    assert captured[1]["url"].endswith(f"/deployment-runs/{run_uid}/")


def test_unified_deployment_run_models_and_filters(monkeypatch):
    captured = {}
    run_uid = "11111111-1111-4111-8111-111111111111"
    target_uid = "22222222-2222-4222-8222-222222222222"
    code_repository_branch_uid = "33333333-3333-4333-8333-333333333333"
    run = models_helpers_mod.DeploymentRun.model_validate(
        {
            "uid": run_uid,
            "target_type": "resource_release",
            "target": {"uid": target_uid, "name": "Orders API", "kind": "fastapi"},
            "code_repository_branch_uid": code_repository_branch_uid,
            "operation": "build_and_deploy",
            "source": "repository_event",
            "commit_sha": "a" * 40,
            "configuration_revision": None,
            "state": "running",
            "outcome": "",
            "pipeline": _resource_release_pipeline_payload(
                running_step="build_code_repository_image"
            ),
            "created_at": "2026-07-19T12:00:00Z",
            "started_at": "2026-07-19T12:00:01Z",
            "finished_at": None,
            "builder_image": "us-docker.pkg.dev/platform/static-builder:latest",
            "builder_runtime": "nodejs22",
            "logs": {
                "state": "available",
                "url": f"/api/v1/deployment-runs/{run_uid}/logs/",
                "retention_expires_at": None,
            },
            "error": None,
            **_deployment_run_cost_projections(
                pricing_state="pending",
                total_cost=None,
                is_complete=False,
            ),
        }
    )

    normalized = models_helpers_mod.DeploymentRun._normalize_filter_kwargs(
        {
            "code_repository_branch_uid": f" {code_repository_branch_uid} ",
            "target_type__in": [" resource_release ", " static_site "],
            "state__in": [" running ", " failed "],
        }
    )

    assert run.target.uid == target_uid
    assert run.pipeline.current_step_key == "build_code_repository_image"
    assert run.pipeline.steps[3].state == "running"
    assert run.pipeline.steps[4].state == "pending"
    assert run.builder_image == "us-docker.pkg.dev/platform/static-builder:latest"
    assert run.builder_runtime == "nodejs22"
    assert run.billing.pricing_state == "pending"
    assert run.billing.total_cost is None
    assert run.runtime_billing.pricing_state == "pending"
    assert run.runtime_billing.base_cost is None
    assert run.cost_summary.total_cost is None
    assert run.cost_summary.is_complete is False
    assert normalized == {
        "code_repository_branch_uid": code_repository_branch_uid,
        "target_type__in": ["resource_release", "static_site"],
        "state__in": ["running", "failed"],
    }

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "run_uid": run_uid,
                "start_time": "2026-07-19T12:00:00Z",
                "end_time": "2026-07-19T13:00:00Z",
                "entries": [
                    {
                        "sequence": 1,
                        "timestamp": "2026-07-19T12:00:02Z",
                        "step_uid": None,
                        "source": "orchestrator",
                        "stream": "stdout",
                        "level": "info",
                        "text": "Deployment run entered running",
                    }
                ],
                "sources": [{"source": "orchestrator", "state": "available"}],
                "next_cursor": None,
                "complete": True,
                "retention_expires_at": None,
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured.update(
            {
                "r_type": r_type,
                "url": url,
                "payload": payload,
                "timeout": time_out,
            }
        )
        return FakeResponse()

    monkeypatch.setattr(models_helpers_mod, "make_request", _fake_make_request)

    page = run.get_logs(
        start_time="2026-07-19T12:00:00Z",
        end_time="2026-07-19T13:00:00Z",
        limit=25,
        source="orchestrator",
        event="deployment_run.running",
        timeout=8,
    )

    assert page.complete is True
    assert page.start_time == datetime.datetime(2026, 7, 19, 12, tzinfo=datetime.UTC)
    assert page.entries[0].source == "orchestrator"
    assert captured == {
        "r_type": "GET",
        "url": f"{models_helpers_mod.DeploymentRun.get_object_url()}/{run_uid}/logs/",
        "payload": {
            "params": {
                "start_time": "2026-07-19T12:00:00Z",
                "end_time": "2026-07-19T13:00:00Z",
                "limit": 25,
                "source": "orchestrator",
                "event": "deployment_run.running",
            }
        },
        "timeout": 8,
    }
