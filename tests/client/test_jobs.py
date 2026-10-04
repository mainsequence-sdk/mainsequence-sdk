import pytest

import mainsequence.client.base as base_mod
import mainsequence.client.models_helpers as models_helpers_mod
from tests.client.support import CODE_REPOSITORY_BRANCH_UID, ENVIRONMENT_UID


def test_job_run_status_uses_status_detail_endpoint(monkeypatch):
    job_run = models_helpers_mod.JobRun(
        uid="4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
        name="demo-run",
        unique_identifier="jobrun_501",
        code_repository_uid=None,
        code_repository_name=None,
        code_repository_branch_uid=None,
        code_repository_branch_name=None,
        organization_environment_uid=ENVIRONMENT_UID,
        runtime_image_uid="6cfdb152-923e-45b9-a150-c4541c68b0d1",
        runtime_image_digest="sha256:" + "b" * 64,
    )

    captured: dict[str, object] = {}

    class _FakeResponse:
        status_code = 200

        def json(self):
            return {"message": "Job status updated to RUNNING."}

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out):
        captured["url"] = url
        captured["r_type"] = r_type
        captured["payload"] = payload
        return _FakeResponse()

    monkeypatch.setattr(models_helpers_mod, "make_request", _fake_make_request)

    payload = job_run.job_run_status(status="RUNNING", git_hash="abc123", timeout=30)

    assert payload == {"message": "Job status updated to RUNNING."}
    assert captured["r_type"] == "POST"
    assert captured["payload"] == {"status": "RUNNING", "git_hash": "abc123"}
    assert str(captured["url"]).endswith(
        "/api/v1/job-runs/4c1d77c8-8a42-42b8-a9c1-06be9a336e5d/status/"
    )


def test_job_run_filters_are_uid_based():
    normalized = models_helpers_mod.JobRun._normalize_filter_kwargs(
        {
            "uid": " 4c1d77c8-8a42-42b8-a9c1-06be9a336e5d ",
            "job__uid__in": [
                " ab6a5d50-8a3e-4f0d-a9bb-7e84180bd50e ",
            ],
        }
    )

    assert normalized == {
        "uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
        "job__uid__in": ["ab6a5d50-8a3e-4f0d-a9bb-7e84180bd50e"],
    }
    with pytest.raises(ValueError, match="job__id"):
        models_helpers_mod.JobRun._normalize_filter_kwargs({"job__id": [501]})


def test_job_run_uid_filters_send_canonical_query_parameters(monkeypatch):
    captured_params = []

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"results": [], "next": None}

    def _fake_make_request(**kwargs):
        captured_params.append(kwargs["payload"]["params"])
        return FakeResponse()

    monkeypatch.setattr(
        models_helpers_mod.JobRun,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    first_uid = "ab6a5d50-8a3e-4f0d-a9bb-7e84180bd50e"
    second_uid = "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da"

    assert models_helpers_mod.JobRun.filter(job__uid=first_uid) == []
    assert models_helpers_mod.JobRun.filter(job__uid__in=[first_uid, second_uid]) == []
    assert captured_params == [
        {"job__uid": first_uid},
        {"job__uid__in": f"{first_uid},{second_uid}"},
    ]


def test_job_run_deserializes_uid_payload_without_id():
    job_run = models_helpers_mod.JobRun(
        uid="4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
        name="demo-run",
        unique_identifier="jobrun_501",
        job_uid="ab6a5d50-8a3e-4f0d-a9bb-7e84180bd50e",
        job_name="daily-training-job",
        code_repository_uid="1d0530c0-65d1-4db0-856b-dc29d8260a09",
        code_repository_name="market-data-service",
        code_repository_branch_uid="5a28020a-0f1b-47ee-aab8-334286234bea",
        code_repository_branch_name="main",
        organization_environment_uid=ENVIRONMENT_UID,
        status="RUNNING",
        cpu_request="1",
        cpu_limit="2",
        memory_request="4Gi",
        memory_limit="8Gi",
        gpu_request="1",
        gpu_type="nvidia-l4",
        runtime_image_uid="6cfdb152-923e-45b9-a150-c4541c68b0d1",
        runtime_image_digest="sha256:" + "b" * 64,
        command_args=["sync"],
    )

    dumped = job_run.model_dump()
    assert dumped["uid"] == "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d"
    assert dumped["job_uid"] == "ab6a5d50-8a3e-4f0d-a9bb-7e84180bd50e"
    assert dumped["organization_environment_uid"] == ENVIRONMENT_UID
    assert "id" not in dumped


def test_job_requires_exact_image_and_exposes_automatic_redeployment_state():
    with pytest.raises(ValueError, match="related_image_uid"):
        models_helpers_mod.Job.model_validate(
            {
                "name": "Daily prices",
                "image_status": "ready",
            }
        )

    job = models_helpers_mod.Job.model_validate(
        {
            "uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
            "name": "Daily prices",
            "code_repository_branch_uid": "5a28020a-0f1b-47ee-aab8-334286234bea",
            "related_image_uid": "6cfdb152-923e-45b9-a150-c4541c68b0d1",
            "code_repository_commit_hash": "a" * 40,
            "image_status": "ready",
            "automatic_deployment": True,
            "automatic_redeployment_policy": {
                "tag_regex": "^v[0-9]+$",
                "policy_revision": 4,
            },
        }
    )

    assert job.related_image_uid == "6cfdb152-923e-45b9-a150-c4541c68b0d1"
    assert job.code_repository_commit_hash == "a" * 40
    assert job.image_status == "ready"
    assert job.automatic_deployment is True
    assert job.automatic_redeployment_policy is not None
    assert job.automatic_redeployment_policy.policy_revision == 4


@pytest.mark.parametrize("environment_uid", [None, ENVIRONMENT_UID])
def test_job_accepts_backend_derived_environment_uid(environment_uid):
    job = models_helpers_mod.Job.model_validate(
        {
            "uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
            "name": "Daily prices",
            "code_repository_branch_uid": CODE_REPOSITORY_BRANCH_UID,
            "organization_environment_uid": environment_uid,
            "related_image_uid": "6cfdb152-923e-45b9-a150-c4541c68b0d1",
            "code_repository_commit_hash": "a" * 40,
            "image_status": "ready",
        }
    )

    assert job.organization_environment_uid == environment_uid


def test_job_filter_parses_backend_derived_environment_uid(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return [
                {
                    "uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
                    "name": "Daily prices",
                    "code_repository_branch_uid": CODE_REPOSITORY_BRANCH_UID,
                    "organization_environment_uid": ENVIRONMENT_UID,
                    "related_image_uid": "6cfdb152-923e-45b9-a150-c4541c68b0d1",
                    "code_repository_commit_hash": "a" * 40,
                    "image_status": "ready",
                }
            ]

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", fake_make_request)

    jobs = models_helpers_mod.Job.filter()

    assert jobs[0].organization_environment_uid == ENVIRONMENT_UID
    assert captured["payload"]["params"] == {
        "code_repository_branch_uid": CODE_REPOSITORY_BRANCH_UID,
    }


def test_job_run_requires_immutable_runtime_image_snapshot():
    payload = {
        "uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
        "name": "daily-prices-run",
        "unique_identifier": "jobrun_2026_08_19_abc123",
        "code_repository_uid": "1d0530c0-65d1-4db0-856b-dc29d8260a09",
        "code_repository_name": "market-data-service",
        "code_repository_branch_uid": "5a28020a-0f1b-47ee-aab8-334286234bea",
        "code_repository_branch_name": "main",
        "organization_environment_uid": ENVIRONMENT_UID,
        "commit_hash": "a" * 40,
        "runtime_image_uid": "6cfdb152-923e-45b9-a150-c4541c68b0d1",
        "runtime_image_digest": "sha256:" + "b" * 64,
    }
    run = models_helpers_mod.JobRun.model_validate(payload)

    assert run.runtime_image_uid == payload["runtime_image_uid"]
    assert run.runtime_image_digest == payload["runtime_image_digest"]
    assert run.commit_hash == payload["commit_hash"]
    assert run.code_repository_uid == payload["code_repository_uid"]
    assert run.code_repository_name == payload["code_repository_name"]
    assert run.code_repository_branch_uid == payload["code_repository_branch_uid"]
    assert run.code_repository_branch_name == payload["code_repository_branch_name"]
    assert run.organization_environment_uid == ENVIRONMENT_UID

    missing_code_repository_context = dict(payload)
    missing_code_repository_context.pop("code_repository_uid")
    with pytest.raises(ValueError, match="code_repository_uid"):
        models_helpers_mod.JobRun.model_validate(missing_code_repository_context)

    with pytest.raises(ValueError, match="runtime_image_uid"):
        models_helpers_mod.JobRun.model_validate(
            {
                "name": "bad-run",
                "unique_identifier": "jobrun_bad",
                "code_repository_uid": None,
                "code_repository_name": None,
                "code_repository_branch_uid": None,
                "code_repository_branch_name": None,
                "organization_environment_uid": ENVIRONMENT_UID,
                "runtime_image_digest": "sha256:" + "b" * 64,
            }
        )
