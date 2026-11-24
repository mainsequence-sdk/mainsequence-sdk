import importlib
import pathlib
import sys
import types

import pytest

pytestmark = pytest.mark.usefixtures("isolated_sdk_imports")


def _load_models_helpers_module():
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    pkg_root = repo_root / "mainsequence"
    client_root = pkg_root / "client"

    for name in (
        "mainsequence.client.models_helpers",
        "mainsequence.client.models_foundry",
        "mainsequence.client.base",
        "mainsequence.client.utils",
        "mainsequence.client",
        "mainsequence.logconf",
        "mainsequence",
    ):
        sys.modules.pop(name, None)

    class _FakeLogger:
        def bind(self, **kwargs):
            return self

        def debug(self, *args, **kwargs):
            return None

        def info(self, *args, **kwargs):
            return None

        def warning(self, *args, **kwargs):
            return None

        def error(self, *args, **kwargs):
            return None

        def exception(self, *args, **kwargs):
            return None

    fake_logger = _FakeLogger()

    pkg = types.ModuleType("mainsequence")
    pkg.__path__ = [str(pkg_root)]
    pkg.logger = fake_logger
    sys.modules["mainsequence"] = pkg

    logconf = types.ModuleType("mainsequence.logconf")
    logconf.logger = fake_logger
    sys.modules["mainsequence.logconf"] = logconf

    subpkg = types.ModuleType("mainsequence.client")
    subpkg.__path__ = [str(client_root)]
    sys.modules["mainsequence.client"] = subpkg

    return importlib.import_module("mainsequence.client.models_helpers")


def test_removed_job_batch_sync_is_not_exposed():
    models_helpers = _load_models_helpers_module()

    assert not hasattr(models_helpers.Job, "bulk_get_or_create")


def test_job_run_job_posts_to_canonical_action(monkeypatch):
    models_helpers = _load_models_helpers_module()
    Job = models_helpers.Job
    captured = {}

    class FakeResponse:
        status_code = 202

        @staticmethod
        def json():
            return {
                "job_run_uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
                "status": "QUEUED",
            }

    monkeypatch.setattr(Job, "build_session", classmethod(lambda cls: object()))
    monkeypatch.setattr(
        Job,
        "get_object_url",
        classmethod(lambda cls: "https://backend.test/api/v1/jobs"),
    )

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured.update(
            r_type=r_type,
            url=url,
            payload=payload,
            timeout=time_out,
        )
        return FakeResponse()

    monkeypatch.setattr(models_helpers, "make_request", _fake_make_request)

    job = Job(
        uid="7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
        name="Simulated Prices",
        code_repository_branch_uid="5a28020a-0f1b-47ee-aab8-334286234bea",
        execution_path="scripts/simulated_prices_launcher.py",
        related_image_uid="f3cb8477-df47-49cb-a151-80b746fb1243",
        image_status="ready",
    )
    out = job.run_job(timeout=30, command_args=["--name", "demo-from-cli"])

    assert captured == {
        "r_type": "POST",
        "url": (
            "https://backend.test/api/v1/jobs/" "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da/run-job/"
        ),
        "payload": {"json": {"command_args": ["--name", "demo-from-cli"]}},
        "timeout": 30,
    }
    assert out == {
        "job_run_uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
        "status": "QUEUED",
    }


def _run_job_and_capture_payload(monkeypatch, **run_job_kwargs):
    models_helpers = _load_models_helpers_module()
    Job = models_helpers.Job
    captured = {}

    class FakeResponse:
        status_code = 202

        @staticmethod
        def json():
            return {
                "message": "Job run requested.",
                "job_run_uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
            }

    monkeypatch.setattr(Job, "build_session", classmethod(lambda cls: object()))
    monkeypatch.setattr(
        Job,
        "get_object_url",
        classmethod(lambda cls: "https://backend.test/api/v1/jobs"),
    )

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured.update(payload=payload)
        return FakeResponse()

    monkeypatch.setattr(models_helpers, "make_request", _fake_make_request)

    job = Job(
        uid="7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
        name="Simulated Prices",
        code_repository_branch_uid="5a28020a-0f1b-47ee-aab8-334286234bea",
        execution_path="scripts/simulated_prices_launcher.py",
        cpu_request="2",
        memory_request="8",
        related_image_uid="f3cb8477-df47-49cb-a151-80b746fb1243",
        image_status="ready",
    )
    job.run_job(**run_job_kwargs)
    return captured["payload"]


def test_job_run_job_sends_the_compute_given_for_the_run(monkeypatch):
    payload = _run_job_and_capture_payload(
        monkeypatch,
        command_args=["--family", "jobs"],
        cpu_request="8",
        memory_request="52",
    )

    assert payload == {
        "json": {
            "command_args": ["--family", "jobs"],
            "cpu_request": "8",
            "memory_request": "52",
        }
    }


def test_job_run_job_sends_compute_without_command_args(monkeypatch):
    payload = _run_job_and_capture_payload(
        monkeypatch,
        cpu_request=0.5,
        memory_request=2,
        gpu_request=1,
        gpu_type="nvidia-l4",
    )

    assert payload == {
        "json": {
            "cpu_request": 0.5,
            "memory_request": 2,
            "gpu_request": 1,
            "gpu_type": "nvidia-l4",
        }
    }


def test_job_run_job_does_not_check_the_compute_it_sends(monkeypatch):
    payload = _run_job_and_capture_payload(monkeypatch, cpu_request="500m", gpu_request="9")

    assert payload == {"json": {"cpu_request": "500m", "gpu_request": "9"}}


@pytest.mark.parametrize(
    ("run_job_kwargs", "expected_payload"),
    [
        ({}, {}),
        ({"command_args": ["--name", "demo"]}, {"json": {"command_args": ["--name", "demo"]}}),
        ({"command_args": []}, {"json": {"command_args": []}}),
        (
            {
                "command_args": ["--name", "demo"],
                "cpu_request": None,
                "memory_request": None,
                "gpu_request": None,
                "gpu_type": None,
            },
            {"json": {"command_args": ["--name", "demo"]}},
        ),
    ],
)
def test_job_run_job_sends_no_compute_when_none_is_given(
    monkeypatch, run_job_kwargs, expected_payload
):
    payload = _run_job_and_capture_payload(monkeypatch, **run_job_kwargs)

    assert payload == expected_payload


@pytest.mark.parametrize(
    "command_args",
    ["--name demo", ("--name", "demo"), ["--name", 1]],
)
def test_job_run_job_rejects_non_list_string_argv(command_args):
    models_helpers = _load_models_helpers_module()
    Job = models_helpers.Job
    job = Job(
        uid="7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
        name="Simulated Prices",
        code_repository_branch_uid="5a28020a-0f1b-47ee-aab8-334286234bea",
        execution_path="scripts/simulated_prices_launcher.py",
        related_image_uid="f3cb8477-df47-49cb-a151-80b746fb1243",
        image_status="ready",
    )

    with pytest.raises(TypeError, match="command_args must be a list of strings"):
        job.run_job(command_args=command_args)


def test_job_models_scheduled_command_args_from_read_and_patch_serializers(monkeypatch):
    models_helpers = _load_models_helpers_module()
    Job = models_helpers.Job
    job_uid = "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da"
    scheduled_args = ["--start-date", "2026-09-08T16:43:00Z", "--family", "jobs"]
    captured = {}

    job = Job(
        uid=job_uid,
        name="Simulated Prices",
        code_repository_branch_uid="5a28020a-0f1b-47ee-aab8-334286234bea",
        execution_path="scripts/simulated_prices_launcher.py",
        scheduled_command_args=scheduled_args,
        related_image_uid="f3cb8477-df47-49cb-a151-80b746fb1243",
        image_status="ready",
    )
    assert job.scheduled_command_args == scheduled_args

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "uid": job_uid,
                "name": "Simulated Prices",
                "code_repository_branch_uid": "5a28020a-0f1b-47ee-aab8-334286234bea",
                "execution_path": "scripts/simulated_prices_launcher.py",
                "scheduled_command_args": scheduled_args,
                "related_image_uid": "f3cb8477-df47-49cb-a151-80b746fb1243",
                "image_status": "ready",
            }

    monkeypatch.setattr(Job, "build_session", classmethod(lambda cls: object()))
    monkeypatch.setattr(
        Job,
        "get_object_url",
        classmethod(lambda cls: "https://backend.test/api/v1/jobs"),
    )

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured.update(r_type=r_type, url=url, payload=payload, timeout=time_out)
        return FakeResponse()

    monkeypatch.setattr(
        sys.modules["mainsequence.client.base"],
        "make_request",
        _fake_make_request,
    )

    patched = Job.patch_by_uid(job_uid, scheduled_command_args=scheduled_args)
    assert captured == {
        "r_type": "PATCH",
        "url": f"https://backend.test/api/v1/jobs/{job_uid}/",
        "payload": {"json": {"scheduled_command_args": scheduled_args}},
        "timeout": None,
    }
    assert patched.scheduled_command_args == scheduled_args


@pytest.mark.parametrize(
    "scheduled_command_args",
    ["--family jobs", ("--family", "jobs"), ["--family", 1]],
)
def test_job_rejects_invalid_scheduled_command_args(scheduled_command_args):
    models_helpers = _load_models_helpers_module()
    Job = models_helpers.Job

    with pytest.raises(
        TypeError,
        match="scheduled_command_args must be a list of strings",
    ):
        Job.patch_by_uid(
            "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
            scheduled_command_args=scheduled_command_args,
        )
