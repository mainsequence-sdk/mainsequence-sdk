import importlib
import pathlib
import sys
import types


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
            "https://backend.test/api/v1/jobs/"
            "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da/run-job/"
        ),
        "payload": {"json": {"command_args": ["--name", "demo-from-cli"]}},
        "timeout": 30,
    }
    assert out == {
        "job_run_uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
        "status": "QUEUED",
    }
