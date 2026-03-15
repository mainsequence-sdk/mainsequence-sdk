import importlib
import pathlib
import sys
import types

import pytest


def _load_models_helpers_module():
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    pkg_root = repo_root / "mainsequence"
    client_root = pkg_root / "client"

    for name in (
        "mainsequence.client.models_helpers",
        "mainsequence.client.models_vam",
        "mainsequence.client.models_tdag",
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


def test_job_bulk_get_or_create_posts_normalized_batch(monkeypatch, tmp_path):
    models_helpers = _load_models_helpers_module()
    Job = models_helpers.Job

    jobs_file = tmp_path / "scheduled_jobs.yaml"
    jobs_file.write_text(
        "\n".join(
            [
                "jobs:",
                '  - name: "Simulated Prices"',
                '    execution_path: "scripts/simulated_prices_launcher.py"',
                "    task_schedule:",
                '      type: "crontab"',
                '      expression: "0 0 * * *"',
                "    related_image_id: 77",
                '    cpu_request: "0.25"',
                '    memory_request: "0.5"',
            ]
        ),
        encoding="utf-8",
    )

    captured = {}

    class FakeResponse:
        status_code = 201

        def json(self):
            return [
                {
                    "id": 91,
                    "name": "Simulated Prices",
                    "project": 123,
                    "execution_path": "scripts/simulated_prices_launcher.py",
                    "related_image": 77,
                    "task_schedule": {
                        "name": "Nightly Run",
                        "task": "tdag.pod_manager.tasks.run_job_in_celery",
                        "schedule": {"type": "crontab", "expression": "0 0 * * *"},
                    },
                    "cpu_request": "0.25",
                    "memory_request": "0.5",
                }
            ]

    monkeypatch.setattr(Job, "build_session", classmethod(lambda cls: object()))
    monkeypatch.setattr(
        Job,
        "get_object_url",
        classmethod(lambda cls: "https://backend.test/orm/api/pods/job"),
    )

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(models_helpers, "make_request", _fake_make_request)

    created = Job.bulk_get_or_create(yaml_file=jobs_file, project_id=123, strict=False, timeout=30)

    assert captured["r_type"] == "POST"
    assert captured["url"] == "https://backend.test/orm/api/pods/job/sync_jobs/"
    assert captured["timeout"] == 30
    assert captured["payload"]["json"] == {
        "project": 123,
        "jobs": [
            {
                "name": "Simulated Prices",
                "execution_path": "scripts/simulated_prices_launcher.py",
                "related_image": 77,
                "cpu_request": "0.25",
                "memory_request": "0.5",
                "task_schedule": {"schedule": {"type": "crontab", "expression": "0 0 * * *"}},
            }
        ],
        "strict": False,
    }
    assert len(created) == 1
    assert created[0].id == 91
    assert created[0].name == "Simulated Prices"


def test_job_bulk_get_or_create_rejects_invalid_yaml_shape(tmp_path):
    models_helpers = _load_models_helpers_module()
    Job = models_helpers.Job

    jobs_file = tmp_path / "scheduled_jobs.yaml"
    jobs_file.write_text('name: "demo"\n', encoding="utf-8")

    with pytest.raises(ValueError, match="top-level 'jobs' key"):
        Job.bulk_get_or_create(yaml_file=jobs_file, project_id=123)


def test_job_bulk_get_or_create_rejects_invalid_job_definition(tmp_path):
    models_helpers = _load_models_helpers_module()
    Job = models_helpers.Job

    jobs_file = tmp_path / "scheduled_jobs.yaml"
    jobs_file.write_text(
        "\n".join(
            [
                "jobs:",
                '  - name: "Simulated Prices"',
                '    execution_path: "scripts/simulated_prices_launcher.py"',
                "    task_schedule:",
                '      type: "crontab"',
                '      expression: "0 0 * * *"',
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=r"jobs\[0\] is invalid"):
        Job.bulk_get_or_create(yaml_file=jobs_file, project_id=123)
