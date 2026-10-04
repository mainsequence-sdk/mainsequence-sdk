from __future__ import annotations

import importlib
import os
import sys
import types

from tests.cli.support import _UNSUPPORTED_REPOSITORY_UID_ENV


def test_list_code_repository_jobs_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")
    monkeypatch.delenv(_UNSUPPORTED_REPOSITORY_UID_ENV, raising=False)
    monkeypatch.setattr(api_mod, "resolve_code_repository_branch_uid", lambda value: str(value))

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeJob:
        ROOT_URL = "https://old.test/api/v1/jobs"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
            captured["filters"].append(kwargs)
            captured["env_code_repository_uid"] = os.environ.get(_UNSUPPORTED_REPOSITORY_UID_ENV)
            if "code_repository_branch_uid" in kwargs:
                return [
                    types.SimpleNamespace(
                        model_dump=lambda: {
                            "uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
                            "name": "daily-run",
                            "organization_environment_uid": (
                                "58218213-5e4e-43de-a5bd-6757f4e1c8f6"
                            ),
                            "code_repository_commit_hash": "abc123",
                            "execution_path": "src.jobs.daily:main",
                            "task_schedule": {
                                "name": "Every hour",
                                "task": "daily-run",
                                "schedule": {"type": "interval", "every": 1, "period": "hours"},
                            },
                            "related_image": 77,
                        }
                    )
                ]
            return []

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_helpers = types.ModuleType("mainsequence.client.models_helpers")
    fake_helpers.Job = FakeJob
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    code_repository_branch_uid = "5a28020a-0f1b-47ee-aab8-334286234bea"
    out = api_mod.list_code_repository_jobs(
        code_repository_branch_uid=code_repository_branch_uid,
        filters={"name__contains": "daily"},
    )
    assert captured["filters"][0] == {
        "name__contains": "daily",
        "code_repository_branch_uid": code_repository_branch_uid,
    }
    assert captured["env_code_repository_uid"] is None
    assert captured["jwt"] == ("acc", "ref")
    assert out == [
        {
            "uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
            "name": "daily-run",
            "organization_environment_uid": ("58218213-5e4e-43de-a5bd-6757f4e1c8f6"),
            "code_repository_commit_hash": "abc123",
            "execution_path": "src.jobs.daily:main",
            "task_schedule": {
                "name": "Every hour",
                "task": "daily-run",
                "schedule": {"type": "interval", "every": 1, "period": "hours"},
            },
            "related_image": 77,
        }
    ]
    assert os.environ.get(_UNSUPPORTED_REPOSITORY_UID_ENV) is None


def test_update_code_repository_job_scheduled_command_args_uses_client_patch(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    class FakeJob:
        @classmethod
        def patch_by_uid(cls, uid, **kwargs):
            captured.update(uid=uid, kwargs=kwargs)
            return types.SimpleNamespace(
                model_dump=lambda mode="json": {
                    "uid": uid,
                    "scheduled_command_args": kwargs["scheduled_command_args"],
                }
            )

    monkeypatch.setattr(
        api_mod,
        "_run_sdk_model_operation",
        lambda *, module_name, class_name, operation: operation(FakeJob),
    )

    scheduled_args = ["--family", "jobs"]
    out = api_mod.update_code_repository_job_scheduled_command_args(
        "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
        scheduled_command_args=scheduled_args,
    )

    assert captured == {
        "uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
        "kwargs": {"scheduled_command_args": scheduled_args},
    }
    assert out == {
        "uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
        "scheduled_command_args": scheduled_args,
    }


def test_run_code_repository_job_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_helpers = types.ModuleType("mainsequence.client.models_helpers")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeJob:
        ROOT_URL = "https://old.test/api/v1/jobs"

        @classmethod
        def get_by_uid(cls, uid, timeout=None):
            captured["job_uid_arg"] = uid
            return types.SimpleNamespace(
                execution_path="scripts/test.py",
                run_job=lambda timeout=None, command_args=None: (
                    captured.update(command_args=command_args)
                    or {
                        "uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
                        "job_uid": uid,
                        "status": "QUEUED",
                        "unique_identifier": "jobrun_abc123",
                    }
                ),
            )

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_helpers.Job = FakeJob
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    job_uid = "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da"
    out = api_mod.run_code_repository_job(job_uid, command_args=["python", "-m", "jobs.daily"])
    assert captured["job_uid_arg"] == job_uid
    assert captured["command_args"] == ["python", "-m", "jobs.daily"]
    assert captured["jwt"] == ("acc", "ref")
    assert out == {
        "uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
        "job_uid": job_uid,
        "status": "QUEUED",
        "unique_identifier": "jobrun_abc123",
        "effective_run": "scripts/test.py python -m jobs.daily",
        "command_args": ["python", "-m", "jobs.daily"],
    }


def test_list_code_repository_job_runs_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    captured = {"filters": []}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_helpers = types.ModuleType("mainsequence.client.models_helpers")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class FakeJobRun:
        ROOT_URL = "https://old.test/api/v1/job-runs"

        @classmethod
        def filter(cls, timeout=None, **kwargs):
            captured["filters"].append(kwargs)
            return [
                types.SimpleNamespace(
                    model_dump=lambda: {
                        "uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
                        "name": "daily-run-1",
                        "status": "COMPLETED",
                        "unique_identifier": "jobrun_abc123",
                    }
                )
            ]

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_helpers.JobRun = FakeJobRun
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    job_uid = "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da"
    out = api_mod.list_code_repository_job_runs(job_uid=job_uid, filters={"status": "COMPLETED"})
    assert captured["filters"][0] == {"status": "COMPLETED", "job__uid": job_uid}
    assert captured["jwt"] == ("acc", "ref")
    assert out == [
        {
            "uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
            "name": "daily-run-1",
            "status": "COMPLETED",
            "unique_identifier": "jobrun_abc123",
        }
    ]


def test_get_code_repository_job_run_logs_uses_client_model(cli_mod, monkeypatch):
    api_mod = importlib.import_module("mainsequence.cli.api")
    real_job_run = importlib.import_module("mainsequence.client.models_helpers").JobRun
    captured = {}

    monkeypatch.setattr(
        api_mod, "get_tokens", lambda: {"access": "acc", "refresh": "ref", "username": "u"}
    )
    monkeypatch.setattr(api_mod, "backend_url", lambda: "https://backend.test")

    fake_client_pkg = types.ModuleType("mainsequence.client")
    fake_utils = types.ModuleType("mainsequence.client.utils")
    fake_base = types.ModuleType("mainsequence.client.base")
    fake_helpers = types.ModuleType("mainsequence.client.models_helpers")

    class FakeLoaders:
        provider = "orig"

        def use_jwt(self, *, access=None, refresh=None):
            captured["jwt"] = (access, refresh)

    fake_utils.loaders = FakeLoaders()
    fake_utils.MAINSEQUENCE_ENDPOINT = "https://old.test"
    fake_utils.API_ENDPOINT = "https://old.test/api/v1"

    class FakeBaseObjectOrm:
        ROOT_URL = "https://old.test/api/v1"

    class CanonicalJobRun(real_job_run):
        @classmethod
        def get(cls, pk, timeout=None):
            captured["job_run_uid_arg"] = pk
            return cls.model_validate(
                {
                    "uid": pk,
                    "name": "daily-run-1",
                    "unique_identifier": "jobrun_abc123",
                    "job_uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
                    "job_name": "daily-prices",
                    "code_repository_uid": "1d0530c0-65d1-4db0-856b-dc29d8260a09",
                    "code_repository_name": "market-data-service",
                    "code_repository_branch_uid": "5a28020a-0f1b-47ee-aab8-334286234bea",
                    "code_repository_branch_name": "main",
                    "organization_environment_uid": ("58218213-5e4e-43de-a5bd-6757f4e1c8f6"),
                    "status": "RUNNING",
                    "runtime_image_uid": "6cfdb152-923e-45b9-a150-c4541c68b0d1",
                    "runtime_image_digest": "sha256:" + "b" * 64,
                }
            )

        def get_logs(self, *, timeout=None, **filters):
            captured["get_logs_timeout"] = timeout
            captured["get_logs_filters"] = filters
            captured["organization_environment_uid"] = self.organization_environment_uid
            return {
                "job_run_uid": self.uid,
                "status": self.status,
                "rows": ["first line"],
            }

    fake_base.BaseObjectOrm = FakeBaseObjectOrm
    fake_helpers.JobRun = CanonicalJobRun
    fake_client_pkg.utils = fake_utils

    monkeypatch.setitem(sys.modules, "mainsequence.client", fake_client_pkg)
    monkeypatch.setitem(sys.modules, "mainsequence.client.utils", fake_utils)
    monkeypatch.setitem(sys.modules, "mainsequence.client.base", fake_base)
    monkeypatch.setitem(sys.modules, "mainsequence.client.models_helpers", fake_helpers)

    out = api_mod.get_code_repository_job_run_logs("4c1d77c8-8a42-42b8-a9c1-06be9a336e5d")
    assert captured["job_run_uid_arg"] == "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d"
    assert captured["get_logs_timeout"] is None
    assert captured["get_logs_filters"] == {
        "start": None,
        "end": None,
        "cursor": None,
        "limit": None,
        "severity": None,
        "request_id": None,
        "event": None,
        "outcome": None,
    }
    assert captured["organization_environment_uid"] == ("58218213-5e4e-43de-a5bd-6757f4e1c8f6")
    assert captured["jwt"] == ("acc", "ref")
    assert out == {
        "job_run_uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
        "status": "RUNNING",
        "rows": ["first line"],
    }


def test_code_repository_jobs_list_defaults_to_env_code_repository_id(
    cli_mod, runner, monkeypatch, tmp_path
):
    target = tmp_path / "demo-123"
    target.mkdir(parents=True, exist_ok=True)
    (target / ".env").write_text("", encoding="utf-8")

    monkeypatch.chdir(target)
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "_resolve_code_repository_branch_uid_for_command",
        lambda *args, **kwargs: "code-repository-branch-uid-123",
    )
    monkeypatch.setattr(
        cli_mod,
        "list_code_repository_jobs",
        lambda code_repository_branch_uid, filters=None, timeout=None: [
            {
                "uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
                "name": "daily-run",
                "code_repository_commit_hash": "abc123",
                "execution_path": "src.jobs.daily:main",
                "task_schedule": {
                    "name": "Every hour",
                    "task": "daily-run",
                    "schedule": {"type": "interval", "every": 1, "period": "hours"},
                },
                "related_image": 77,
            }
        ],
    )

    result = runner.invoke(cli_mod.app, ["code-repository", "jobs", "list"])
    assert result.exit_code == 0
    assert "CodeRepository Jobs" in result.output
    assert "daily-ru" in result.output
    assert "abc123" in result.output
    assert "Every" in result.output
    assert "hour:" in result.output
    assert "every 1" in result.output
    assert "hours" in result.output
    assert "Total jobs: 1" in result.output


def test_code_repository_jobs_list_show_filters_mentions_code_repository_scope(
    cli_mod, runner, monkeypatch
):
    monkeypatch.setattr(cli_mod, "build_cli_model_filter_rows", lambda model_ref: [])

    result = runner.invoke(cli_mod.app, ["code-repository", "jobs", "list", "--show-filters"])
    assert result.exit_code == 0
    assert "No additional model filters exposed by CodeRepository Jobs." in result.output
    assert "Always Applied Filters" in result.output
    assert "code_repository" in result.output


def test_code_repository_jobs_run(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    captured = {}
    job_uid = "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da"
    monkeypatch.setattr(
        cli_mod,
        "get_code_repository_job",
        lambda job_uid_arg, timeout=None: {
            "uid": job_uid_arg,
            "name": "daily-run",
            "execution_path": "scripts/test.py",
        },
    )
    monkeypatch.setattr(
        cli_mod,
        "run_code_repository_job",
        lambda job_uid, command_args=None, timeout=None: captured.update(
            job_uid=job_uid,
            command_args=command_args,
            timeout=timeout,
        )
        or {
            "uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
            "job_uid": job_uid,
            "status": "QUEUED",
            "unique_identifier": "jobrun_abc123",
            "effective_run": "scripts/test.py --name demo-from-cli",
        },
    )

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "jobs", "run", job_uid, "--", "--name", "demo-from-cli"],
    )
    assert result.exit_code == 0
    assert captured == {
        "job_uid": job_uid,
        "command_args": ["--name", "demo-from-cli"],
        "timeout": None,
    }
    assert "Effective run: scripts/test.py --name demo-from-cli" in result.output
    assert f"CodeRepository job run requested: job_uid={job_uid}" in result.output
    assert "jobrun_abc123" in result.output
    assert "QUEUED" in result.output


def test_code_repository_jobs_run_with_arg_option(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    captured = {}
    job_uid = "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da"
    monkeypatch.setattr(
        cli_mod,
        "get_code_repository_job",
        lambda job_uid_arg, timeout=None: {
            "uid": job_uid_arg,
            "name": "daily-run",
            "execution_path": "scripts/test.py",
        },
    )
    monkeypatch.setattr(
        cli_mod,
        "run_code_repository_job",
        lambda job_uid, command_args=None, timeout=None: captured.update(
            job_uid=job_uid,
            command_args=command_args,
            timeout=timeout,
        )
        or {
            "uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
            "job_uid": job_uid,
            "status": "QUEUED",
            "unique_identifier": "jobrun_abc123",
            "effective_run": "scripts/test.py demo-from-cli",
        },
    )

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "jobs", "run", job_uid, "--arg", "demo-from-cli"],
    )
    assert result.exit_code == 0
    assert captured == {
        "job_uid": job_uid,
        "command_args": ["demo-from-cli"],
        "timeout": None,
    }
    assert "Effective run: scripts/test.py demo-from-cli" in result.output


def test_code_repository_jobs_update_replaces_scheduled_command_args(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    captured = {}
    job_uid = "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da"

    def _update(job_uid_arg, *, scheduled_command_args):
        captured.update(
            job_uid=job_uid_arg,
            scheduled_command_args=scheduled_command_args,
        )
        return {
            "uid": job_uid_arg,
            "scheduled_command_args": scheduled_command_args,
        }

    monkeypatch.setattr(
        cli_mod,
        "update_code_repository_job_scheduled_command_args",
        _update,
    )

    result = runner.invoke(
        cli_mod.app,
        [
            "code-repository",
            "jobs",
            "update",
            job_uid,
            "--scheduled-arg=--family",
            "--scheduled-arg",
            "jobs",
        ],
    )

    assert result.exit_code == 0
    assert captured == {
        "job_uid": job_uid,
        "scheduled_command_args": ["--family", "jobs"],
    }
    assert "--family jobs" in result.output


def test_code_repository_jobs_update_clears_scheduled_command_args(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    captured = {}
    job_uid = "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da"

    monkeypatch.setattr(
        cli_mod,
        "update_code_repository_job_scheduled_command_args",
        lambda job_uid_arg, *, scheduled_command_args: captured.update(
            job_uid=job_uid_arg,
            scheduled_command_args=scheduled_command_args,
        )
        or {"uid": job_uid_arg, "scheduled_command_args": scheduled_command_args},
    )

    result = runner.invoke(
        cli_mod.app,
        [
            "code-repository",
            "jobs",
            "update",
            job_uid,
            "--clear-scheduled-args",
        ],
    )

    assert result.exit_code == 0
    assert captured == {"job_uid": job_uid, "scheduled_command_args": []}
    assert "None" in result.output


def test_code_repository_job_runs_list(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "list_code_repository_job_runs",
        lambda job_uid, filters=None, timeout=None: [
            {
                "uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
                "name": "daily-run-1",
                "status": "COMPLETED",
                "execution_start": "2026-03-14T09:00:00Z",
                "execution_end": "2026-03-14T09:10:00Z",
                "unique_identifier": "jobrun_abc123",
                "commit_hash": "abc123",
            }
        ],
    )

    result = runner.invoke(
        cli_mod.app,
        [
            "code-repository",
            "jobs",
            "runs",
            "list",
            "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
        ],
    )
    assert result.exit_code == 0
    assert "CodeRepository Job Runs" in result.output
    assert "daily-ru" in result.output
    assert "jobrun_ab" in result.output
    assert "Total job runs: 1" in result.output


def test_code_repository_job_runs_list_passes_cli_filters(cli_mod, runner, monkeypatch):
    captured = {}

    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})

    def _parse(model_ref, entries):
        captured["entries"] = list(entries or [])
        return {"status": "COMPLETED"}

    def _list_code_repository_job_runs(job_uid, filters=None, timeout=None):
        captured["job_uid"] = job_uid
        captured["filters"] = filters
        return []

    monkeypatch.setattr(cli_mod, "parse_cli_model_filters", _parse)
    monkeypatch.setattr(cli_mod, "list_code_repository_job_runs", _list_code_repository_job_runs)

    result = runner.invoke(
        cli_mod.app,
        [
            "code-repository",
            "jobs",
            "runs",
            "list",
            "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
            "--filter",
            "status=COMPLETED",
        ],
    )
    assert result.exit_code == 0
    assert captured["job_uid"] == "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da"
    assert captured["entries"] == ["status=COMPLETED"]
    assert captured["filters"] == {"status": "COMPLETED"}


def test_code_repository_job_runs_logs(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    monkeypatch.setattr(
        cli_mod,
        "get_code_repository_job_run_logs",
        lambda job_run_uid, timeout=None, **kwargs: {
            "job_run_uid": job_run_uid,
            "status": "COMPLETED",
            "rows": [
                {"timestamp": "2026-03-14T09:00:00Z", "level": "info", "event": "job started"},
                {"timestamp": "2026-03-14T09:10:00Z", "level": "info", "event": "job finished"},
            ],
        },
    )

    result = runner.invoke(
        cli_mod.app,
        ["code-repository", "jobs", "runs", "logs", "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d"],
    )
    assert result.exit_code == 0
    assert "Job Run Logs" in result.output
    assert "job started" in result.output
    assert "job finished" in result.output
    assert "COMPLETED" in result.output


def test_code_repository_job_runs_logs_polls_and_prints_incrementally(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    responses = iter(
        [
            {
                "job_run_uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
                "status": "PENDING",
                "rows": ["first line"],
            },
            {
                "job_run_uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
                "status": "RUNNING",
                "rows": ["first line", "second line"],
            },
            {
                "job_run_uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
                "status": "COMPLETED",
                "rows": ["first line", "second line", "third line"],
            },
        ]
    )
    sleeps = []

    monkeypatch.setattr(
        cli_mod,
        "get_code_repository_job_run_logs",
        lambda job_run_uid, timeout=None, **kwargs: next(responses),
    )
    monkeypatch.setattr(cli_mod.time, "sleep", lambda seconds: sleeps.append(seconds))

    result = runner.invoke(
        cli_mod.app,
        [
            "code-repository",
            "jobs",
            "runs",
            "logs",
            "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
            "--poll-interval",
            "3",
        ],
    )
    assert result.exit_code == 0
    assert result.output.count("first line") == 1
    assert result.output.count("second line") == 1
    assert result.output.count("third line") == 1
    assert "Polling again in 3s" in result.output
    assert sleeps == [3, 3]


def test_code_repository_job_runs_logs_stops_after_max_wait(cli_mod, runner, monkeypatch):
    monkeypatch.setattr(cli_mod, "_require_login", lambda: {"username": "u"})
    responses = iter(
        [
            {
                "job_run_uid": "6d2f9e1a-7d5a-46d0-a01e-61c80f702c8a",
                "status": "PENDING",
                "rows": ["first line"],
            },
            {
                "job_run_uid": "6d2f9e1a-7d5a-46d0-a01e-61c80f702c8a",
                "status": "RUNNING",
                "rows": ["first line", "second line"],
            },
        ]
    )
    sleeps = []
    monotonic_values = iter([100.0, 100.0, 106.0])

    monkeypatch.setattr(
        cli_mod,
        "get_code_repository_job_run_logs",
        lambda job_run_uid, timeout=None, **kwargs: next(responses),
    )
    monkeypatch.setattr(cli_mod.time, "sleep", lambda seconds: sleeps.append(seconds))
    monkeypatch.setattr(cli_mod.time, "monotonic", lambda: next(monotonic_values))

    result = runner.invoke(
        cli_mod.app,
        [
            "code-repository",
            "jobs",
            "runs",
            "logs",
            "6d2f9e1a-7d5a-46d0-a01e-61c80f702c8a",
            "--poll-interval",
            "3",
            "--max-wait-seconds",
            "5",
        ],
    )
    assert result.exit_code == 0
    assert result.output.count("first line") == 1
    assert result.output.count("second line") == 1
    assert "Stopping log polling after 5s while job run is still RUNNING." in result.output
    assert sleeps == [3]
