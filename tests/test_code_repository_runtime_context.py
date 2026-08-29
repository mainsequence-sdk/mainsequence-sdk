from __future__ import annotations

import concurrent.futures
import pathlib
import subprocess
import threading
import time
import types

import pytest

import mainsequence.client.base as client_base
import mainsequence.client.metatables as models_metatables
import mainsequence.client.metatables.core as metatables_core
import mainsequence.client.models_foundry as models_foundry
import mainsequence.client.utils as client_utils
import mainsequence.code_repository_context as code_repository_context
from mainsequence.client.base import (
    BaseObjectOrm,
    CurrentCodeRepositoryBranchCollectionMixin,
)

CODE_REPOSITORY_UID = "1d0530c0-65d1-4db0-856b-dc29d8260a09"
CODE_REPOSITORY_BRANCH_UID = "5a28020a-0f1b-47ee-aab8-334286234bea"
DATA_SOURCE_UID = "864e7c22-482a-464a-8758-0d3408abd77f"
ENVIRONMENT_UID = "a5e95092-a77a-45a6-835c-46d327e8b5e7"
COMMIT_SHA = "a" * 40


@pytest.fixture(autouse=True)
def _reset_context(monkeypatch):
    code_repository_context._reset_code_repository_context()
    monkeypatch.setattr(
        models_metatables,
        "SessionDataSource",
        models_metatables.PodDataSource(),
    )
    yield
    code_repository_context._reset_code_repository_context()


def _source(*, branch: str = "main", commit_sha: str = COMMIT_SHA):
    return code_repository_context.GitCodeRepositorySourceContext(
        repository_root=pathlib.Path.cwd().resolve(),
        canonical_repository_identity="github.com/mainsequence-sdk/example-repository",
        repository_branch=branch,
        repository_ref=f"refs/heads/{branch}",
        commit_sha=commit_sha,
    )


def _data_source(*, status: str = "AVAILABLE"):
    return types.SimpleNamespace(
        uid=DATA_SOURCE_UID,
        data_source_uid=DATA_SOURCE_UID,
        display_name="Remote Timescale",
        class_type="timescale_db",
        status=status,
    )


def _code_repository_branch(*, branch: str = "main", data_source=None):
    return types.SimpleNamespace(
        uid=CODE_REPOSITORY_BRANCH_UID,
        code_repository_uid=CODE_REPOSITORY_UID,
        repository_branch=branch,
        organization_environment_uid=ENVIRONMENT_UID,
        metatables_data_source=data_source,
    )


def _resolve(
    monkeypatch,
    *,
    source=None,
    registered=True,
    data_source=None,
):
    source = source or _source()
    monkeypatch.setattr(code_repository_context, "_resolve_git_source_context", lambda path: source)

    def load_context(resolved_source):
        if not registered:
            error = RuntimeError("not found")
            error.status_code = 404
            raise error
        return types.SimpleNamespace(
            canonical_repository_identity=resolved_source.canonical_repository_identity,
            repository_branch=resolved_source.repository_branch,
            repository_ref=resolved_source.repository_ref,
            commit_sha=resolved_source.commit_sha,
            code_repository_branch=_code_repository_branch(
                branch=resolved_source.repository_branch,
                data_source=data_source,
            ),
        )

    return code_repository_context.get_code_repository_context(
        code_repository_dir=source.repository_root,
        _code_repository_branch_context_loader=load_context,
    )


def _install_authenticated_runtime_context(
    *,
    code_repository_uid: str = CODE_REPOSITORY_UID,
    code_repository_branch_uid: str = CODE_REPOSITORY_BRANCH_UID,
    repository_branch: str = "main",
    organization_environment_uid: str = ENVIRONMENT_UID,
):
    code_repository_context._install_authenticated_runtime_code_repository_context(
        {
            "code_repository_uid": code_repository_uid,
            "code_repository_branch_uid": code_repository_branch_uid,
            "repository_branch": repository_branch,
            "organization_environment_uid": organization_environment_uid,
        }
    )


def _git(repo: pathlib.Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=repo,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def test_git_source_context_reads_attached_branch_remote_and_full_commit(tmp_path):
    repo = tmp_path / "code-repository"
    repo.mkdir()
    _git(repo, "init", "-b", "development")
    _git(repo, "config", "user.email", "sdk@example.test")
    _git(repo, "config", "user.name", "SDK Test")
    (repo / "README.md").write_text("test\n", encoding="utf-8")
    _git(repo, "add", "README.md")
    _git(repo, "commit", "-m", "Initial")
    _git(repo, "remote", "add", "origin", "git@github.com:MainSequence-SDK/Example.git")

    source = code_repository_context._resolve_git_source_context(repo)

    assert source.repository_root == repo.resolve()
    assert source.repository_branch == "development"
    assert source.repository_ref == "refs/heads/development"
    assert source.commit_sha == _git(repo, "rev-parse", "HEAD")
    assert source.canonical_repository_identity == "github.com/MainSequence-SDK/Example"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("git@github.com:org/repo.git", "github.com/org/repo"),
        ("ssh://git@github.com/org/repo.git", "github.com/org/repo"),
        ("https://token@github.com/org/repo.git", "github.com/org/repo"),
    ],
)
def test_repository_identity_normalizes_transport_and_credentials(value, expected):
    assert code_repository_context.normalize_github_repository_binding_identity(value) == expected


def test_code_repository_branch_git_context_uses_canonical_backend_action(monkeypatch):
    captured = {}

    class Response:
        status_code = 200

        @staticmethod
        def json():
            return {
                "canonical_repository_identity": ("github.com/mainsequence-sdk/example-repository"),
                "repository_branch": "main",
                "repository_ref": "refs/heads/main",
                "commit_sha": COMMIT_SHA,
                "code_repository_branch": {
                    "uid": CODE_REPOSITORY_BRANCH_UID,
                    "code_repository_uid": CODE_REPOSITORY_UID,
                    "code_repository_name": "Example CodeRepository",
                    "repository_branch": "main",
                    "metatables_data_source": None,
                    "metatables_data_source_uid": None,
                    "organization_environment_uid": ENVIRONMENT_UID,
                    "organization_environment_name": "Development",
                    "default_base_image": {"uid": "base-image-uid"},
                    "sdks": [],
                    "github_repository_binding_uid": "repository-uid",
                    "latest_git_version": COMMIT_SHA,
                    "is_initialized": True,
                    "created_by": None,
                },
            }

    monkeypatch.setattr(
        models_foundry.CodeRepositoryBranch,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(
        models_foundry,
        "make_request",
        lambda **kwargs: captured.update(kwargs) or Response(),
    )

    resolution = models_foundry.CodeRepositoryBranch.resolve_git_context(
        repository_identity="github.com/mainsequence-sdk/example-repository",
        repository_branch="main",
        commit_sha=COMMIT_SHA,
    )

    assert captured["url"].endswith("/code-repository-branches/resolve-git-context/")
    assert captured["payload"]["json"] == {
        "repository_identity": "github.com/mainsequence-sdk/example-repository",
        "repository_branch": "main",
        "commit_sha": COMMIT_SHA,
    }
    assert resolution.code_repository_branch.uid == CODE_REPOSITORY_BRANCH_UID


def test_context_resolves_once_and_returns_identical_snapshot(monkeypatch):
    calls = {"source": 0, "backend": 0}

    def load_source(path):
        calls["source"] += 1
        return _source()

    monkeypatch.setattr(code_repository_context, "_resolve_git_source_context", load_source)

    def load_context(source):
        calls["backend"] += 1
        return types.SimpleNamespace(
            canonical_repository_identity=source.canonical_repository_identity,
            repository_branch=source.repository_branch,
            repository_ref=source.repository_ref,
            commit_sha=source.commit_sha,
            code_repository_branch=_code_repository_branch(
                branch=source.repository_branch,
                data_source=_data_source(),
            ),
        )

    loaders = {"_code_repository_branch_context_loader": load_context}

    first = code_repository_context.get_code_repository_context(**loaders)
    second = code_repository_context.get_code_repository_context(**loaders)

    assert first is second
    assert first.status == "resolved"
    assert first.code_repository_branch_uid == CODE_REPOSITORY_BRANCH_UID
    assert calls == {"source": 1, "backend": 1}


def test_concurrent_first_call_performs_one_resolution(monkeypatch):
    calls = 0
    calls_lock = threading.Lock()

    def load_source(path):
        nonlocal calls
        with calls_lock:
            calls += 1
        time.sleep(0.03)
        return _source()

    monkeypatch.setattr(code_repository_context, "_resolve_git_source_context", load_source)

    def resolve():
        return code_repository_context.get_code_repository_context(
            _code_repository_branch_context_loader=lambda source: types.SimpleNamespace(
                canonical_repository_identity=source.canonical_repository_identity,
                repository_branch=source.repository_branch,
                repository_ref=source.repository_ref,
                commit_sha=source.commit_sha,
                code_repository_branch=_code_repository_branch(branch=source.repository_branch),
            ),
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        contexts = list(executor.map(lambda _: resolve(), range(8)))

    assert calls == 1
    assert len({id(context) for context in contexts}) == 1


def test_identity_environment_variables_never_select_context(monkeypatch):
    monkeypatch.setenv("MAIN_SEQUENCE_PROJECT_UID", "wrong-code-repository")
    monkeypatch.setenv("MAIN_SEQUENCE_PROJECT_BRANCH_UID", "wrong-branch")
    monkeypatch.setenv("MAINSEQUENCE_REPOSITORY_BRANCH", "wrong-branch-name")
    monkeypatch.setenv(
        "MAIN_SEQUENCE_ORGANIZATION_PROJECT_ENVIRONMENT_UID",
        "wrong-environment",
    )
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")

    context = _resolve(monkeypatch)

    assert context.code_repository_uid == CODE_REPOSITORY_UID
    assert context.repository_branch == "main"
    assert context.code_repository_branch_uid == CODE_REPOSITORY_BRANCH_UID
    assert context.organization_environment_uid == ENVIRONMENT_UID


def test_authenticated_runtime_context_requires_matching_git(monkeypatch):
    data_source = _data_source()
    _install_authenticated_runtime_context()

    context = _resolve(monkeypatch, data_source=data_source)

    assert context.is_authenticated_runtime is True
    assert context.source_context == _source()
    assert context.code_repository_uid == CODE_REPOSITORY_UID
    assert context.code_repository_branch_uid == CODE_REPOSITORY_BRANCH_UID
    assert context.repository_branch == "main"
    assert context.repository_ref == "refs/heads/main"
    assert context.commit_sha == COMMIT_SHA
    assert context.organization_environment_uid == ENVIRONMENT_UID
    assert context.metatables_data_source is data_source
    assert code_repository_context.validate_code_repository_source_context(context=context) is context

    assert models_metatables.MetaTable._sdk_owned_query_context("MetaTable.filter") == {}
    assert metatables_core._with_current_metatable_code_repository_context(
        {"management_mode": "platform_managed"}
    ) == {"management_mode": "platform_managed"}
    assert metatables_core._with_current_metatable_code_repository_context(
        {"management_mode": "external_registered"}
    ) == {"management_mode": "external_registered"}


def test_authenticated_runtime_context_has_no_missing_git_fallback(monkeypatch):
    _install_authenticated_runtime_context()

    def missing_git(path):
        raise code_repository_context.CodeRepositoryContextError(
            "missing deployed Git checkout"
        )

    monkeypatch.setattr(
        code_repository_context,
        "_resolve_git_source_context",
        missing_git,
    )

    with pytest.raises(code_repository_context.CodeRepositoryContextError, match="missing deployed Git"):
        code_repository_context.get_code_repository_context()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("code_repository_uid", "different-code-repository"),
        ("code_repository_branch_uid", "different-code-repository-branch"),
        ("repository_branch", "development"),
        ("organization_environment_uid", "different-environment"),
    ],
)
def test_authenticated_runtime_context_rejects_git_resolved_target_mismatch(
    monkeypatch,
    field,
    value,
):
    runtime_context = {
        "code_repository_uid": CODE_REPOSITORY_UID,
        "code_repository_branch_uid": CODE_REPOSITORY_BRANCH_UID,
        "repository_branch": "main",
        "organization_environment_uid": ENVIRONMENT_UID,
    }
    runtime_context[field] = value
    _install_authenticated_runtime_context(**runtime_context)

    with pytest.raises(code_repository_context.CodeRepositoryContextError, match=field):
        _resolve(monkeypatch)


def test_authenticated_runtime_context_detects_git_drift(monkeypatch):
    _install_authenticated_runtime_context()
    context = _resolve(monkeypatch)
    monkeypatch.setattr(
        code_repository_context,
        "_resolve_git_source_context",
        lambda path: _source(branch="development", commit_sha="b" * 40),
    )

    with pytest.raises(code_repository_context.CodeRepositorySourceContextDriftError):
        code_repository_context.validate_code_repository_source_context(context=context)


def test_configured_runtime_credential_exchanges_context_before_resolution(monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID", "credential-id")
    monkeypatch.setenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET", "credential-secret")
    events = []
    monkeypatch.setattr(
        code_repository_context,
        "_resolve_git_source_context",
        lambda path: events.append("git") or _source(),
    )
    exchange_calls = []

    def exchange(*, force=False, session=None):
        events.append("exchange")
        exchange_calls.append((force, session))
        _install_authenticated_runtime_context()
        return {"Authorization": "Bearer runtime-access"}

    monkeypatch.setattr(client_utils.loaders, "refresh_headers", exchange)

    def resolve_git_context(source):
        events.append("backend")
        return types.SimpleNamespace(
            canonical_repository_identity=source.canonical_repository_identity,
            repository_branch=source.repository_branch,
            repository_ref=source.repository_ref,
            commit_sha=source.commit_sha,
            code_repository_branch=_code_repository_branch(branch=source.repository_branch),
        )

    context = code_repository_context.get_code_repository_context(
        _code_repository_branch_context_loader=resolve_git_context,
    )

    assert context.is_authenticated_runtime is True
    assert context.source_context == _source()
    assert exchange_calls == [(True, None)]
    assert events == ["exchange", "git", "backend"]


def test_unregistered_git_context_is_nonfatal_until_branch_context_is_required(monkeypatch):
    context = _resolve(monkeypatch, registered=False)

    assert context.status == "code_repository_branch_not_registered"
    with pytest.raises(code_repository_context.CodeRepositoryBranchContextRequiredError):
        code_repository_context.require_code_repository_branch_context("Create Job", context=context)


def test_unregistered_branch_is_nonfatal_until_branch_context_is_required(monkeypatch):
    context = _resolve(
        monkeypatch,
        source=_source(branch="feature/local"),
        registered=False,
    )

    assert context.status == "code_repository_branch_not_registered"
    assert context.code_repository_uid is None
    assert context.code_repository_branch_uid is None
    with pytest.raises(code_repository_context.CodeRepositoryBranchContextRequiredError):
        code_repository_context.require_code_repository_branch_context("Create Job", context=context)


def test_code_repository_environment_uid_comes_from_frozen_branch_context(monkeypatch):
    _resolve(monkeypatch)

    assert code_repository_context.resolve_organization_environment_uid("Create Secret") == ENVIRONMENT_UID


def test_code_repository_environment_operation_fails_when_branch_has_no_environment(monkeypatch):
    context = _resolve(monkeypatch)
    missing_environment_context = code_repository_context.CodeRepositoryContext(
        source_context=context.source_context,
        code_repository_uid=context.code_repository_uid,
        code_repository_branch_uid=context.code_repository_branch_uid,
        organization_environment_uid=None,
        metatables_data_source=context.metatables_data_source,
        status=context.status,
        process_id=context.process_id,
        code_repository_branch=context.code_repository_branch,
        detail=context.detail,
    )
    monkeypatch.setattr(
        code_repository_context,
        "require_code_repository_branch_context",
        lambda operation: missing_environment_context,
    )

    with pytest.raises(
        code_repository_context.CodeRepositoryEnvironmentContextRequiredError,
        match="none was returned",
    ):
        code_repository_context.resolve_organization_environment_uid("Create Secret")


def test_runtime_target_mismatch_is_not_treated_as_unregistered(monkeypatch):
    source = _source()
    monkeypatch.setattr(
        code_repository_context,
        "_resolve_git_source_context",
        lambda path: source,
    )

    def forbidden(resolved_source):
        error = RuntimeError("runtime Git context mismatch")
        error.status_code = 403
        raise error

    with pytest.raises(code_repository_context.CodeRepositoryContextError, match="mismatch"):
        code_repository_context.get_code_repository_context(
            _code_repository_branch_context_loader=forbidden,
        )


def test_context_drift_is_rejected(monkeypatch):
    initial = _source(branch="main", commit_sha="a" * 40)
    context = _resolve(monkeypatch, source=initial)
    monkeypatch.setattr(
        code_repository_context,
        "_resolve_git_source_context",
        lambda path: _source(branch="development", commit_sha="b" * 40),
    )

    with pytest.raises(code_repository_context.CodeRepositorySourceContextDriftError):
        code_repository_context.validate_code_repository_source_context(context=context)


def test_forked_process_discards_inherited_snapshot(monkeypatch):
    process_id = 101
    monkeypatch.setattr(code_repository_context.os, "getpid", lambda: process_id)
    first = _resolve(monkeypatch, source=_source(branch="main"))

    process_id = 202
    monkeypatch.setattr(
        code_repository_context,
        "_resolve_git_source_context",
        lambda path: _source(branch="development", commit_sha="b" * 40),
    )
    second = code_repository_context.get_code_repository_context(
        _code_repository_branch_context_loader=lambda source: types.SimpleNamespace(
            canonical_repository_identity=source.canonical_repository_identity,
            repository_branch=source.repository_branch,
            repository_ref=source.repository_ref,
            commit_sha=source.commit_sha,
            code_repository_branch=_code_repository_branch(branch=source.repository_branch),
        ),
    )

    assert first.process_id == 101
    assert second.process_id == 202
    assert second.repository_branch == "development"


def test_failed_initialization_is_cached(monkeypatch):
    calls = 0

    def fail(path):
        nonlocal calls
        calls += 1
        raise code_repository_context.CodeRepositoryContextError("missing Git metadata")

    monkeypatch.setattr(code_repository_context, "_resolve_git_source_context", fail)
    for _ in range(2):
        with pytest.raises(code_repository_context.CodeRepositoryContextError, match="missing Git"):
            code_repository_context.get_code_repository_context()
    assert calls == 1


def test_code_repository_data_source_requires_exact_registered_branch(monkeypatch):
    data_source = _data_source()
    context = _resolve(monkeypatch, data_source=data_source)

    assert (
        code_repository_context.require_code_repository_metatables_data_source(
            "CodeRepositoryBranch-derived data access",
            context=context,
        )
        is data_source
    )


class _ScopedCollection(CurrentCodeRepositoryBranchCollectionMixin, BaseObjectOrm):
    ENDPOINT = "scoped-collection"
    FILTERSET_FIELDS = {"code_repository_branch_uid": ["exact", "in"]}
    FILTER_VALUE_NORMALIZERS = {
        "code_repository_branch_uid": "uid",
        "code_repository_branch_uid__in": "uid",
    }


def test_authenticated_runtime_collection_omits_code_repository_branch_selector(monkeypatch):
    _install_authenticated_runtime_context()
    _resolve(monkeypatch)
    captured = {}

    class Response:
        status_code = 200
        content = b"[]"

        @staticmethod
        def json():
            return []

    monkeypatch.setattr(
        _ScopedCollection,
        "get_object_url",
        classmethod(lambda cls: "https://api/scoped-collection"),
    )
    monkeypatch.setattr(_ScopedCollection, "build_session", classmethod(lambda cls: object()))
    monkeypatch.setattr(
        client_base,
        "make_request",
        lambda **kwargs: captured.update(kwargs) or Response(),
    )

    assert _ScopedCollection.filter() == []
    assert captured["payload"] == {}
    with pytest.raises(code_repository_context.CodeRepositoryBranchContextRequiredError):
        _ScopedCollection.filter(code_repository_branch_uid=CODE_REPOSITORY_BRANCH_UID)


def test_current_branch_collection_is_scoped_and_admin_path_is_explicit(monkeypatch):
    _resolve(monkeypatch)
    captured = []

    class Response:
        status_code = 200
        content = b"[]"

        @staticmethod
        def json():
            return []

    monkeypatch.setattr(
        _ScopedCollection,
        "get_object_url",
        classmethod(lambda cls: "https://api/scoped-collection"),
    )
    monkeypatch.setattr(_ScopedCollection, "build_session", classmethod(lambda cls: object()))
    monkeypatch.setattr(
        client_base,
        "make_request",
        lambda **kwargs: captured.append(kwargs) or Response(),
    )

    assert _ScopedCollection.filter() == []
    assert captured[-1]["payload"]["params"] == {"code_repository_branch_uid": CODE_REPOSITORY_BRANCH_UID}
    assert _ScopedCollection.filter_admin() == []
    assert captured[-1]["payload"] == {}
    with pytest.raises(code_repository_context.CodeRepositoryBranchContextRequiredError):
        _ScopedCollection.filter(code_repository_branch_uid="other-branch")


def test_table_update_always_submits_git_resolved_branch(monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "runtime_credential")
    _resolve(monkeypatch)
    captured = {}

    class Response:
        status_code = 201

        @staticmethod
        def json():
            return {
                "uid": "update-uid-1",
                "update_hash": "abc123",
                "build_configuration": {
                    "configuration_schema_version": 2,
                    "table_updater_class_import_path": {
                        "module": "tests.test_code_repository_context",
                        "qualname": "ExampleUpdater",
                    },
                },
                "orm_class": "TimeIndexTableUpdate",
                "output_table": "storage-1",
                "labels": [],
                "description": None,
                "update_details": None,
                "run_configuration": None,
            }

    monkeypatch.setattr(
        models_metatables.TimeIndexTableUpdate,
        "build_session",
        classmethod(lambda cls: types.SimpleNamespace(headers={})),
    )
    monkeypatch.setattr(
        models_metatables,
        "make_request",
        lambda **kwargs: captured.update(kwargs) or Response(),
    )

    models_metatables.TimeIndexTableUpdate.get_or_create(
        update_hash="abc123",
        output_table_uid="storage-1",
        build_configuration={
            "configuration_schema_version": 2,
            "table_updater_class_import_path": {
                "module": "tests.test_code_repository_context",
                "qualname": "ExampleUpdater",
            },
        },
    )

    assert captured["payload"]["json"]["current_code_repository_branch_uid"] == CODE_REPOSITORY_BRANCH_UID


def test_metatable_request_always_submits_git_resolved_context(monkeypatch):
    _resolve(monkeypatch)

    payload = models_metatables._with_current_metatable_code_repository_context(
        {
            "management_mode": "platform_managed",
            "data_source_uid": DATA_SOURCE_UID,
        }
    )

    assert payload["code_repository_context"] == {
        "code_repository_branch_uid": CODE_REPOSITORY_BRANCH_UID,
    }


def test_code_repository_model_rejects_removed_default_data_source_fields():
    payload = {
        "uid": CODE_REPOSITORY_UID,
        "code_repository_name": "Example CodeRepository",
        "code_repository_type": "python",
        "primary_language": "python",
        "framework": "mainsequence",
        "github_repository_binding_uid": "3c2113e7-40ba-4d8c-ad65-51ca236c3b0c",
        "archived": False,
        "created_by": "user-4",
        "labels": [],
        "branches": [],
        "default_metatables_data_source_uid": DATA_SOURCE_UID,
    }

    with pytest.raises(ValueError, match="default_metatables_data_source_uid"):
        models_foundry.CodeRepository.model_validate(payload)
