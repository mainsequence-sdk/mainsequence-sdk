from __future__ import annotations

import concurrent.futures
import json
import pathlib
import subprocess
import sys
from types import SimpleNamespace

import pytest

import mainsequence.client.base as base
import mainsequence.client.models_foundry as foundry
import mainsequence.client.models_user as users
import mainsequence.client.utils as utils
import mainsequence.code_repository_context as ctx
from mainsequence.client.exceptions import AuthenticationError, NotFoundError, PermissionDeniedError

ENV = "58218213-5e4e-43de-a5bd-6757f4e1c8f6"


class Response:
    def __init__(self, payload, status=200):
        self.payload = payload
        self.status_code = status
        self.content = json.dumps(payload).encode()
        self.text = json.dumps(payload)

    def json(self):
        return self.payload


@pytest.fixture(autouse=True)
def isolated(monkeypatch):
    ctx._reset_code_repository_context()
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "jwt")
    monkeypatch.delenv("MAINSEQUENCE_ACCESS_TOKEN", raising=False)
    monkeypatch.delenv("MAINSEQUENCE_REFRESH_TOKEN", raising=False)
    monkeypatch.setattr(utils.loaders, "provider", None)
    yield
    ctx._reset_code_repository_context()


@pytest.fixture
def platform(monkeypatch):
    state = {
        "registered": False,
        "branch_env": ENV,
        "lookups": 0,
        "source": ctx.GitCodeRepositorySourceContext(
            pathlib.Path.cwd().resolve(),
            "github.com/example/repository",
            "test",
            "refs/heads/test",
            "a" * 40,
        ),
    }
    monkeypatch.setattr(ctx, "_resolve_git_source_context", lambda path: state["source"])

    def resolve(source):
        state["lookups"] += 1
        if state.get("lookup_error"):
            raise state["lookup_error"]
        if not state["registered"]:
            raise NotFoundError("not registered", response=Response({}, 404))
        return SimpleNamespace(
            canonical_repository_identity=source.canonical_repository_identity,
            repository_branch=source.repository_branch,
            repository_ref=source.repository_ref,
            commit_sha=source.commit_sha,
            code_repository_branch=SimpleNamespace(
                uid="branch-uid",
                code_repository_uid="repository-uid",
                repository_branch=source.repository_branch,
                organization_environment_uid=state["branch_env"],
            ),
        )

    monkeypatch.setattr(ctx, "_default_code_repository_branch_context_loader", resolve)
    monkeypatch.setattr(
        users.User, "get_authenticated_user_details", lambda: pytest.fail("Identity preflight")
    )
    monkeypatch.setattr(
        users.OrganizationEnvironment,
        "get_by_uid",
        lambda *a, **kw: pytest.fail("Environment preflight"),
    )
    return state


def test_source_discovery_is_independent_of_authentication_and_platform_lookup(
    platform, monkeypatch
):
    monkeypatch.setattr(
        ctx,
        "_exchange_authenticated_runtime_context_if_configured",
        lambda: pytest.fail("auth exchange"),
    )
    assert ctx.get_git_source_context().repository_branch == "test"
    assert platform["lookups"] == 0


def test_real_unregistered_git_checkout_needs_no_sdk_auth(tmp_path):
    def git(*args):
        return subprocess.check_output(["git", "-C", str(tmp_path), *args], text=True).strip()

    git("init", "-b", "test")
    git(
        "-c",
        "user.name=SDK Test",
        "-c",
        "user.email=sdk@example.test",
        "commit",
        "--allow-empty",
        "-m",
        "Initial",
    )
    git("remote", "add", "origin", "git@github.com:example/repository.git")
    source = ctx.get_git_source_context(code_repository_dir=tmp_path)
    assert source.commit_sha == git("rev-parse", "HEAD")
    assert source.repository_branch == "test"
    assert source.canonical_repository_identity == "github.com/example/repository"


@pytest.mark.parametrize("registered", [False, True])
def test_missing_environment_is_valid_context(platform, registered):
    platform.update(registered=registered, branch_env=None)
    context = ctx.get_code_repository_context()
    assert context.organization_environment_uid is None
    assert context.source_context is ctx.get_git_source_context()
    assert context.status == ("resolved" if registered else "code_repository_branch_not_registered")
    if registered:
        assert ctx.require_code_repository_branch_context("Job.create") is context
    else:
        with pytest.raises(ctx.CodeRepositoryBranchContextRequiredError):
            ctx.require_code_repository_branch_context("Job.create")


@pytest.mark.parametrize("registered", [False, True])
@pytest.mark.parametrize(
    "resource", [foundry.Secret, foundry.Constant, foundry.Bucket, foundry.Artifact]
)
@pytest.mark.parametrize("operation", ["filter", "get_by_uid", "create"])
def test_environment_is_required_only_at_environment_resource_operation(
    platform, monkeypatch, registered, resource, operation
):
    platform.update(registered=registered, branch_env=None)
    monkeypatch.setattr(
        base, "make_request", lambda **kw: pytest.fail("resource request without Environment")
    )
    context = ctx.get_code_repository_context()
    with pytest.raises(
        ctx.CodeRepositoryEnvironmentContextRequiredError, match="test.*no registered Environment"
    ):
        if operation == "get_by_uid":
            resource.get_by_uid(ENV)
        elif operation == "create":
            resource.create(name="test", value="test")
        else:
            resource.filter()
    assert ctx.get_code_repository_context() is context
    assert ctx.validate_git_source_context() is context.source_context
    assert platform["lookups"] == 1


def test_environment_failure_does_not_block_unrelated_platform_requests(platform, monkeypatch):
    with pytest.raises(ctx.CodeRepositoryEnvironmentContextRequiredError):
        foundry.Secret.filter()
    monkeypatch.setattr(base, "make_request", lambda **kw: Response([]))
    assert users.User.filter() == []
    assert users.Organization.filter() == []
    assert users.OrganizationEnvironment.filter() == []
    assert ctx.get_git_source_context().repository_branch == "test"
    assert platform["lookups"] == 1


@pytest.mark.parametrize(
    "resource", [foundry.Secret, foundry.Constant, foundry.Bucket, foundry.Artifact]
)
def test_registered_branch_environment_is_used_without_identity_or_ownership_preflight(
    platform, monkeypatch, resource
):
    platform["registered"] = True
    requests = []
    monkeypatch.setattr(base, "make_request", lambda **kw: requests.append(kw) or Response([]))
    assert resource.filter() == []
    assert requests[0]["payload"]["params"]["organization_environment_uid"] == ENV
    assert requests[0]["loaders"] is utils.loaders
    with pytest.raises(ValueError, match="cannot override"):
        resource.filter(organization_environment_uid=ENV)


def test_backend_denial_is_propagated_without_sdk_ownership_policy(platform, monkeypatch):
    platform["registered"] = True

    def denied(**kwargs):
        raise PermissionDeniedError("backend denied", response=Response({}, 403))

    monkeypatch.setattr(base, "make_request", denied)
    with pytest.raises(PermissionDeniedError):
        foundry.Secret.filter()
    assert ctx.resolve_organization_environment_uid("Secret.filter") == ENV


def test_cached_context_is_not_revalidated_against_account_or_endpoint(platform, monkeypatch):
    platform["registered"] = True
    original = ctx.get_code_repository_context()
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "refreshed-token")
    monkeypatch.setattr(users.User, "ROOT_URL", "https://another.example.test/api/v1")
    assert ctx.get_code_repository_context() is original
    assert ctx.resolve_organization_environment_uid("Secret.filter") == ENV
    assert platform["lookups"] == 1


@pytest.mark.parametrize(
    "error",
    [
        AuthenticationError("unauthorized", response=Response({}, 401)),
        PermissionDeniedError("forbidden", response=Response({}, 403)),
        RuntimeError("unavailable"),
    ],
)
def test_platform_failure_is_not_misclassified_as_missing_registration(platform, error):
    source = ctx.get_git_source_context()
    platform["lookup_error"] = error
    with pytest.raises(ctx.CodeRepositoryContextError):
        ctx.get_code_repository_context()
    assert ctx.get_git_source_context() is source


def test_concurrent_resolutions_share_one_platform_snapshot(platform):
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: ctx.get_code_repository_context(), range(16)))
    assert all(value is results[0] for value in results)
    assert platform["lookups"] == 1


def test_git_drift_is_detected_without_platform_requests(platform):
    original = ctx.get_git_source_context()
    platform["source"] = ctx.GitCodeRepositorySourceContext(
        original.repository_root,
        original.canonical_repository_identity,
        "other",
        "refs/heads/other",
        "b" * 40,
    )
    with pytest.raises(ctx.CodeRepositorySourceContextDriftError):
        ctx.validate_git_source_context()
    assert ctx.get_git_source_context() is original
    assert platform["lookups"] == 0


def test_wrong_requested_repository_does_not_poison_cached_context(platform):
    platform["registered"] = True
    original = ctx.get_code_repository_context()
    with pytest.raises(ctx.CodeRepositoryContextError, match="Requested CodeRepository"):
        ctx.get_code_repository_context(code_repository_uid="other-repository")
    assert ctx.get_code_repository_context() is original


def test_fork_does_not_reuse_parent_platform_metadata(platform, monkeypatch):
    parent = ctx.get_code_repository_context()
    platform["registered"] = True
    monkeypatch.setattr(ctx.os, "getpid", lambda: parent.process_id + 1)
    child = ctx.get_code_repository_context()
    assert child.status == "resolved" and parent.status == "code_repository_branch_not_registered"
    assert platform["lookups"] == 2


def test_after_fork_reinitializes_lock_and_source(platform):
    old_lock = ctx._STATE_CONDITION
    ctx.get_code_repository_context()
    ctx._after_fork()
    assert ctx._STATE_CONDITION is not old_lock
    assert ctx._SOURCE_STATE.phase == ctx._STATE.phase == "uninitialized"


def test_nested_checkout_cannot_replace_frozen_source(platform):
    original = ctx.get_git_source_context()
    nested = original.repository_root / "nested"
    platform["source"] = ctx.GitCodeRepositorySourceContext(
        nested, "github.com/example/nested", "main", "refs/heads/main", "b" * 40
    )
    with pytest.raises(ctx.CodeRepositorySourceContextDriftError):
        ctx.get_git_source_context(code_repository_dir=nested)
    assert ctx.get_git_source_context() is original


def test_import_does_not_resolve_source_or_load_database_engines():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import sys
import mainsequence.code_repository_context as context
assert context._SOURCE_STATE.phase == context._STATE.phase == "uninitialized"
assert not {"duckdb", "sqlite3", "sqlalchemy", "metatables"}.intersection(sys.modules)
""",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
