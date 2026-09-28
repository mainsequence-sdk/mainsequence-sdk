from __future__ import annotations

import base64
import concurrent.futures
import json
import pathlib
import subprocess
import sys
import threading
from types import SimpleNamespace

import pytest

import mainsequence.client.base as base
import mainsequence.client.models_foundry as foundry
import mainsequence.client.models_user as users
import mainsequence.client.utils as utils
import mainsequence.code_repository_context as ctx
from mainsequence.client.exceptions import AuthenticationError, NotFoundError, PermissionDeniedError

ENV = "58218213-5e4e-43de-a5bd-6757f4e1c8f6"
OTHER_ENV = "68218213-5e4e-43de-a5bd-6757f4e1c8f6"
ORG = "a8218213-5e4e-43de-a5bd-6757f4e1c8f6"
USER = "b8218213-5e4e-43de-a5bd-6757f4e1c8f6"


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
        "user": USER,
        "organization": ORG,
        "environment_owner": ORG,
        "lookups": 0,
        "identities": 0,
        "environments": 0,
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

    def identity(cls):
        state["identities"] += 1
        if state.get("identity_error"):
            raise state["identity_error"]
        return SimpleNamespace(
            uid=state["user"], organization=SimpleNamespace(uid=state["organization"])
        )

    def environment(cls, uid, **kwargs):
        state["environments"] += 1
        if state.get("environment_error"):
            raise state["environment_error"]
        return SimpleNamespace(uid=uid, organization_owner_uid=state["environment_owner"])

    state["original_user_details"] = users.User.get_authenticated_user_details
    state["original_environment_detail"] = users.OrganizationEnvironment.get_by_uid
    monkeypatch.setattr(ctx, "_default_code_repository_branch_context_loader", resolve)
    monkeypatch.setattr(users.User, "get_authenticated_user_details", classmethod(identity))
    monkeypatch.setattr(users.OrganizationEnvironment, "get_by_uid", classmethod(environment))
    return state


def runtime_token():
    payload = (
        base64.urlsafe_b64encode(
            json.dumps({"runtime_auth_mode": "session_jwt", "scope": "job_run_runtime"}).encode()
        )
        .decode()
        .rstrip("=")
    )
    return f"header.{payload}.signature"


def select(uid=ENV):
    ctx.configure_development_environment(ctx.DevelopmentEnvironmentSelection(uid))


def install_runtime():
    ctx._install_authenticated_runtime_code_repository_context(
        {
            "code_repository_uid": "repository-uid",
            "code_repository_branch_uid": "branch-uid",
            "repository_branch": "test",
            "organization_environment_uid": ENV,
        }
    )


def test_source_is_network_free_and_shared_by_optional_enrichment(platform, monkeypatch):
    monkeypatch.setattr(
        ctx, "_exchange_authenticated_runtime_context_if_configured", lambda: pytest.fail("auth")
    )
    source = ctx.get_git_source_context()
    assert source.repository_branch == "test"
    assert platform["lookups"] == platform["identities"] == 0
    monkeypatch.setattr(ctx, "_exchange_authenticated_runtime_context_if_configured", lambda: None)
    enriched = ctx.get_code_repository_context()
    assert enriched.source_context is source
    assert enriched.status == "code_repository_branch_not_registered"
    assert enriched.code_repository_uid is enriched.code_repository_branch_uid is None


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


@pytest.mark.parametrize(
    "error",
    [RuntimeError("unavailable"), AuthenticationError("login"), PermissionDeniedError("denied")],
)
def test_failed_enrichment_preserves_git_and_requires_explicit_retry(platform, error):
    platform["lookup_error"] = error
    source = ctx.get_git_source_context()
    for _ in range(2):
        with pytest.raises(ctx.CodeRepositoryContextError):
            ctx.get_code_repository_context()
    assert platform["lookups"] == 1
    assert ctx.get_git_source_context() is source
    platform["lookup_error"] = None
    ctx.retry_failed_context_resolution()
    assert ctx.get_code_repository_context().source_context is source
    assert platform["lookups"] == 2


def test_unregistered_branch_with_explicit_environment_uses_same_resource_path(
    platform, monkeypatch
):
    select()
    requests = []
    monkeypatch.setattr(base, "make_request", lambda **kw: requests.append(kw) or Response([]))
    assert foundry.Secret.filter(name="API_KEY") == []
    scope = ctx.get_organization_environment_context()
    assert (scope.principal_uid, scope.organization_uid, scope.source) == (
        USER,
        ORG,
        "explicit_development",
    )
    assert requests[0]["payload"]["params"] == {
        "name": "API_KEY",
        "organization_environment_uid": ENV,
    }
    assert platform["lookups"] == platform["environments"] == 1
    with pytest.raises(ctx.CodeRepositoryBranchContextRequiredError):
        ctx.require_code_repository_branch_context("Create Job")


def test_missing_environment_error_is_distinct_and_does_not_block_unrelated_calls(platform):
    with pytest.raises(ctx.CodeRepositoryEnvironmentContextRequiredError) as error:
        foundry.Secret.filter()
    assert not isinstance(error.value, ctx.CodeRepositoryBranchContextRequiredError)
    assert users.User.get_authenticated_user_details().uid == USER
    assert ctx.get_git_source_context().repository_branch == "test"
    with pytest.raises(ctx.CodeRepositoryBranchContextRequiredError):
        ctx.scope_current_code_repository_branch_filters("Job.filter", {})
    ctx.retry_failed_context_resolution()
    select()
    assert ctx.resolve_organization_environment_uid("Secret.filter") == ENV


@pytest.mark.parametrize("registered", [False, True])
def test_selection_authorized_through_public_environment_detail(platform, registered):
    platform["registered"] = registered
    select()
    assert ctx.get_organization_environment_context().organization_environment_uid == ENV
    assert platform["environments"] == 1


def test_registered_branch_default_is_preserved(platform):
    platform["registered"] = True
    scope = ctx.get_organization_environment_context()
    assert scope.source == "registered_branch"
    assert scope.organization_environment_uid == ENV


def test_explicit_selection_conflicting_with_registered_branch_fails(platform):
    platform["registered"] = True
    select(OTHER_ENV)
    with pytest.raises(ctx.OrganizationEnvironmentContextError, match="conflicts"):
        ctx.get_organization_environment_context()
    assert platform["environments"] == 0


@pytest.mark.parametrize(
    "error",
    [NotFoundError("not visible"), PermissionDeniedError("denied"), AuthenticationError("expired")],
)
def test_inaccessible_environment_does_not_become_missing_branch(platform, error):
    select()
    platform["environment_error"] = error
    with pytest.raises(type(error)):
        ctx.get_organization_environment_context()
    assert ctx.get_git_source_context() is platform["source"]


def test_wrong_organization_environment_is_rejected(platform):
    select()
    platform["environment_owner"] = "other-organization"
    with pytest.raises(ctx.OrganizationEnvironmentContextError, match="authenticated Organization"):
        ctx.get_organization_environment_context()


@pytest.mark.parametrize("field", ["user", "organization"])
def test_identity_change_requires_reset_and_retry_cannot_retarget(platform, field):
    select()
    original = ctx.get_organization_environment_context()
    platform[field] = "different-identity"
    for _ in range(2):
        with pytest.raises(ctx.AuthenticatedContextChangedError):
            ctx.get_organization_environment_context()
        ctx.retry_failed_context_resolution()
    assert original.principal_uid == USER
    assert original.organization_uid == ORG
    ctx.reset_development_context()
    platform["environment_owner"] = platform["organization"]
    select()
    assert ctx.get_organization_environment_context().principal_uid == platform["user"]


def test_same_principal_token_refresh_keeps_identical_scope(platform, monkeypatch):
    select()
    original = ctx.get_organization_environment_context()
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "new-token-same-principal")
    assert ctx.get_organization_environment_context() is original
    assert platform["identities"] == 2
    assert platform["environments"] == 1


def test_selection_is_typed_validated_and_frozen(platform):
    with pytest.raises(TypeError):
        ctx.configure_development_environment(ENV)
    with pytest.raises(ValueError):
        ctx.DevelopmentEnvironmentSelection("local-workspace")
    select()
    ctx.get_organization_environment_context()
    with pytest.raises(ctx.OrganizationEnvironmentContextError, match="before resolving"):
        select(OTHER_ENV)


def test_runtime_retains_scope_and_omits_development_selector(platform, monkeypatch):
    platform["registered"] = True
    install_runtime()
    requests = []
    monkeypatch.setattr(base, "make_request", lambda **kw: requests.append(kw) or Response([]))
    assert foundry.Secret.filter() == []
    assert ctx.get_organization_environment_context().source == "authenticated_runtime"
    assert requests[0]["payload"].get("params", {}) == {}
    with pytest.raises(ctx.OrganizationEnvironmentContextError, match="runtime"):
        select()
    with pytest.raises(ctx.CodeRepositoryContextError, match="fresh process"):
        ctx.reset_development_context()


def test_runtime_cannot_install_over_development_selection(platform):
    select()
    with pytest.raises(ctx.OrganizationEnvironmentContextError, match="runtime"):
        install_runtime()


@pytest.mark.parametrize("mode", ["session_jwt", "runtime_credential"])
def test_runtime_auth_modes_reject_development_selection_before_exchange(
    platform, monkeypatch, mode
):
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", mode)
    if mode == "session_jwt":
        monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", runtime_token())
    with pytest.raises(ctx.OrganizationEnvironmentContextError, match="runtime"):
        select()
    assert platform["lookups"] == 0


def test_session_runtime_context_uses_server_verified_git_target(platform, monkeypatch):
    platform["registered"] = True
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "session_jwt")
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", runtime_token())
    assert ctx.get_organization_environment_context().source == "authenticated_runtime"
    assert ctx.is_authenticated_runtime_code_repository_context()


def test_runtime_target_mismatch_is_preserved_for_environment_calls(platform):
    platform["registered"] = True
    platform["branch_env"] = OTHER_ENV
    install_runtime()
    with pytest.raises(ctx.CodeRepositoryContextError, match="authenticated runtime target"):
        ctx.get_organization_environment_context()
    assert platform["environments"] == 0


def test_concurrent_source_and_scope_resolution_use_shared_snapshots(platform):
    select()
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        sources = list(pool.map(lambda _: ctx.get_git_source_context(), range(16)))
        scopes = list(pool.map(lambda _: ctx.get_organization_environment_context(), range(16)))
    assert all(source is sources[0] for source in sources)
    assert all(scope is scopes[0] for scope in scopes)
    assert platform["lookups"] == platform["environments"] == 1


def test_reset_and_selection_rejected_during_environment_resolution(platform, monkeypatch):
    select()
    entered, proceed = threading.Event(), threading.Event()
    original = ctx._verify_environment_access

    def slow_verify(uid, org):
        entered.set()
        assert proceed.wait(5)
        original(uid, org)

    monkeypatch.setattr(ctx, "_verify_environment_access", slow_verify)
    with concurrent.futures.ThreadPoolExecutor() as pool:
        pending = pool.submit(ctx.get_organization_environment_context)
        assert entered.wait(5)
        try:
            with pytest.raises(ctx.CodeRepositoryContextError, match="in progress"):
                ctx.reset_development_context()
            with pytest.raises(ctx.CodeRepositoryContextError, match="in progress"):
                ctx.retry_failed_context_resolution()
            with pytest.raises(ctx.OrganizationEnvironmentContextError):
                select(OTHER_ENV)
        finally:
            proceed.set()
        assert pending.result().organization_environment_uid == ENV


def test_fork_reinitializes_all_process_owned_state(platform, monkeypatch):
    select()
    original = ctx.get_organization_environment_context()
    monkeypatch.setattr(ctx.os, "getpid", lambda: original.process_id + 1)
    assert ctx.get_git_source_context() is platform["source"]
    with pytest.raises(ctx.CodeRepositoryEnvironmentContextRequiredError):
        ctx.get_organization_environment_context()
    assert platform["lookups"] == 2


def test_after_fork_does_not_reuse_parent_condition(platform):
    old = ctx._STATE_CONDITION
    ctx._after_fork()
    assert ctx._STATE_CONDITION is not old
    assert ctx._SOURCE_STATE.phase == "uninitialized"


def test_network_free_source_drift_and_explicit_reset(platform):
    original = ctx.get_git_source_context()
    platform["source"] = ctx.GitCodeRepositorySourceContext(
        original.repository_root,
        original.canonical_repository_identity,
        "another",
        "refs/heads/another",
        "b" * 40,
    )
    with pytest.raises(ctx.CodeRepositorySourceContextDriftError):
        ctx.validate_git_source_context()
    assert ctx.get_git_source_context() is original
    ctx.reset_development_context()
    assert ctx.get_git_source_context().repository_branch == "another"
    assert platform["lookups"] == 0


def test_explicit_directory_cannot_select_different_repository(platform, tmp_path):
    ctx.get_git_source_context()
    with pytest.raises(ctx.CodeRepositorySourceContextDriftError):
        ctx.get_code_repository_context(code_repository_dir=tmp_path)


def test_public_environment_contract_and_resource_transport_on_unregistered_branch(
    platform, monkeypatch
):
    monkeypatch.setattr(
        users.User, "get_authenticated_user_details", platform["original_user_details"]
    )
    monkeypatch.setattr(
        users.OrganizationEnvironment, "get_by_uid", platform["original_environment_detail"]
    )
    requests = []

    def request(**kw):
        requests.append(kw)
        url = kw["url"]
        if url.endswith("/users/me/"):
            return Response(
                {
                    "uid": USER,
                    "username": "developer",
                    "email": "developer@example.test",
                    "organization": {
                        "uid": ORG,
                        "name": "Example",
                        "organization_domain": "example.test",
                    },
                    "date_joined": "2026-09-28T00:00:00Z",
                    "is_active": True,
                    "api_request_limit": 100,
                    "mfa_enabled": True,
                }
            )
        if "/organization-environments/" in url:
            return Response(
                {
                    "uid": ENV,
                    "name": "Development",
                    "organization_owner_uid": ORG,
                    "required_repository_branch": "development",
                    "is_production": False,
                    "future_field": "ignored",
                }
            )
        if url.endswith("/secrets/"):
            return Response(
                {"uid": OTHER_ENV, "name": "TEST", "organization_environment_uid": ENV}, 201
            )
        pytest.fail(f"Unexpected request {url}")

    monkeypatch.setattr(users, "make_request", request)
    monkeypatch.setattr(base, "make_request", request)
    select()
    result = foundry.Secret.create(name="TEST", value="test-value")
    assert result.organization_environment_uid == ENV
    assert requests[1]["url"].endswith(f"/organization-environments/{ENV}/")
    assert requests[1]["payload"]["params"] == {}
    assert requests[-1]["payload"]["json"]["organization_environment_uid"] == ENV
    assert all(kw["loaders"] is utils.loaders for kw in requests)


def test_import_does_not_resolve_source_or_load_database_engines():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import sys
import mainsequence.code_repository_context as context
assert context._SOURCE_STATE.phase == "uninitialized"
assert context._STATE.phase == "uninitialized"
assert context._ENVIRONMENT_STATE.phase == "uninitialized"
assert not {"duckdb", "sqlite3", "sqlalchemy", "metatables"}.intersection(sys.modules)
""",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "resource", [foundry.Secret, foundry.Constant, foundry.Bucket, foundry.Artifact]
)
def test_all_environment_resources_support_unregistered_branch(platform, monkeypatch, resource):
    select()
    requests = []
    monkeypatch.setattr(base, "make_request", lambda **kw: requests.append(kw) or Response([]))
    assert resource.filter() == []
    assert requests[0]["payload"]["params"]["organization_environment_uid"] == ENV
    with pytest.raises(ValueError):
        resource.filter(organization_environment_uid=OTHER_ENV)


def test_runtime_auth_switch_cannot_reuse_development_branch_snapshot(platform, monkeypatch):
    platform["registered"] = True
    ctx.get_code_repository_context()
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "session_jwt")
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", runtime_token())
    with pytest.raises(ctx.AuthenticatedContextChangedError, match="authentication mode"):
        ctx.get_organization_environment_context()


def test_environment_lookup_outage_requires_retry_and_keeps_source(platform):
    select()
    source = ctx.get_git_source_context()
    platform["environment_error"] = RuntimeError("temporary outage")
    with pytest.raises(RuntimeError, match="temporary outage"):
        ctx.get_organization_environment_context()
    platform["environment_error"] = None
    with pytest.raises(RuntimeError, match="temporary outage"):
        ctx.get_organization_environment_context()
    ctx.retry_failed_context_resolution()
    assert ctx.get_git_source_context() is source
    assert ctx.get_organization_environment_context().organization_environment_uid == ENV


def test_environment_discovery_does_not_resolve_git(platform, monkeypatch):
    monkeypatch.setattr(ctx, "get_code_repository_context", lambda: pytest.fail("branch lookup"))
    monkeypatch.setattr(ctx, "get_git_source_context", lambda: pytest.fail("source lookup"))
    monkeypatch.setattr(base, "make_request", lambda **kw: Response([]))
    assert users.OrganizationEnvironment.filter() == []
    assert users.User.get_authenticated_user_details().uid == USER


@pytest.mark.parametrize(
    "token",
    ["forwarded-human-token", "header.W10.signature", "malformed..token", "header._w.signature"],
)
def test_session_jwt_mode_alone_does_not_turn_human_into_runtime(platform, monkeypatch, token):
    monkeypatch.setenv("MAINSEQUENCE_AUTH_MODE", "session_jwt")
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", token)
    select()
    assert ctx.get_organization_environment_context().source == "explicit_development"


def test_runtime_label_cannot_supply_context_without_platform_verification(platform, monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", runtime_token())
    platform["lookup_error"] = AuthenticationError("unverified token", response=Response({}, 401))
    with pytest.raises(ctx.CodeRepositoryContextError, match="authentication"):
        ctx.get_organization_environment_context()
    assert platform["environments"] == 0


@pytest.mark.parametrize("field", ["user", "organization"])
def test_platform_branch_cache_cannot_cross_identity_before_environment_use(platform, field):
    original = ctx.get_code_repository_context()
    platform[field] = "changed-identity"
    for _ in range(2):
        with pytest.raises(ctx.AuthenticatedContextChangedError):
            ctx.get_code_repository_context()
        ctx.retry_failed_context_resolution()
    assert original.principal_uid == USER
    assert platform["lookups"] == 1
    assert ctx.get_git_source_context() is original.source_context


def test_principal_without_organization_can_read_source_but_not_environment(platform):
    platform["organization"] = None
    select()
    assert ctx.get_code_repository_context().organization_uid is None
    with pytest.raises(ctx.OrganizationEnvironmentContextError, match="User and Organization"):
        ctx.get_organization_environment_context()
    assert ctx.get_git_source_context().repository_branch == "test"


def test_same_principal_refresh_keeps_identical_enriched_snapshot(platform, monkeypatch):
    original = ctx.get_code_repository_context()
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "new-human-access-token")
    assert ctx.get_code_repository_context() is original
    assert platform["lookups"] == 1


def test_temporary_identity_outage_preserves_binding_after_retry(platform):
    select()
    original = ctx.get_organization_environment_context()
    platform["identity_error"] = RuntimeError("identity unavailable")
    with pytest.raises(RuntimeError, match="identity unavailable"):
        ctx.get_organization_environment_context()
    platform["identity_error"] = None
    ctx.retry_failed_context_resolution()
    assert ctx.get_organization_environment_context() is original


def test_wrong_requested_repository_does_not_poison_resolved_snapshot(platform):
    platform["registered"] = True
    original = ctx.get_code_repository_context()
    with pytest.raises(ctx.CodeRepositoryContextError, match="Requested CodeRepository"):
        ctx.get_code_repository_context(code_repository_uid="other-repository")
    assert ctx.get_code_repository_context() is original


def test_nested_explicit_checkout_cannot_replace_frozen_source(platform):
    original = ctx.get_git_source_context()
    nested = original.repository_root / "nested-repository"
    platform["source"] = ctx.GitCodeRepositorySourceContext(
        nested,
        "github.com/example/nested",
        "main",
        "refs/heads/main",
        "b" * 40,
    )
    with pytest.raises(ctx.CodeRepositorySourceContextDriftError):
        ctx.get_git_source_context(code_repository_dir=nested)
    assert ctx.get_git_source_context() is original
