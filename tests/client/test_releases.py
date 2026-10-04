import pytest
from pydantic import ValidationError

import mainsequence.client.base as base_mod
import mainsequence.client.models_helpers as models_helpers_mod
from tests.client.support import CODE_REPOSITORY_BRANCH_UID, _runtime_access_payload


def test_resource_release_model_accepts_collection_payload_without_runtime_child_fields():
    release_uid = "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed"
    code_repository_branch_uid = "9d81d63f-b8c9-404d-9f1a-5f2ad29dbf16"

    release = models_helpers_mod.ResourceRelease.model_validate(
        {
            "uid": release_uid,
            "code_repository_branch_uid": code_repository_branch_uid,
            "name": "Competition Analysis",
            "release_kind": "static_site",
            "automatic_deployment": True,
        }
    )

    assert release.uid == release_uid
    assert release.code_repository_branch_uid == code_repository_branch_uid
    assert release.name == "Competition Analysis"
    assert release.release_kind == models_helpers_mod.ResourceReleaseKind.STATIC_SITE
    assert release.resource_uid is None
    assert release.readme_resource_uid is None
    assert release.related_job_uid is None
    assert "subdomain" not in models_helpers_mod.ResourceRelease.model_fields


def test_resource_release_model_supports_automatic_deployment_payloads():
    release_uid = "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed"
    resource_uid = "857bec7b-dd77-4272-aecd-13fc2138eacc"
    job_uid = "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da"

    release = models_helpers_mod.ResourceRelease.model_validate(
        {
            "uid": release_uid,
            "resource_uid": resource_uid,
            "readme_resource_uid": None,
            "related_job_uid": job_uid,
            "release_kind": "fastapi",
            "automatic_deployment": True,
            "automatic_redeployment_policy": {
                "tag_regex": None,
                "policy_revision": 3,
            },
            "revision_retention_count": 3,
            "active_revision": None,
            "desired_revision": "2a9370a7-c07f-439c-bcd9-629e3e916699",
        }
    )

    assert release.uid == release_uid
    assert release.release_kind == models_helpers_mod.ResourceReleaseKind.FAST_API
    assert release.automatic_deployment is True
    assert release.automatic_redeployment_policy is not None
    assert release.automatic_redeployment_policy.tag_regex is None
    assert release.automatic_redeployment_policy.policy_revision == 3
    assert release.revision_retention_count == 3
    assert release.active_revision is None
    assert release.desired_revision == "2a9370a7-c07f-439c-bcd9-629e3e916699"


def test_resource_release_filter_accepts_canonical_revision_lifecycle_code_repositoryion(
    monkeypatch,
):
    release_uid = "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed"
    desired_revision_uid = "2a9370a7-c07f-439c-bcd9-629e3e916699"
    response_payload = {
        "uid": release_uid,
        "code_repository_branch_uid": CODE_REPOSITORY_BRANCH_UID,
        "name": "Pricing API",
        "release_kind": "fastapi",
        "automatic_deployment": True,
        "automatic_redeployment_policy": {
            "tag_regex": None,
            "policy_revision": 2,
        },
        "revision_retention_count": 3,
        "active_revision": None,
        "desired_revision": desired_revision_uid,
    }

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return [dict(response_payload)]

    monkeypatch.setattr(base_mod, "make_request", lambda **kwargs: FakeResponse())

    releases = models_helpers_mod.ResourceRelease.filter(
        code_repository_branch_uid=CODE_REPOSITORY_BRANCH_UID
    )

    assert len(releases) == 1
    assert releases[0].uid == release_uid
    assert releases[0].revision_retention_count == 3
    assert releases[0].active_revision is None
    assert releases[0].desired_revision == desired_revision_uid


def test_legacy_resource_release_deployment_run_model_is_not_exported():
    import mainsequence.client as client_package

    legacy_name = "ResourceReleaseAutomaticDeploymentRun"

    assert not hasattr(models_helpers_mod, legacy_name)
    assert not hasattr(client_package, legacy_name)
    with pytest.raises(KeyError, match=legacy_name):
        models_helpers_mod.get_model_class(legacy_name)


def test_streamlit_dashboard_contract_is_removed():
    assert not hasattr(models_helpers_mod.ResourceReleaseKind, "STREAMLIT_DASHBOARD")
    assert not hasattr(models_helpers_mod.CodeRepositoryResource, "create_dashboard")

    with pytest.raises(ValidationError):
        models_helpers_mod.CodeRepositoryResource.model_validate(
            {
                "uid": "857bec7b-dd77-4272-aecd-13fc2138eacc",
                "resource_type": "dashboard",
            }
        )

    # ADR 0033: the retired kind is no longer declared, so a response carrying it
    # is read as sent rather than failing the release and the listing it is in.
    retired = models_helpers_mod.ResourceRelease.model_validate(
        {
            "uid": "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed",
            "release_kind": "streamlit_dashboard",
        }
    )
    assert retired.release_kind == "streamlit_dashboard"
    assert retired.release_kind not in set(models_helpers_mod.ResourceReleaseKind)

    with pytest.raises(ValidationError):
        models_helpers_mod.ResourceRelease.model_validate(
            {"uid": "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed"}
        )


@pytest.mark.parametrize(
    "resource_type",
    [
        "configuration",
        "notebook",
        "script",
        "agent",
        "fastapi",
        "code_repository_agent_card",
        "markdown",
    ],
)
def test_code_repository_resource_accepts_canonical_backend_resource_types(resource_type):
    resource = models_helpers_mod.CodeRepositoryResource.model_validate(
        {
            "uid": "857bec7b-dd77-4272-aecd-13fc2138eacc",
            "code_repository_branch_uid": CODE_REPOSITORY_BRANCH_UID,
            "name": ".agents/agent_card.json",
            "resource_type": resource_type,
            "path": ".agents/agent_card.json",
            "repo_commit_sha": "abc123",
        }
    )

    assert resource.resource_type == resource_type


def test_resource_release_get_accepts_fastapi_cors_allowed_origins(monkeypatch):
    captured = {}
    release_uid = "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed"
    response_payload = {
        "uid": release_uid,
        "resource_uid": "857bec7b-dd77-4272-aecd-13fc2138eacc",
        "readme_resource_uid": None,
        "related_job_uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
        "release_kind": "fastapi",
        "automatic_deployment": True,
        "automatic_redeployment_policy": None,
        "revision_retention_count": 4,
        "active_revision": "19128ab6-d72f-460c-8525-d758fa92676a",
        "desired_revision": None,
        "cors_allowed_origins": [
            "https://app.example.com",
            "https://*.site-dev.main-sequence.app",
        ],
    }

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return dict(response_payload)

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

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    release = models_helpers_mod.ResourceRelease.get(pk=release_uid, timeout=13)

    assert release.release_kind == models_helpers_mod.ResourceReleaseKind.FAST_API
    assert release.revision_retention_count == 4
    assert release.active_revision == "19128ab6-d72f-460c-8525-d758fa92676a"
    assert release.desired_revision is None
    assert release.cors_allowed_origins == response_payload["cors_allowed_origins"]
    assert captured["r_type"] == "GET"
    assert captured["url"].endswith(f"/resource-releases/{release_uid}/")
    assert captured["timeout"] == 13


def test_resource_release_patch_supports_positive_revision_retention_count(monkeypatch):
    captured = {}
    release_uid = "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed"
    release = models_helpers_mod.ResourceRelease(
        uid=release_uid,
        release_kind="fastapi",
        revision_retention_count=3,
    )

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "revision_retention_count": 6,
                "active_revision": None,
                "desired_revision": None,
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured.update({"r_type": r_type, "url": url, "payload": payload})
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    patched = release.patch(revision_retention_count=6)

    assert patched is release
    assert patched.revision_retention_count == 6
    assert captured["r_type"] == "PATCH"
    assert captured["url"].endswith(f"/resource-releases/{release_uid}/")
    assert captured["payload"]["json"] == {"revision_retention_count": 6}


def test_resource_release_resolve_runtime_access_posts_django_command(monkeypatch):
    captured = {}
    release = models_helpers_mod.ResourceRelease(
        uid="2f4c4c3d-5669-4da5-9d86-b84633c1e6ed",
        release_kind="fastapi",
    )

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return _runtime_access_payload(state="ready", can_request=True)

    def _fake_make_request(**kwargs):
        captured.update(kwargs)
        return FakeResponse()

    monkeypatch.setattr(models_helpers_mod, "make_request", _fake_make_request)
    access = release.resolve_runtime_access(
        static_site_release_uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        timeout=9,
    )

    assert access.runtime_access.can_request is True
    assert access.access["token"] == "runtime-token"
    assert captured["r_type"] == "POST"
    assert captured["url"].endswith(
        "/resource-releases/2f4c4c3d-5669-4da5-9d86-b84633c1e6ed/resolve-runtime-access/"
    )
    assert captured["payload"]["json"] == {
        "static_site_release_uid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    }


def test_resource_release_waits_on_django_retry_after(monkeypatch):
    release = models_helpers_mod.ResourceRelease(
        uid="2f4c4c3d-5669-4da5-9d86-b84633c1e6ed",
        release_kind="fastapi",
    )
    responses = iter(
        [
            _runtime_access_payload(state="waking", can_request=False, retry_after_ms=2000),
            _runtime_access_payload(state="ready", can_request=True),
        ]
    )
    monkeypatch.setattr(
        models_helpers_mod.ResourceRelease,
        "resolve_runtime_access",
        lambda self, **kwargs: models_helpers_mod.ResourceReleaseRuntimeAccess.model_validate(
            next(responses)
        ),
    )
    slept = []
    monkeypatch.setattr(models_helpers_mod.time, "sleep", slept.append)
    monotonic = iter([0.0, 0.0, 2.0])
    monkeypatch.setattr(models_helpers_mod.time, "monotonic", lambda: next(monotonic))

    access = release.wait_for_runtime_access(wait_timeout_seconds=10)

    assert access.runtime_access.can_request is True
    assert slept == [2.0]


@pytest.mark.parametrize("invalid_value", [None, 0, -1, True, 1.5, "3"])
def test_resource_release_patch_rejects_invalid_revision_retention_count(
    monkeypatch, invalid_value
):
    monkeypatch.setattr(
        base_mod,
        "make_request",
        lambda **kwargs: pytest.fail("invalid retention must fail before the request"),
    )

    with pytest.raises(
        ValueError,
        match="revision_retention_count must be a positive integer",
    ):
        models_helpers_mod.ResourceRelease.patch_by_uid(
            "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed",
            revision_retention_count=invalid_value,
        )


def test_resource_release_does_not_offer_manual_deployment():
    assert not hasattr(models_helpers_mod.ResourceRelease, "deploy_current_version")


@pytest.mark.parametrize("method_name", ["filter", "get"])
def test_resource_release_name_lookup_keeps_current_branch_scope(monkeypatch, method_name):
    captured = {}
    release_uid = "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed"

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return [
                {
                    "uid": release_uid,
                    "name": "Shared API",
                    "code_repository_branch_uid": CODE_REPOSITORY_BRANCH_UID,
                    "release_kind": "fastapi",
                    "automatic_deployment": True,
                }
            ]

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", fake_make_request)
    result = getattr(models_helpers_mod.ResourceRelease, method_name)(name="Shared API")

    assert captured["payload"]["params"] == {
        "name": "Shared API",
        "code_repository_branch_uid": CODE_REPOSITORY_BRANCH_UID,
    }
    release = result[0] if method_name == "filter" else result
    assert release.uid == release_uid


def test_resource_release_name_admin_filter_uses_the_explicit_owning_branch(monkeypatch):
    owning_branch_uid = "42c4b562-4da5-49bb-a3b4-1372c9491738"
    captured = {}

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return []

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", fake_make_request)
    assert (
        models_helpers_mod.ResourceRelease.filter_admin(
            name="Shared API",
            code_repository_branch_uid=owning_branch_uid,
            release_kind="fastapi",
        )
        == []
    )
    assert captured["payload"]["params"] == {
        "name": "Shared API",
        "code_repository_branch_uid": owning_branch_uid,
        "release_kind": "fastapi",
    }


def test_resource_release_name_filter_supports_only_exact_matching():
    with pytest.raises(ValueError, match="Unsupported ResourceRelease filter"):
        models_helpers_mod.ResourceRelease._normalize_filter_kwargs({"name__contains": "API"})
