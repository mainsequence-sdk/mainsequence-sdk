import datetime

import pytest

import mainsequence.client.base as base_mod
import mainsequence.client.models_foundry as models_foundry_mod
import mainsequence.client.models_helpers as models_helpers_mod
import mainsequence.client.models_user as models_user_mod
from tests.client.support import ENVIRONMENT_UID


def test_nullable_response_fields_accept_null_from_the_backend():
    """#131: fields the backend documents as nullable must parse when null."""
    job = models_helpers_mod.Job.model_validate(
        {
            "uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
            "name": "Daily prices",
            "code_repository_branch_uid": "5a28020a-0f1b-47ee-aab8-334286234bea",
            "related_image_uid": None,
            "image_status": "building",
            "automatic_deployment": True,
        }
    )
    assert job.related_image_uid is None

    run = models_helpers_mod.JobRun.model_validate(
        {
            "name": "daily-prices-run",
            "unique_identifier": "jobrun_1",
            "code_repository_uid": None,
            "code_repository_name": None,
            "code_repository_branch_uid": None,
            "code_repository_branch_name": None,
            "organization_environment_uid": ENVIRONMENT_UID,
            "runtime_image_uid": None,
            "runtime_image_digest": None,
        }
    )
    assert run.runtime_image_uid is None
    assert run.runtime_image_digest is None

    organization_payload = {
        "uid": "8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123",
        "name": "Main Sequence",
        "url": "https://backend.test",
        "organization_domain": None,
        "identity_platform_tenant_id": None,
        "has_pending_invoices": False,
        "production_environment_uid": "00000000-0000-4000-8000-000000000002",
    }
    organization = models_user_mod.Organization.model_validate(organization_payload)
    assert organization.organization_domain is None

    user = models_user_mod.User.model_validate(
        {
            "id": 4,
            "uid": "fdf409f7-d16f-4f71-986b-9057db6c7eca",
            "username": "jose",
            "email": "jose@main-sequence.io",
            "profile_picture": None,
            "last_login": None,
            "api_request_limit": 10000,
            "mfa_enabled": False,
            "organization": organization_payload,
            "plan": None,
            "groups": [],
            "phone_number": None,
            "organization_teams": [],
            "is_active": True,
            "date_joined": "2026-01-01T00:00:00Z",
        }
    )
    assert user.organization.organization_domain is None


def test_secret_constant_bucket_artifact_accept_uid_identity_payloads():
    secret = models_foundry_mod.Secret(uid="11111111-1111-4111-8111-111111111111", name="API_KEY")
    constant = models_foundry_mod.Constant(
        uid="22222222-2222-4222-8222-222222222222",
        name="APP__MODE",
        value="production",
    )
    bucket = models_foundry_mod.Bucket(
        uid="33333333-3333-4333-8333-333333333333",
        name="default_bucket",
    )
    artifact = models_foundry_mod.Artifact(
        uid="44444444-4444-4444-8444-444444444444",
        name="report.pdf",
        bucket_name="default_bucket",
        bucket_uid="33333333-3333-4333-8333-333333333333",
        content="https://signed.example/report.pdf",
        creation_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
    )

    assert secret.uid == "11111111-1111-4111-8111-111111111111"
    assert constant.uid == "22222222-2222-4222-8222-222222222222"
    assert bucket.uid == "33333333-3333-4333-8333-333333333333"
    assert artifact.uid == "44444444-4444-4444-8444-444444444444"


@pytest.mark.parametrize(
    "model",
    [
        models_foundry_mod.CodeRepositoryImage,
        models_helpers_mod.Job,
        models_helpers_mod.ResourceRelease,
    ],
)
def test_retired_collection_create_fails_before_http(monkeypatch, model):
    monkeypatch.setattr(
        base_mod,
        "make_request",
        lambda **kwargs: pytest.fail("retired collection create must not send HTTP"),
    )

    with pytest.raises(NotImplementedError, match="repository workflow declarations"):
        model.create()
