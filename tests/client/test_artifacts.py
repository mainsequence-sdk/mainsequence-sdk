"""`Artifact.upload_file()` and `get_or_create()` raise the SDK's typed errors (#232).

The HTTP layer is mocked at the `make_request` the Artifact model calls; the
current branch's Environment comes from the package's conftest.
"""

from __future__ import annotations

import json

import pytest

import mainsequence.client.models_foundry as models_foundry
from mainsequence.client.exceptions import (
    BadRequestError,
    NotFoundError,
    PermissionDeniedError,
    ServerError,
)
from mainsequence.client.models_foundry import Artifact
from tests.client.support import ENVIRONMENT_UID

BUCKET_UID = "33333333-3333-4333-8333-333333333333"
ARTIFACT_UID = "44444444-4444-4444-8444-444444444444"
REFUSAL = {
    "detail": "This needs the buckets.add_artifact action.",
    "code": "missing_action",
    "required_action": "buckets.add_artifact",
    "granting_policies": ["buckets.contributor", "buckets.editor", "buckets.owner"],
    "resource_type": "buckets",
    "resource_uid": BUCKET_UID,
}


class _Response:
    def __init__(self, status_code, body=None):
        self.status_code = status_code
        self._body = body
        self.text = "" if body is None else json.dumps(body)
        self.content = self.text.encode()
        self.headers = {}

    def json(self):
        if self._body is None:
            raise ValueError("No JSON body")
        return self._body


@pytest.fixture()
def report(tmp_path):
    path = tmp_path / "report.pdf"
    path.write_bytes(b"report")
    return path


def _answer(monkeypatch, response):
    sent = []

    def fake_make_request(**kwargs):
        sent.append(kwargs)
        return response

    monkeypatch.setattr(models_foundry, "make_request", fake_make_request)
    return sent


UPLOADS = [
    lambda path: Artifact.upload_file(str(path), "report.pdf", bucket_name="vendor_prices"),
    lambda path: Artifact.get_or_create(str(path), "report.pdf", "vendor_prices"),
]


@pytest.mark.parametrize("upload", UPLOADS, ids=["upload_file", "get_or_create"])
def test_a_refused_upload_raises_permission_denied_with_the_platforms_answer(
    monkeypatch, report, upload
):
    # The caller holds no contributor policy on the existing bucket it names.
    sent = _answer(monkeypatch, _Response(403, REFUSAL))

    with pytest.raises(PermissionDeniedError) as raised:
        upload(report)

    assert raised.value.status_code == 403
    assert raised.value.payload == REFUSAL
    assert "This needs the buckets.add_artifact action." in str(raised.value)
    assert sent[0]["r_type"] == "POST"
    assert sent[0]["url"].endswith("/artifacts/get-or-create/")
    assert sent[0]["payload"]["json"] == {
        "name": "report.pdf",
        "bucket_name": "vendor_prices",
        "organization_environment_uid": ENVIRONMENT_UID,
    }


@pytest.mark.parametrize(
    ("response", "error"),
    [
        (_Response(400, {"bucket_name": ["This field may not be blank."]}), BadRequestError),
        (_Response(404, {"detail": "Not found."}), NotFoundError),
        (_Response(500), ServerError),
    ],
    ids=["bad-request", "not-found", "server-error"],
)
def test_another_failed_upload_raises_the_matching_api_error(monkeypatch, report, response, error):
    _answer(monkeypatch, response)

    with pytest.raises(error) as raised:
        Artifact.upload_file(str(report), "report.pdf", bucket_name="vendor_prices")

    assert raised.value.status_code == response.status_code
    assert "Failed to get artifact" not in str(raised.value)


@pytest.mark.parametrize("status_code", [200, 201])
def test_a_successful_upload_returns_the_artifact(monkeypatch, report, status_code):
    _answer(
        monkeypatch,
        _Response(
            status_code,
            {
                "uid": ARTIFACT_UID,
                "name": "report.pdf",
                "bucket_name": "vendor_prices",
                "bucket_uid": BUCKET_UID,
                "content": "https://signed.example/report.pdf",
                "creation_date": "2026-10-18T09:00:00Z",
                "organization_environment_uid": ENVIRONMENT_UID,
                "organization_environment_name": "Development",
            },
        ),
    )

    artifact = Artifact.upload_file(str(report), "report.pdf", bucket_name="vendor_prices")

    assert artifact.uid == ARTIFACT_UID
    assert artifact.bucket_uid == BUCKET_UID
