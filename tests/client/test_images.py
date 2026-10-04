import datetime

import pytest

import mainsequence.client.base as base_mod
import mainsequence.client.models_foundry as models_foundry_mod
from tests.client.support import _code_repository_image_response


def test_code_repository_image_accepts_creation_date():
    from mainsequence.client.models_foundry import CodeRepositoryImage

    image = CodeRepositoryImage(
        uid="f3cb8477-df47-49cb-a151-80b746fb1243",
        code_repository_commit_hash="abc123",
        related_code_repository_branch_uid="5a28020a-0f1b-47ee-aab8-334286234bea",
        base_image=None,
        build_error=False,
        is_ready=False,
        source_provenance={
            "verification_state": "unverified",
        },
        creation_date="2026-04-07T09:00:00Z",
    )

    assert image.creation_date == datetime.datetime(
        2026,
        4,
        7,
        9,
        0,
        tzinfo=datetime.UTC,
    )


@pytest.mark.parametrize("build_error", [False, True])
def test_code_repository_image_accepts_boolean_build_error(build_error):
    image = models_foundry_mod.CodeRepositoryImage.model_validate(
        _code_repository_image_response(
            uid="f3cb8477-df47-49cb-a151-80b746fb1243",
            build_error=build_error,
        )
    )

    assert image.build_error is build_error


@pytest.mark.parametrize("build_error", [False, True])
def test_code_repository_image_get_accepts_boolean_build_error(monkeypatch, build_error):
    payload = _code_repository_image_response(
        uid="f3cb8477-df47-49cb-a151-80b746fb1243",
        build_error=build_error,
    )

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return payload

    monkeypatch.setattr(
        models_foundry_mod.CodeRepositoryImage,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(base_mod, "make_request", lambda **kwargs: FakeResponse())

    image = models_foundry_mod.CodeRepositoryImage.get(pk=payload["uid"])

    assert image.build_error is build_error


def test_code_repository_image_filter_accepts_boolean_build_error(monkeypatch):
    payloads = [
        _code_repository_image_response(
            uid="f3cb8477-df47-49cb-a151-80b746fb1243",
            build_error=False,
        ),
        _code_repository_image_response(
            uid="39dc72dc-d905-43af-8012-d67c026c2970",
            build_error=True,
        ),
    ]

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"results": payloads, "next": None}

    monkeypatch.setattr(
        models_foundry_mod.CodeRepositoryImage,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(base_mod, "make_request", lambda **kwargs: FakeResponse())

    images = models_foundry_mod.CodeRepositoryImage.filter(
        related_code_repository_branch_uid="5a28020a-0f1b-47ee-aab8-334286234bea"
    )

    assert [image.build_error for image in images] == [False, True]
