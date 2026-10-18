import pytest

import mainsequence.client.base as base_mod
from tests.client.support import (
    DemoAliasedPatchModel,
    DemoDestroyModel,
    DemoFilterModel,
    DemoIdOnlyResource,
    DemoPatchModel,
    DemoReadModel,
    _IdRef,
)


def test_normalize_filter_kwargs_coerces_supported_values():
    normalized = DemoFilterModel._normalize_filter_kwargs(
        {
            "name__contains": "  momentum  ",
            "name__in": [" growth ", "value"],
            "parent__id__in": [1, _IdRef(2), {"id": 3}],
            "active__isnull": "true",
        }
    )

    assert normalized == {
        "name__contains": "momentum",
        "name__in": ["growth", "value"],
        "parent__id__in": [1, 2, 3],
        "active__isnull": True,
    }


def test_normalize_filter_kwargs_rejects_unsupported_filters():
    with pytest.raises(ValueError, match="Unsupported DemoFilterModel filter"):
        DemoFilterModel._normalize_filter_kwargs({"unsupported": 1})


def test_normalize_destroy_kwargs_coerces_supported_values():
    normalized = DemoDestroyModel._normalize_destroy_kwargs(
        {
            "full_delete_selected": "true",
            "override_protection": False,
        }
    )

    assert normalized == {
        "full_delete_selected": "true",
        "override_protection": "false",
    }


def test_destroy_by_uid_uses_query_params(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 204

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    DemoDestroyModel.destroy_by_uid(
        "uid-7",
        full_delete_selected=True,
        override_protection="false",
        timeout=30,
    )

    assert captured == {
        "r_type": "DELETE",
        "url": "https://backend.test/demo/uid-7/",
        "payload": {
            "params": {
                "full_delete_selected": "true",
                "override_protection": "false",
            }
        },
        "timeout": 30,
    }


def test_iter_filter_merges_read_query_params(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {"results": [], "next": None}

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    results = list(
        DemoReadModel.iter_filter(
            id=7,
            include_relations_detail=True,
            timeout=12,
        )
    )

    assert results == []
    assert captured == {
        "r_type": "GET",
        "url": "https://backend.test/demo-read/",
        "payload": {"params": {"id": 7, "include_relations_detail": "true"}},
        "timeout": 12,
    }


def test_get_by_uid_normalizes_read_query_params(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {"id": 9}

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    result = DemoReadModel.get_by_uid("uid-9", include_relations_detail=False, timeout=8)

    assert isinstance(result, DemoReadModel)
    assert result.id == 9
    assert captured == {
        "r_type": "GET",
        "url": "https://backend.test/demo-read/uid-9/",
        "payload": {"params": {"include_relations_detail": "false"}},
        "timeout": 8,
    }


def test_patch_by_uid_ignores_unmapped_response_fields(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "id": 9,
                "schema": {"name": "customers"},
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    instance = DemoPatchModel(id=9)
    patched = DemoPatchModel.patch_by_uid("uid-9", _into=instance, label="patched")

    # Tolerant reading (ADR 0032): a response key the model does not declare is
    # dropped, exactly as parsing drops it.
    assert patched is instance
    assert patched.id == 9
    assert "schema" not in DemoPatchModel.model_fields
    assert "schema" not in patched.__dict__
    assert captured == {
        "r_type": "PATCH",
        "url": "https://backend.test/demo-patch/uid-9/",
        "payload": {"json": {"label": "patched"}},
        "timeout": None,
    }


def test_patch_by_uid_updates_aliased_field_on_existing_instance(monkeypatch):
    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "id": 9,
                "schema": {"name": "customers"},
            }

    monkeypatch.setattr(
        base_mod,
        "make_request",
        lambda *args, **kwargs: FakeResponse(),
    )

    instance = DemoAliasedPatchModel(id=9)
    patched = DemoAliasedPatchModel.patch_by_uid("uid-9", _into=instance)

    assert patched is instance
    assert instance.schema_payload == {"name": "customers"}


def test_id_only_resource_patch_does_not_route_by_integer_id(monkeypatch):
    def _unexpected_request(**kwargs):
        raise AssertionError("id-only resource should not make a PATCH request")

    monkeypatch.setattr(base_mod, "make_request", _unexpected_request)

    instance = DemoPatchModel(id=9)

    with pytest.raises(ValueError, match="non-empty uid"):
        instance.patch(label="patched")


def test_id_only_resource_delete_does_not_route_by_integer_id(monkeypatch):
    def _unexpected_request(**kwargs):
        raise AssertionError("id-only resource should not make a DELETE request")

    monkeypatch.setattr(base_mod, "make_request", _unexpected_request)

    with pytest.raises(ValueError, match="non-empty uid"):
        DemoIdOnlyResource(9).delete()


def test_id_only_resource_detail_action_does_not_route_by_integer_id(monkeypatch):
    def _unexpected_request(**kwargs):
        raise AssertionError("id-only resource should not make a detail action request")

    monkeypatch.setattr(base_mod, "make_request", _unexpected_request)

    with pytest.raises(ValueError, match="non-empty uid"):
        DemoIdOnlyResource(9).get_access()
