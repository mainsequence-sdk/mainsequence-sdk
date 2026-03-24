import ast
import pathlib

import pytest
from pydantic import ConfigDict, Field

import mainsequence.client.base as base_mod
import mainsequence.client.models_user as models_user_mod
from mainsequence.client.base import BaseObjectOrm, BasePydanticModel, ShareableObjectMixin


class _IdRef:
    def __init__(self, value: int):
        self.id = value


class DemoFilterModel(BaseObjectOrm):
    FILTERSET_FIELDS = {
        "name": ["exact", "contains", "in"],
        "parent__id": ["exact", "in"],
        "active": ["isnull"],
    }
    FILTER_VALUE_NORMALIZERS = {
        "parent__id": "id",
    }


class DemoDestroyModel(BaseObjectOrm):
    DESTROY_QUERY_PARAMS = {
        "full_delete_selected": "bool",
        "override_protection": "bool",
    }

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        return "https://backend.test/demo"

    @classmethod
    def build_session(cls):
        return object()


class DemoReadModel(BaseObjectOrm):
    FILTERSET_FIELDS = {
        "id": ["exact"],
    }
    FILTER_VALUE_NORMALIZERS = {
        "id": "id",
    }
    READ_QUERY_PARAMS = {
        "include_relations_detail": "bool",
    }

    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.payload = kwargs

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        return "https://backend.test/demo-read"

    @classmethod
    def build_session(cls):
        return object()


class DemoShareableModel(ShareableObjectMixin, BaseObjectOrm):
    def __init__(self, object_id: int):
        self.id = object_id

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        return "https://backend.test/demo-shareable"

    @classmethod
    def build_session(cls):
        return object()


class DemoPatchModel(BasePydanticModel, BaseObjectOrm):
    id: int
    label: str | None = None

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        return "https://backend.test/demo-patch"

    @classmethod
    def build_session(cls):
        return object()


class DemoAliasedPatchModel(BasePydanticModel, BaseObjectOrm):
    model_config = ConfigDict(populate_by_name=True)

    id: int
    schema_payload: dict | None = Field(default=None, alias="schema")

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        return "https://backend.test/demo-patch"

    @classmethod
    def build_session(cls):
        return object()


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


def test_destroy_by_id_uses_query_params(monkeypatch):
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

    DemoDestroyModel.destroy_by_id(
        7,
        full_delete_selected=True,
        override_protection="false",
        timeout=30,
    )

    assert captured == {
        "r_type": "DELETE",
        "url": "https://backend.test/demo/7/",
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


def test_get_by_pk_normalizes_read_query_params(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200

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

    result = DemoReadModel.get(pk=9, include_relations_detail=False, timeout=8)

    assert isinstance(result, DemoReadModel)
    assert result.id == 9
    assert captured == {
        "r_type": "GET",
        "url": "https://backend.test/demo-read/9/",
        "payload": {"params": {"include_relations_detail": "false"}},
        "timeout": 8,
    }


def test_patch_by_id_raises_with_context_for_unmapped_response_fields(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200

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

    with pytest.raises(ValueError) as exc_info:
        DemoPatchModel.patch_by_id(9, _into=DemoPatchModel(id=9), label="patched")

    assert str(exc_info.value) == (
        "Failed to apply PATCH response to DemoPatchModel at field 'schema'. "
        "Response fragment: {'schema': {'name': 'customers'}}. "
        'Original error: "DemoPatchModel" object has no field "schema"'
    )
    assert captured == {
        "r_type": "PATCH",
        "url": "https://backend.test/demo-patch/9/",
        "payload": {"json": {"label": "patched"}},
        "timeout": None,
    }


def test_patch_by_id_updates_aliased_field_on_existing_instance(monkeypatch):
    class FakeResponse:
        status_code = 200

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
    patched = DemoAliasedPatchModel.patch_by_id(9, _into=instance)

    assert patched is instance
    assert instance.schema_payload == {"name": "customers"}


def test_shareable_action_posts_user_id(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200
        content = b'{"detail":"ok"}'

        @staticmethod
        def json():
            return {"detail": "ok"}

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    response = DemoShareableModel(11).add_to_edit(_IdRef(7), timeout=15)

    assert response == {"detail": "ok"}
    assert captured == {
        "r_type": "POST",
        "url": "https://backend.test/demo-shareable/11/add-to-edit/",
        "payload": {"json": {"user_id": 7}},
        "timeout": 15,
    }


def test_shareable_action_returns_empty_dict_on_no_content(monkeypatch):
    class FakeResponse:
        status_code = 200
        content = b""

    monkeypatch.setattr(base_mod, "make_request", lambda **kwargs: FakeResponse())

    response = DemoShareableModel(9).remove_from_view(3)

    assert response == {}


def test_shareable_team_action_posts_team_id(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200
        content = b'{"detail":"ok"}'

        @staticmethod
        def json():
            return {"detail": "ok"}

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    response = DemoShareableModel(11).add_team_to_view(_IdRef(5), timeout=21)

    assert response == {"detail": "ok"}
    assert captured == {
        "r_type": "POST",
        "url": "https://backend.test/demo-shareable/11/add-team-to-view/",
        "payload": {"json": {"team_id": 5}},
        "timeout": 21,
    }


def test_shareable_can_view_parses_permission_state(monkeypatch):
    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "object_id": 11,
                "object_type": "tdag.constant",
                "access_level": "view",
                "users": [
                    {
                        "id": 7,
                        "first_name": "Jose",
                        "last_name": "Ambrosino",
                        "username": "jose@main-sequence.io",
                        "email": "jose@main-sequence.io",
                        "phone_number": None,
                    }
                ],
                "teams": [],
            }

    monkeypatch.setattr(base_mod, "make_request", lambda **kwargs: FakeResponse())

    access_state = DemoShareableModel(11).can_view()

    assert access_state.object_id == 11
    assert access_state.access_level == "view"
    assert len(access_state.users) == 1
    assert access_state.users[0].id == 7
    assert access_state.users[0].email == "jose@main-sequence.io"
    assert access_state.teams == []


def test_shareable_can_edit_parses_permission_state(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "object_id": 15,
                "object_type": "tdag.secret",
                "access_level": "edit",
                "users": [
                    {
                        "id": 9,
                        "first_name": "Ana",
                        "last_name": "Smith",
                        "username": "ana@example.com",
                        "email": "ana@example.com",
                        "phone_number": "+43123456789",
                    }
                ],
                "teams": [
                    {
                        "id": 5,
                        "name": "Research",
                        "description": "Research team",
                        "member_count": 4,
                    }
                ],
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    access_state = DemoShareableModel(15).list_users_can_edit(timeout=20)

    assert access_state.object_id == 15
    assert access_state.access_level == "edit"
    assert len(access_state.users) == 1
    assert access_state.users[0].id == 9
    assert access_state.teams[0].name == "Research"
    assert access_state.teams[0].member_count == 4
    assert captured == {
        "r_type": "GET",
        "url": "https://backend.test/demo-shareable/15/can-edit/",
        "payload": {},
        "timeout": 20,
    }


def test_team_uses_user_api_team_endpoint():
    assert models_user_mod.Team.get_object_url().endswith("/user/api/team")


def test_team_list_members_uses_team_members_endpoint(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return [
                {
                    "id": 21,
                    "first_name": "Ana",
                    "last_name": "Smith",
                    "username": "ana@example.com",
                    "email": "ana@example.com",
                }
            ]

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    team = models_user_mod.Team(id=11, name="Platform")
    members = team.list_members(timeout=12)

    assert len(members) == 1
    assert members[0].id == 21
    assert members[0].phone_number is None
    assert captured == {
        "r_type": "GET",
        "url": f"{models_user_mod.Team.get_object_url()}/11/members/",
        "payload": {},
        "timeout": 12,
    }


def test_team_manage_members_posts_bulk_membership_payload(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "team_id": 11,
                "member_count": 4,
                "selected": 2,
                "added": 2,
                "removed": 0,
                "skipped": 0,
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    team = models_user_mod.Team(id=11, name="Platform")
    result = team.add_members([_IdRef(21), 22], timeout=18)

    assert result.team_id == 11
    assert result.member_count == 4
    assert result.added == 2
    assert captured == {
        "r_type": "POST",
        "url": f"{models_user_mod.Team.get_object_url()}/11/manage-members/",
        "payload": {"json": {"action": "add", "user_ids": [21, 22]}},
        "timeout": 18,
    }


def _class_base_names_from_source(path: pathlib.Path) -> dict[str, list[str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    out: dict[str, list[str]] = {}
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            base_names: list[str] = []
            for base in node.bases:
                if isinstance(base, ast.Name):
                    base_names.append(base.id)
                elif isinstance(base, ast.Attribute):
                    base_names.append(base.attr)
            out[node.name] = base_names
    return out


def test_shareable_models_keep_shareable_object_mixin():
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    models_tdag_bases = _class_base_names_from_source(repo_root / "mainsequence" / "client" / "models_tdag.py")
    models_helpers_bases = _class_base_names_from_source(repo_root / "mainsequence" / "client" / "models_helpers.py")

    expected = {
        "Artifact": models_tdag_bases,
        "Bucket": models_tdag_bases,
        "Project": models_tdag_bases,
        "Constant": models_tdag_bases,
        "Secret": models_tdag_bases,
        "ResourceRelease": models_helpers_bases,
    }

    for class_name, source_bases in expected.items():
        assert class_name in source_bases, f"{class_name} class not found"
        assert "ShareableObjectMixin" in source_bases[class_name], (
            f"{class_name} must inherit ShareableObjectMixin"
        )


def test_team_uses_permission_managed_object_mixin():
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    models_user_bases = _class_base_names_from_source(repo_root / "mainsequence" / "client" / "models_user.py")

    assert "Team" in models_user_bases, "Team class not found"
    assert "PermissionManagedObjectMixin" in models_user_bases["Team"], (
        "Team must inherit PermissionManagedObjectMixin"
    )
    assert "ShareableObjectMixin" not in models_user_bases["Team"], (
        "Team should not inherit ShareableObjectMixin directly"
    )
