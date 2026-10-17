import datetime
import json
from contextlib import contextmanager
from contextvars import copy_context

import pytest

import mainsequence.client.base as base_mod
import mainsequence.client.models_user as models_user_mod
from mainsequence._request_identity import (
    CALLER_ASSERTION_HEADER,
    RequestIdentityError,
    _request_scope,
)
from mainsequence.client.exceptions import ApiError, BadRequestError, NotFoundError
from mainsequence.server.fastapi import reads_as_caller
from tests.client.support import DemoShareableModel, _UidRef


def test_team_uses_user_api_team_endpoint():
    assert models_user_mod.Team.get_object_url().endswith("/api/v1/teams")


def test_user_team_and_organization_filters_use_uid_references():
    team_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    org_uid = "8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"
    user_uid = "fdf409f7-d16f-4f71-986b-9057db6c7eca"

    team_filters = models_user_mod.Team._normalize_filter_kwargs(
        {
            "uid": {"uid": team_uid},
            "uid__in": [_UidRef(team_uid)],
            "organization_uid": {"uid": org_uid},
            "is_active": "true",
        }
    )
    assert team_filters == {
        "uid": team_uid,
        "uid__in": [team_uid],
        "organization_uid": org_uid,
        "is_active": True,
    }

    user_filters = models_user_mod.User._normalize_filter_kwargs(
        {
            "uid": {"uid": user_uid},
            "uid__in": [_UidRef(user_uid)],
            "email__contains": "main-sequence.io",
        }
    )
    assert user_filters == {
        "uid": user_uid,
        "uid__in": [user_uid],
        "email__contains": "main-sequence.io",
    }

    with pytest.raises(ValueError, match="Unsupported Team filter"):
        models_user_mod.Team._normalize_filter_kwargs({"id": 11})


def test_team_list_members_uses_team_members_endpoint(monkeypatch):
    captured = {}
    team_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    user_uid = "fdf409f7-d16f-4f71-986b-9057db6c7eca"

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return [
                {
                    "id": 21,
                    "uid": user_uid,
                    "first_name": "Ana",
                    "last_name": "Smith",
                    "username": "ana@example.com",
                    "email": "ana@example.com",
                    "member_kind": "person",
                },
                {"uid": WORKLOAD_USER_UID, "member_kind": "workload"},
            ]

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    team = models_user_mod.Team(id=11, uid=team_uid, name="Platform")
    members = team.list_members(timeout=12)

    assert len(members) == 2
    assert members[0].uid == user_uid
    assert members[0].member_kind == "person"
    assert not hasattr(members[0], "id")
    assert members[0].phone_number is None
    assert members[1].uid == WORKLOAD_USER_UID
    assert members[1].member_kind == "workload"
    assert members[1].username is None
    assert members[1].email is None
    assert captured == {
        "r_type": "GET",
        "url": f"{models_user_mod.Team.get_object_url()}/{team_uid}/members/",
        "payload": {},
        "timeout": 12,
    }


class _TeamResponse:
    def __init__(self, status_code=200, body=None):
        self.status_code = status_code
        self._body = body
        self.content = b"" if body is None else json.dumps(body).encode()
        self.text = self.content.decode()
        self.headers = {}

    def json(self):
        if self._body is None:
            raise ValueError("No JSON body")
        return self._body


def _team_requests(monkeypatch, *responses):
    """Answer each Team request with the next response and record what was sent."""
    sent = []
    queue = list(responses)

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        sent.append({"r_type": r_type, "url": url, "payload": payload, "timeout": time_out})
        return queue.pop(0)

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)
    return sent


MEMBER_TEAM_UID = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
MEMBER_UID = "fdf409f7-d16f-4f71-986b-9057db6c7eca"


def _member_team():
    return models_user_mod.Team(uid=MEMBER_TEAM_UID, name="Platform")


def _member_team_url(path):
    return f"{models_user_mod.Team.get_object_url()}/{MEMBER_TEAM_UID}/{path}"


def _member_row():
    return {
        "uid": MEMBER_UID,
        "username": "ana",
        "email": "ana@example.com",
        "first_name": "Ana",
        "last_name": "Smith",
        "member_kind": "person",
    }


@pytest.mark.parametrize("status_code", [201, 200], ids=["added", "already-a-member"])
def test_team_add_member_posts_one_user_uid_and_returns_the_member(monkeypatch, status_code):
    sent = _team_requests(monkeypatch, _TeamResponse(status_code, _member_row()))

    member = _member_team().add_member(_UidRef(MEMBER_UID.upper()), timeout=18)

    assert sent == [
        {
            "r_type": "POST",
            "url": _member_team_url("members/"),
            "payload": {"json": {"user_uid": MEMBER_UID}},
            "timeout": 18,
        }
    ]
    assert isinstance(member, models_user_mod.UserSummary)
    assert member.uid == MEMBER_UID
    assert member.member_kind == "person"


def test_team_add_member_outside_the_callers_scope_raises_bad_request(monkeypatch):
    answer = {
        "detail": "This user is outside the people you can add to the Team.",
        "code": "invalid_user_uids",
        "invalid_user_uids": [MEMBER_UID],
    }
    _team_requests(monkeypatch, _TeamResponse(400, answer))

    with pytest.raises(BadRequestError) as raised:
        _member_team().add_member(MEMBER_UID)

    assert raised.value.payload == answer


def test_team_remove_member_deletes_the_membership(monkeypatch):
    sent = _team_requests(monkeypatch, _TeamResponse(204))

    assert _member_team().remove_member({"uid": MEMBER_UID}, timeout=6) is None

    assert sent == [
        {
            "r_type": "DELETE",
            "url": _member_team_url(f"members/{MEMBER_UID}/"),
            "payload": {},
            "timeout": 6,
        }
    ]


def test_team_remove_member_of_someone_not_in_the_team_raises_not_found(monkeypatch):
    _team_requests(monkeypatch, _TeamResponse(404, {"detail": "No such member."}))

    with pytest.raises(NotFoundError, match="No such member."):
        _member_team().remove_member(MEMBER_UID)


def test_team_leave_posts_to_the_leave_route(monkeypatch):
    sent = _team_requests(monkeypatch, _TeamResponse(204))

    assert _member_team().leave() is None

    assert sent == [
        {"r_type": "POST", "url": _member_team_url("leave/"), "payload": {}, "timeout": None}
    ]


def test_team_list_member_candidates_sends_the_filters(monkeypatch):
    sent = _team_requests(
        monkeypatch,
        _TeamResponse(200, [_member_row()]),
        _TeamResponse(200, [{"uid": WORKLOAD_USER_UID, "member_kind": "workload"}]),
    )

    people = _member_team().list_member_candidates()
    workloads = _member_team().list_member_candidates(
        identity_type="workload", search=" nightly ", timeout=2
    )

    assert sent == [
        {
            "r_type": "GET",
            "url": _member_team_url("member-candidates/"),
            "payload": {},
            "timeout": None,
        },
        {
            "r_type": "GET",
            "url": _member_team_url("member-candidates/"),
            "payload": {"params": {"identity_type": "workload", "search": "nightly"}},
            "timeout": 2,
        },
    ]
    assert people[0].email == "ana@example.com"
    assert workloads[0].uid == WORKLOAD_USER_UID
    assert workloads[0].member_kind == "workload"


def test_team_list_member_candidates_refuses_an_unknown_identity_type(monkeypatch):
    def _no_request(**kwargs):
        raise AssertionError("no request for a malformed identity type")

    monkeypatch.setattr(base_mod, "make_request", _no_request)

    with pytest.raises(ValueError, match="identity_type must be 'human' or 'workload'"):
        _member_team().list_member_candidates(identity_type="robot")


def test_team_list_held_access_reads_the_bindings_the_team_holds(monkeypatch):
    sent = _team_requests(
        monkeypatch,
        _TeamResponse(
            200,
            {
                "bindings": [
                    {
                        "binding_uid": "7d3c6a51-0b8e-4c2f-9a14-5e6f7a8b9c0d",
                        "resource_type": "secrets",
                        "object_uid": SHAREABLE_UID,
                        "object_name": "POLYGON_API_KEY",
                        "policy": "secrets.reader",
                        "source": "migrated",
                        "granted_by": {
                            "type": "user",
                            "uid": MEMBER_UID,
                            "name": "Ana Smith",
                            "email": "ana@example.com",
                            "identity_type": "human",
                        },
                        "created_at": "2026-10-17T09:00:00+00:00",
                    },
                    {
                        "binding_uid": "1b2c3d4e-5f60-4718-8293-a4b5c6d7e8f9",
                        "resource_type": "organization-teams",
                        "object_uid": TEAM_UID,
                        "object_name": None,
                        "policy": "organization-teams.viewer",
                        "source": "explicit",
                        "granted_by": None,
                        "created_at": "2026-10-17T10:00:00+00:00",
                    },
                ]
            },
        ),
    )

    held = _member_team().list_held_access(timeout=8)

    assert sent == [
        {
            "r_type": "GET",
            "url": _member_team_url("held-access/"),
            "payload": {},
            "timeout": 8,
        }
    ]
    assert [type(row) for row in held] == [models_user_mod.TeamHeldAccess] * 2
    assert held[0].policy == "secrets.reader"
    assert held[0].object_name == "POLYGON_API_KEY"
    assert held[0].source == "migrated"
    assert held[0].granted_by.email == "ana@example.com"
    assert held[0].created_at == datetime.datetime(2026, 10, 17, 9, 0, tzinfo=datetime.UTC)
    assert held[1].granted_by is None


def test_team_list_held_access_refuses_an_answer_without_bindings(monkeypatch):
    _team_requests(monkeypatch, _TeamResponse(200, [{"binding_uid": "x"}]))

    with pytest.raises(ApiError, match="held-access"):
        _member_team().list_held_access()


def test_team_create_returns_the_detail_with_members_and_allowed_actions(monkeypatch):
    sent = _team_requests(
        monkeypatch,
        _TeamResponse(
            201,
            {
                "id": 11,
                "uid": MEMBER_TEAM_UID,
                "name": "Research",
                "description": "Model validation",
                "is_active": True,
                "created_by": _member_row(),
                "member_count": 1,
                "members": [_member_row()],
                "is_full_directory": False,
                "allowed_actions": ["organization-teams.delete", "organization-teams.update"],
            },
        ),
    )

    team = models_user_mod.Team.create(name="Research", description="Model validation", timeout=5)

    assert sent == [
        {
            "r_type": "POST",
            "url": f"{models_user_mod.Team.get_object_url()}/",
            "payload": {
                "json": {"name": "Research", "description": "Model validation", "is_active": True}
            },
            "timeout": 5,
        }
    ]
    assert team.uid == MEMBER_TEAM_UID
    assert team.members[0].uid == MEMBER_UID
    assert team.created_by.uid == MEMBER_UID
    assert team.allowed_actions == ["organization-teams.delete", "organization-teams.update"]


def test_a_listed_team_carries_no_allowed_actions():
    team = models_user_mod.Team.model_validate(
        {"uid": MEMBER_TEAM_UID, "name": "Research", "member_count": 3}
    )

    assert team.allowed_actions is None


def test_user_org_team_models_deserialize_current_backend_uid_payloads():
    org_uid = "8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"
    production_environment_uid = "00000000-0000-4000-8000-000000000002"
    user_uid = "fdf409f7-d16f-4f71-986b-9057db6c7eca"
    team_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    organization_payload = {
        "uid": org_uid,
        "name": "Main Sequence",
        "url": "https://backend.test",
        "organization_domain": "main-sequence.io",
        "identity_platform_tenant_id": None,
        "has_pending_invoices": False,
        "production_environment_uid": production_environment_uid,
    }

    organization = models_user_mod.Organization.model_validate(organization_payload)
    assert organization.uid == org_uid
    assert organization.production_environment_uid == production_environment_uid
    assert "id" not in organization.model_dump()

    current_user = models_user_mod.User.model_validate(
        {
            "id": 4,
            "uid": user_uid,
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
    assert current_user.uid == user_uid
    assert current_user.organization.production_environment_uid == production_environment_uid
    assert current_user.user_permissions == []

    user = models_user_mod.User.model_validate(
        {
            "id": 4,
            "uid": user_uid,
            "username": "jose",
            "email": "jose@main-sequence.io",
            "first_name": "Jose",
            "last_name": "Ambrosino",
            "profile_picture": None,
            "phone_number": None,
            "organization": organization_payload,
            "is_verified": True,
            "blocked_access": False,
            "api_request_limit": 10000,
            "mfa_enabled": False,
            "requires_password_change": False,
            "identity_platform_uid": None,
            "active_plan_type": None,
            "is_active": True,
            "date_joined": "2026-01-01T00:00:00Z",
            "last_login": None,
            "groups": [],
            "user_permissions": [],
            "organization_teams": [],
        }
    )
    assert user.uid == user_uid
    assert user.id == 4
    assert "id" not in user.model_dump()

    team = models_user_mod.Team.model_validate(
        {
            "id": 11,
            "uid": team_uid,
            "name": "Platform",
            "description": "Platform team",
            "is_active": True,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-02T00:00:00Z",
            "organization": organization_payload,
            "created_by": {
                "id": 4,
                "uid": user_uid,
                "username": "jose",
                "email": "jose@main-sequence.io",
                "first_name": "Jose",
                "last_name": "Ambrosino",
            },
            "member_count": 1,
            "members": [
                {
                    "id": 4,
                    "uid": user_uid,
                    "username": "jose",
                    "email": "jose@main-sequence.io",
                    "first_name": "Jose",
                    "last_name": "Ambrosino",
                }
            ],
        }
    )
    assert team.uid == team_uid
    assert team.created_by.uid == user_uid
    assert team.members[0].uid == user_uid


def test_user_get_by_uid_uses_user_uid_detail_route(monkeypatch):
    captured = {}
    user_uid = "fdf409f7-d16f-4f71-986b-9057db6c7eca"

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "id": 4,
                "uid": user_uid,
                "username": "jose",
                "email": "jose@main-sequence.io",
                "date_joined": "2026-01-01T00:00:00Z",
                "is_active": True,
                "api_request_limit": 10000,
                "mfa_enabled": False,
                "groups": [],
                "user_permissions": [],
                "organization_teams": [],
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    user = models_user_mod.User.get_by_uid(user_uid, timeout=9)

    assert user.uid == user_uid
    assert captured == {
        "r_type": "GET",
        "url": f"{models_user_mod.User.get_object_url()}/{user_uid}/",
        "payload": {"params": {}},
        "timeout": 9,
    }


# --- Workload identities -------------------------------------------------------
#
# A deployed Job, FastAPI release or Agent runs as its own User. Looked up by UID
# or listed with `identity_type=workload`, the platform answers such a User with
# the form below: no username, email, join date or name.

WORKLOAD_USER_UID = "66666666-6666-4666-8666-666666666666"
WORKLOAD_JOB_UID = "88888888-8888-4888-8888-888888888888"
PERSON_UID = "fdf409f7-d16f-4f71-986b-9057db6c7eca"
PERSON_ONLY_FIELDS = (
    "username",
    "email",
    "date_joined",
    "api_request_limit",
    "mfa_enabled",
    "first_name",
    "last_name",
)


def _workload_user_payload(**workload_uids):
    return {
        "uid": WORKLOAD_USER_UID,
        "identity_type": "workload",
        "is_active": True,
        "job_uid": None,
        "resource_release_uid": None,
        "agent_uid": None,
        **workload_uids,
    }


def _person_payload():
    return {
        "id": 4,
        "uid": PERSON_UID,
        "username": "jose",
        "email": "jose@main-sequence.io",
        "first_name": "Jose",
        "last_name": "Ambrosino",
        "profile_picture": None,
        "phone_number": None,
        "is_verified": True,
        "blocked_access": False,
        "api_request_limit": 10000,
        "mfa_enabled": False,
        "requires_password_change": False,
        "identity_platform_uid": None,
        "active_plan_type": None,
        "is_active": True,
        "date_joined": "2026-01-01T00:00:00Z",
        "last_login": None,
        "groups": [],
        "user_permissions": [],
        "organization_teams": [],
    }


class _JsonResponse:
    status_code = 200
    content = b'{"ok": true}'

    def __init__(self, body):
        self._body = body

    def json(self):
        return self._body


def _serve(monkeypatch, *bodies):
    """Answer each request with the next body and record what was sent."""
    sent = []
    answers = iter(bodies)

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        sent.append({"r_type": r_type, "url": url, "payload": payload, "timeout": time_out})
        return _JsonResponse(next(answers))

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)
    return sent


@pytest.mark.parametrize(
    "workload_uids",
    [
        {"job_uid": WORKLOAD_JOB_UID},
        {"resource_release_uid": "99999999-9999-4999-8999-999999999999"},
        {"agent_uid": "77777777-7777-4777-8777-777777777777"},
    ],
    ids=["job", "resource_release", "agent"],
)
def test_user_get_by_uid_reads_a_workload_identity(monkeypatch, workload_uids):
    sent = _serve(monkeypatch, _workload_user_payload(**workload_uids))

    user = models_user_mod.User.get_by_uid(WORKLOAD_USER_UID, timeout=9)

    assert sent == [
        {
            "r_type": "GET",
            "url": f"{models_user_mod.User.get_object_url()}/{WORKLOAD_USER_UID}/",
            "payload": {"params": {}},
            "timeout": 9,
        }
    ]
    assert user.uid == WORKLOAD_USER_UID
    assert user.identity_type == "workload"
    assert user.is_active is True
    for field_name in ("job_uid", "resource_release_uid", "agent_uid"):
        assert getattr(user, field_name) == workload_uids.get(field_name)
    for field_name in PERSON_ONLY_FIELDS:
        assert getattr(user, field_name) is None


def test_user_filter_by_identity_type_sends_the_query_parameter(monkeypatch):
    sent = _serve(
        monkeypatch,
        {"results": [_workload_user_payload(job_uid=WORKLOAD_JOB_UID)], "next": None},
    )

    workloads = models_user_mod.User.filter(identity_type="workload", timeout=7)

    assert sent == [
        {
            "r_type": "GET",
            "url": f"{models_user_mod.User.get_object_url()}/",
            "payload": {"params": {"identity_type": "workload"}},
            "timeout": 7,
        }
    ]
    assert [(user.uid, user.identity_type, user.job_uid) for user in workloads] == [
        (WORKLOAD_USER_UID, "workload", WORKLOAD_JOB_UID)
    ]
    assert models_user_mod.User._normalize_filter_kwargs({"identity_type": " workload "}) == {
        "identity_type": "workload"
    }
    with pytest.raises(ValueError, match="Unsupported User filter"):
        models_user_mod.User._normalize_filter_kwargs({"identity_type__in": ["workload"]})


def test_user_people_read_exactly_as_before(monkeypatch):
    person_payload = _person_payload()
    sent = _serve(monkeypatch, [_person_payload()])

    people = models_user_mod.User.filter()

    # No identity_type is added: without the filter the listing is people only.
    assert sent[0]["payload"] == {}
    (person,) = people
    dumped = person.model_dump(mode="json")
    excluded_from_dump = {"id", "user_permissions"}
    for field_name, value in person_payload.items():
        if field_name in excluded_from_dump:
            continue
        assert dumped[field_name] == value, field_name
    assert person.id == 4
    assert person.user_permissions == []
    assert person.date_joined == datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    assert person.identity_type is None
    assert (person.job_uid, person.resource_release_uid, person.agent_uid) == (None, None, None)
    assert person.workload_name is None


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        ({"workload_name": "nightly-prices"}, "nightly-prices"),
        ({"workload_name": None}, None),
        ({}, None),
    ],
    ids=["named", "null", "absent"],
)
def test_user_get_by_uid_preserves_workload_name(monkeypatch, row, expected):
    sent = _serve(monkeypatch, {**_workload_user_payload(job_uid=WORKLOAD_JOB_UID), **row})

    user = models_user_mod.User.get_by_uid(WORKLOAD_USER_UID)

    assert sent[0]["url"] == (f"{models_user_mod.User.get_object_url()}/{WORKLOAD_USER_UID}/")
    assert user.workload_name == expected
    assert user.model_dump(mode="json")["workload_name"] == expected
    assert user.job_uid == WORKLOAD_JOB_UID


@pytest.mark.parametrize(
    "personal_fields",
    [{}, dict.fromkeys(("username", "email", "first_name", "last_name"))],
    ids=["absent", "null"],
)
def test_team_parses_workload_members_and_creator(personal_fields):
    workload = {
        "uid": WORKLOAD_USER_UID,
        "member_kind": "workload",
        **personal_fields,
    }
    person = {
        key: value
        for key, value in _person_payload().items()
        if key in ("uid", "username", "email", "first_name", "last_name")
    }
    person["member_kind"] = "person"
    team = models_user_mod.Team.model_validate(
        {
            "uid": "3f1cc452-43ec-49cb-b2ba-87dbac164d29",
            "name": "Research",
            "member_count": 2,
            "members": [person, workload],
            "created_by": workload,
        }
    )

    assert [member.uid for member in team.members] == [PERSON_UID, WORKLOAD_USER_UID]
    assert team.members[0].member_kind == "person"
    for field_name in ("username", "email", "first_name", "last_name"):
        assert getattr(team.members[0], field_name) == person[field_name]
        assert getattr(team.members[1], field_name) is None
        assert getattr(team.created_by, field_name) is None
    assert team.members[1].member_kind == "workload"
    assert team.created_by.uid == WORKLOAD_USER_UID
    assert team.created_by.member_kind == "workload"


def test_identity_type_keeps_a_value_this_release_does_not_declare():
    for declared in ("human", "service_account", "deleted_user", "workload"):
        assert (
            models_user_mod.User.model_validate(
                {**_workload_user_payload(), "identity_type": declared}
            ).identity_type
            == declared
        )

    user = models_user_mod.User.model_validate(
        {**_workload_user_payload(), "identity_type": "future_identity"}
    )

    assert user.identity_type == "future_identity"


def test_workload_user_repr_str_hash_and_dump_need_no_username():
    user = models_user_mod.User.model_validate(_workload_user_payload(job_uid=WORKLOAD_JOB_UID))

    assert repr(user) == f"User: {WORKLOAD_USER_UID}"
    assert WORKLOAD_USER_UID in str(user)
    assert hash(user) == hash(WORKLOAD_USER_UID)
    dumped = user.model_dump(mode="json")
    assert dumped["identity_type"] == "workload"
    assert dumped["job_uid"] == WORKLOAD_JOB_UID
    assert dumped["username"] is None
    assert dumped["email"] is None


def test_application_resolves_a_workload_caller_and_grants_it_access(monkeypatch):
    """Check the principal of a received request, then grant that workload access."""
    sent = _serve(
        monkeypatch,
        _workload_user_payload(job_uid=WORKLOAD_JOB_UID),
        {"detail": "ok"},
    )
    shareable_uid = "24001fc7-098c-40fa-b398-1d2352b7c224"

    with _request_scope() as context:
        context.user = models_user_mod.RequestUserIdentity(uid=WORKLOAD_USER_UID)
        caller = models_user_mod.User.get_logged_user()
        user = models_user_mod.User.get_by_uid(caller.uid)
        assert caller.username is None
        assert user.identity_type == "workload"
        assert user.job_uid == WORKLOAD_JOB_UID
        response = DemoShareableModel(shareable_uid).add_to_view(user)

    assert response == {"detail": "ok"}
    assert sent[1] == {
        "r_type": "POST",
        "url": f"https://backend.test/demo-shareable/{shareable_uid}/add-to-view/",
        "payload": {"json": {"user_uid": WORKLOAD_USER_UID}},
        "timeout": None,
    }


# --- Reads as the request's caller (ADR-0036, amended for #198) ------------------
#
# With the caller assertion a hosted request arrived with, /users/ and /teams/
# answer with what the caller may see. Only those reads present it, and only
# inside reads_as_caller().

CALLER_ASSERTION = "header.caller-proof-of-this-request.signature"
TEAM_UID = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
SHAREABLE_UID = "24001fc7-098c-40fa-b398-1d2352b7c224"


def _team_payload():
    return {"uid": TEAM_UID, "name": "Research", "members": [], "member_count": 0}


def _record(monkeypatch, *bodies):
    """Answer each request with the next body and record the headers it added."""
    sent = []
    answers = iter(bodies)

    def _fake_make_request(*, s, loaders, r_type, url, payload=None, time_out=None):
        sent.append({"r_type": r_type, "url": url, "headers": (payload or {}).get("headers")})
        return _JsonResponse(next(answers))

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)
    monkeypatch.setattr(models_user_mod, "make_request", _fake_make_request)
    return sent


@contextmanager
def _caller_request(assertion=CALLER_ASSERTION):
    """A request admitted in assertion mode, as the request identity binds it."""
    with _request_scope() as context:
        context.user = models_user_mod.RequestUserIdentity(uid=PERSON_UID)
        context.caller_assertion = assertion
        yield context


def test_user_filter_search_sends_the_query_parameter(monkeypatch):
    sent = _serve(
        monkeypatch,
        {"results": [_workload_user_payload(job_uid=WORKLOAD_JOB_UID)], "next": None},
    )

    workloads = models_user_mod.User.filter(identity_type="workload", search="nightly-prices")

    assert sent[0]["payload"] == {
        "params": {"identity_type": "workload", "search": "nightly-prices"}
    }
    assert [user.uid for user in workloads] == [WORKLOAD_USER_UID]


@pytest.mark.parametrize(
    ("row", "expected"),
    [({"managed_by_caller": True}, True), ({"managed_by_caller": False}, False), ({}, None)],
    ids=["managed", "not_managed", "absent"],
)
def test_workload_row_reads_managed_by_caller(row, expected):
    user = models_user_mod.User.model_validate({**_workload_user_payload(), **row})

    assert user.managed_by_caller is expected


def test_reads_as_caller_sends_the_assertion_only_on_directory_reads(monkeypatch):
    page_two = f"{models_user_mod.User.get_object_url()}/?search=prices&offset=1"
    workload_member = {"uid": WORKLOAD_USER_UID, "member_kind": "workload"}
    team_row = {**_team_payload(), "created_by": workload_member, "member_count": 1}
    team_row.pop("members")
    sent = _record(
        monkeypatch,
        {"results": [_workload_user_payload(managed_by_caller=True)], "next": page_two},
        {"results": [_workload_user_payload()], "next": None},
        _workload_user_payload(managed_by_caller=False),
        {"results": [team_row], "next": None},
        {**team_row, "members": [workload_member]},
        _person_payload(),
        {"detail": "ok"},
        [_person_payload()],
    )

    with _caller_request():
        with reads_as_caller():
            workloads = models_user_mod.User.filter(identity_type="workload", search="prices")
            models_user_mod.User.get_by_uid(WORKLOAD_USER_UID)
            teams = models_user_mod.Team.filter()
            team = models_user_mod.Team.get_by_uid(TEAM_UID)
            # Neither the session's own user nor a sharing call presents it.
            models_user_mod.User.get_authenticated_user_details()
            DemoShareableModel(SHAREABLE_UID).add_to_view(workloads[0])
        # Outside the helper the application reads as itself.
        models_user_mod.User.filter()

    caller = {CALLER_ASSERTION_HEADER: CALLER_ASSERTION}
    assert [request["headers"] for request in sent] == [caller] * 5 + [None] * 3
    assert sent[1]["url"] == page_two
    assert sent[5]["url"] == f"{models_user_mod.User.get_object_url()}/me/"
    assert workloads[0].managed_by_caller is True
    assert teams[0].created_by.uid == WORKLOAD_USER_UID
    assert teams[0].created_by.member_kind == "workload"
    assert team.created_by.uid == WORKLOAD_USER_UID
    assert team.members[0].uid == WORKLOAD_USER_UID
    assert team.members[0].member_kind == "workload"


def test_reads_as_caller_never_falls_back_to_the_application(monkeypatch):
    sent = _record(monkeypatch)

    with pytest.raises(RequestIdentityError, match="authenticated request"):
        with reads_as_caller():
            pass
    with _request_scope():  # a public route: no caller
        with pytest.raises(RequestIdentityError, match="authenticated request"):
            with reads_as_caller():
                pass
    with _caller_request(assertion=None):  # local mode or a WebSocket
        with pytest.raises(RequestIdentityError, match="no caller assertion"):
            with reads_as_caller():
                pass

    with _caller_request():
        with reads_as_caller():
            copied = copy_context()
    with pytest.raises(RequestIdentityError, match="has ended"):
        copied.run(models_user_mod.User.filter)

    assert sent == []


def test_caller_assertion_is_sent_only_to_the_platform_endpoint(monkeypatch):
    sent = _record(
        monkeypatch,
        {
            "results": [_workload_user_payload()],
            "next": "https://elsewhere.test/api/v1/users/?offset=1",
        },
    )

    with _caller_request(), reads_as_caller():
        with pytest.raises(RequestIdentityError, match="configured platform endpoint"):
            models_user_mod.User.filter(identity_type="workload")

    assert [request["url"] for request in sent] == [f"{models_user_mod.User.get_object_url()}/"]


class _RefusedResponse(_JsonResponse):
    status_code = 403


def test_caller_assertion_stays_out_of_repr_and_error_messages(monkeypatch):
    def _refuse(*, s, loaders, r_type, url, payload=None, time_out=None):
        return _RefusedResponse({"detail": "Refused."})

    monkeypatch.setattr(base_mod, "make_request", _refuse)

    with _caller_request() as context:
        assert CALLER_ASSERTION not in repr(context)
        with reads_as_caller(), pytest.raises(ApiError) as refused:
            models_user_mod.User.filter()

    assert CALLER_ASSERTION not in str(refused.value)
    assert CALLER_ASSERTION not in repr(refused.value.payload)


def test_reads_as_caller_refuses_a_requester_bound_request(monkeypatch):
    """The helper refuses requester-bearing assertions before any directory read."""
    sent = _record(monkeypatch)

    with _request_scope() as context:
        context.user = models_user_mod.RequestUserIdentity(uid=WORKLOAD_USER_UID)
        context.caller_assertion = CALLER_ASSERTION
        context.requester = models_user_mod.RequestUserIdentity(uid=PERSON_UID)
        with pytest.raises(RequestIdentityError, match="requester-bound"):
            with reads_as_caller():
                models_user_mod.User.filter()

    assert sent == []
