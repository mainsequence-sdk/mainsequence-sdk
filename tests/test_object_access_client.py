"""The per-object access routes: AccessManagedObjectMixin and its models.

The HTTP layer is mocked at `make_request`; every test checks the route, the
method and the payload the SDK sends, and how it reads the answer.
"""

from __future__ import annotations

import datetime
import json

import pytest

import mainsequence.client as client_pkg
import mainsequence.client.base as base_mod
import mainsequence.code_repository_context as code_repository_context
from mainsequence.client.base import BaseObjectOrm
from mainsequence.client.exceptions import (
    ApiError,
    BadRequestError,
    NotFoundError,
    PermissionDeniedError,
)
from mainsequence.client.models_foundry import Constant
from mainsequence.client.models_user import OrganizationEnvironment, Team
from mainsequence.client.object_access import (
    AccessBinding,
    AccessListing,
    AccessManagedObjectMixin,
    AccessPrincipal,
    CallerAccess,
)

TEAM_UID = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
USER_UID = "8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"
OTHER_TEAM_UID = "9a1e2d3c-4b5a-4c6d-8e7f-0a1b2c3d4e5f"
WORKLOAD_UID = "66666666-6666-4666-8666-666666666666"
BINDING_UID = "7d3c6a51-0b8e-4c2f-9a14-5e6f7a8b9c0d"
OTHER_BINDING_UID = "1b2c3d4e-5f60-4718-8293-a4b5c6d7e8f9"


class FakeResponse:
    def __init__(self, status_code: int = 200, body=None):
        self.status_code = status_code
        self._body = body
        self.content = b"" if body is None else json.dumps(body).encode()
        self.text = self.content.decode()
        self.headers = {}

    def json(self):
        if self._body is None:
            raise ValueError("No JSON body")
        return self._body


class _UidRef:
    def __init__(self, uid: str):
        self.uid = uid


def _install(monkeypatch, *responses: FakeResponse) -> list[dict]:
    """Answer each request with the next response and record what was sent."""

    queue = list(responses)
    sent: list[dict] = []

    def fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        sent.append({"r_type": r_type, "url": url, "payload": payload, "timeout": time_out})
        if not queue:
            raise AssertionError(f"Unexpected request: {r_type} {url}")
        return queue.pop(0)

    monkeypatch.setattr(base_mod, "make_request", fake_make_request)
    return sent


def _refuse_requests(monkeypatch) -> None:
    def no_request(**kwargs):
        raise AssertionError("A malformed argument must not send a request")

    monkeypatch.setattr(base_mod, "make_request", no_request)


def _team() -> Team:
    return Team(uid=TEAM_UID, name="Research")


def _team_url(path: str = "") -> str:
    return f"{Team.get_object_url()}/{TEAM_UID}/{path}"


def _person(**overrides):
    principal = {
        "type": "user",
        "uid": USER_UID,
        "name": "Ana Smith",
        "email": "ana@example.com",
        "identity_type": "human",
    }
    principal.update(overrides)
    return principal


def _binding(**overrides):
    binding = {
        "uid": BINDING_UID,
        "policy": {"id": "organization-teams.viewer", "label": "Viewer"},
        "principal": _person(),
        "source": "explicit",
        "granted_by": {"uid": OTHER_TEAM_UID, "email": "owner@example.com"},
        "workflow_declaration": None,
        "created_at": "2026-10-17T09:00:00Z",
    }
    binding.update(overrides)
    return binding


@pytest.mark.parametrize("model", [Team, OrganizationEnvironment, Constant])
def test_policy_decided_models_serve_the_access_methods(model):
    assert issubclass(model, AccessManagedObjectMixin)
    for name in (
        "get_access",
        "get_my_access",
        "list_access_candidates",
        "attach_policy",
        "detach_policy",
        "apply_access_changes",
        "explain_access",
    ):
        assert callable(getattr(model, name))


def test_access_names_are_exported_from_the_client_package():
    for name in (
        "AccessBinding",
        "AccessComputedRow",
        "AccessExplanation",
        "AccessExplanationPath",
        "AccessGrantor",
        "AccessListing",
        "AccessManagedObjectMixin",
        "AccessObjectReference",
        "AccessPolicyReference",
        "AccessPrincipal",
        "AccessWorkflowDeclaration",
        "CallerAccess",
        "TeamHeldAccess",
    ):
        assert hasattr(client_pkg, name), name


def test_get_access_reads_the_bindings_and_the_computed_rows(monkeypatch):
    team_principal = {
        "type": "team",
        "uid": OTHER_TEAM_UID,
        "name": "Platform",
        "email": None,
        "identity_type": None,
    }
    workload_principal = _person(uid=WORKLOAD_UID, name="", email=None, identity_type="workload")
    sent = _install(
        monkeypatch,
        FakeResponse(
            200,
            {
                "object": {"resource_type": "organization-teams", "uid": TEAM_UID},
                "bindings": [
                    _binding(),
                    _binding(
                        uid=OTHER_BINDING_UID,
                        policy={"id": "organization-teams.owner", "label": "Owner"},
                        principal=team_principal,
                        source="workflow",
                        granted_by=None,
                        workflow_declaration={
                            "path": ".mainsequence/workflows/access.yaml",
                            "key": "research",
                        },
                    ),
                    _binding(uid=BINDING_UID, principal=workload_principal, source="creator"),
                ],
                "computed": [
                    {
                        "source": "role",
                        "detail": "Organization administrators hold every action.",
                        "policy": None,
                    },
                    {
                        "source": "membership",
                        "detail": "Members hold the member policy.",
                        "policy": "organization-teams.member",
                    },
                ],
            },
        ),
    )

    listing = _team().get_access(timeout=7)

    assert sent == [{"r_type": "GET", "url": _team_url("access/"), "payload": {}, "timeout": 7}]
    assert isinstance(listing, AccessListing)
    assert listing.object.resource_type == "organization-teams"
    assert listing.object.uid == TEAM_UID
    viewer, owner, workload = listing.bindings
    assert viewer.uid == BINDING_UID
    assert viewer.policy.id == "organization-teams.viewer"
    assert viewer.policy.label == "Viewer"
    assert viewer.principal.type == "user"
    assert viewer.principal.email == "ana@example.com"
    assert viewer.source == "explicit"
    assert viewer.granted_by.email == "owner@example.com"
    assert viewer.workflow_declaration is None
    assert viewer.created_at == datetime.datetime(2026, 10, 17, 9, 0, tzinfo=datetime.UTC)
    assert owner.principal.type == "team"
    assert owner.principal.name == "Platform"
    assert owner.principal.identity_type is None
    assert owner.granted_by is None
    assert owner.workflow_declaration.path == ".mainsequence/workflows/access.yaml"
    assert owner.workflow_declaration.key == "research"
    assert workload.principal.identity_type == "workload"
    assert workload.principal.email is None
    assert workload.principal.name == ""
    assert [(row.source, row.policy) for row in listing.computed] == [
        ("role", None),
        ("membership", "organization-teams.member"),
    ]


def test_get_my_access_reads_the_callers_actions_and_policies(monkeypatch):
    sent = _install(
        monkeypatch,
        FakeResponse(
            200,
            {
                "allowed_actions": ["organization-teams.leave", "organization-teams.read"],
                "policies": ["organization-teams.member"],
            },
        ),
    )

    mine = _team().get_my_access()

    assert sent == [
        {"r_type": "GET", "url": _team_url("access/me/"), "payload": {}, "timeout": None}
    ]
    assert isinstance(mine, CallerAccess)
    assert mine.allowed_actions == ["organization-teams.leave", "organization-teams.read"]
    assert mine.policies == ["organization-teams.member"]


def test_list_access_candidates_sends_the_policy_and_the_filters(monkeypatch):
    sent = _install(
        monkeypatch,
        FakeResponse(
            200, [_person(uid=WORKLOAD_UID, name="", email=None, identity_type="workload")]
        ),
        FakeResponse(200, [{"type": "team", "uid": OTHER_TEAM_UID, "name": "Platform"}]),
    )

    workloads = _team().list_access_candidates(
        "viewer", identity_type="workload", search="  nightly ", timeout=3
    )
    teams = _team().list_access_candidates("organization-teams.viewer", principal_type="team")

    assert sent == [
        {
            "r_type": "GET",
            "url": _team_url("access/candidates/"),
            "payload": {
                "params": {
                    "policy": "viewer",
                    "principal_type": "user",
                    "identity_type": "workload",
                    "search": "nightly",
                }
            },
            "timeout": 3,
        },
        {
            "r_type": "GET",
            "url": _team_url("access/candidates/"),
            "payload": {
                "params": {"policy": "organization-teams.viewer", "principal_type": "team"}
            },
            "timeout": None,
        },
    ]
    assert [type(item) for item in workloads + teams] == [AccessPrincipal, AccessPrincipal]
    assert workloads[0].identity_type == "workload"
    assert teams[0].type == "team"
    assert teams[0].email is None


def test_list_access_candidates_returns_an_empty_list(monkeypatch):
    _install(monkeypatch, FakeResponse(200, []))

    assert _team().list_access_candidates("owner", principal_type="team") == []


def test_attach_policy_posts_the_binding_request_and_returns_the_binding(monkeypatch):
    sent = _install(monkeypatch, FakeResponse(201, _binding()))

    binding = _team().attach_policy(
        " viewer ", principal_type="user", principal_uid=USER_UID.upper(), timeout=9
    )

    assert sent == [
        {
            "r_type": "POST",
            "url": _team_url("access/bindings/"),
            "payload": {
                "json": {"policy": "viewer", "principal_type": "user", "principal_uid": USER_UID}
            },
            "timeout": 9,
        }
    ]
    assert isinstance(binding, AccessBinding)
    assert binding.uid == BINDING_UID


@pytest.mark.parametrize(
    "principal",
    [_UidRef(OTHER_TEAM_UID), {"uid": OTHER_TEAM_UID}, Team(uid=OTHER_TEAM_UID, name="Platform")],
    ids=["object", "dict", "team"],
)
def test_attach_policy_takes_a_principal_that_carries_its_uid(monkeypatch, principal):
    sent = _install(monkeypatch, FakeResponse(201, _binding()))

    _team().attach_policy("viewer", principal_type="team", principal_uid=principal)

    assert sent[0]["payload"]["json"]["principal_uid"] == OTHER_TEAM_UID


def test_detach_policy_deletes_the_binding(monkeypatch):
    sent = _install(monkeypatch, FakeResponse(204), FakeResponse(204))

    assert _team().detach_policy(BINDING_UID, timeout=4) is None
    assert _team().detach_policy(AccessBinding.model_validate(_binding())) is None

    assert sent == [
        {
            "r_type": "DELETE",
            "url": _team_url(f"access/bindings/{BINDING_UID}/"),
            "payload": {},
            "timeout": 4,
        },
        {
            "r_type": "DELETE",
            "url": _team_url(f"access/bindings/{BINDING_UID}/"),
            "payload": {},
            "timeout": None,
        },
    ]


def test_apply_access_changes_posts_every_change_and_returns_the_bindings(monkeypatch):
    sent = _install(
        monkeypatch,
        FakeResponse(200, {"bindings": [_binding(uid=OTHER_BINDING_UID)]}),
    )

    bindings = _team().apply_access_changes(
        attach=[
            {"policy": "editor", "principal_type": "team", "principal_uid": OTHER_TEAM_UID},
        ],
        detach=[BINDING_UID, _UidRef(OTHER_BINDING_UID)],
        timeout=11,
    )

    assert sent == [
        {
            "r_type": "POST",
            "url": _team_url("access/changes/"),
            "payload": {
                "json": {
                    "attach": [
                        {
                            "policy": "editor",
                            "principal_type": "team",
                            "principal_uid": OTHER_TEAM_UID,
                        }
                    ],
                    "detach": [BINDING_UID, OTHER_BINDING_UID],
                }
            },
            "timeout": 11,
        }
    ]
    assert [binding.uid for binding in bindings] == [OTHER_BINDING_UID]


def test_apply_access_changes_needs_at_least_one_change(monkeypatch):
    _refuse_requests(monkeypatch)

    with pytest.raises(ValueError, match="at least one"):
        _team().apply_access_changes()


def test_explain_access_sends_the_action_and_reads_every_path(monkeypatch):
    sent = _install(
        monkeypatch,
        FakeResponse(
            200,
            {
                "action": "organization-teams.update",
                "allowed": True,
                "in_scope": True,
                "paths": [
                    {"source": "role", "detail": "Organization administrator"},
                    {
                        "source": "team",
                        "binding_uid": BINDING_UID,
                        "policy": "organization-teams.editor",
                        "team_uid": OTHER_TEAM_UID,
                    },
                ],
                "blocked_by_rule": False,
                "granting_policies": ["organization-teams.editor", "organization-teams.owner"],
            },
        ),
        FakeResponse(
            200,
            {
                "action": "organization-teams.delete",
                "allowed": False,
                "in_scope": True,
                "paths": [],
                "blocked_by_rule": False,
                "granting_policies": ["organization-teams.owner"],
            },
        ),
    )

    mine = _team().explain_access("organization-teams.update")
    theirs = _team().explain_access(
        "organization-teams.delete", principal_uid=_UidRef(USER_UID), timeout=5
    )

    assert sent == [
        {
            "r_type": "GET",
            "url": _team_url("access/explain/"),
            "payload": {"params": {"action": "organization-teams.update"}},
            "timeout": None,
        },
        {
            "r_type": "GET",
            "url": _team_url("access/explain/"),
            "payload": {
                "params": {"action": "organization-teams.delete", "principal_uid": USER_UID}
            },
            "timeout": 5,
        },
    ]
    assert mine.allowed is True
    assert [path.source for path in mine.paths] == ["role", "team"]
    assert mine.paths[1].team_uid == OTHER_TEAM_UID
    assert mine.paths[1].binding_uid == BINDING_UID
    assert theirs.allowed is False
    assert theirs.paths == []
    assert theirs.granting_policies == ["organization-teams.owner"]


def test_a_refused_action_raises_permission_denied_with_the_platforms_answer(monkeypatch):
    refusal = {
        "detail": "This needs the organization-teams.manage_access action.",
        "code": "missing_action",
        "required_action": "organization-teams.manage_access",
        "granting_policies": ["organization-teams.owner"],
        "resource_type": "organization-teams",
        "resource_uid": TEAM_UID,
    }
    _install(monkeypatch, FakeResponse(403, refusal))

    with pytest.raises(PermissionDeniedError) as raised:
        _team().attach_policy("viewer", principal_type="user", principal_uid=USER_UID)

    assert raised.value.status_code == 403
    assert raised.value.payload == refusal
    assert "This needs the organization-teams.manage_access action." in str(raised.value)


def test_a_change_the_platform_refuses_raises_bad_request_with_its_code(monkeypatch):
    answer = {
        "detail": "This person or Team is outside the people you can give access to.",
        "code": "invalid_principal_uids",
        "invalid_principal_uids": [USER_UID],
    }
    _install(monkeypatch, FakeResponse(400, answer))

    with pytest.raises(BadRequestError) as raised:
        _team().attach_policy("viewer", principal_type="user", principal_uid=USER_UID)

    assert raised.value.payload["code"] == "invalid_principal_uids"


def test_an_unreadable_object_raises_not_found(monkeypatch):
    _install(monkeypatch, FakeResponse(404, {"detail": "Not found."}))

    with pytest.raises(NotFoundError):
        _team().get_access()


@pytest.mark.parametrize(
    ("call", "message"),
    [
        (
            lambda team: team.attach_policy("", principal_type="user", principal_uid=USER_UID),
            "policy must be a non-empty string",
        ),
        (
            lambda team: team.attach_policy(
                "viewer", principal_type="group", principal_uid=USER_UID
            ),
            "principal_type must be 'user' or 'team'",
        ),
        (
            lambda team: team.attach_policy("viewer", principal_type="user", principal_uid="ana"),
            "principal_uid must be a UUID",
        ),
        (lambda team: team.detach_policy("../members"), "binding_uid must be a UUID"),
        (lambda team: team.detach_policy(None), "binding_uid must be a UUID"),
        (lambda team: team.explain_access("  "), "action must be a non-empty string"),
        (
            lambda team: team.explain_access("organization-teams.read", principal_uid="me"),
            "principal_uid must be a UUID",
        ),
        (lambda team: team.list_access_candidates(None), "policy must be a non-empty string"),
        (
            lambda team: team.list_access_candidates("viewer", identity_type="robot"),
            "identity_type must be 'human' or 'workload'",
        ),
        (
            lambda team: team.apply_access_changes(attach=[("viewer", "user", USER_UID)]),
            "Each attach entry must be a mapping",
        ),
        (
            lambda team: team.apply_access_changes(
                attach=[{"policy": "viewer", "principal_type": "user"}]
            ),
            "principal_uid must be a UUID",
        ),
        (lambda team: team.apply_access_changes(detach=["x"]), "detach must be a UUID"),
    ],
)
def test_a_malformed_argument_raises_before_anything_is_sent(monkeypatch, call, message):
    _refuse_requests(monkeypatch)

    with pytest.raises(ValueError, match=message):
        call(_team())


def test_an_answer_of_the_wrong_shape_raises_api_error(monkeypatch):
    _install(monkeypatch, FakeResponse(200, []), FakeResponse(200, {"bindings": {}}))

    with pytest.raises(ApiError, match="access"):
        _team().get_access()
    with pytest.raises(ApiError, match="access/changes"):
        _team().apply_access_changes(detach=[BINDING_UID])


def test_values_and_fields_this_release_does_not_declare_are_read_tolerantly(monkeypatch):
    _install(
        monkeypatch,
        FakeResponse(
            200,
            {
                "object": {"resource_type": "organization-teams", "uid": TEAM_UID},
                "bindings": [
                    _binding(
                        source="imported",
                        principal=_person(type="service", future_field=1),
                        future_field="dropped",
                    )
                ],
                "computed": [{"source": "delegation", "detail": "Someone.", "policy": None}],
                "future_section": [],
            },
        ),
    )

    listing = _team().get_access()

    binding = listing.bindings[0]
    assert binding.source == "imported"
    assert binding.principal.type == "service"
    assert listing.computed[0].source == "delegation"
    assert "future_field" not in binding.__dict__
    assert "future_field" not in binding.principal.__dict__
    assert "future_section" not in listing.__dict__


class DemoAccessModel(AccessManagedObjectMixin, BaseObjectOrm):
    """Any object that serves the access routes takes the mixin the same way."""

    def __init__(self, uid: str):
        self.uid = uid

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        return "https://backend.test/api/v1/demo-objects"

    @classmethod
    def build_session(cls):
        return object()


def test_the_mixin_addresses_the_routes_under_its_models_detail_url(monkeypatch):
    sent = _install(
        monkeypatch,
        FakeResponse(200, {"allowed_actions": [], "policies": []}),
        FakeResponse(204),
    )

    DemoAccessModel(USER_UID).get_my_access()
    DemoAccessModel(USER_UID).detach_policy(BINDING_UID)

    assert [(item["r_type"], item["url"]) for item in sent] == [
        ("GET", f"https://backend.test/api/v1/demo-objects/{USER_UID}/access/me/"),
        (
            "DELETE",
            f"https://backend.test/api/v1/demo-objects/{USER_UID}/access/bindings/{BINDING_UID}/",
        ),
    ]


ENVIRONMENT_UID = "2c4e6a8b-1d3f-4a5b-9c7d-8e9f0a1b2c3d"


def test_an_environment_serves_the_access_routes_under_its_detail_url(monkeypatch):
    environment = OrganizationEnvironment(
        uid=ENVIRONMENT_UID,
        name="research",
        organization_owner_uid=TEAM_UID,
        required_repository_branch="research",
        is_production=False,
    )
    sent = _install(
        monkeypatch,
        FakeResponse(
            201,
            _binding(policy={"id": "organization-environments.member", "label": "Environment member"}),
        ),
        FakeResponse(
            403,
            {
                "detail": "This needs the organization-environments.manage_access action.",
                "code": "missing_action",
                "required_action": "organization-environments.manage_access",
                "granting_policies": [],
            },
        ),
    )

    binding = environment.attach_policy("member", principal_type="user", principal_uid=USER_UID)
    with pytest.raises(PermissionDeniedError) as refused:
        environment.attach_policy("observer", principal_type="user", principal_uid=USER_UID)

    base = f"{OrganizationEnvironment.get_object_url()}/{ENVIRONMENT_UID}/access/bindings/"
    assert [(item["r_type"], item["url"]) for item in sent] == [("POST", base), ("POST", base)]
    assert sent[0]["payload"]["json"] == {
        "policy": "member",
        "principal_type": "user",
        "principal_uid": USER_UID,
    }
    assert binding.policy.id == "organization-environments.member"
    assert refused.value.payload["required_action"] == "organization-environments.manage_access"


CONSTANT_UID = "22222222-2222-4222-8222-222222222222"


@pytest.fixture()
def no_environment(monkeypatch):
    """The current branch has no Environment: an operation that needs one raises."""

    def _resolve(operation):
        raise code_repository_context.CodeRepositoryEnvironmentContextRequiredError(
            f"{operation} requires an Organization Environment."
        )

    monkeypatch.setattr(code_repository_context, "resolve_organization_environment_uid", _resolve)


def _constant() -> Constant:
    return Constant(
        uid=CONSTANT_UID,
        name="APP__MODE",
        value="production",
        organization_environment_uid=ENVIRONMENT_UID,
        organization_environment_name="Development",
    )


def test_a_constant_serves_the_access_routes_by_its_uid_alone(monkeypatch, no_environment):
    sent = _install(
        monkeypatch,
        FakeResponse(
            200,
            {
                "object": {"resource_type": "constants", "uid": CONSTANT_UID},
                "bindings": [
                    _binding(
                        policy={"id": "constants.owner", "label": "Constant owner"},
                        source="creator",
                    )
                ],
                "computed": [
                    {
                        "source": "role",
                        "detail": "Organization administrators hold every action.",
                        "policy": None,
                    }
                ],
            },
        ),
        FakeResponse(201, _binding(policy={"id": "constants.reader", "label": "Constant reader"})),
        FakeResponse(
            200,
            {
                "allowed_actions": ["constants.read", "constants.update"],
                "policies": ["constants.editor"],
            },
        ),
        FakeResponse(204),
    )

    constant = _constant()
    listing = constant.get_access()
    binding = constant.attach_policy("reader", principal_type="team", principal_uid=OTHER_TEAM_UID)
    mine = constant.get_my_access()
    constant.detach_policy(binding)

    # No organization_environment_uid is resolved or sent: the branch has none.
    base = f"{Constant.get_object_url()}/{CONSTANT_UID}/access/"
    assert [(item["r_type"], item["url"], item["payload"]) for item in sent] == [
        ("GET", base, {}),
        (
            "POST",
            f"{base}bindings/",
            {
                "json": {
                    "policy": "reader",
                    "principal_type": "team",
                    "principal_uid": OTHER_TEAM_UID,
                }
            },
        ),
        ("GET", f"{base}me/", {}),
        ("DELETE", f"{base}bindings/{BINDING_UID}/", {}),
    ]
    assert listing.object.resource_type == "constants"
    assert listing.bindings[0].policy.id == "constants.owner"
    assert listing.bindings[0].source == "creator"
    assert binding.policy.id == "constants.reader"
    assert mine.policies == ["constants.editor"]


def test_a_refused_change_on_a_constant_names_the_policies_that_give_it(
    monkeypatch, no_environment
):
    refusal = {
        "detail": "This needs the constants.manage_access action.",
        "code": "missing_action",
        "required_action": "constants.manage_access",
        "granting_policies": ["constants.owner"],
        "resource_type": "constants",
        "resource_uid": CONSTANT_UID,
    }
    _install(monkeypatch, FakeResponse(403, refusal))

    with pytest.raises(PermissionDeniedError) as raised:
        _constant().attach_policy("editor", principal_type="user", principal_uid=USER_UID)

    assert raised.value.payload["required_action"] == "constants.manage_access"
    assert raised.value.payload["granting_policies"] == ["constants.owner"]


def test_reading_a_constant_still_uses_the_current_branchs_environment(monkeypatch, no_environment):
    _refuse_requests(monkeypatch)

    with pytest.raises(code_repository_context.CodeRepositoryEnvironmentContextRequiredError):
        Constant.get_by_uid(CONSTANT_UID)
