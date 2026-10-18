from __future__ import annotations

from uuid import UUID

import pytest

import mainsequence.client as client_pkg
import mainsequence.client.access_catalog as access_catalog
from mainsequence.client.access_catalog import (
    AccessAction,
    AccessCatalog,
    AccessGuard,
    AccessInheritanceRule,
    AccessPolicy,
    AccessResourceType,
    AccessResourceTypeListing,
)
from mainsequence.client.exceptions import AuthenticationError, NotFoundError, TransportError
from mainsequence.client.utils import DoesNotExist

API = "https://backend.example/api/v1"
POLICY_UID = "6f0c2e8a-3b1d-4c5e-9f7a-2d4b6c8e0a12"


class Response:
    def __init__(self, status_code: int, data, *, code: str | None = None):
        self.status_code = status_code
        self._data = data
        self.content = b"{}"
        self.text = ""
        self.headers = {}
        if code is not None:
            self.code = code

    def json(self):
        return self._data


def _action(key: str = "read", **overrides):
    action = {
        "id": f"secrets.{key}",
        "resource_type": "secrets",
        "key": key,
        "label": "See the Secret",
        "description": "See the Secret's name.",
        "access_level": "read",
        "is_deprecated": False,
    }
    action.update(overrides)
    return action


def _policy(**overrides):
    policy = {
        "uid": POLICY_UID,
        "id": "secrets.reader",
        "resource_type": "secrets",
        "name": "reader",
        "label": "Secret reader",
        "summary": "See the Secret's name.",
        "impact": "Shows that the Secret exists.",
        "not_included": "Reading the value.",
        "actions": ["secrets.read"],
        "attachable": True,
        "principal_kinds": ["people", "workloads", "teams"],
        "stage": "active",
    }
    policy.update(overrides)
    return policy


def _resource_type(**overrides):
    resource_type = {
        "key": "secrets",
        "label": "Secret",
        "description": "A value kept out of code.",
        "environment_scoped": True,
        "actions": [_action(), _action("read_value", access_level="use")],
        "policies": [_policy()],
        "inheritance": [
            {
                "relation": "project-secret",
                "parent_type": "organization-projects",
                "parent_policy": "organization-projects.reader",
                "child_policy": "secrets.reader",
            }
        ],
        "guards": [
            {
                "name": "managed-secret-value",
                "description": "A managed Secret refuses a new value.",
                "actions": ["secrets.update"],
            }
        ],
    }
    resource_type.update(overrides)
    return resource_type


@pytest.fixture(autouse=True)
def _backend(monkeypatch):
    monkeypatch.setattr(access_catalog.client_utils, "API_ENDPOINT", API)


@pytest.fixture
def serve(monkeypatch):
    """Answer each request with the next queued response and record what was sent."""

    sent = []
    queued = []

    def fake_make_request(**kwargs):
        sent.append(kwargs)
        return queued.pop(0)

    monkeypatch.setattr(access_catalog, "make_request", fake_make_request)

    def queue(*responses):
        queued.extend(responses)
        return sent

    return queue


def test_access_catalog_and_its_models_are_exported_from_the_client_package():
    for name in access_catalog.__all__:
        assert getattr(client_pkg, name) is getattr(access_catalog, name)
    assert set(access_catalog.__all__) == {
        "AccessAction",
        "AccessCatalog",
        "AccessGuard",
        "AccessInheritanceRule",
        "AccessPolicy",
        "AccessResourceType",
        "AccessResourceTypeListing",
    }


def test_list_resource_types_reads_every_type_and_the_catalog_hash(serve):
    sent = serve(Response(200, {"catalog_hash": "c0ffee", "resource_types": [_resource_type()]}))

    listing = AccessCatalog().list_resource_types()

    assert sent == [
        {
            "s": access_catalog.client_utils.session,
            "loaders": access_catalog.client_utils.loaders,
            "r_type": "GET",
            "url": f"{API}/access/resource-types/",
            "payload": {},
            "time_out": None,
        }
    ]
    assert isinstance(listing, AccessResourceTypeListing)
    assert listing.catalog_hash == "c0ffee"
    secrets = listing.resource_types[0]
    assert isinstance(secrets, AccessResourceType)
    assert secrets.key == "secrets"
    assert secrets.environment_scoped is True
    assert [action.id for action in secrets.actions] == ["secrets.read", "secrets.read_value"]
    assert all(isinstance(action, AccessAction) for action in secrets.actions)
    assert secrets.actions[1].access_level == "use"
    assert isinstance(secrets.policies[0], AccessPolicy)
    assert secrets.policies[0].principal_kinds == ["people", "workloads", "teams"]
    assert secrets.inheritance == [
        AccessInheritanceRule(
            relation="project-secret",
            parent_type="organization-projects",
            parent_policy="organization-projects.reader",
            child_policy="secrets.reader",
        )
    ]
    assert secrets.guards == [
        AccessGuard(
            name="managed-secret-value",
            description="A managed Secret refuses a new value.",
            actions=["secrets.update"],
        )
    ]


def test_get_resource_type_addresses_one_type_by_its_key(serve):
    sent = serve(Response(200, _resource_type()), Response(200, _resource_type()))

    resource_type = AccessCatalog().get_resource_type(" secrets ", timeout=7)
    AccessCatalog().get_resource_type("odd/key?")

    assert resource_type.key == "secrets"
    assert sent[0]["url"] == f"{API}/access/resource-types/secrets/"
    assert sent[0]["time_out"] == 7
    assert sent[0]["payload"] == {}
    # The key stays one path segment; it never adds a path or a query.
    assert sent[1]["url"] == f"{API}/access/resource-types/odd%2Fkey%3F/"


def test_list_policies_lists_every_policy_or_one_types_policies(serve):
    sent = serve(
        Response(200, [_policy(), _policy(id="buckets.reader", resource_type="buckets")]),
        Response(200, [_policy()]),
    )

    every_policy = AccessCatalog().list_policies()
    secrets_policies = AccessCatalog().list_policies(resource_type="secrets", timeout=3)

    assert [policy.id for policy in every_policy] == ["secrets.reader", "buckets.reader"]
    assert all(isinstance(policy, AccessPolicy) for policy in every_policy)
    assert [policy.id for policy in secrets_policies] == ["secrets.reader"]
    assert sent[0]["url"] == f"{API}/access/policies/"
    assert sent[0]["payload"] == {}
    assert sent[1]["url"] == f"{API}/access/policies/"
    assert sent[1]["payload"] == {"params": {"resource_type": "secrets"}}
    assert sent[1]["time_out"] == 3


def test_get_policy_reads_one_policy_by_its_canonical_uid(serve):
    sent = serve(Response(200, _policy()), Response(200, _policy()))

    policy = AccessCatalog().get_policy(POLICY_UID.upper())
    AccessCatalog().get_policy(UUID(POLICY_UID))

    assert sent[0]["url"] == f"{API}/access/policies/{POLICY_UID}/"
    assert sent[1]["url"] == f"{API}/access/policies/{POLICY_UID}/"
    assert policy.uid == POLICY_UID
    assert policy.summary == "See the Secret's name."
    assert policy.impact == "Shows that the Secret exists."
    assert policy.not_included == "Reading the value."
    assert policy.actions == ["secrets.read"]


def test_list_actions_lists_every_action_or_one_types_actions(serve):
    sent = serve(
        Response(200, [_action(), _action("update", access_level="write")]),
        Response(200, [_action()]),
    )

    every_action = AccessCatalog().list_actions()
    secrets_actions = AccessCatalog().list_actions(resource_type="secrets")

    assert [action.access_level for action in every_action] == ["read", "write"]
    assert all(isinstance(action, AccessAction) for action in every_action)
    assert [action.id for action in secrets_actions] == ["secrets.read"]
    assert sent[0]["url"] == f"{API}/access/actions/"
    assert sent[0]["payload"] == {}
    assert sent[1]["payload"] == {"params": {"resource_type": "secrets"}}


def test_a_computed_policy_has_no_principal_kinds(serve):
    serve(
        Response(
            200,
            [
                _policy(
                    id="organization-teams.member",
                    resource_type="organization-teams",
                    name="member",
                    attachable=False,
                    principal_kinds=[],
                )
            ],
        )
    )

    (policy,) = AccessCatalog().list_policies(resource_type="organization-teams")

    assert policy.attachable is False
    assert policy.principal_kinds == []


@pytest.mark.parametrize(
    "call",
    [
        pytest.param(lambda catalog: catalog.get_policy("secrets.reader"), id="policy-id"),
        pytest.param(lambda catalog: catalog.get_policy(""), id="empty-uid"),
        pytest.param(lambda catalog: catalog.get_resource_type(""), id="empty-key"),
        pytest.param(lambda catalog: catalog.get_resource_type("  "), id="blank-key"),
        pytest.param(lambda catalog: catalog.list_policies(resource_type=""), id="empty-policies"),
        pytest.param(lambda catalog: catalog.list_actions(resource_type=" "), id="blank-actions"),
    ],
)
def test_a_malformed_argument_is_refused_before_any_request(serve, call):
    sent = serve()

    with pytest.raises(ValueError):
        call(AccessCatalog())

    assert sent == []


def test_an_unknown_policy_raises_not_found(serve):
    serve(Response(404, {"detail": "No AccessPolicy matches the given query."}))

    with pytest.raises(NotFoundError) as caught:
        AccessCatalog().get_policy(POLICY_UID)

    assert isinstance(caught.value, DoesNotExist)
    assert "No AccessPolicy matches the given query." in str(caught.value)


def test_an_unknown_resource_type_raises_not_found(serve):
    serve(Response(404, {"detail": "Not found."}))

    with pytest.raises(NotFoundError):
        AccessCatalog().get_resource_type("unknown-type")


def test_a_missing_session_raises_authentication_error(serve):
    serve(Response(401, {"detail": "Authentication credentials were not provided."}))

    with pytest.raises(AuthenticationError):
        AccessCatalog().list_policies()


def test_an_unreachable_platform_raises_transport_error(serve):
    serve(Response(500, None, code="expired"))

    with pytest.raises(TransportError):
        AccessCatalog().list_resource_types()


@pytest.mark.parametrize(
    ("call", "body"),
    [
        pytest.param(lambda catalog: catalog.list_policies(), {"results": []}, id="list-object"),
        pytest.param(lambda catalog: catalog.list_actions(), {"results": []}, id="actions-object"),
        pytest.param(lambda catalog: catalog.get_policy(POLICY_UID), [], id="detail-array"),
        pytest.param(lambda catalog: catalog.list_resource_types(), [], id="catalog-array"),
    ],
)
def test_an_answer_of_the_wrong_shape_raises_type_error(serve, call, body):
    serve(Response(200, body))

    with pytest.raises(TypeError):
        call(AccessCatalog())


def test_the_catalog_is_read_tolerantly(serve):
    serve(
        Response(
            200,
            [
                _policy(
                    principal_kinds=["people", "service-accounts"],
                    stage="retiring",
                    future_field="dropped",
                )
            ],
        ),
        Response(200, [_action(access_level="audit", future_field="dropped")]),
    )

    (policy,) = AccessCatalog().list_policies()
    (action,) = AccessCatalog().list_actions()

    assert policy.principal_kinds == ["people", "service-accounts"]
    assert policy.stage == "retiring"
    assert "future_field" not in policy.__dict__
    assert action.access_level == "audit"
    assert "future_field" not in action.__dict__
