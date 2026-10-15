"""Team membership on demand: ``RequestUserIdentity.in_teams()`` (ADR 0036, #225).

Requests carry real assertions signed with a test key. The platform's
membership check is a fake ``make_request`` that answers from a set of
memberships and records every call.
"""

import asyncio
import time
import warnings
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import ed25519
from fastapi import FastAPI
from fastapi.testclient import TestClient

import mainsequence._team_memberships as team_memberships_mod
import mainsequence.client.models_user as models_user_mod
from mainsequence._team_memberships import MAX_CACHED_ANSWERS, TeamMembershipCache
from mainsequence.client import RequestIdentityError, User
from mainsequence.client.exceptions import ApiError
from mainsequence.client.models_user import RequestUserIdentity
from mainsequence.server.caller_assertions import (
    ASSERTION_HEADER,
    ASSERTION_TYPE,
    CallerAssertionVerifier,
)
from mainsequence.server.fastapi import install_request_identity

CALLER = str(uuid4())
PERSON = str(uuid4())
RELEASE = str(uuid4())
ENVIRONMENT = str(uuid4())
ISSUER = "https://platform.example.test"


def teams(count):
    return sorted(str(uuid4()) for _ in range(count))


@pytest.fixture(autouse=True)
def environment(monkeypatch):
    for name in (
        "APP_NAME",
        "FASTAPI_PUBLIC_BASE_URL",
        "MAINSEQUENCE_CALLER_ASSERTION_ISSUER",
        "MAINSEQUENCE_CALLER_ASSERTION_JWKS_URL",
        "FASTAPI_PUBLIC_INGRESS",
        "MAINSEQUENCE_ENDPOINT",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("MAINSEQUENCE_ACCESS_TOKEN", "workload-session")


@pytest.fixture
def sign(monkeypatch):
    """Assertions as the platform signs them, verified by the real middleware."""
    key = ed25519.Ed25519PrivateKey.generate()
    public = jwt.algorithms.OKPAlgorithm.to_jwk(key.public_key(), as_dict=True)
    jwk = {name: public[name] for name in ("kty", "crv", "x")}
    verifier = CallerAssertionVerifier(
        issuer=ISSUER,
        jwks_url=ISSUER + "/fastapi/caller-keys/",
        release_uid=RELEASE,
        environment_uid=ENVIRONMENT,
        fetch_jwks=Mock(
            return_value={"keys": [{"kid": "test", "alg": "EdDSA", "use": "sig", **jwk}]}
        ),
    )
    monkeypatch.setenv("MAINSEQUENCE_CALLER_AUTH_MODE", "assertion")
    monkeypatch.setattr(CallerAssertionVerifier, "from_environment", lambda: verifier)

    def token(user=CALLER, **claims):
        now = int(time.time())
        payload = dict(
            iss=ISSUER,
            aud="urn:mainsequence:fapi:" + RELEASE,
            sub=user,
            resource_release_uid=RELEASE,
            organization_environment_uid=ENVIRONMENT,
            iat=now,
            nbf=now,
            exp=now + 120,
            is_organization_admin=False,
        )
        payload.update(claims)
        return jwt.encode(
            payload, key, algorithm="EdDSA", headers={"kid": "test", "typ": ASSERTION_TYPE}
        )

    return token


class Platform:
    """The membership check: answers from ``members`` and records each call."""

    def __init__(self, members=(), status=200):
        self.members = set(members)
        self.status = status
        self.calls = []

    def __call__(self, *, s, loaders, r_type, url, payload=None, time_out=None):
        person = url.rstrip("/").split("/")[-2]
        asked = payload["params"]["team_uid__in"].split(",")
        self.calls.append(SimpleNamespace(person=person, asked=asked, payload=payload, url=url))
        answered = sorted(team for team in asked if (person, team) in self.members)
        return SimpleNamespace(
            status_code=self.status,
            json=lambda: (
                {"user_uid": person, "team_uids": answered}
                if self.status == 200
                else {"detail": "unavailable"}
            ),
            text="",
        )


@pytest.fixture
def platform(monkeypatch):
    fake = Platform()
    monkeypatch.setattr(models_user_mod, "make_request", fake)
    return fake


@pytest.fixture
def clock(monkeypatch):
    now = {"t": 1000.0}
    monkeypatch.setattr(team_memberships_mod, "_now", lambda: now["t"])
    return now


def app_with(handler, **install):
    app = FastAPI()
    app.get("/check")(handler)
    install_request_identity(app, **install)
    return app


def test_the_caller_learns_which_of_the_asked_teams_it_belongs_to(sign, platform):
    research, operations, unknown = teams(3)
    platform.members = {(CALLER, research)}
    proof = sign()

    def handler():
        return sorted(User.get_logged_user().in_teams([research, operations, unknown]))

    with TestClient(app_with(handler)) as client:
        answer = client.get("/check", headers={ASSERTION_HEADER: proof}).json()

    assert answer == [research]
    (call,) = platform.calls
    assert call.url.endswith(f"/users/{CALLER}/team-memberships/")
    assert sorted(call.asked) == sorted([research, operations, unknown])
    assert call.payload["headers"] == {ASSERTION_HEADER: proof}


def test_an_async_handler_asks_through_a_worker_thread_as_its_caller(sign, platform):
    # The call blocks, so an async handler runs it in a worker thread; the
    # thread keeps the request's identity (the skill and knowledge page say so).
    research, operations = teams(2)
    platform.members = {(CALLER, research)}
    proof = sign()

    async def handler():
        caller = User.get_logged_user()
        return sorted(await asyncio.to_thread(caller.in_teams, [research, operations]))

    with TestClient(app_with(handler)) as client:
        answer = client.get("/check", headers={ASSERTION_HEADER: proof}).json()

    assert answer == [research]
    (call,) = platform.calls
    assert call.person == CALLER
    assert call.payload["headers"] == {ASSERTION_HEADER: proof}


def test_the_requester_and_the_acting_caller_are_asked_about_separately(sign, platform):
    research, operations = teams(2)
    platform.members = {(PERSON, research), (CALLER, operations)}
    proof = sign(requester={"sub": PERSON, "is_organization_admin": False})

    def handler():
        return {
            "requester": sorted(User.get_requester().in_teams([research, operations])),
            "caller": sorted(User.get_logged_user().in_teams([research, operations])),
        }

    with TestClient(app_with(handler)) as client:
        answer = client.get("/check", headers={ASSERTION_HEADER: proof}).json()

    assert answer == {"requester": [research], "caller": [operations]}
    assert [call.person for call in platform.calls] == [PERSON, CALLER]


def test_an_admin_is_asked_about_too(sign, platform):
    # The admin flag stays in the proof; membership still comes from the platform.
    (research,) = teams(1)
    platform.members = {(CALLER, research)}

    def handler():
        user = User.get_logged_user()
        return {"admin": user.is_organization_admin, "teams": sorted(user.in_teams([research]))}

    with TestClient(app_with(handler)) as client:
        answer = client.get(
            "/check", headers={ASSERTION_HEADER: sign(is_organization_admin=True)}
        ).json()

    assert answer == {"admin": True, "teams": [research]}
    assert len(platform.calls) == 1


def test_only_the_requests_caller_or_requester_can_be_asked_about(sign, platform):
    def handler():
        RequestUserIdentity(uid=str(uuid4())).in_teams(teams(1))

    with TestClient(app_with(handler), raise_server_exceptions=True) as client:
        with pytest.raises(RequestIdentityError):
            client.get("/check", headers={ASSERTION_HEADER: sign()})
    assert platform.calls == []


def test_one_request_asks_each_question_once(sign, platform):
    research, operations = teams(2)
    platform.members = {(CALLER, research)}

    def handler():
        user = User.get_logged_user()
        first = user.in_teams([research])
        again = user.in_teams([research, operations])
        return [sorted(first), sorted(again)]

    with TestClient(app_with(handler)) as client:
        answer = client.get("/check", headers={ASSERTION_HEADER: sign()}).json()

    assert answer == [[research], [research]]
    assert [call.asked for call in platform.calls] == [[research], [operations]]


def test_without_a_lifetime_each_request_asks_again(sign, platform, clock):
    (research,) = teams(1)

    def handler():
        return sorted(User.get_logged_user().in_teams([research]))

    with TestClient(app_with(handler)) as client:
        for _ in range(2):
            client.get("/check", headers={ASSERTION_HEADER: sign()})

    assert len(platform.calls) == 2


def test_answers_are_reused_across_requests_within_the_lifetime(sign, platform, clock):
    (research,) = teams(1)
    platform.members = {(CALLER, research)}

    def handler():
        return sorted(User.get_logged_user().in_teams([research]))

    with TestClient(app_with(handler, team_membership_cache_seconds=60)) as client:
        first = client.get("/check", headers={ASSERTION_HEADER: sign()}).json()
        platform.members = set()  # the membership ends
        clock["t"] += 59
        reused = client.get("/check", headers={ASSERTION_HEADER: sign()}).json()
        clock["t"] += 2
        expired = client.get("/check", headers={ASSERTION_HEADER: sign()}).json()

    assert (first, reused, expired) == ([research], [research], [])
    assert len(platform.calls) == 2


def test_max_age_seconds_sets_the_lifetime_for_one_call(sign, platform, clock):
    (research,) = teams(1)
    platform.members = {(CALLER, research)}
    answers = []

    def handler():
        answers.append(sorted(User.get_logged_user().in_teams([research], max_age_seconds=10)))

    with TestClient(app_with(handler)) as client:
        client.get("/check", headers={ASSERTION_HEADER: sign()})
        clock["t"] += 5
        client.get("/check", headers={ASSERTION_HEADER: sign()})
        clock["t"] += 6
        client.get("/check", headers={ASSERTION_HEADER: sign()})

    assert answers == [[research]] * 3
    assert len(platform.calls) == 2


def test_a_fresh_check_skips_both_reuses_and_stores_its_answer(sign, platform, clock):
    (research,) = teams(1)
    platform.members = {(CALLER, research)}

    def handler():
        user = User.get_logged_user()
        cached = sorted(user.in_teams([research]))
        platform.members = set()  # the membership ends mid-request
        fresh = sorted(user.in_teams([research], max_age_seconds=0))
        after = sorted(user.in_teams([research]))
        return [cached, fresh, after]

    with TestClient(app_with(handler, team_membership_cache_seconds=3600)) as client:
        answer = client.get("/check", headers={ASSERTION_HEADER: sign()}).json()
        next_request = client.get("/check", headers={ASSERTION_HEADER: sign()}).json()

    assert answer == [[research], [], []]
    # The next request reuses the fresh answer, then asks fresh again.
    assert next_request == [[], [], []]
    assert len(platform.calls) == 3


def test_answers_are_isolated_by_trusted_release_and_environment(platform, monkeypatch):
    (research,) = teams(1)
    platform.members = {(CALLER, research)}
    cache = TeamMembershipCache()

    def ask(release, environment):
        scope = SimpleNamespace(
            release_uid=release,
            environment_uid=environment,
            team_memberships={},
            team_membership_cache=cache,
            team_membership_cache_seconds=3600,
            caller_assertion="proof",
        )
        monkeypatch.setattr(models_user_mod, "_team_membership_scope", lambda uid: scope)
        return models_user_mod._member_team_uids(CALLER, [research], None)

    ask(RELEASE, ENVIRONMENT)
    ask(RELEASE, ENVIRONMENT)
    ask(str(uuid4()), ENVIRONMENT)
    ask(RELEASE, str(uuid4()))

    assert len(platform.calls) == 3


@pytest.mark.parametrize("count", [1000, 10000])
def test_many_teams_are_asked_in_calls_of_at_most_100(sign, platform, count):
    asked = teams(count)
    platform.members = {(CALLER, team) for team in asked[::2]} | {
        (PERSON, team) for team in asked[1::2]
    }
    proof = sign(requester={"sub": PERSON, "is_organization_admin": True})

    def handler():
        return {
            "caller": len(User.get_logged_user().in_teams(asked)),
            "requester": len(User.get_requester().in_teams(asked)),
        }

    with TestClient(app_with(handler)) as client:
        answer = client.get("/check", headers={ASSERTION_HEADER: proof}).json()

    assert answer == {"caller": count // 2, "requester": count // 2}
    assert len(platform.calls) == 2 * count // 100
    assert max(len(call.asked) for call in platform.calls) == 100


def test_the_process_cache_keeps_at_most_10000_answers(clock):
    cache = TeamMembershipCache()
    keys = [(RELEASE, ENVIRONMENT, CALLER, team) for team in teams(MAX_CACHED_ANSWERS + 1)]
    for key in keys:
        cache.put(key, True)

    assert MAX_CACHED_ANSWERS == 10000
    assert len(cache) == MAX_CACHED_ANSWERS
    assert cache.get(keys[0], max_age=60) is None
    assert cache.get(keys[-1], max_age=60) is True


@pytest.mark.parametrize("status", [403, 503])
def test_a_failed_check_raises_instead_of_answering_not_a_member(sign, platform, status):
    platform.status = status

    def handler():
        User.get_logged_user().in_teams(teams(1))

    with TestClient(app_with(handler), raise_server_exceptions=True) as client:
        with pytest.raises(ApiError):
            client.get("/check", headers={ASSERTION_HEADER: sign()})


def test_local_mode_has_no_assertion_to_ask_with(platform, monkeypatch):
    monkeypatch.setenv("MAINSEQUENCE_ENDPOINT", ISSUER)
    monkeypatch.setattr(
        "mainsequence.server.fastapi.requests.get",
        Mock(return_value=SimpleNamespace(status_code=200, json=lambda: {"uid": CALLER})),
    )

    def handler():
        user = User.get_logged_user()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            assert user.team_uids == ()
        user.in_teams(teams(1))

    with TestClient(app_with(handler), raise_server_exceptions=True) as client:
        with pytest.raises(RequestIdentityError):
            client.get("/check", headers={"Authorization": "Bearer developer-token"})
    assert platform.calls == []


def test_team_uids_is_deprecated_and_never_answers_empty_for_a_missing_list(sign, platform):
    (research,) = teams(1)
    seen = {}

    def handler():
        user = User.get_logged_user()
        with pytest.warns(DeprecationWarning):
            try:
                seen["teams"] = user.team_uids
            except RequestIdentityError as error:
                seen["error"] = str(error)

    with TestClient(app_with(handler)) as client:
        client.get("/check", headers={ASSERTION_HEADER: sign(team_uids=[research])})
        signed = dict(seen)
        seen.clear()
        client.get("/check", headers={ASSERTION_HEADER: sign()})

    assert signed == {"teams": (research,)}
    assert "in_teams()" in seen["error"]


@pytest.mark.parametrize(
    ("call", "error"),
    [
        (lambda user: user.in_teams(str(uuid4())), TypeError),
        (lambda user: user.in_teams(["not-a-uuid"]), ValueError),
        (lambda user: user.in_teams(teams(1), max_age_seconds=-1), ValueError),
        (lambda user: user.in_teams(teams(1), max_age_seconds=True), ValueError),
    ],
    ids=["single_uid", "invalid_uid", "negative_age", "boolean_age"],
)
def test_bad_arguments_are_refused(sign, platform, call, error):
    def handler():
        call(User.get_logged_user())

    with TestClient(app_with(handler), raise_server_exceptions=True) as client:
        with pytest.raises(error):
            client.get("/check", headers={ASSERTION_HEADER: sign()})
    assert platform.calls == []


@pytest.mark.parametrize("lifetime", [-1, float("inf"), True, "60"])
def test_the_cache_lifetime_must_be_a_non_negative_number(sign, lifetime):
    with pytest.raises(ValueError):
        install_request_identity(FastAPI(), team_membership_cache_seconds=lifetime)
