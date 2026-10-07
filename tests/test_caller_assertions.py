from __future__ import annotations

import json
import time
from uuid import uuid4

import jwt
import pytest
import requests
from cryptography.hazmat.primitives.asymmetric import ed25519

from mainsequence.server.caller_assertions import (
    ASSERTION_TYPE,
    CallerAssertionUnavailable,
    CallerAssertionVerifier,
    InvalidCallerAssertion,
)

USER = str(uuid4())
RELEASE = str(uuid4())
ENVIRONMENT = str(uuid4())
ISSUER = "https://platform.example.test"
KEYS_URL = f"{ISSUER}/caller-keys/"


@pytest.fixture(scope="module")
def keys():
    result = []
    for kid in ("first", "second"):
        key = ed25519.Ed25519PrivateKey.generate()
        public = jwt.algorithms.OKPAlgorithm.to_jwk(key.public_key(), as_dict=True)
        result.append(
            (
                key,
                {
                    "kid": kid,
                    "alg": "EdDSA",
                    "use": "sig",
                    **{name: public[name] for name in ("kty", "crv", "x")},
                },
            )
        )
    return result


def token(key, *, kid="first", headers=None, omit=(), **changes):
    """An assertion as the platform signs it, with the caller's facts; ``omit`` drops claims."""
    now = int(time.time())
    claims = {
        "iss": ISSUER,
        "aud": f"urn:mainsequence:fapi:{RELEASE}",
        "sub": USER,
        "resource_release_uid": RELEASE,
        "organization_environment_uid": ENVIRONMENT,
        "iat": now,
        "nbf": now,
        "exp": now + 120,
        "team_uids": [],
        "is_organization_admin": False,
    }
    claims.update(changes)
    for name in omit:
        del claims[name]
    return jwt.encode(
        claims, key, algorithm="EdDSA", headers=headers or {"kid": kid, "typ": ASSERTION_TYPE}
    )


def verifier(fetch):
    return CallerAssertionVerifier(
        issuer=ISSUER,
        jwks_url=KEYS_URL,
        release_uid=RELEASE,
        environment_uid=ENVIRONMENT,
        fetch_jwks=fetch,
    )


TEAMS = sorted([str(uuid4()), str(uuid4())])


def test_exposes_the_callers_teams_and_admin_flag(keys):
    key, public = keys[0]
    check = verifier(lambda: {"keys": [public]})

    caller = check.verify(token(key, team_uids=TEAMS, is_organization_admin=True))

    assert caller.team_uids == tuple(TEAMS)
    assert caller.is_organization_admin is True


def test_a_caller_in_no_team_who_is_not_an_admin(keys):
    key, public = keys[0]

    caller = verifier(lambda: {"keys": [public]}).verify(token(key))

    assert caller.team_uids == ()
    assert caller.is_organization_admin is False


@pytest.mark.parametrize(
    "omit",
    [("team_uids", "is_organization_admin"), ("team_uids",), ("is_organization_admin",)],
    ids=["no_facts", "no_team_uids", "no_admin_flag"],
)
def test_rejects_an_assertion_without_the_callers_facts(keys, omit):
    """Every assertion states the caller's teams and admin flag; there is no older shape."""
    key, public = keys[0]
    with pytest.raises(InvalidCallerAssertion):
        verifier(lambda: {"keys": [public]}).verify(token(key, omit=omit))


@pytest.mark.parametrize(
    "changes",
    [
        {"team_uids": None},
        {"team_uids": TEAMS[0]},
        {"team_uids": list(reversed(TEAMS))},
        {"team_uids": [TEAMS[0], TEAMS[0]]},
        {"team_uids": [TEAMS[0].upper()]},
        {"is_organization_admin": None},
        {"is_organization_admin": "true"},
        {"is_organization_admin": 1},
        {"is_organization_admin": 0},
    ],
)
def test_rejects_malformed_caller_facts(keys, changes):
    key, public = keys[0]
    with pytest.raises(InvalidCallerAssertion):
        verifier(lambda: {"keys": [public]}).verify(token(key, **changes))


# --- Requester-bound calls (ADR-0036) ----------------------------------------------
#
# Another application calls while it works for a person, the requester. The
# assertion's `sub` and facts stay the caller's, the acting application, and
# `requester` names the person with the person's own facts: exactly `sub`,
# `team_uids` and `is_organization_admin`.

PERSON = "5b0f9a8e-3c2d-4e1f-9a7b-6c5d4e3f2a1b"
REQUESTER = {"sub": PERSON, "team_uids": TEAMS, "is_organization_admin": True}


@pytest.mark.parametrize(
    ("caller_admin", "person_admin"),
    [(False, True), (True, False)],
    ids=["admin_person", "admin_caller"],
)
def test_exposes_the_requester_beside_the_acting_caller(keys, caller_admin, person_admin):
    key, public = keys[0]
    caller_teams = [str(uuid4())]
    check = verifier(lambda: {"keys": [public]})

    caller = check.verify(
        token(
            key,
            team_uids=caller_teams,
            is_organization_admin=caller_admin,
            requester={**REQUESTER, "is_organization_admin": person_admin},
        )
    )

    # Each identity keeps its own facts: neither admin flag reaches the other.
    assert (caller.user_uid, caller.team_uids, caller.is_organization_admin) == (
        USER,
        tuple(caller_teams),
        caller_admin,
    )
    requester = caller._requester
    assert (requester.user_uid, requester.team_uids, requester.is_organization_admin) == (
        PERSON,
        tuple(TEAMS),
        person_admin,
    )


def test_a_requester_in_no_team_who_is_not_an_admin(keys):
    key, public = keys[0]

    caller = verifier(lambda: {"keys": [public]}).verify(
        token(key, requester={"sub": PERSON, "team_uids": [], "is_organization_admin": False})
    )

    requester = caller._requester
    assert (requester.user_uid, requester.team_uids, requester.is_organization_admin) == (
        PERSON,
        (),
        False,
    )


def test_a_call_that_is_not_requester_bound_has_no_requester(keys):
    key, public = keys[0]
    check = verifier(lambda: {"keys": [public]})

    assert check.verify(token(key))._requester is None
    assert check.verify(token(key, team_uids=TEAMS, is_organization_admin=True))._requester is None


@pytest.mark.parametrize(
    "requester",
    [
        None,
        PERSON,
        [PERSON, TEAMS, True],
        {},
        {"sub": PERSON, "team_uids": TEAMS},
        {"sub": PERSON, "is_organization_admin": True},
        {"team_uids": TEAMS, "is_organization_admin": True},
        {**REQUESTER, "username": "person"},
        {**REQUESTER, "requester": {"sub": PERSON}},
        {**REQUESTER, "is_organization_admin": None},
        {**REQUESTER, "is_organization_admin": 1},
        {**REQUESTER, "is_organization_admin": 0},
        {**REQUESTER, "is_organization_admin": "true"},
        {**REQUESTER, "is_organization_admin": "false"},
        {**REQUESTER, "sub": PERSON.upper()},
        {**REQUESTER, "sub": PERSON.replace("-", "")},
        {**REQUESTER, "sub": "not-a-uuid"},
        {**REQUESTER, "sub": None},
        {**REQUESTER, "sub": 1},
        {**REQUESTER, "team_uids": None},
        {**REQUESTER, "team_uids": TEAMS[0]},
        {**REQUESTER, "team_uids": [1]},
        {**REQUESTER, "team_uids": [TEAMS[0].upper()]},
        {**REQUESTER, "team_uids": list(reversed(TEAMS))},
        {**REQUESTER, "team_uids": [TEAMS[0], TEAMS[0]]},
    ],
    ids=[
        "null",
        "string",
        "list",
        "empty",
        "two_field_without_admin_flag",
        "missing_team_uids",
        "missing_sub",
        "extra_key",
        "nested_requester",
        "null_admin_flag",
        "integer_true_admin_flag",
        "integer_false_admin_flag",
        "string_true_admin_flag",
        "string_false_admin_flag",
        "uppercase_sub",
        "unhyphenated_sub",
        "invalid_sub",
        "null_sub",
        "integer_sub",
        "null_team_uids",
        "string_team_uids",
        "integer_team_uid",
        "uppercase_team_uid",
        "unsorted_team_uids",
        "duplicate_team_uids",
    ],
)
def test_rejects_a_malformed_requester(keys, requester):
    key, public = keys[0]
    with pytest.raises(InvalidCallerAssertion):
        verifier(lambda: {"keys": [public]}).verify(token(key, requester=requester))


@pytest.mark.parametrize(
    ("changes", "omit"),
    [
        ({"extra": "not an identity claim"}, ()),
        ({"requester_is_organization_admin": True}, ()),
        ({}, ("team_uids", "is_organization_admin")),
        ({}, ("team_uids",)),
        ({}, ("is_organization_admin",)),
    ],
    ids=[
        "unknown_claim",
        "requester_admin_claim",
        "no_caller_facts",
        "no_caller_team_uids",
        "no_caller_admin_flag",
    ],
)
def test_a_requester_bound_assertion_admits_no_other_shape(keys, changes, omit):
    key, public = keys[0]
    with pytest.raises(InvalidCallerAssertion):
        verifier(lambda: {"keys": [public]}).verify(
            token(key, requester=REQUESTER, omit=omit, **changes)
        )


def test_returns_verified_identity_without_retaining_raw_assertion(keys):
    key, public = keys[0]
    proof = token(key)
    caller = verifier(lambda: {"keys": [public]}).verify(proof)
    assert (caller.user_uid, caller.release_uid, caller.environment_uid) == (
        USER,
        RELEASE,
        ENVIRONMENT,
    )
    assert caller.expires_at > caller.issued_at
    assert proof not in repr(caller)
    assert not hasattr(caller, "assertion")


@pytest.mark.parametrize(
    "changes",
    [
        {"iss": "https://other.test"},
        {"aud": "wrong-release"},
        {"resource_release_uid": str(uuid4())},
        {"organization_environment_uid": str(uuid4())},
        {"sub": USER.upper()},
        {"sub": "bad"},
        {"sub": None},
        {"exp": 1},
        {"exp": int(time.time()) + 1000},
        {"nbf": int(time.time()) + 1000},
        {"iat": int(time.time()) + 1000},
        {"iat": True},
        {"exp": "9999999999"},
        {"extra": "not an identity claim"},
    ],
)
def test_rejects_invalid_scope_or_lifetime(keys, changes):
    key, public = keys[0]
    with pytest.raises(InvalidCallerAssertion):
        verifier(lambda: {"keys": [public]}).verify(token(key, **changes))


def test_rejects_signature_algorithm_type_and_missing_claims(keys):
    key, public = keys[0]
    check = verifier(lambda: {"keys": [public]})
    with pytest.raises(InvalidCallerAssertion):
        check.verify(token(keys[1][0]))
    with pytest.raises(InvalidCallerAssertion):
        check.verify(token(key, headers={"kid": "first", "typ": "JWT"}))
    claims = jwt.decode(token(key), options={"verify_signature": False})
    forged = jwt.encode(
        claims,
        "unrelated-secret-value-of-at-least-32-bytes",
        algorithm="HS256",
        headers={"kid": "first", "typ": ASSERTION_TYPE},
    )
    with pytest.raises(InvalidCallerAssertion):
        check.verify(forged)
    claims.pop("nbf")
    with pytest.raises(InvalidCallerAssertion):
        check.verify(
            jwt.encode(
                claims, key, algorithm="EdDSA", headers={"kid": "first", "typ": ASSERTION_TYPE}
            )
        )


@pytest.mark.parametrize("proof", [None, "", "not-a-token", "a" * 16385])
def test_rejects_missing_or_unbounded_proof_without_key_fetch(proof):
    def unexpected_fetch():
        pytest.fail("Malformed proof should not fetch keys")

    with pytest.raises(InvalidCallerAssertion):
        verifier(unexpected_fetch).verify(proof)


@pytest.mark.parametrize("kid", ["", "a" * 129])
def test_rejects_bad_key_id_without_discovery(keys, kid):
    def unexpected_fetch():
        pytest.fail("Malformed key ID should not fetch keys")

    with pytest.raises(InvalidCallerAssertion):
        verifier(unexpected_fetch).verify(token(keys[0][0], kid=kid))


def test_keys_are_cached_and_unknown_key_refreshes(keys):
    documents = [{"keys": [keys[0][1]]}, {"keys": [k[1] for k in keys]}]
    check = verifier(lambda: documents.pop(0))
    check.verify(token(keys[0][0]))
    check.verify(token(keys[0][0]))
    assert len(documents) == 1
    check.verify(token(keys[1][0], kid="second"))
    assert documents == []


def test_failed_rotation_discards_cached_keys(keys):
    available = True

    def fetch():
        if not available:
            raise requests.ConnectionError("private-response-detail")
        return {"keys": [keys[0][1]]}

    check = verifier(fetch)
    check.verify(token(keys[0][0]))
    available = False
    with pytest.raises(CallerAssertionUnavailable):
        check.verify(token(keys[1][0], kid="second"))
    with pytest.raises(CallerAssertionUnavailable):
        check.verify(token(keys[0][0]))


def test_expired_key_cache_requires_refresh(keys, monkeypatch):
    documents = [{"keys": [keys[0][1]]}]
    check = verifier(lambda: documents.pop(0))
    check.verify(token(keys[0][0]))
    monkeypatch.setattr(check, "_keys_until", 0)
    with pytest.raises(CallerAssertionUnavailable):
        check.verify(token(keys[0][0]))


@pytest.mark.parametrize("document", [None, {}, {"keys": []}, {"keys": [{}]}, {"keys": [{}] * 17}])
def test_malformed_key_sets_are_unavailable(keys, document):
    with pytest.raises(CallerAssertionUnavailable):
        verifier(lambda: document).verify(token(keys[0][0]))


class KeyResponse:
    def __init__(self, content, status=200):
        self.content = content
        self.status_code = status
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.closed = True

    def iter_content(self, chunk_size):
        for pos in range(0, len(self.content), chunk_size):
            yield self.content[pos : pos + chunk_size]


def test_jwks_fetch_is_bounded_unauthenticated_and_closes_response(keys, monkeypatch):
    reply = KeyResponse(json.dumps({"keys": [keys[0][1]]}).encode())
    calls = []

    def get(url, **kwargs):
        calls.append((url, kwargs))
        return reply

    monkeypatch.setattr(requests, "get", get)
    assert verifier(None).verify(token(keys[0][0])).user_uid == USER
    assert calls == [(KEYS_URL, {"timeout": 2.0, "allow_redirects": False, "stream": True})]
    assert reply.closed


@pytest.mark.parametrize(
    ("content", "status"),
    [(b"private-body", 500), (b"redirect", 302), (b"invalid-json", 200), (b"x" * 65537, 200)],
)
def test_jwks_fetch_rejects_bad_responses(keys, monkeypatch, content, status):
    reply = KeyResponse(content, status)
    monkeypatch.setattr(requests, "get", lambda *a, **kw: reply)
    with pytest.raises(CallerAssertionUnavailable) as error:
        verifier(None).verify(token(keys[0][0]))
    assert "private-body" not in str(error.value)
    assert reply.closed


def test_environment_configuration_and_missing_values(keys, monkeypatch):
    values = {
        "MAINSEQUENCE_CALLER_ASSERTION_ISSUER": ISSUER,
        "MAINSEQUENCE_CALLER_ASSERTION_JWKS_URL": KEYS_URL,
        "APP_NAME": RELEASE,
        "MAINSEQUENCE_ORGANIZATION_ENVIRONMENT_UID": ENVIRONMENT,
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    check = CallerAssertionVerifier.from_environment(fetch_jwks=lambda: {"keys": [keys[0][1]]})
    assert check.verify(token(keys[0][0])).user_uid == USER
    monkeypatch.delenv("APP_NAME")
    with pytest.raises(CallerAssertionUnavailable):
        CallerAssertionVerifier.from_environment()


@pytest.mark.parametrize(
    "url",
    ["http://keys.test", "https://user:secret@keys.test", "missing-host", "", None, "https://[bad"],
)
def test_rejects_untrusted_key_url_configuration(url):
    with pytest.raises(CallerAssertionUnavailable):
        CallerAssertionVerifier(
            issuer=ISSUER, jwks_url=url, release_uid=RELEASE, environment_uid=ENVIRONMENT
        )
