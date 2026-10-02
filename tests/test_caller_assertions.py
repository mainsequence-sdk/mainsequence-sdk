from __future__ import annotations

import json
import time
from uuid import uuid4

import pytest
import requests

jwt = pytest.importorskip("jwt")
ed25519 = pytest.importorskip("cryptography.hazmat.primitives.asymmetric.ed25519")

from mainsequence.server.caller_assertions import (  # noqa: E402
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


def token(key, *, kid="first", headers=None, **changes):
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
    }
    claims.update(changes)
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
