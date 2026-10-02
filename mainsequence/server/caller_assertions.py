"""Verify platform-signed caller identity independently of a web framework."""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import urlsplit

import requests

try:
    import jwt
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
    from jwt import InvalidKeyError, InvalidTokenError
except ImportError as exc:
    raise ImportError("Caller verification requires the mainsequence[server] extra.") from exc

ASSERTION_HEADER = "X-MainSequence-Caller-Assertion"
ASSERTION_TYPE = "mainsequence-caller-assertion+jwt"
ASSERTION_ALGORITHM = "EdDSA"
MAX_ASSERTION_SECONDS = 300
KEY_CACHE_SECONDS = 60
REQUIRED_CLAIMS = frozenset(
    {
        "iss",
        "aud",
        "sub",
        "resource_release_uid",
        "organization_environment_uid",
        "iat",
        "nbf",
        "exp",
    }
)


class InvalidCallerAssertion(ValueError):
    """The request does not carry valid proof for this runtime."""


class CallerAssertionUnavailable(RuntimeError):
    """Runtime trust configuration or public-key discovery is unavailable."""


def _canonical_uid(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("A UID must be a string.")
    canonical = str(uuid.UUID(value))
    if canonical != value:
        raise ValueError("A UID must be a canonical lowercase UUID.")
    return canonical


@dataclass(frozen=True)
class AuthenticatedCaller:
    user_uid: str
    release_uid: str
    environment_uid: str
    issued_at: int
    expires_at: int


class CallerAssertionVerifier:
    """Verify the signed actor proof against deployment-owned target values.

    Public keys are cached briefly by key ID. An expired cache must refresh;
    an unknown key triggers an immediate refresh. Neither case accepts stale
    public-key data when discovery is unavailable.
    """

    def __init__(
        self,
        *,
        issuer: str,
        jwks_url: str,
        release_uid: str,
        environment_uid: str,
        fetch_jwks: Callable[[], object] | None = None,
    ) -> None:
        try:
            self.release_uid = _canonical_uid(release_uid)
            self.environment_uid = _canonical_uid(environment_uid)
        except ValueError as exc:
            raise CallerAssertionUnavailable(
                "Trusted runtime target configuration is invalid."
            ) from exc
        if not issuer or not isinstance(issuer, str):
            raise CallerAssertionUnavailable("Caller assertion issuer is not configured.")
        try:
            if not isinstance(jwks_url, str):
                raise ValueError
            parsed_url = urlsplit(jwks_url)
        except ValueError:
            raise CallerAssertionUnavailable("Caller public-key URL must use HTTPS.") from None
        if (
            parsed_url.scheme != "https"
            or not parsed_url.hostname
            or parsed_url.username
            or parsed_url.password
        ):
            raise CallerAssertionUnavailable("Caller public-key URL must use HTTPS.")
        self.issuer = issuer
        self.jwks_url = jwks_url
        self._fetch_jwks = fetch_jwks or self._fetch_public_keys
        self._keys: dict[str, Ed25519PublicKey] = {}
        self._keys_until = 0.0
        self._lock = threading.Lock()

    @classmethod
    def from_environment(
        cls, *, fetch_jwks: Callable[[], object] | None = None
    ) -> CallerAssertionVerifier:
        """Read trusted release configuration, never request-supplied scope."""
        return cls(
            issuer=os.environ.get("MAINSEQUENCE_CALLER_ASSERTION_ISSUER", ""),
            jwks_url=os.environ.get("MAINSEQUENCE_CALLER_ASSERTION_JWKS_URL", ""),
            release_uid=os.environ.get("APP_NAME", ""),
            environment_uid=os.environ.get("MAINSEQUENCE_ORGANIZATION_ENVIRONMENT_UID", ""),
            fetch_jwks=fetch_jwks,
        )

    def _fetch_public_keys(self) -> object:
        try:
            with requests.get(
                self.jwks_url, timeout=2.0, allow_redirects=False, stream=True
            ) as response:
                if response.status_code != 200:
                    raise CallerAssertionUnavailable("Caller public keys are unavailable.")
                content = bytearray()
                for chunk in response.iter_content(chunk_size=8192):
                    content.extend(chunk)
                    if len(content) > 65536:
                        raise CallerAssertionUnavailable("Caller public-key response is too large.")
                return json.loads(content)
        except (requests.RequestException, ValueError):
            raise CallerAssertionUnavailable("Caller public keys are unavailable.") from None

    @staticmethod
    def _parse_public_keys(document: object) -> dict[str, Ed25519PublicKey]:
        if not isinstance(document, dict) or set(document) != {"keys"}:
            raise CallerAssertionUnavailable("Caller public-key document is invalid.")
        entries = document["keys"]
        if not isinstance(entries, list) or not 1 <= len(entries) <= 16:
            raise CallerAssertionUnavailable("Caller public-key set is invalid.")
        keys: dict[str, Ed25519PublicKey] = {}
        for entry in entries:
            if (
                not isinstance(entry, dict)
                or set(entry) != {"kid", "alg", "use", "kty", "crv", "x"}
                or entry.get("alg") != ASSERTION_ALGORITHM
                or entry.get("use") != "sig"
                or entry.get("kty") != "OKP"
                or entry.get("crv") != "Ed25519"
            ):
                raise CallerAssertionUnavailable("Caller public key is invalid.")
            kid = entry["kid"]
            if not isinstance(kid, str) or not kid or len(kid) > 128 or kid in keys:
                raise CallerAssertionUnavailable("Caller public-key ID is invalid.")
            try:
                key = jwt.algorithms.OKPAlgorithm.from_jwk(entry)
            except (ValueError, TypeError, InvalidKeyError) as exc:
                raise CallerAssertionUnavailable("Caller public key is invalid.") from exc
            if not isinstance(key, Ed25519PublicKey):
                raise CallerAssertionUnavailable("Caller public key is not an Ed25519 public key.")
            keys[kid] = key
        return keys

    def _key_for(self, kid: str) -> Ed25519PublicKey:
        with self._lock:
            if time.monotonic() >= self._keys_until or kid not in self._keys:
                self._keys = {}
                self._keys_until = 0.0
                try:
                    keys = self._parse_public_keys(self._fetch_jwks())
                except CallerAssertionUnavailable:
                    raise
                except Exception as exc:
                    raise CallerAssertionUnavailable("Caller public keys are unavailable.") from exc
                self._keys = keys
                self._keys_until = time.monotonic() + KEY_CACHE_SECONDS
            try:
                return self._keys[kid]
            except KeyError as exc:
                raise InvalidCallerAssertion("Caller signing key is unknown.") from exc

    def verify(self, assertion: str) -> AuthenticatedCaller:
        if not isinstance(assertion, str) or not assertion or len(assertion) > 16384:
            raise InvalidCallerAssertion("Caller assertion is missing or too large.")
        try:
            header = jwt.get_unverified_header(assertion)
            if (
                set(header) != {"alg", "kid", "typ"}
                or header["alg"] != ASSERTION_ALGORITHM
                or header["typ"] != ASSERTION_TYPE
                or not isinstance(header["kid"], str)
                or not 1 <= len(header["kid"]) <= 128
            ):
                raise InvalidCallerAssertion("Caller assertion header is invalid.")
            key = self._key_for(header["kid"])
            payload = jwt.decode(
                assertion,
                key=key,
                algorithms=[ASSERTION_ALGORITHM],
                issuer=self.issuer,
                audience=f"urn:mainsequence:fapi:{self.release_uid}",
                options={"require": list(REQUIRED_CLAIMS)},
            )
            if set(payload) != REQUIRED_CLAIMS:
                raise InvalidCallerAssertion("Caller assertion claims are invalid.")
            if (
                payload["aud"] != f"urn:mainsequence:fapi:{self.release_uid}"
                or _canonical_uid(payload["resource_release_uid"]) != self.release_uid
                or _canonical_uid(payload["organization_environment_uid"]) != self.environment_uid
            ):
                raise InvalidCallerAssertion("Caller assertion target does not match.")
            user_uid = _canonical_uid(payload["sub"])
            iat, nbf, exp = (payload[name] for name in ("iat", "nbf", "exp"))
            if not all(type(value) is int for value in (iat, nbf, exp)):
                raise InvalidCallerAssertion("Caller assertion times are invalid.")
            if not nbf <= iat < exp <= iat + MAX_ASSERTION_SECONDS:
                raise InvalidCallerAssertion("Caller assertion lifetime is invalid.")
            return AuthenticatedCaller(
                user_uid=user_uid,
                release_uid=self.release_uid,
                environment_uid=self.environment_uid,
                issued_at=iat,
                expires_at=exp,
            )
        except (InvalidTokenError, KeyError, TypeError, ValueError) as exc:
            raise InvalidCallerAssertion("Caller assertion is invalid.") from exc


__all__ = [
    "ASSERTION_HEADER",
    "AuthenticatedCaller",
    "CallerAssertionVerifier",
    "CallerAssertionUnavailable",
    "InvalidCallerAssertion",
]
