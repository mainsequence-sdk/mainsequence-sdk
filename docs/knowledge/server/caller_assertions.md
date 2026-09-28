# Verify platform caller assertions

Server integrations that receive `X-MainSequence-Caller-Assertion` can use the SDK
to verify the platform's signed proof of caller identity. The verifier is a
framework-independent authentication primitive in the optional server extra:

```bash
pip install 'mainsequence[server]'
```

Ordinary protected FastAPI handlers continue to use the
[platform-injected request context](../fastapi/index.md). This module is for the
server integration that consumes signed assertions at its request boundary; it
does not install middleware or replace the application's authorization rules.

## Configuration and use

Configure these values through trusted deployment configuration:

| Variable | Meaning |
| --- | --- |
| `MAINSEQUENCE_CALLER_ASSERTION_ISSUER` | Expected assertion issuer. |
| `MAINSEQUENCE_CALLER_ASSERTION_JWKS_URL` | HTTPS URL for the platform's public signing keys. |
| `APP_NAME` | Target ResourceRelease UID, as a canonical lowercase UUID. |
| `MAINSEQUENCE_ORGANIZATION_ENVIRONMENT_UID` | Target Environment UID, as a canonical lowercase UUID. |

Create one verifier for the server process and call it at the request boundary:

```python
from mainsequence.server.caller_assertions import (
    ASSERTION_HEADER,
    CallerAssertionUnavailable,
    CallerAssertionVerifier,
    InvalidCallerAssertion,
)

verifier = CallerAssertionVerifier.from_environment()

def verified_caller(headers):
    try:
        return verifier.verify(headers.get(ASSERTION_HEADER, ""))
    except InvalidCallerAssertion:
        # Reject the request as unauthenticated (for HTTP servers, typically 401).
        raise
    except CallerAssertionUnavailable:
        # Fail closed (for HTTP servers, typically 503).
        raise
```

Alternatively, pass `issuer`, `jwks_url`, `release_uid`, and `environment_uid` to
the constructor explicitly. Never derive these expected values from request
headers or unverified assertion claims. Configuration errors raise
`CallerAssertionUnavailable` when constructing the verifier.

Verification checks the exact assertion header and claim contract, RS256
signature, issuer, release audience, release and Environment UIDs, canonical user
UID, and validity times. The assertion lifetime may not exceed five minutes.
The returned immutable `AuthenticatedCaller` contains `user_uid`, `release_uid`,
`environment_uid`, `issued_at`, and `expires_at`; it does not retain the token.
Authorize the caller's intended operation separately using `user_uid`.

Public-key responses are limited to 64 KiB, fetched without SDK credentials, and
must contain the platform's bounded RSA signing-key set. Discovery uses a
two-second timeout and does not follow redirects. Keys are cached for 60 seconds;
an expired cache or unknown key ID requires a refresh. Failed discovery discards
the cache and fails closed. A successful refresh without the requested key makes
the assertion invalid.

Applications choose their HTTP error mapping and request-state integration. The
verifier performs synchronous I/O, so async servers should invoke it through
their normal synchronous dependency or thread-pool mechanism. `fetch_jwks=` accepts
an injected discovery function for testing or an application-owned transport.
