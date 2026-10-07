# Caller verification inside request identity

PyJWT and cryptography are required SDK dependencies, installed automatically by
`pip install mainsequence`.

Application handlers use `User.get_logged_user()`. Install request identity once in the application as described in [FastAPI request identity](../fastapi/index.md). Handler code does not parse headers, call the verifier, or bind ContextVars.

The integration uses the existing framework-independent caller verifier internally. It checks the EdDSA (Ed25519) signature, purpose, issuer, audience, target release/Environment, canonical User UID, the caller's facts and validity times. The platform publishes one Ed25519 public key (`kty` `OKP`, `crv` `Ed25519`) whose `kid` is its RFC 7638 thumbprint; a new key arrives with a new `kid`, and any other algorithm or key type is rejected. The maximum lifetime is five minutes. The immutable internal result does not retain the raw proof. The request scope keeps the verified assertion only so that `reads_as_caller()` can present it, and drops it when the request ends.

Every assertion carries the caller's facts: `team_uids`, the caller's canonical active team UIDs, sorted ascending and unique, and `is_organization_admin`, a boolean. An assertion without either, or with a malformed value, is rejected.

A requester-bound call, made by another application while it works for a person, carries one more claim, `requester`: an object with exactly `sub`, the requester's canonical User UID, `team_uids`, the requester's canonical team UIDs, sorted ascending and unique, and `is_organization_admin`, the requester's own Organization-admin flag as a boolean. The assertion's own `sub`, `team_uids` and `is_organization_admin` stay the caller's: the acting application. A `requester` with a missing or extra key, a value of the wrong type, an admin flag that is not a boolean, a UID that is not canonical, or unsorted or repeated team UIDs rejects the assertion, as any other unknown claim does; so does the earlier two-field requester, `sub` and `team_uids` only. The verified requester is kept beside the caller, and the request scope binds it, admin flag included, for `User.get_requester()`; see [Requester-bound calls](../fastapi/index.md#requester-bound-calls).

## Trusted hosted configuration

Django projects these values into a deployed application:

| Variable | Meaning |
| --- | --- |
| MAINSEQUENCE_CALLER_AUTH_MODE | assertion for deployed HTTP |
| MAINSEQUENCE_CALLER_ASSERTION_ISSUER | Expected issuer |
| MAINSEQUENCE_CALLER_ASSERTION_JWKS_URL | Trusted HTTPS public-key URL |
| APP_NAME | Exact release UID |
| MAINSEQUENCE_ORGANIZATION_ENVIRONMENT_UID | Exact Environment UID |

Missing configuration fails setup. UID headers and the SDK process account cannot replace invalid/missing proof. Local execution without hosted markers retains explicit incoming Bearer validation through the same integration.

Public keys use bounded HTTPS retrieval without process credentials or redirects. Discovery has a two-second timeout and a 64 KiB response bound. Keys are cached for 60 seconds; unknown keys or expired cache require refresh. Failure denies verification. Blocking discovery runs outside the event loop.

The low-level verifier module remains importable for compatibility and infrastructure tests. It does not bind a request scope and is not an alternative application caller API. See [ADR-0036](../../adr/0036-request-scoped-logged-user.md).
