# ADR-0036: Request-scoped logged user

Amended 2026-10-06 for [SDK issue #198](https://github.com/mainsequence-sdk/mainsequence-sdk/issues/198): an application can read the directory as its caller. The platform API accepts the call: with the release's workload credential and the caller assertion the application received, `GET /api/v1/users/`, `/users/<uid>/`, `/teams/` and `/teams/<uid>/` answer with the caller's visibility, and every other action refuses the assertion. The request scope now keeps the verified assertion, and `mainsequence.server.fastapi.reads_as_caller()` is the only way to present it. See [Reads as the caller](#reads-as-the-caller).

Implementation record 2026-09-28 for the owner's instruction to implement and commit the reviewed platform authentication plan.

This extends the retained authentication ownership in [ADR-0034](0034-extract-metatables-python-package.md). The existing low-level verifier remains framework-independent; this integration supplies its request lifetime.

## Contract

Caller verification is part of the standard SDK. PyJWT and cryptography are
required runtime dependencies, supplied by a plain `mainsequence` installation.

Application handlers use User.get_logged_user(). Applications install mainsequence.server.fastapi.install_request_identity(app) once. The SDK integration uses the existing caller assertion verifier, binds one isolated context, invokes the application, and invalidates/resets the context on completion, error, or cancellation. The verifier is an internal building block rather than another handler recipe.

PodDeploymentOrchestrator remains independent of the SDK. It validates a package-neutral app.state.mainsequence_request_identity declaration (installed, mode, public_ingress) before serving. The declaration is a wiring check, not caller proof. Hosting, health, CORS, WebSocket transport policy and observability remain launcher responsibilities.

Django projects MAINSEQUENCE_CALLER_AUTH_MODE=assertion, the expected issuer and HTTPS key URL, APP_NAME and the Environment UID. Hosted HTTP requires signed proof without UID/process-account fallback. Local execution without deployment markers retains direct Bearer validation inside the same integration. Public/OPTIONS scopes are anonymous; WebSockets keep the existing gateway ticket/header contract with connection-scoped context.

The getter preserves the minimal uid/optional username result. Signed assertions have no username, so it is null. Request-state compatibility fields project the same identity. The process-level get_authenticated_user_details remains distinct.

## Lifecycle and migration

Missing/invalid proof is 401; required verification unavailable is 503. Context resets in finally and an expired request scope cannot be reused from a copied child task. Synchronous routes inherit framework-propagated context. Outside an authenticated scope the getter raises RequestIdentityError.

Migrate existing factories to install the integration before using the matching launcher. Remove manual header/context/verifier handling. No public HTTP route, JWT wire format, package version, database model, or service is introduced. Keep Django signing and gateway proof transport ready before enabling proof-required applications.

## Reads as the caller

Added 2026-10-06 for #198.

- In assertion mode, the scope of an admitted HTTP request keeps the verified assertion next to the user. It expires with the scope, also in copied child contexts, and never enters `repr`, `str`, error messages or logs. Local mode, WebSockets and public or OPTIONS requests have none. The verifier's result still does not hold it.
- `reads_as_caller()` is a context manager. Inside it, the reads of `/users/`, `/users/<uid>/`, `/teams/` and `/teams/<uid>/`, that is `filter`, `iter_filter`, `get` and `get_by_uid` on `User` and `Team`, add `X-MainSequence-Caller-Assertion` with the request's own assertion. No other call sends it, and nothing sends it outside the helper: the SDK never attaches it implicitly.
- The header is set on each request, never on the process-wide HTTP session, and goes only to the configured platform endpoint. A paging link on another origin raises instead of being followed with or without the header.
- No fallback. Entering the helper outside an authenticated request, or in a request without an assertion, raises `RequestIdentityError`, and so does a read inside the helper after its request ended. Neither reads as the workload instead. The assertion lives at most 300 seconds and is not refreshed; a read after it expires fails with the platform's answer.
- The platform answers these reads `no-store`; the SDK does not cache them.
- Process credentials are unchanged. The workload credential still authenticates every call; the assertion only selects whose visibility the answer has.
