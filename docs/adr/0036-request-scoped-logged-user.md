# ADR-0036: Request-scoped logged user

Amended 2026-10-07 for [SDK issue #206](https://github.com/mainsequence-sdk/mainsequence-sdk/issues/206): requester-bound identity is not a read-only permission. Application REST handlers and MCP tools authorize each read or write against the verified person's object- and operation-specific member-level rights. The signed claim shape, getters, request lifetime and directory-read helper are unchanged; platform write opt-ins and platform-authorized Agent delegation do not give this SDK a forwarding or impersonation API. See the [access guide](../knowledge/infrastructure/users_and_access.md#applications-that-act-for-their-requester) for the coordinated platform rollout and write risk.

Amended 2026-10-06: an application can receive requester-bound calls. Another application, for example an Agent, calls it while it works for a person, the requester, and the platform signs that person into the caller assertion it already sends, as the claim `requester`. The assertion's `sub` stays the calling application. The verifier accepts the claim in its exact shape, the request scope binds the verified requester next to the caller, `User.get_requester()` returns it, `User.get_logged_user()` is unchanged, and `reads_as_caller()` refuses a requester-bound request. See [Requester-bound calls](#requester-bound-calls).

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

## Requester-bound calls

Added 2026-10-06.

- The assertion of a requester-bound call carries one more claim, `requester`: a JSON object with exactly `sub`, the requester's User UID as a canonical lowercase UUID, and `team_uids`, the requester's team UIDs, canonical, sorted ascending and unique. It never carries an admin flag. The assertion's own `sub` and its optional `team_uids`/`is_organization_admin` pair stay the caller's: the acting application.
- The verifier accepts the base claims, the optional caller facts pair and the optional `requester` claim, and nothing else. A `requester` with a missing or extra key, a value of the wrong type, a UID that is not canonical, or unsorted or repeated team UIDs rejects the whole assertion, so the request gets 401. The verifier's result keeps the verified requester beside the caller, under a private attribute.
- The request scope binds the requester next to the caller, as a `RequestUserIdentity` with the claim's `team_uids` and `is_organization_admin` false. It expires with the scope, also in copied child contexts. Local mode, WebSockets and public or OPTIONS requests have none.
- `User.get_requester()` returns it, or `None` when the request is not requester-bound. Outside an authenticated request it raises `RequestIdentityError`, as `User.get_logged_user()` does. `get_logged_user()` is unchanged: it returns the caller, the acting application.
- An application authorizes requester-bound REST and MCP reads and writes against the requester's permission for the exact object and action, and gives the acting application no rights of its own unless its policy grants them. A read grant or team membership is not write permission. It fails closed when an operation needs a person and `get_requester()` is `None`, and never takes a person's UID from a request body, header or query parameter. The getter proves identity, not permission, and never enables platform write endpoints. Requester authority remains member-level, without admin powers or Secret values; application-owned authorization checks are necessary for model-driven writes because of prompt-injection risk.
- `reads_as_caller()` raises `RequestIdentityError` as soon as it is entered in a requester-bound request, and never reads as the application instead. A requester-bearing assertion cannot be forwarded to the platform's directory reads; the SDK fails early and says why. Platform-authorized Agent delegation is distinct: the platform preserves the original person and request time, without increasing permissions or restarting the 24-hour maximum. Receiving applications cannot establish that binding themselves, and their request scope still expires with the inbound request.
- SDK releases before this amendment reject every assertion that carries `requester`. Requester-bound calls to an application on such a release fail closed, and its other calls are unchanged.
