# ADR-0036: Request-scoped logged user

Amended 2026-10-07 for [SDK issue #208](https://github.com/mainsequence-sdk/mainsequence-sdk/issues/208): one delegation rule. A requester-bound call carries the person's ordinary permissions, including administrative ones. The platform signs `requester` as exactly `sub`, `team_uids` and `is_organization_admin`, the person's own facts, and every assertion carries the caller's `team_uids` and `is_organization_admin`. The verifier rejects any other shape, including the two-field requester and an assertion without the caller's facts; there is no compatibility path or deprecation period. The request scope binds the person's admin flag, so `User.get_requester()` returns it, and each handler or tool decides whether it needs a person. SDK releases up to 9.0.17 reject the new requester claim, and an SDK with this amendment works only with a platform that signs it. See [Requester-bound calls](#requester-bound-calls).

Amended 2026-10-07 for [SDK issue #206](https://github.com/mainsequence-sdk/mainsequence-sdk/issues/206): requester-bound identity is not a read-only permission. Application REST handlers and MCP tools authorize each read or write against the verified person's object- and operation-specific rights. The signed claim shape, getters, request lifetime and directory-read helper are unchanged; platform write opt-ins and platform-authorized Agent delegation do not give this SDK a forwarding or impersonation API. See the [access guide](../knowledge/infrastructure/users_and_access.md#applications-that-act-for-their-requester) for the coordinated platform rollout and write risk.

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

Added 2026-10-06; amended 2026-10-07 for [#208](https://github.com/mainsequence-sdk/mainsequence-sdk/issues/208).

The platform applies one rule to every call an Agent makes for its work: without a delegation, the caller's own ordinary permissions; with a valid delegation, the person's ordinary permissions, including administrative roles; an invalid or expired delegation is rejected and never switched to another identity.

- Every assertion carries the caller's facts: `team_uids`, the caller's active team UIDs, canonical, sorted ascending and unique, and `is_organization_admin`, a boolean. The assertion of a requester-bound call carries one more claim, `requester`: a JSON object with exactly `sub`, the requester's User UID as a canonical lowercase UUID, `team_uids`, the requester's team UIDs in the same form, and `is_organization_admin`, the requester's own Organization-admin flag as a boolean. The assertion's own `sub`, `team_uids` and `is_organization_admin` stay the caller's: the acting application.
- The verifier accepts the base claims with the caller's facts and the optional `requester` claim, and nothing else. An assertion without `team_uids` or `is_organization_admin`, or a `requester` with a missing or extra key, a value of the wrong type, an admin flag that is not a boolean, a UID that is not canonical, or unsorted or repeated team UIDs rejects the whole assertion, so the request gets 401 before any handler or tool runs. The two-field requester of earlier releases, `sub` and `team_uids`, is one of these shapes. The verifier's result keeps the verified requester, with its admin flag, beside the caller, under a private attribute.
- The request scope binds the requester next to the caller, as a `RequestUserIdentity` with the claim's `team_uids` and `is_organization_admin`. It expires with the scope, also in copied child contexts. Local mode, WebSockets and public or OPTIONS requests have none.
- `User.get_requester()` returns it, the checked delegation, or `None` when the request is not requester-bound. Outside an authenticated request it raises `RequestIdentityError`, as `User.get_logged_user()` does. `get_logged_user()` is unchanged: it returns the caller, the acting application.
- Each handler or tool decides whether it needs a person. One that needs a person fails with its own error when `get_requester()` is `None`. One where the person is optional authorizes against `get_requester()` when present, and against `get_logged_user()` otherwise. None parses assertions, headers or MCP `_meta`, takes a person's UID from a request body, header or query parameter, or combines the caller's grants with the person's. The SDK keeps no list of which handlers or tools accept a person.
- An application authorizes requester-bound REST and MCP reads and writes against the person's ordinary permissions, including administrative ones, for the exact object and action. A read grant or team membership is not write permission. The getter proves identity, not permission, and never enables platform endpoints. Application-owned authorization checks are necessary for model-driven writes because of prompt-injection risk.
- `reads_as_caller()` raises `RequestIdentityError` as soon as it is entered in a requester-bound request, and never reads as the application instead. A requester-bearing assertion cannot be forwarded to the platform's directory reads; the SDK fails early and says why. Platform-authorized Agent delegation is distinct: the platform preserves the original person and request time, without increasing permissions or restarting the 24-hour maximum. Receiving applications cannot establish that binding themselves, and their request scope still expires with the inbound request.
- SDK releases up to 9.0.17 reject the requester claim the platform now signs: up to 9.0.14 every `requester` claim, and 9.0.15 to 9.0.17 any but the two-field one. Requester-bound calls to an application on such a release fail closed with 401, and its other calls are unchanged. From this amendment on, the SDK accepts only the shapes above, so it works only with a platform that signs them; there is no deprecation period.
