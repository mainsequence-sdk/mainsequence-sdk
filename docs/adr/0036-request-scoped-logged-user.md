# ADR-0036: Request-scoped logged user

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
