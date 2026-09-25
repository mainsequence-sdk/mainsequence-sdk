# FastAPI Request Context And Public Ingress

Protected FastAPI routes receive the authenticated human making the current
HTTP request through state injected by the Main Sequence platform. No SDK
authentication setup is required in application code.

Protected route code does not detect whether it is running locally or
deployed, parse authentication headers, or resolve the user itself. It consumes
the same platform-injected request state in every environment.

For protected requests, the result identifies the human making the current
request. It does not identify the deployment owner, release creator, or runtime
workload credential.

## Route Usage

```python
from fastapi import FastAPI, Request

app = FastAPI(title="My API", version="0.1.0")


@app.get("/me")
def get_me(request: Request) -> dict[str, str | None]:
    request_user = request.state.user
    return {
        "uid": request.state.user_uid,
        "username": request_user.username,
    }
```

For protected routes, the Main Sequence platform injects:

- `request.state.user`: a minimal runtime identity containing `uid` and optional
  `username`
- `request.state.user_uid`: the same canonical user UUID as a string

Protected routes use these values identically in local and deployed execution.
They must not inspect authentication headers to select a mode.

There is no `request.state.user_id`. Request identity never uses a numeric
database ID.

The request-user projection describes the human making this request. It is not the
release creator, deployment owner, runtime workload principal, CodeRepositoryBranch,
ResourceRelease, or hostname-selected runtime target. It is intentionally not a
full account profile and has no email, organization, plan, or permission fields.

## Passing Identity To Shared Code

FastAPI route handlers use `request.state.user` or
`request.state.user_uid`. Pass that value explicitly to shared application
services that need the caller. `User.get_logged_user()` is not the FastAPI
entry point, and the SDK no longer installs or exports FastAPI middleware.

Use `User.get_authenticated_user_details()` instead when a standalone CLI,
notebook, or script needs the full user profile associated with its SDK login.
That method calls `/api/v1/users/me/` using the process authentication session;
it does not mean "the human calling this FastAPI endpoint."

## Authorization Boundary

The Main Sequence platform injects authenticated request identity. That
identity alone does not decide whether the user may read, mutate, or administer
a resource.
Every protected endpoint must still perform its resource-level authorization
using `request.state.user_uid` and the authoritative application/backend policy.

## Public Provider Callbacks And Webhooks

An ordinary FastAPI release may expose exact `GET` or `POST` paths to external
OAuth providers or webhook senders through the release's `public_ingress`
policy. A FastAPI decorator or CORS setting alone does not bypass the platform
Bearer check. Declare each literal method/path pair under the `kind: fastapi`
resource's `spec.public_ingress` in `.mainsequence/workflows/`; fetch and
validate the current branch workflow template before committing. The default
empty policy exposes no public paths. Do not put query strings, route
parameters, wildcards, provider secrets, or the reserved
`FASTAPI_PUBLIC_INGRESS` value in that policy.

After the repository event deploys a ready active revision, read the release
detail and confirm the pair is in both `public_ingress` and
`effective_public_ingress`. Use the returned `public_url` plus that literal
path when registering the provider URI. An added pair is private until the
active revision includes it; removing a desired pair denies it immediately.

An admitted request has `request.state.user is None`,
`request.state.user_uid is None`, and
`request.state.auth_outcome == "public_ingress"`. The application must verify
OAuth state or the webhook signature and replay identity before acting. An
unlisted path or wrong method remains protected. See the installed
`mainsequence-command-center-fastapi` skill for the deployment and verification
procedure.
