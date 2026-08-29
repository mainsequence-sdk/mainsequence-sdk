# Authentication

Main Sequence SDK authentication is based on bearer access tokens.

The practical question is not "which class handles auth?" but "where does the access token come from, and what happens when it expires?"

There are three supported functional auth models:

- JWT auth
- request-bound access-token auth
- runtime credential auth

JWT auth can be supplied by the CLI's persisted login state or directly through environment variables.

`MAINSEQUENCE_TOKEN` is not supported.

## Quick Decision Rule

Use JWT auth when:

- a developer is running `mainsequence` commands locally
- a script is running in a normal authenticated shell
- the process can use the credentials produced by `mainsequence login`
- a controlled launcher injects `MAINSEQUENCE_ACCESS_TOKEN` and `MAINSEQUENCE_REFRESH_TOKEN`

Use request-bound access-token auth when:

- code is running inside an already authenticated request context
- a platform dashboard or API forwards the current request identity
- there is an access token for this request, but no refresh token

Use runtime credential auth when:

- a long-running runtime needs to authenticate without a user login prompt
- the backend launcher injects a runtime credential id and secret
- the process should mint short-lived access tokens as needed

## JWT Auth

JWT auth is the normal authenticated-user model.

It has two common delivery paths:

- CLI-managed login state
- environment variables

Both use the same functional token model:

- an access token authenticates API requests
- a refresh token can renew the access token
- the request is sent as `Authorization: Bearer <access token>`

## CLI-Managed JWT Auth

CLI-managed JWT auth is the normal local developer mode.

The user signs in with:

```bash
mainsequence login
```

After login, the CLI has enough information to authenticate later commands without asking for the password again.

Functionally:

- the access token is sent as `Authorization: Bearer <token>`
- the refresh token is used to obtain a new access token when needed
- SDK and CLI calls can continue after the original access token expires

Use this for:

- local CLI commands
- local scripts launched from an authenticated environment
- development workflows where a human user signs in

### MCP-Assisted CLI Login

When a coding agent already has an authenticated Main Sequence MCP connection
but the local CLI has no session, run:

```bash
mainsequence login --mcp
```

The CLI generates PKCE state, verifier, and challenge. It sends only the state
and challenge to the configured Main Sequence backend. The backend creates the
short-lived handoff and returns its exact callback URI and the
`auth.cli_authorize` invocation. The CLI never creates or submits a redirect
URI in this flow.

Call the printed MCP tool with only its `handoff_uid` while the command remains
running. After MCP binds the handoff to its authenticated principal, the CLI
exchanges its private verifier at the backend callback. The normal tracked JWT
pair returns directly to the CLI and is persisted through existing CLI auth
storage. No bearer, authorization code, access token, refresh token, or PKCE
verifier is returned by the MCP tool.

Do not combine `--mcp` with `--export` or manual token arguments. A process
using `MAINSEQUENCE_AUTH_MODE=runtime_credential` already has a noninteractive
authentication lane; the CLI rejects `--mcp` in that mode, so run ordinary
`mainsequence login` instead.

If a local shell, IDE, or subprocess cannot see auth credentials, refresh or export them with the CLI login flow used by your environment.

## Environment JWT Auth

Some processes receive JWT tokens through environment variables:

```bash
MAINSEQUENCE_AUTH_MODE=jwt
MAINSEQUENCE_ACCESS_TOKEN=<jwt access token>
MAINSEQUENCE_REFRESH_TOKEN=<jwt refresh token>
```

Functionally this is the same token model as CLI-managed JWT auth:

- `MAINSEQUENCE_ACCESS_TOKEN` is used for bearer requests
- `MAINSEQUENCE_REFRESH_TOKEN` allows the SDK to obtain a fresh access token
- the process can survive access-token expiration as long as the refresh token remains valid

This mode is useful when a launcher, signed terminal, or controlled runtime injects tokens into the environment instead of relying on persisted CLI storage.

## Request-Bound Access-Token Auth

Request-bound access-token auth is for code running inside an authenticated platform request.

In this mode, the runtime already has the identity for the current request. The SDK should use that request's access token to make backend calls as the same user.

Functionally:

- the access token belongs to the current request context
- the token is used as `Authorization: Bearer <token>`
- there is no refresh token
- if the request token expires or is rejected, the request should fail instead of silently becoming a different identity

When this is configured explicitly for a process, use:

```bash
MAINSEQUENCE_AUTH_MODE=session_jwt
MAINSEQUENCE_ACCESS_TOKEN=<request or session access token>
```

Use this for:

- FastAPI request handlers running behind the platform
- Streamlit apps running with platform-provided user context
- code that explicitly binds request headers into the SDK auth context

Do not use this mode for standalone scripts that need to run independently for a long time.

## Runtime Credential Auth

Runtime credential auth is for non-interactive runtimes that need to obtain
short-lived access tokens from a durable runtime credential. In deployed Main
Sequence workloads, the backend injects this authentication mode and its
credential. It is not a user-facing runtime or branch selector.

The backend launcher supplies:

```bash
MAINSEQUENCE_AUTH_MODE=runtime_credential
MAINSEQUENCE_RUNTIME_CREDENTIAL_ID=<credential id>
MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET=<credential secret>
```

Inside that already provisioned runtime, explicitly perform the exchange with:

```bash
mainsequence login
```

In runtime credential mode, `mainsequence login` does not open browser login and
does not persist CLI JWT refresh tokens. It exchanges the backend-injected
runtime credential and stores the returned access token in
`MAINSEQUENCE_ACCESS_TOKEN` for that process.

If the parent shell needs the exchanged token, use:

```bash
eval "$(mainsequence login --export)"
```

Local CodeRepository provisioning is also runtime-credential aware:

```bash
mainsequence code-repository set-up-locally <CODE_REPOSITORY_UID>
mainsequence code-repository refresh-token --path .
```

When an already authenticated coding-agent runtime uses
`MAINSEQUENCE_AUTH_MODE=runtime_credential`, these commands preserve the
backend-injected runtime credential auth shape in the CodeRepository `.env`:

```bash
MAINSEQUENCE_AUTH_MODE=runtime_credential
MAINSEQUENCE_ACCESS_TOKEN=<exchanged short-lived access token>
MAINSEQUENCE_RUNTIME_CREDENTIAL_ID=<credential id>
MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET=<credential secret>
MAINSEQUENCE_ENDPOINT=<platform API origin>
```

They do not require or write `MAINSEQUENCE_REFRESH_TOKEN` in runtime credential mode.
Both local CodeRepository commands preserve unrelated `.env` entries while rendering
the current supported authentication shape. They remove obsolete token aliases
and superseded repository, branch, and Environment identity
entries.

CodeRepository source identity is separate from authentication. In local and deployed
code repository images, the SDK reads the containing sanitized Git checkout, attached
branch, and exact HEAD commit, then maps that source to CodeRepository and
CodeRepositoryBranch through the platform API. Switching branches does not rewrite
credentials and takes effect in the next process. Runtime credentials authorize
the deployed target but do not select repository or branch identity.

Functionally:

- the credential id and secret identify the runtime
- the SDK exchanges that credential for a short-lived JWT access token
- the returned access token is used as `Authorization: Bearer <token>`
- the returned access token is stored in `MAINSEQUENCE_ACCESS_TOKEN` for the current process environment
- child processes launched after the exchange can inherit `MAINSEQUENCE_ACCESS_TOKEN`
- when the access token is missing, near expiry, expired, or rejected with `401`, the SDK exchanges the runtime credential again

Runtime credential auth behaves like JWT access-only auth for normal requests. The difference is how a new access token is obtained.

Important constraints:

- `MAINSEQUENCE_REFRESH_TOKEN` is not used in this mode
- runtime credential mode wins when `MAINSEQUENCE_AUTH_MODE=runtime_credential`
- the exchanged access token should be treated as short-lived runtime material
- CodeRepository `.env` files may contain runtime credential material; keep `.env` out of version control
- users and application code never set CodeRepository, CodeRepositoryBranch, repository
  branch, or Organization Environment values to choose context
- deployed branch-owned SDK requests carry the Git-resolved CodeRepositoryBranch; the
  backend requires equality with the authenticated JobRun, CodeRepository Executor, or
  ResourceRelease target
- a genuine local checkout may select a Git branch, but the SDK resolves its
  persisted CodeRepositoryBranch internally and never treats that local choice as a
  deployed runtime authority

Use this for:

- runtime jobs
- service-like processes
- long-running workers that cannot depend on a human login session

## Auth Mode Summary

| Mode | Main inputs | Refresh behavior | Best for |
| --- | --- | --- | --- |
| JWT via CLI | `mainsequence login` credentials | refresh token renews access | local CLI and developer scripts |
| JWT via environment | `MAINSEQUENCE_ACCESS_TOKEN` and `MAINSEQUENCE_REFRESH_TOKEN` | refresh token renews access | signed terminals and controlled launches |
| Request-bound access token | request-provided access token | no refresh | FastAPI, Streamlit, request-context code |
| Runtime credential | runtime credential id and secret | exchange credential for new access | long-running non-interactive runtimes |

## Getting The Current User

Authentication and current-user resolution are related, but they are not the same thing.

Use `User.get_logged_user()` when SDK code is running with an SDK-bound identity context:

- Streamlit
- code that explicitly binds request headers into the SDK auth context

Use `User.get_authenticated_user_details()` in standalone CLI or script code that is authenticated but not request-bound.

`User.get_logged_user()` returns a UID-only `RequestUserIdentity` for the human
making the current request. It does not return a full `User` account and it does
not identify the release owner or runtime workload principal.

For FastAPI releases, the Main Sequence platform injects the authenticated
human into `request.state.user` and `request.state.user_uid`. Handlers read that
state and pass the identity explicitly to shared code. Do not use
`User.get_logged_user()` as the FastAPI handler entry point; route-level
authorization remains application-owned.

The distinction matters because request-bound code resolves the human caller
from the active request identity, while standalone code resolves the account
associated with the process authentication session.
