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
- an authenticated platform gateway forwards the current request identity
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

After login, the CLI has enough information to authenticate later commands without asking for the password again. On import, the SDK also bootstraps missing access/refresh tokens from that persisted CLI session. Endpoint resolution preserves explicit process values, then uses the checkout `.env` endpoint or saved CLI configuration. This local bootstrap does not authenticate against the network.

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

## Request-bound caller identity

FastAPI applications install the [SDK request identity integration](../fastapi/index.md) once. Handlers and services call `User.get_logged_user()`. The integration verifies the platform assertion and binds a request scope; it never changes process credentials.

The gateway consumes the original release Bearer token. Do not put an inbound token into process `MAINSEQUENCE_ACCESS_TOKEN` or select `session_jwt` for each HTTP caller. The runtime's SDK authentication remains independent.

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

CodeRepository source identity is separate from authentication. The network-free
`get_git_source_context()` reads the actual checkout, attached branch, and HEAD.
`get_code_repository_context()` optionally maps those same facts to a registered
platform branch. Login and User/Organization requests do not require registration.
Missing Environment metadata is allowed during context discovery. Operations that
require an Environment raise if the current branch has none. Runtime credentials
keep their authenticated target scope. See [Git source and Environment context](context.md).

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
- repository and branch context come from Git; Environment context comes from the
  registered branch, with no developer override or fallback
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
| Request-bound access token | request-provided access token | no refresh | FastAPI and explicitly bound request-context code |
| Runtime credential | runtime credential id and secret | exchange credential for new access | long-running non-interactive runtimes |

## Getting The Current User

Authentication and current-user resolution are related, but they are not the same thing.

Use `User.get_logged_user()` when SDK code is running with an SDK-bound identity context:

- code that explicitly binds request headers into the SDK auth context

Use `User.get_authenticated_user_details()` in standalone CLI or script code that is authenticated but not request-bound.

`User.get_logged_user()` returns a UID-only `RequestUserIdentity` for the human
making the current request. It does not return a full `User` account and it does
not identify the release owner or runtime workload principal.

For FastAPI releases, install SDK request identity once and use `User.get_logged_user()` in handlers and services. Request-state fields are compatibility projections of that same identity; resource policy remains application-owned.

The distinction matters because request-bound code resolves the human caller
from the active request identity, while standalone code resolves the account
associated with the process authentication session.
