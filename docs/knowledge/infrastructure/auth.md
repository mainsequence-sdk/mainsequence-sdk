# Authentication

Main Sequence SDK authentication is based on bearer access tokens.

## Shared Client Request Budgets And Retries

The shared SDK client automatically retries only `GET`, `HEAD`, and `OPTIONS`.
The default is at most three retries, with exponential backoff, for transient
connection/read failures and HTTP `429`, `500`, `502`, `503`, and `504` responses.
`POST`, `PUT`, `PATCH`, and `DELETE` are sent once: connection failures, timeouts,
server errors, and authentication rejection never trigger automatic replay.
A timed-out write may still be running on the server; check its outcome before
explicitly retrying, and preserve any operation-specific idempotency identity.

A numeric `timeout` is one total budget in seconds, not a fresh timeout per
attempt. A `(connect, read)` tuple retains those phase limits and has a total
budget equal to their sum. The default `(5, 120)` therefore allows at most
125 seconds across attempts and backoff. Each attempt receives only the remaining
budget. A `Retry-After` delay that does not fit ends retries rather than sleeping
past the deadline.

Authentication renewal before sending uses the same budget. A rejected read-only
request may renew credentials and replay once within that budget; a rejected
write is returned to the caller without replay. There is no second outer retry
loop. These are synchronous HTTP transport limits, not cancellation of work
already accepted by the server or a deadline on consuming a streaming response.

The runtime credential exchange is a `POST` that renews credentials. A `429` or
`503` answer to it exchanged nothing, so it is retried a bounded number of times
within the same budget; see [the exchange](#the-exchange).

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
- the platform injects a runtime credential ID with its proof: a projected
  workload identity token file or a bootstrap secret
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

Persistent CLI credentials use the operating system credential store, one
record per backend:

| System | Store | How it is reached |
| --- | --- | --- |
| macOS | Login Keychain | Apple's `security` program, with the secret on standard input |
| Linux | Secret Service | The `keyring` library's Secret Service backend, named explicitly |
| Windows | Credential Manager | The `keyring` library's recommended backend |

On macOS the Keychain grants access per program. An entry written through the
Security framework belongs to the interpreter that wrote it, and every other
interpreter build, including the same one after an upgrade, waits on a consent
dialog when it reads that entry. `security` is one program for every
interpreter, so a login made from one CodeRepository is read from another
without a dialog. The cost is that any program of the same user can read the
entry the same way.

The CLI marks every Keychain entry it writes, and asks for the secret only of
an entry that carries its mark. Asking `security` for an entry that another
program wrote would show the consent dialog in every process, so such an entry
is never asked for. A session that another version of the CLI saved is
therefore not read: `mainsequence doctor` and `mainsequence auth status` say
so, and one `mainsequence login` replaces it. A released version installed in
another CodeRepository reads the new entry and keeps the mark when it updates
it, so the two share the session.

When the Keychain is locked, the CLI waits at most 10 seconds and continues
without a saved session, and the same two commands say that the credential
store could not be read.

Linux requires an available, unlocked desktop keyring that implements Secret
Service. If no store is available, login remains valid only for the current
process and the CLI does not fall back to a plaintext token file. Existing
`auth.json` credentials are migrated and removed only after secure-store write
and readback succeed.

The record is ASCII JSON with a version, the backend it belongs to, the
username, and the tokens. A record with a refresh token and no access token is
a complete session: the access token is renewed from it. A record that names
another backend is refused. See
[ADR 0037](../../adr/0037-machine-session-in-the-os-credential-store.md).

A CodeRepository `.env` holds the backend endpoint and no credential. The CLI
does not write a token there. `mainsequence refresh-token` renews the saved
session from any directory, and removes a credential that an earlier version
or another tool left in the `.env` of the directory it runs in.

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

### Other Local Tools

A local tool that does not read the credential store itself obtains a
short-lived access token from the CLI:

```bash
mainsequence auth token --json
```

The refresh token never leaves the store through this command. The output and
exit codes are in the [CLI reference](../../cli/index.md#handing-the-session-to-another-local-tool).
`mainsequence auth status` reports the session without any token value.

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

Credentials already in the process environment win over the saved CLI session.
When the backend refuses to renew that pair, the SDK does not try the saved
session, because the pair may belong to another user or backend. The error
instead says, without any token value:

- the backend, and whether the refresh token expired (with its date) or was
  refused before its expiry, which means it was revoked or issued by another
  backend;
- where the pair came from: the working directory's `.env` when it holds those
  tokens, which an IDE run configuration or a launcher loaded into the process,
  otherwise the program that started the process;
- the saved session of that backend: its user and until when it is usable, or
  that there is none;
- the repair. With a usable saved session, run `mainsequence refresh-token` in
  that directory, which removes the token lines, or start the process without
  the two variables; the next start uses the saved session. Without one, sign
  in first with `mainsequence login` (another backend needs its address and
  base folder).

A renewal that fails for another reason than a refusal (any answer other than
`400`, `401` or `403`) is reported as a backend failure, not as a credential
problem.

## Request-bound caller identity

FastAPI applications install the [SDK request identity integration](../fastapi/index.md) once. Handlers and services call `User.get_logged_user()`. The integration verifies the platform assertion and binds a request scope; it never changes process credentials.

The gateway consumes the original release Bearer token. Do not put an inbound token into process `MAINSEQUENCE_ACCESS_TOKEN` or select `session_jwt` for each HTTP caller. The runtime's SDK authentication remains independent.

## Runtime Credential Auth

Runtime credential auth is for non-interactive runtimes that need to obtain
short-lived access tokens from a durable runtime credential. In deployed Main
Sequence workloads, the backend injects this authentication mode and its
credential. It is not a user-facing runtime or branch selector.

The credential is an ID and one proof of the runtime's identity. A runtime
deployed with a projected workload identity token receives:

```bash
MAINSEQUENCE_AUTH_MODE=runtime_credential
MAINSEQUENCE_RUNTIME_CREDENTIAL_ID=<credential id>
MAINSEQUENCE_RUNTIME_IDENTITY_TOKEN_FILE=/var/run/secrets/mainsequence.io/runtime-identity/token
```

A runtime deployed with a bootstrap secret receives:

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

- the credential ID and its proof identify the runtime
- the SDK exchanges that credential for a short-lived JWT access token
- the returned access token is used as `Authorization: Bearer <token>`
- the returned access token is stored in `MAINSEQUENCE_ACCESS_TOKEN` for the current process environment
- child processes launched after the exchange can inherit `MAINSEQUENCE_ACCESS_TOKEN`
- when the access token is missing, near expiry, or expired, the SDK exchanges
  the runtime credential before sending the request; a read-only request rejected
  with `401` may exchange again and replay once, within its remaining timeout
- a write rejected with `401` is not automatically replayed

Runtime credential auth behaves like JWT access-only auth for normal requests. The difference is how a new access token is obtained.

Important constraints:

- `MAINSEQUENCE_REFRESH_TOKEN` is not used in this mode
- runtime credential mode wins when `MAINSEQUENCE_AUTH_MODE=runtime_credential`
- the exchanged access token should be treated as short-lived runtime material
- the CLI does not write the runtime credential or an exchanged token into a CodeRepository `.env`; a `.env` written by an earlier version may still contain them, so keep `.env` out of version control and remove them by running `mainsequence refresh-token` in that checkout
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

### The Exchange

The SDK exchanges the credential at `POST /api/v1/runtime-credentials/token/`.
The request carries `credential_id` and exactly one proof:

- With `MAINSEQUENCE_RUNTIME_IDENTITY_TOKEN_FILE` set, the proof is
  `workload_identity_token`: the projected Kubernetes ServiceAccount token in
  that file. The kubelet rotates the file, so the SDK reads it for every
  exchange, never once at start-up. It never reads or sends
  `MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET` in this mode. A missing, unreadable or
  empty file is an error that names the file; the SDK does not fall back to the
  secret.
- Without it, the proof is `credential_secret`, the bootstrap secret, exactly as
  in earlier releases.

The token stays inside the exchange request. The SDK does not copy it into an
environment variable, persist it, log it, put it in an error message, or hand it
to application code. Only the access token it is exchanged for is kept, in
`MAINSEQUENCE_ACCESS_TOKEN`. An access token obtained with the identity token
lives 900 seconds, and the SDK exchanges again before it expires.

| Exchange answer | SDK behavior |
| --- | --- |
| `200` | Uses the access token and stores it in `MAINSEQUENCE_ACCESS_TOKEN`. |
| `401` | The proof was refused. Fails at once, without a retry and without trying another proof. |
| `429` | The exchange was throttled. Retries. |
| `503` | Verification is temporarily unavailable. Retries. |

A retry waits the longer of an exponential backoff (0.5, 1 and 2 seconds) and
the answer's `Retry-After`, at most three times, within the request's timeout
budget. A wait that would not end before the budget does ends the retries, and
the error reports the last answer's status. The exchange the SDK makes at
start-up, to load a job run's start-up state, uses the same proof and the same
retries.

## Auth Mode Summary

| Mode | Main inputs | Refresh behavior | Best for |
| --- | --- | --- | --- |
| JWT via CLI | `mainsequence login` credentials | refresh token renews access | local CLI and developer scripts |
| JWT via environment | `MAINSEQUENCE_ACCESS_TOKEN` and `MAINSEQUENCE_REFRESH_TOKEN` | refresh token renews access | signed terminals and controlled launches |
| Request-bound access token | request-provided access token | no refresh | FastAPI and explicitly bound request-context code |
| Runtime credential | runtime credential ID with an identity token file or a secret | exchange credential for new access | long-running non-interactive runtimes |

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
