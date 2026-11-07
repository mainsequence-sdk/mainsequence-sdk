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

The CLI's own requests (login, logout, session renewal and the session check of
every authenticated command) use the same default budget when the command sets
no timeout of its own, and are not retried automatically.

The A2A calls `AgentSession.send_a2a_message`, `AgentSession.get_a2a_task` and
`AgentSession.cancel_a2a_task`, and `mainsequence agent session a2a send
--timeout`, apply one such budget to the whole call, response body included: a
runtime that sends its answer slowly cannot extend it. Their default is
`(5, 900)`, 905 seconds in total. Waiting for an Agent runtime that is still
checking, starting, waking or updating spends the same budget: the SDK
re-resolves runtime access when the platform asks, and when the platform's next
retry no longer fits in the budget the call raises `AgentRuntimeNotReadyError`
and nothing is sent to the runtime. So the SDK waits for a waking runtime at
most for the call's `timeout`, or 905 seconds when none is given. The calls
are not retried automatically; the only
resend is the same request, with the same `messageId`, after the runtime rejects
the credentials with `401` or `403`. When the budget runs out the call raises
`requests.Timeout`, and the request may already have reached the runtime. A
budget can only shorten a wait; it cannot extend a platform limit, such as the
platform's 90-second limit for answering a direct A2A Message (see
[ADR 0029](../../adr/0029-a2a-message-send-only.md#message-limit)).

The runtime credential exchange is a `POST` that renews credentials. A `429` or
`503` answer to it exchanged nothing, so it is retried a bounded number of times
within the same budget; see [the exchange](#the-exchange). The workload identity
exchange is retried the same way; see
[its exchange](#the-workload-identity-exchange).

The practical question is not "which class handles auth?" but "where does the access token come from, and what happens when it expires?"

There are four supported functional auth models:

- JWT auth
- request-bound access-token auth
- runtime credential auth
- workload identity auth

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
- the platform injects a runtime credential ID and the projected workload
  identity token file that proves it
- the process should mint short-lived access tokens as needed

Use workload identity auth when:

- a program runs outside Main Sequence, for example a service in your own cloud
  account, a CI job or a scheduler
- its cloud already gives it an identity, such as an Azure managed identity
- it should sign in as its own external workload instead of with a person's
  API token or saved session

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

FastAPI applications install the [SDK request identity integration](../fastapi/index.md) once. Handlers and services call `User.get_logged_user()`. The integration verifies the platform assertion and binds a request scope; it never changes process credentials. On a requester-bound call, made by another application while it works for a person, `User.get_requester()` returns that person; see [Requester-bound calls](../fastapi/index.md#requester-bound-calls).

The gateway consumes the original release Bearer token. Do not put an inbound token into process `MAINSEQUENCE_ACCESS_TOKEN` or select `session_jwt` for each HTTP caller. The runtime's SDK authentication remains independent. Inside `reads_as_caller()`, the directory reads also present the caller's assertion; see [Reading the directory as the caller](../fastapi/index.md#reading-the-directory-as-the-caller).

## Runtime Credential Auth

Runtime credential auth is for non-interactive runtimes that need to obtain
short-lived access tokens from a durable runtime credential. In deployed Main
Sequence workloads, the backend injects this authentication mode and its
credential. It is not a user-facing runtime or branch selector.

The credential is an ID and the projected workload identity token that proves
the runtime's identity. A deployed runtime receives:

```bash
MAINSEQUENCE_AUTH_MODE=runtime_credential
MAINSEQUENCE_RUNTIME_CREDENTIAL_ID=<credential id>
MAINSEQUENCE_RUNTIME_IDENTITY_TOKEN_FILE=/var/run/secrets/mainsequence.io/runtime-identity/token
```

Runtime credential auth requires the projected identity token file. Without
`MAINSEQUENCE_RUNTIME_IDENTITY_TOKEN_FILE`, or with a file that is missing,
unreadable or empty, the exchange fails with an error that names the variable
or the file. Earlier releases could also exchange a bootstrap secret; the SDK no
longer reads `MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET` and never sends it.

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

- the credential ID and the identity token identify the runtime
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
The request body holds exactly `credential_id` and `workload_identity_token`,
the projected Kubernetes ServiceAccount token in the file that
`MAINSEQUENCE_RUNTIME_IDENTITY_TOKEN_FILE` names. The kubelet rotates the file,
so the SDK reads it for every exchange, never once at start-up. The token is the
only proof: an unset variable, or a missing, unreadable or empty file, is an
error that names the variable or the file, raised instead of sending the request.

The token stays inside the exchange request. The SDK does not copy it into an
environment variable, persist it, log it, put it in an error message, or hand it
to application code. Only the access token it is exchanged for is kept, in
`MAINSEQUENCE_ACCESS_TOKEN`. An access token obtained with the identity token
lives 900 seconds, and the SDK exchanges again before it expires.

| Exchange answer | SDK behavior |
| --- | --- |
| `200` | Uses the access token and stores it in `MAINSEQUENCE_ACCESS_TOKEN`. |
| `401` | The token was refused. Fails at once, without a retry. |
| `429` | The exchange was throttled. Retries. |
| `503` | Verification is temporarily unavailable. Retries. |

A retry waits the longer of an exponential backoff (0.5, 1 and 2 seconds) and
the answer's `Retry-After`, at most three times, within the request's timeout
budget. A wait that would not end before the budget does ends the retries, and
the error reports the last answer's status. The exchange the SDK makes at
start-up, to load a job run's start-up state, uses the same token and the same
retries; without a usable token file it sends nothing.

## Workload Identity Auth

Workload identity auth is for a program that Main Sequence did not deploy and
that should call it with its own identity, not with a person's credentials. An
Organization admin creates an **external workload** for it in Command Center and
adds a **federated credential** that names the identity provider's issuer, the
exact subject of the program's identity, an audience dedicated to Main Sequence
and, optionally, claim conditions. The program then proves who it is with a
token from its own identity provider, the *subject token*. No Main Sequence
secret is issued, stored or shipped with the program.

The SDK obtains the subject token, exchanges it for a short-lived Main Sequence
access token and sends that as `Authorization: Bearer <token>`. What the
workload may do is decided by the access its external workload is granted in
Command Center.

### Configuration

```bash
MAINSEQUENCE_ENDPOINT=https://<your Main Sequence address>
MAINSEQUENCE_AUTH_MODE=workload_identity
MAINSEQUENCE_WORKLOAD_IDENTITY_SOURCE=<environment | file | azure_managed_identity>
```

`MAINSEQUENCE_WORKLOAD_IDENTITY_SOURCE` selects where the subject token comes
from. Each source needs one more setting:

| Source | Setting | The subject token is |
| --- | --- | --- |
| `environment` | `MAINSEQUENCE_WORKLOAD_IDENTITY_TOKEN` | the value of that variable, read for every exchange |
| `file` | `MAINSEQUENCE_WORKLOAD_IDENTITY_TOKEN_FILE` | the content of that file, read for every exchange, so a rotated file is picked up |
| `azure_managed_identity` | `MAINSEQUENCE_WORKLOAD_IDENTITY_AUDIENCE`, and `MAINSEQUENCE_WORKLOAD_IDENTITY_AZURE_CLIENT_ID` for a user-assigned identity | a token the host's Azure managed identity issues for that audience |

The `azure_managed_identity` source asks the managed identity endpoint that
Azure Container Apps, App Service and Functions announce in `IDENTITY_ENDPOINT`
and `IDENTITY_HEADER`. Without those two variables, for example on an Azure
virtual machine, it asks the instance metadata service. Without
`MAINSEQUENCE_WORKLOAD_IDENTITY_AZURE_CLIENT_ID` the system-assigned identity
answers.

A missing or unusable setting is an error that names it, raised before
anything is sent to Main Sequence. In this mode the SDK does not use
`MAINSEQUENCE_ACCESS_TOKEN`, `MAINSEQUENCE_REFRESH_TOKEN` or a saved CLI
session, even when they are present. JWT auth remains the default mode, and
runtime credential auth is unchanged.

### Your own subject token source

Any callable that takes no argument and returns the subject token as a string
can be the source, for example one that asks another cloud's identity service:

```python
from mainsequence.client.utils import loaders


def subject_token() -> str:
    return my_identity_client.get_token("api://main-sequence")


loaders.use_workload_identity(subject_token)
```

The process must still run with `MAINSEQUENCE_AUTH_MODE=workload_identity`;
`use_workload_identity()` raises `AuthError` otherwise. The source is called
once per exchange, about every 15 minutes. `EnvironmentSubjectToken`,
`FileSubjectToken` and `AzureManagedIdentitySubjectToken` in
`mainsequence.workload_identity` are the built-in sources.

### The workload identity exchange

The SDK posts the subject token to `POST /api/v1/workload-identity/token/` as an
[RFC 8693](https://www.rfc-editor.org/rfc/rfc8693) token exchange, form encoded:

```text
grant_type          urn:ietf:params:oauth:grant-type:token-exchange
subject_token       <the subject token>
subject_token_type  urn:ietf:params:oauth:token-type:jwt
```

and uses the `access_token` of the answer. That access token lasts
`expires_in` seconds (900) and cannot be refreshed: 30 seconds before it
expires, the next request makes a new exchange with a new subject token. A
read-only request rejected with `401`, for example because the credential was
disabled, exchanges again and replays once, within its remaining timeout; a
write rejected with `401` is not replayed.

| Exchange answer | SDK behavior |
| --- | --- |
| `200` | Uses `access_token` as the Bearer until shortly before `expires_in` runs out. |
| `401` | The subject token was not accepted. Fails at once with `AuthError`, without asking the source for another token. |
| `429` | The exchange was throttled. Retries. |
| `503` | Verification is temporarily unavailable. Retries. |

A retry waits the longer of an exponential backoff (0.5, 1 and 2 seconds) and
the answer's `Retry-After`, at most three times, within the request's timeout
budget, and reuses the same subject token.

A `401` never says which check failed. Compare the claims of the subject token
with the federated credential: `iss` with the issuer, `sub` with the subject,
`aud` with the audience, and every claim condition. The credential must be
enabled and its external workload active.

Neither token is logged, put in an error message or written to the
environment. The access token is kept only in the memory of the process: a
child process does not inherit it and makes its own exchange. The `mainsequence`
CLI does not sign in with workload identity; use it from the SDK client in your
program.

### Example: a service on Azure Container Apps

A backend on Azure Container Apps that today calls Main Sequence with a
person's credentials can use its managed identity instead.

1. **Give the container app a managed identity.** A system-assigned one:

    ```bash
    az containerapp identity assign --name my-backend --resource-group my-rg --system-assigned
    ```

    Note the `principalId` it prints: the object ID of the identity, which is
    the `sub` of its tokens. For a user-assigned identity, also note its
    client ID.

2. **Choose an audience dedicated to Main Sequence.** In Microsoft Entra ID,
    register an application, for example "Main Sequence", and give it an
    Application ID URI such as `api://main-sequence`. It needs no secret, no
    certificate and no API permissions; it only names the audience. Do not use
    a generic audience such as `api://AzureADTokenExchange`: Main Sequence
    refuses it.

3. **Read the claims of one token** from inside the container app, to fill in
    the credential exactly. This prints the claims, not the token:

    ```python
    import base64
    import json

    from mainsequence.workload_identity import AzureManagedIdentitySubjectToken

    token = AzureManagedIdentitySubjectToken(audience="api://main-sequence")()
    payload = token.split(".")[1]
    claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    print({name: claims.get(name) for name in ("iss", "sub", "aud", "tid", "oid")})
    ```

4. **Create the external workload in Command Center.** An Organization admin
    creates it in the Environment the backend works in and adds a federated
    credential: provider Microsoft Entra ID, the issuer (`iss`), the subject
    (`sub`, the managed identity's object ID), the audience (`aud`), and
    optionally the condition `tid` for your tenant. Then grant the workload the
    access the backend needs.

5. **Configure the container app:**

    ```bash
    az containerapp update --name my-backend --resource-group my-rg \
      --set-env-vars \
        MAINSEQUENCE_ENDPOINT=https://<your Main Sequence address> \
        MAINSEQUENCE_AUTH_MODE=workload_identity \
        MAINSEQUENCE_WORKLOAD_IDENTITY_SOURCE=azure_managed_identity \
        MAINSEQUENCE_WORKLOAD_IDENTITY_AUDIENCE=api://main-sequence
    ```

    For a user-assigned identity, also set
    `MAINSEQUENCE_WORKLOAD_IDENTITY_AZURE_CLIENT_ID=<its client ID>`.

6. **Remove the person's credentials** from the container app
   (`MAINSEQUENCE_ACCESS_TOKEN`, `MAINSEQUENCE_REFRESH_TOKEN`, or the secret
   that held them), and revoke that API token.

The application code does not change: the SDK client exchanges the managed
identity's token on the first request and again whenever the access token is
about to expire.

## Auth Mode Summary

| Mode | Main inputs | Refresh behavior | Best for |
| --- | --- | --- | --- |
| JWT via CLI | `mainsequence login` credentials | refresh token renews access | local CLI and developer scripts |
| JWT via environment | `MAINSEQUENCE_ACCESS_TOKEN` and `MAINSEQUENCE_REFRESH_TOKEN` | refresh token renews access | signed terminals and controlled launches |
| Request-bound access token | request-provided access token | no refresh | FastAPI and explicitly bound request-context code |
| Runtime credential | runtime credential ID and its identity token file | exchange credential for new access | long-running non-interactive runtimes |
| Workload identity | a subject token source and its setting | exchange a new subject token for new access | programs outside Main Sequence with a cloud identity |

## Credentials In Logs And Errors

The SDK objects that hold a credential leave it out of `repr()` and `str()`, so
printing, formatting or logging one does not show it:

| Object | Left out |
| --- | --- |
| `JWTAuthProvider`, `SessionJWTAuthProvider` | `access_token`, `refresh_token` |
| `WorkloadIdentityAuthProvider` | `access_token`, `subject_token_source` |
| `AgentSessionRuntimeAccess` | `token` |
| `ResourceReleaseRuntimeAccess` | `access`, which holds the runtime token |
| `mainsequence.cli.browser_auth.BrowserAuthCallback` | `code`, the authorization code |

`RuntimeCredentialAuthProvider` holds no credential to leave out: it reads the
identity token for each exchange and keeps only the file's path.

A validation error raised while reading `AgentSessionRuntimeAccess`,
`ResourceReleaseRuntimeAccess` or `Secret` does not echo its input either. The
attributes still hold the values, and `model_dump()` still includes them: read
them only where a request needs them, and do not log what they return.

## Getting The Current User

Authentication and current-user resolution are related, but they are not the same thing.

Use `User.get_logged_user()` when SDK code is running with an SDK-bound identity context:

- code that explicitly binds request headers into the SDK auth context

Use `User.get_authenticated_user_details()` in standalone CLI or script code that is authenticated but not request-bound.

`User.get_logged_user()` returns a UID-only `RequestUserIdentity` for the caller
of the current request, a person or a workload identity. It does not return a
full `User` account and it does not identify the release owner or the
application's own runtime workload principal. `User.get_by_uid(uid)` reads the
caller's User; see
[Workload identities](users_and_access.md#workload-identities).

`User.get_requester()` returns the requester of a requester-bound request, the
person the calling application works for, as the same type with the person's
own `is_organization_admin` and `in_teams()` for its teams, or `None` when the
request is not
requester-bound. Authorize reads and writes against that person's ordinary
permissions, including administrative ones, for the exact object and
operation; the getter verifies identity, not permission. Never substitute the
Agent's grants for the person's or add them to the person's. A handler or tool
that needs a person fails with its own error when there is none; one where the
person is optional authorizes against the person when present and the caller
otherwise. Never take a person's UID from a request body, header or query
parameter. See
[Requester-bound calls](../fastapi/index.md#requester-bound-calls) for MCP tools
and [requester access and write risk](users_and_access.md#applications-that-act-for-their-requester).

For FastAPI releases, install SDK request identity once and use `User.get_logged_user()` in handlers and services. Request-state fields are compatibility projections of that same identity; resource policy remains application-owned.

The distinction matters because request-bound code resolves the caller
from the active request identity, while standalone code resolves the account
associated with the process authentication session.
