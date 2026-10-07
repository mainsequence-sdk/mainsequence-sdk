---
name: mainsequence-command-center-fastapi
description: Build, contract-test, release, and verify a FastAPI code repository resource serving the Command Center frontend, including applications with public OAuth callback or webhook routes. Use when implementing a Command Center wire contract, declaring exact public ingress, or validating the deployed API.
---

# Command Center FastAPI Release Lifecycle

## Goal

Build a FastAPI provider that the Command Center frontend can use, prove that
its Command Center contract-bearing payloads conform to the selected Command
Center SDK repository revision, and release the exact tested commit through the
Main Sequence platform.

This skill coordinates the provider lifecycle. The canonical
[`mainsequence-sdk/command-center-sdk`](https://github.com/mainsequence-sdk/command-center-sdk)
GitHub repository is authoritative for every endpoint that adopts a Command
Center wire contract.

## Contract Authority

Apply the Command Center SDK default per endpoint, not to the whole API.

- When an endpoint is being created to serve a frontend, first treat the pinned
  Command Center SDK repository contracts as the default unless the user
  explicitly names another frontend or contract authority.
- Classify every endpoint independently. A single FastAPI application can mix
  Command Center-facing routes with health, internal, administrative, webhook,
  external-integration, and backend-to-backend routes.
- Do not force non-frontend endpoints into Command Center contracts merely
  because they share an application with frontend-serving routes.
- Do not force every frontend operation into one generic envelope. Select the
  exact manifest contract whose role and semantics match that endpoint.

For each endpoint classified as Command Center contract-bearing, before
designing Pydantic models or route serialization:

1. Select a branch, tag, or commit from
   `https://github.com/mainsequence-sdk/command-center-sdk`. Use `main` unless
   the user or consuming frontend requires a specific compatible revision, and
   record the resolved commit SHA.
2. Open `command-center-sdk/contracts/manifest.json` at that exact revision:
   `https://github.com/mainsequence-sdk/command-center-sdk/blob/<revision>/command-center-sdk/contracts/manifest.json`.
3. Select the exact contract by contract ID and role.
4. Load the manifest-referenced draft-2020-12 JSON Schema and every indexed
   valid and invalid fixture.
5. Load the corresponding contract skill from
   `command-center-sdk/agent_scaffold/skills/contracts/` at the same revision
   when one exists. Otherwise use `implement-command-center-contract` there.

The manifest, referenced schema, and indexed fixtures define the wire format.
TypeScript declarations and human documentation explain the contract but do not
replace that language-neutral definition.

Do not install the Node package in the Python API environment to discover or
validate these contracts. Read them from a checkout or GitHub URLs pinned to the
resolved repository commit.

Do not copy schemas, fixture payloads, contract IDs, or field inventories into
this skill or into Main Sequence SDK client models. Generate or maintain
provider-side Python models against the selected repository schema and validate
serialized responses at the HTTP boundary.

If no published contract applies to an application-specific frontend endpoint,
record that decision and define an explicit application-owned route contract.
Do not mislabel it as a Command Center SDK contract or distort a nearby schema
to make it fit.

If the endpoint claims an existing Command Center contract but that contract
cannot express the required behavior, stop that endpoint implementation and
produce a Command Center SDK handoff containing:

- Command Center SDK repository commit SHA
- contract ID and schema `$id`
- rejected payload or failing fixture
- exact missing capability or compatibility requirement

Do not invent a local wire extension while waiting for a contract change.

## Lifecycle

### 1. Establish the API boundary

Record:

- the Command Center frontend flow consuming the API
- the per-endpoint classification: frontend-facing, health, internal,
  administrative, webhook, external integration, or backend-to-backend
- the exact contract ID and role for each request or response body
- route paths, HTTP methods, authentication expectations, and error semantics
- upstream domain-package, service, or external data dependencies
- whether the API uses backend transport or a contract-defined direct
  development transport

Install the SDK's request-identity integration once when creating the FastAPI
application. Inspect the existing application factory first: the generated
platform template already calls `install_request_identity(app)`, and a second
installation raises an error. For an application without that setup:

```python
from fastapi import FastAPI
from mainsequence.client import User
from mainsequence.server.fastapi import install_request_identity


app = FastAPI()
install_request_identity(app)


@app.get("/me")
def get_me() -> dict[str, str | None]:
    user = User.get_logged_user()
    return {"uid": user.uid, "username": user.username}
```

Use `User.get_logged_user()` in authenticated synchronous or asynchronous
handlers and their shared services. The middleware verifies the caller before
the handler, binds one isolated request context, and invalidates it on
completion, errors, or cancellation. Repeated getter calls do not perform
additional authentication requests.

The same installation handles both HTTP execution modes:

- Local execution without hosted deployment markers validates the incoming
  Bearer token against `MAINSEQUENCE_ENDPOINT/api/v1/users/me/`.
- Platform-hosted execution verifies the gateway's signed caller assertion
  using deployment-owned issuer, public-key, release, and Environment settings.
  Hosted configuration cannot select local mode, and failed proof never falls
  back to UID headers or the process's credentials.

Route code must not select the mode, parse authentication headers, or implement
another caller resolver. The launcher checks the application's installation
declaration before serving; it does not install the middleware itself.

The identity contains canonical `uid` and optional `username`; signed HTTP
proofs contain no username. `request.state.user` and `request.state.user_uid`
remain projections of the same identity, not a separate resolver. Outside an
authenticated request, including public routes, `User.get_logged_user()` raises
`RequestIdentityError`.

On a protected route, this identity is the caller of the current HTTP request:
a person, or the workload identity a deployed Job, FastAPI release or Agent runs
as. It is not the release creator, deployment owner, the application's own
runtime workload principal, or the process account returned by
`User.get_authenticated_user_details()`. To tell a workload caller from a
person, read `User.get_by_uid(uid)`: a workload identity has `identity_type`
`"workload"`. Caller authentication is not resource authorization; apply the
endpoint's application policy using the verified user UID.

To list the people, Teams and workloads the caller can see, for example to
offer sharing candidates, read inside `reads_as_caller()` from
`mainsequence.server.fastapi`: `User.filter`, `User.get_by_uid`, `Team.filter`
and `Team.get_by_uid` then answer with the caller's visibility, and workload
rows carry `managed_by_caller`. Outside a signed HTTP request it raises
`RequestIdentityError`; it never reads as the application instead.

WebSockets retain the platform's gateway ticket/header contract rather than
the HTTP Bearer/assertion flow; the getter is available while the authenticated
connection is active. See the version-matched
[FastAPI identity documentation](https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/fastapi/)
for the lifecycle and trust contract.

### Requester-bound calls

Another application, for example an Agent, can call the API while it works for
a person: the requester. The platform signs the requester into the caller
assertion it sends to the API, and the SDK verifies it before the handler runs.
`User.get_logged_user()` still returns the caller, the acting application.
`User.get_requester()` returns the requester, the checked delegation, with the
person's own `uid`, `team_uids` and `is_organization_admin`, or `None` when the
call carries no delegation. A requester-bound call carries the person's
ordinary permissions, including administrative ones.

Each handler decides whether it needs a person. One that needs a person fails
with its own error when there is none; one where the person is optional
authorizes against the person when present, and against the caller otherwise:

```python
from fastapi import HTTPException
from mainsequence.client import RequestUserIdentity, User


def require_requester() -> RequestUserIdentity:
    """The person this call works for; refuse the call when there is none."""
    requester = User.get_requester()
    if requester is None:
        raise HTTPException(status_code=403, detail="This operation needs a requester.")
    return requester


@app.get("/team-reports/{team_uid}")
def team_report(team_uid: str) -> dict[str, str]:
    requester = require_requester()
    if not (requester.is_organization_admin or team_uid in requester.team_uids):
        raise HTTPException(status_code=403, detail="The requester cannot read this report.")
    return {"team_uid": team_uid, "requested_by": requester.uid}


def acting_identity() -> RequestUserIdentity:
    """The person on a requester-bound call, the caller on any other; never both."""
    return User.get_requester() or User.get_logged_user()
```

- Authorize against one identity: the person on a requester-bound call, the
  caller on any other. Never combine the caller's grants with the person's,
  and never fall back to the caller's rights when the person may not.
- Before a write, check the exact object and action; the example's
  team-membership check is not a general edit, run, share or delete grant. The
  getter verifies identity, not operation permission.
- Never parse the assertion, headers or MCP `_meta`, never read a person's UID
  from the request body, a header or a query parameter, and never let a caller
  name whom it acts for. Anyone can write a UID there. Only the signed
  assertion names the requester, and the SDK has verified that it is addressed
  to this release; an invalid one is rejected with 401 before any handler runs.
- Inside a requester-bound call, `reads_as_caller()` raises
  `RequestIdentityError`: a receiving application cannot forward its inbound
  requester-bearing assertion to the directory endpoints. Platform-authorized
  Agent delegation is separate and keeps the original person and request time.
- The platform bounds work to at most 24 hours after the original request and
  checks access on every call; delegation does not restart that limit. This
  SDK does not enable platform endpoints or extend the inbound request scope.
- A model-driven Agent can be steered by prompt injection. Its writes can
  cause damage within the person's permissions; keep object/action checks and
  any mutation approval required by the application's policy outside model
  claims or tool arguments.
- Test requester-authorized reads and writes, writes refused for a requester
  without the necessary grant, requester-required operations refused without
  a requester, and writes the calling Agent may make but the person may not
  refused on a requester-bound call.

Which applications may act for their requester, what that access covers, and
what people are told:
`.agents/skills/mainsequence/platform_operations/access_control_and_sharing/SKILL.md`.
See also
[Requester-bound calls](https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/fastapi/#requester-bound-calls).

### MCP tools

To serve MCP tools from the same application, add the `mcp` extra and call
`install_mcp(app, mcp.streamable_http_app(), lifespan=mcp.session_manager.run)`
after `install_request_identity(app)`, with a stateless FastMCP server that
returns JSON. Tools authorize with `User.get_logged_user()` and
`User.get_requester()` exactly like REST handlers; admission to the release
does not authorize every tool. Each tool decides whether it needs a person: a
tool that needs one fails with its own error when the getter returns `None`,
and a tool where the person is optional authorizes against the person when
present and the caller otherwise, never both. Verify object- and
action-specific permission before mutation; read access and MCP annotations
are not write authority. The identity reaches tasks a tool starts and worker
threads it uses through `asyncio.to_thread` or anyio's `to_thread.run_sync`,
and ends with the call. See
[Serving MCP](https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/fastapi/#serving-mcp).

### Public provider callbacks and webhooks

An external OAuth redirect or webhook cannot supply a Main Sequence Bearer token.
For a FastAPI release that needs one, use the platform's exact `public_ingress`
policy. CORS configuration and an app-declared route alone do not make a path
public. Ordinary routes still require an authenticated caller through the
request-identity integration.

1. Implement the provider handler in the FastAPI app loaded by the workflow's
   `source_path`. Declare only its exact `GET` or `POST` method and literal
   root-relative path under the `kind: fastapi` resource's
   `spec.public_ingress`. The policy is not inferred from decorators or OpenAPI.
   Do not include query strings, route parameters, wildcards, or a hostname.
   The default empty list exposes no public paths. In a validated workflow
   resource, the declaration has this shape:

   ```yaml
   kind: fastapi
   spec:
     source_path: api/provider/main.py
     public_ingress:
       - method: GET
         path: /integrations/provider/oauth/callback/
       - method: POST
         path: /integrations/provider/webhook/
   ```

2. Fetch the exact CodeRepositoryBranch's current workflow template; use its
   advertised version and fields. Validate the proposed workflow file through
   the branch's `validate-workflow` action, then commit the handler and workflow
   file and push the intended branch. Do not set the reserved
   `FASTAPI_PUBLIC_INGRESS` environment value yourself.
3. Follow the repository event and DeploymentRun to a ready active revision.
   A validated file or successful push is not proof of public admission. Read
   `resource_release.get` and confirm the pair appears in both desired
   `public_ingress` and active `effective_public_ingress`. Only then append the
   declared path to the backend-returned `public_url` and register that full
   callback or webhook URI with the provider. Do not construct a hostname.
4. Send a provider-style request without a Main Sequence Bearer token to the
   exact public pair. Check that an unlisted path or wrong method is denied.
   An admitted request has `request.state.user` and `user_uid` set to `None`
   and `request.state.auth_outcome == "public_ingress"`;
   `User.get_logged_user()` raises `RequestIdentityError` in that anonymous
   scope. The handler must verify
   OAuth state and exchange codes, or verify the webhook signature against the
   original body and reject replays. Public admission supplies no User or
   provider identity. Keep provider secrets and live codes out of workflow YAML,
   logs, and test evidence.

An existing release can change its desired policy through the canonical
`resource_release.update` operation. Adding a pair still requires a later ready
revision containing the route before it becomes public. Removing a desired pair
denies it immediately. The platform skill
`mainsequence://platform/skills/code-repository-workflows` owns the detailed
automatic deployment procedure and current workflow shape.

### 2. Implement the provider

Keep Command Center contract-bearing route code focused on transport and
application behavior:

1. validate request bodies and parameters
2. invoke repository-owned services and Main Sequence data access
3. serialize the declared Command Center contract
4. validate the serialized body against the schema from the selected repository
   commit before it crosses the HTTP boundary

Python and Pydantic models are implementation tools, not contract authority.
Preserve schema requirements, nullability, enums, numeric constraints,
additional-property policy, contract IDs, and semantic rules from the selected
Command Center SDK repository revision.

### 3. Prove contract conformance

Tests for each selected Command Center contract must:

- compile the selected schema using draft 2020-12
- accept every manifest-indexed valid fixture
- reject every manifest-indexed invalid fixture
- validate representative route responses after JSON serialization
- test request validation, authentication context, authorization decisions, and
  declared errors
- assert that secrets and runtime-only objects never enter response payloads
- record the Command Center SDK repository commit SHA, contract ID, and schema
  `$id` in test evidence

An OpenAPI document and passing Pydantic validation are useful but do not replace
validation against the authoritative Command Center SDK schema and fixtures.

### 4. Test with the frontend before release

Run the FastAPI application locally and exercise it through the actual Command
Center frontend flow, not only with isolated HTTP calls.

Start the ASGI application with the local development runner, wait for its
lifespan startup to complete, and run the repository-pinned contract tests
against its local URL. This local process is the readiness source for local and
debug execution.

For Adapter From API development, use the direct transport mode defined by the
selected Command Center SDK repository contract and expose the local API through
a temporary authenticated Cloudflare tunnel. Exercise the actual frontend flow
against that local API before creating repeated API deployments. Validate
discovery, queries, health behavior, exact response contracts, and secret
redaction through that path.

Do not guess transport field names or values from this skill. Read them from the
Adapter From API schema and fixtures at the selected repository commit.

### 5. Release the tested commit

Before release, verify local CodeRepository resolution:

```bash
mainsequence code-repository current --debug --json
```

Declare the FastAPI resource in `.mainsequence/workflows/`, validate the
workflow against the backend template, then commit and push the tested commit
with Git and inspect the result:

```bash
git commit -m "Release Command Center API"
git push
mainsequence code-repository resources list --filter resource_type=fastapi
```

When the dependencies changed, run `mainsequence code-repository sync --path .`
before the commit and include the refreshed `uv.lock` and `requirements.txt`.
The command runs no Git command and makes no platform request.

Verify that:

- the push went to the intended Git branch
- when the workflow sets a `tag_regex`, the repository's CI tagged the pushed
  commit with a matching tag; without a `tag_regex` every push deploys
- the workflow declaration identifies the tested FastAPI source path
- resource discovery found that path at the exact deployed commit
- Django resolved and built the exact image for the workflow event
- the resulting release kind is `fastapi`
- any declared `public_ingress` pair matches a mounted FastAPI handler
- compute and spot settings are intentional

Enable automatic deployment in the repository workflow when the route paths,
resource path, and frontend contracts are stable enough for future repository
events to promote exact images. Automatic deployment and its `tag_regex` are
set in the workflow file only; a release update does not accept them.

### 6. Verify the deployed API

Request runtime access for protected routes on the deployed FastAPI
`ResourceRelease` through Django and wait for Django's ready result. Use the
returned access bundle for authenticated checks. For declared public routes,
use the provider URI without a Main Sequence Bearer token:

1. verify protected-route authentication and, when configured, exact
   unauthenticated public-ingress method/path behavior
2. execute representative contract-bearing requests
3. validate returned JSON against the same repository-pinned schemas
4. exercise the consuming Command Center frontend flow
5. inspect platform logs for serialization, authorization, and runtime failures

When automatic deployment is enabled, also inspect the deployment run and prove
that it selected the expected repository revision, resource path, and image.

## Completion Evidence

Report:

- Command Center SDK repository URL and resolved commit SHA
- implemented contract IDs and schema `$id` values
- conformance and route-test results
- local frontend integration path used
- Git commit, code repository image UID, code repository resource UID, and release identity
- deployed contract validation and Command Center frontend result
- whether automatic deployment is enabled and why
- for public provider routes, the declared method/path pairs, observed
  `effective_public_ingress`, backend-returned provider URI, and unauthenticated
  positive and negative request results

The Command Center-facing release is complete only when the frontend consumes
the deployed contract-bearing responses and they pass the contracts from the
recorded Command Center SDK repository revision.
