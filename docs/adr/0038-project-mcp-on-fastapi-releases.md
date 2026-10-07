# ADR 0038: Project MCP on FastAPI releases

Amended 2026-10-07 for [SDK issue #208](https://github.com/mainsequence-sdk/mainsequence-sdk/issues/208):
a requester-bound tool call carries the person's ordinary permissions,
including administrative ones, and `User.get_requester()` returns the person's
own `team_uids` and `is_organization_admin`. Each tool decides whether it needs
a person; the SDK keeps no list of tools that accept one. Tests at the tool
handler cover asynchronous tasks, thread pools, concurrent calls for two people
and for a workload, and reset after completion, failure and cancellation. See
[section 3](#3-reuse-the-request-scoped-caller-identity) and
[ADR 0036](0036-request-scoped-logged-user.md#requester-bound-calls).

Date: 2026-10-06

Status: Accepted 2026-10-06 and implemented in the SDK; see
[Implementation](#implementation). The launcher's check of the declaration and
the live client and hosting-provider verification remain open.

Owners: Main Sequence SDK maintainers.

## Context

A project can expose its business operations through MCP in the same FastAPI
application that serves its HTTP API, including an application whose only
product interface is MCP. The capability should reuse the existing release,
hosting and caller authentication.

The platform API describes it on `ResourceRelease`. A FastAPI release declares
`mcp_enabled`, and the active MCP-enabled revision is advertised as
`mcp_connection`, whose endpoint is the release URL plus `/mcp`. MCP clients
authenticate with the platform through OAuth. As for every request to a
release, the application receives the platform's signed caller assertion, never
the client's token.

At SDK revision `6a271cb74065578829053bc3aed052d64ab7c21f`, the SDK provides
`mainsequence.server.fastapi.install_request_identity(app)`, the caller
assertion verifier, and the request scope read by `User.get_logged_user()` and
`User.get_requester()` ([ADR 0036](0036-request-scoped-logged-user.md)). It
also provides the `ResourceRelease` client. It has no MCP connection fields and
no MCP hosting integration. MCP is not a runtime dependency, and FastAPI is a
development dependency only. As an MCP client, the CLI already speaks protocol
version `2025-11-25`. These are source observations, not evidence that a
deployed MCP application works.

This record follows the
[MCP 2025-11-25 specification](https://modelcontextprotocol.io/specification/2025-11-25).
Its compatibility target is protocol version `2025-11-25`, the version the
SDK's CLI speaks. Negotiating an older protocol version belongs to the
application's MCP library.

## Decision

### 1. Optional integration around an application-owned MCP server

The SDK composes an application-provided MCP ASGI application with the FastAPI
app, its lifecycle and the existing verified caller context. The project
chooses its tools, schemas, results and authorization policy. Sharing Python
functions between REST, MCP and agents is an application choice. There is no
required Main Sequence tool registry, decorator, base class, REST generator or
harness adapter.

Use the
[Streamable HTTP transport](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)
at the exact path `/mcp`, initially with stateless requests and bounded JSON
responses. The specification allows both: a server may answer a POST with a
JSON response and may decline the GET stream with HTTP 405. Stateful sessions,
resumable streams, subscriptions and the experimental tasks utility are outside
this initial integration. An ordinary tool may still submit work to the
application's existing workflows and return its identifier.

The installation interface has three inputs: the existing FastAPI app, the
application-owned MCP ASGI app, and its asynchronous lifespan context manager
factory. Its public helper name and exact Python signature must be reviewed
before implementation. The interface accepts no tool catalog, user token or
alternative identity resolver. Applications install the existing request
identity once; the MCP integration uses that installation.

Keep the integration's imports and dependencies optional. Ordinary
`mainsequence` installations and REST-only apps must not acquire an MCP server
dependency. Choose an optional dependency group or extra, and record the tested
FastAPI, ASGI framework and MCP library ranges before shipping. The
[official MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)
is the first compatibility target. Its adapter and version range require tests,
and projects are not required to adopt its tool definitions.

### 2. Mounting, lifecycle and launcher compatibility

The integration must:

- Serve `/mcp` directly, without an accidental `/mcp/mcp`, a trailing-slash
  redirect or a second advertised path.
- Keep the mount inside the request-identity middleware. A FastAPI dependency
  on REST routes does not protect a mounted ASGI application.
- Compose the FastAPI lifespan with the MCP lifespan: start each exactly once
  before readiness, keep application startup state, shut down in reverse order,
  and unwind completed startup when a later step fails.
- Reject conflicting mounts, duplicate installation, missing identity coverage
  and any public-ingress configuration that makes `/mcp` anonymous. Fail
  startup when installation or MCP startup is invalid; never run a business
  tool as a readiness probe.
- Publish a package-neutral installation declaration next to the
  request-identity declaration, for the platform launcher to check as it checks
  that one (ADR 0036). It states the installed path, transport profile and
  identity coverage. Its fields are recorded under
  [Implementation](#implementation); the launcher's check follows them.

A declaration proves expected wiring, not the authenticity of a request or that
a tool is available. The launcher does not import tools or start a separate MCP
process.

### 3. Reuse the request-scoped caller identity

Hosted requests reach the app with the platform's signed caller assertion, as
REST requests do. The existing verifier checks it, including the `requester`
claim of a requester-bound call, and binds the request scope before any tool
runs. Tools read the caller with `User.get_logged_user()` and, in a
requester-bound call, the person the caller works for with
`User.get_requester()`, keeping ADR 0036's distinction between the two. The
person is the checked delegation, with the person's own `team_uids` and
`is_organization_admin`; without a delegation the getter returns `None`.

MCP libraries may dispatch work into task groups created at startup or into
thread pools. Context propagation from the middleware must be tested at the
actual tool handler. If an adapter has to propagate context explicitly, it
transfers only the verified, still-live request scope through the existing
mechanism, and that change amends ADR 0036. It never resolves a user from tool
arguments, headers, a session identifier or process credentials.

Each HTTP request has its own identity, even when a client sends a session
identifier or reuses a connection. Completion, failure and cancellation reset
the scope; a copied context keeps no authority after its request ends. A
missing or invalid assertion stays HTTP 401, unavailable verification HTTP 503,
and a tool that finds no authenticated scope fails closed. Background work uses
the application's existing job authority.

Application code owns operation and data permissions. Admission to a release
does not authorize every tool, and a read-only tool annotation is not access
control. Each tool decides whether it needs a person, and the SDK keeps no list
of tools that accept one:

- a tool that needs a person fails with its own error when
  `User.get_requester()` is `None`;
- a tool where the person is optional authorizes against
  `User.get_requester()` when present, and against `User.get_logged_user()`
  otherwise;
- no tool parses assertions, headers or MCP `_meta`, or combines the caller's
  grants with the person's.

A requester-bound call carries the person's ordinary permissions, including
administrative ones. A requester-bound write checks the verified person's
permission for the target object and operation before mutating anything.
Reading the object or the calling Agent's own grants cannot authorize the
write. Workload credentials stay separate from the inbound caller, and the
integration adds no delegation: `reads_as_caller()` keeps its directory-read
scope and its refusal in requester-bound calls.

Local mode is unchanged. ADR 0036 validates the incoming Bearer token against
`/api/v1/users/me/`, and the integration adds no OAuth challenge. The SDK adds
no token parser and no identity fallback for this. The platform API refuses a
token issued for another resource, such as a project's MCP URL, so local
admission answers 401. The developer's saved session is never the inbound
caller.

### 4. Direct connection; OAuth belongs to the platform

A user adds the release's MCP URL directly to a compatible client, such as
Codex, and signs in. No Main Sequence setup command or central MCP connection
is a prerequisite.

```text
Client                              Release URL
------                              -----------
POST /mcp without a token  ------->  401, WWW-Authenticate with resource_metadata
Read the metadata; sign in with
the platform's authorization server
(authorization code + PKCE)
POST /mcp with the token   ------->  the platform checks the token and the
                                     caller's access to the release
                                        |
                                     the app receives the request with the
                                     signed caller assertion, not the token
                                        |
                                     the SDK verifies it and binds the
                                     request scope
                                        |
                                     the tool checks permissions and runs
```

The challenge's `resource_metadata` is the URL the API also reports as
`mcp_connection.resource_metadata_url`; 2025-11-25 clients use it before any
`.well-known` fallback. The specification requires a server to implement one
of the two discovery mechanisms and clients to support both, so this
challenge is sufficient. Client registration, authorization, token audience,
scopes, refresh and revocation follow the platform's
[authorization](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization)
contract. The application serves no metadata or login endpoint.

The SDK's part:

- It is neither an OAuth server nor a token store. It never receives,
  verifies, forwards or refreshes the client's token; it trusts only the caller
  assertion.
- It does not replace the developer session of
  [ADR 0037](0037-machine-session-in-the-os-credential-store.md).
- A client connects without calling `ResourceRelease.resolve_runtime_access()`
  first. The SDK adds no wake loop and never replays a tool call that may have
  started.

### 5. Consume the release connection metadata

Extend the `ResourceRelease` model and its filters with the platform API's
fields. Do not create another endpoint registry or derive hostnames from
release names or identifiers.

| Wire field or filter | SDK meaning |
| --- | --- |
| `mcp_enabled` | Desired capability of a FastAPI release; false when omitted. |
| `mcp_connection` | Connection of the active MCP-enabled revision, or null. |
| `mcp_connection.url` | Public endpoint, including `/mcp`. |
| `mcp_connection.transport` | `streamable-http`. |
| `mcp_connection.resource_metadata_url` | OAuth protected-resource metadata URL. |
| `mcp_connection.organization_environment_uid` | The connection's Environment. |
| `mcp_connection.active_revision_uid` | The revision advertising the capability. |
| `mcp_available=true\|false` | Collection filter on advertised availability. |

Rows of other release kinds may carry false/null or omit these fields; both
read as false/null, as does an older response. Follow
[ADR 0032](0032-tolerant-response-reading.md) and
[ADR 0033](0033-tolerant-value-set-reading.md): read an unknown transport, but
never connect with one the SDK does not support. Keep outgoing filter values
validated.

Workflow API `2.3.0` accepts an optional `spec.mcp_enabled` for FastAPI.
Applying a workflow that omits it resets the desired state to false; a PATCH
that omits it keeps the state. SDK updates send `mcp_enabled` only when the
caller sets it. This does not reintroduce direct ResourceRelease creation.

Desired `mcp_enabled` and advertised availability can differ during a
deployment. A failed deployment keeps the old connection; a rollback follows
the selected revision. Availability is an advertised capability, not a health
check, and setting the flag installs no server. Discovery is read-only and
permission-filtered; it never wakes an application or returns credentials.

**Release order.** The fields read tolerantly, so the models can ship before
the production API sends them. The `mcp_available` filter cannot: an API that
does not serve it ignores the parameter and returns every release. Ship the
filter, and any helper that uses it, only once the production API serves it.

### 6. Connection setup automation is separate and optional

The initial SDK scope is the hosting integration and typed release discovery.
Client-specific installation commands are deferred. A future local helper may
list accessible connections and configure the ones the user selects. It must
keep unrelated client settings, write no tokens, and use the client's own OAuth
flow and credential storage. No discovery response installs a connection by
itself.

The central Main Sequence MCP may list release connections. It does not
import, republish or proxy a project's tools, and direct registration works
without it.

## Scope

| Owner | Work |
| --- | --- |
| Main Sequence SDK | Optional ASGI and lifespan integration, request-identity integration, typed connection fields and filter, focused tests, a generic example and SDK documentation. |
| Application author | MCP server, tools, schemas, business logic and per-operation authorization. |
| Platform API | Release state and connection metadata, OAuth, release access and signed caller assertions. |
| MCP client | Connection configuration, browser sign-in, credential storage and refresh. |

Future SDK changes belong in `mainsequence/server/fastapi.py` and the
request-scope machinery, `mainsequence/client/models_helpers.py`, optional
packaging metadata and their tests. Update the FastAPI, caller-identity and
ResourceRelease guides with the implementation. The launcher's check of the
declaration belongs to the launcher.

## Acceptance criteria before release

1. A generic application serves REST and MCP together; REST-only applications
   keep their behavior, and a plain SDK import needs no MCP installation.
2. Tests cover exact `/mcp` routing, initialization and tool calls, JSON
   responses, the `MCP-Protocol-Version` header, unsupported methods (405) and
   Origin validation (403 for an invalid Origin). No protocol path bypasses
   authenticated admission.
3. The application and MCP lifespans start and stop once under the launcher;
   partial startup unwinds, invalid setup fails readiness, and no health check
   invokes a business tool.
4. Tests reach real asynchronous and threaded tool dispatch with two concurrent
   users. They cover forged and missing assertions, requester-bound calls,
   errors, cancellation and expired copied contexts, with no identity leakage
   and no fallback to the workload or developer account.
5. Model and filter tests cover full and older responses, null connections,
   other release kinds, unknown transports and desired versus advertised state.
   The SDK wire-contract checks pass against the platform API schema.
6. The `mcp_available` filter ships only once the production API serves it.
7. A live compatible client, including Codex, registers only the release's MCP
   URL, discovers authorization, signs in, calls an authorized tool and
   refreshes. The platform refuses a revoked token and a token issued for
   another release.
8. Each supported hosting provider is verified for the challenge, MCP HTTP
   behavior and cold start without replaying an uncertain call. Record the
   tested SDK, library, protocol, client and launcher versions and the
   remaining limitations before claiming support.

## Implementation

Implemented 2026-10-06:

- `mainsequence.server.fastapi.install_mcp(app, mcp_app, *, lifespan)`.
  `mcp_app` answers Streamable HTTP at `/mcp`; `lifespan` is a zero-argument
  callable returning its async context manager. With the official SDK they are
  `mcp.streamable_http_app()` and `mcp.session_manager.run`. Request identity
  is installed first.
- The `/mcp` route is the application's first route, inside the
  request-identity middleware, and is left out of the OpenAPI schema. Only
  authenticated POSTs reach the server. Other methods answer 405 with
  `Allow: POST`, and the `Mcp-Session-Id` header is removed, so a session
  identifier never selects server state.
- A request carrying an `Origin` is admitted only from the release's public
  origin (`FASTAPI_PUBLIC_BASE_URL`) or one of its CORS origins
  (`FASTAPI_CORS_ALLOW_ORIGINS`, exact or `https://*.example.com`); any other,
  or a repeated `Origin`, answers 403. A request without `Origin`, as
  non-browser clients send, is unaffected.
- The application's lifespan starts first and stops last. A second start
  raises, and a route under `/mcp` added after installation fails startup.
- Declaration: `app.state.mainsequence_mcp` is
  `{"installed": True, "path": "/mcp", "transport": "streamable-http",
  "stateless": True, "request_identity": True}`.
- Packaging: the `mcp` extra adds `mcp>=1.28,<2`; ordinary installations and
  `mainsequence.server.fastapi` do not import it. Tested with mcp 1.30.0,
  FastAPI 0.141.1 and Starlette 1.7.0. FastMCP is configured with
  `stateless_http=True`, `json_response=True` and DNS-rebinding protection
  off: its default admits only localhost hosts, while the integration checks
  `Origin` and the platform routes the host.
- With the official SDK in stateless mode, the caller's context reaches
  asynchronous tools, the tasks they start and the worker threads they use
  through `asyncio.to_thread` or anyio's `to_thread.run_sync`, without explicit
  propagation, so ADR 0036's request scope needs no adapter. A thread that
  does not receive a copy of the context, such as one started with
  `loop.run_in_executor`, has no identity and fails closed. Each call keeps its
  own identity under concurrency, and its end, by completion, failure or
  cancellation, also ends it for tasks and contexts copied from it.
- `ResourceRelease` reads `mcp_enabled` and `mcp_connection`
  (`ResourceReleaseMcpConnection`), filters on `mcp_available`, and sends
  `mcp_enabled` in a PATCH only when the caller sets it.

Acceptance criteria 1, 2, 4 and 5 are covered by SDK tests. Open: 3 under the
real launcher, 6 (the filter reaches a final release only once the production
API serves it), and 7 and 8 with a live client and each hosting provider.

## Consequences and remaining decisions

This adds one optional interface to an existing deployment. Tool reuse remains
an application choice, and authentication keeps its platform and SDK
ownership. Lifespan composition and request-context propagation are real
integration work even though HTTP routing is reused.

The installer signature, the `mcp` extra and its tested range, and the
declaration are settled under [Implementation](#implementation); the
launcher's check of the declaration remains with its owners. Client setup
automation stays deferred. This record adds no infrastructure and does not
declare untested interoperability ready.

Change classification: a new ADR for a feature, with its SDK implementation.
Existing ADRs are not amended; an implementation that changes how the request
scope propagates would amend ADR 0036.
