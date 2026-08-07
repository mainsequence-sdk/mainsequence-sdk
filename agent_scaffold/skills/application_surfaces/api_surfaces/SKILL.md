---
name: mainsequence-command-center-fastapi
description: Build, contract-test, release, and verify a FastAPI project resource that serves the Command Center frontend. Use when a Main Sequence API must implement a wire contract defined by the mainsequence-sdk/command-center-sdk GitHub repository and then move through local testing, project sync, image and resource resolution, FastAPI ResourceRelease creation, and deployed frontend validation.
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
- upstream `APIDataNode`, MetaTable, service, or external data dependencies
- whether the API uses backend transport or a contract-defined direct
  development transport

Use `LoggedUserContextMiddleware` when the platform-authenticated request user
must be available through `request.state`. This middleware binds request headers
to the SDK request context; it is not an authorization policy by itself.

### 2. Implement the provider

Keep Command Center contract-bearing route code focused on transport and
application behavior:

1. validate request bodies and parameters
2. invoke project-owned services and Main Sequence data access
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

For Adapter From API development, use the direct transport mode defined by the
selected Command Center SDK repository contract and expose the local API through
a temporary authenticated Cloudflare tunnel. This allows the workspace
connection to be tested against local code before creating repeated API
deployments. Validate discovery, queries, health behavior, exact response
contracts, and secret redaction through that path.

Do not guess transport field names or values from this skill. Read them from the
Adapter From API schema and fixtures at the selected repository commit.

### 5. Release the tested commit

Before release, verify local project resolution:

```bash
mainsequence project current --debug --json
```

Then move the tested code through the canonical lifecycle:

```bash
mainsequence project sync -m "Release Command Center API"
mainsequence project images create
mainsequence project project_resource list --filter resource_type=fastapi
mainsequence project project_resource create_fastapi
```

Verify that:

- project sync used the intended Git branch
- the selected image contains the exact tested commit
- resource discovery found the expected FastAPI path at that commit
- the selected resource UID and image UID refer to the same commit
- the created release kind is `fastapi`
- compute and spot settings are intentional

Use `--automatic-deployment` only after the route paths, resource path, and
frontend contracts are stable and the release should follow future repository
syncs. Otherwise keep the release pinned and create deliberate replacements.

### 6. Verify the deployed API

Do not treat local success or release creation as completion. Through the
deployed platform route:

1. verify health and authentication behavior
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
- Git commit, project image UID, project resource UID, and release identity
- deployed contract validation and Command Center frontend result
- whether automatic deployment is enabled and why

The Command Center-facing release is complete only when the frontend consumes
the deployed contract-bearing responses and they pass the contracts from the
recorded Command Center SDK repository revision.
