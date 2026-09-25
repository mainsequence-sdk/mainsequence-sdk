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
- upstream `TimeIndexTableRef`, MetaTable, service, or external data dependencies
- whether the API uses backend transport or a contract-defined direct
  development transport

Authenticated FastAPI routes receive the human caller from the Main Sequence
platform through injected request state. No SDK authentication setup is
required in repository code:

```python
from fastapi import FastAPI, Request


app = FastAPI()


@app.get("/me")
def get_me(request: Request) -> dict[str, str | None]:
    return {
        "uid": request.state.user_uid,
        "username": request.state.user.username,
    }
```

Protected route code must not detect local versus deployed execution, parse
authentication headers, or resolve the request user itself. Consume the
platform-injected human caller identity:

- `request.state.user` contains canonical `uid` and optional `username`
- `request.state.user_uid` is the canonical public user UUID
- `request.state.user_id` does not exist

On a protected route, this identity is the human making the current HTTP
request. It is not the release creator, deployment owner, runtime workload
principal, or runtime target. Pass request state explicitly to shared services;
do not use `User.get_logged_user()` as a FastAPI entry point. Platform-injected identity is
not resource authorization; use `request.state.user_uid` when applying the
endpoint's authorization policy.

### Public provider callbacks and webhooks

An external OAuth redirect or webhook cannot supply a Main Sequence Bearer token.
For a FastAPI release that needs one, use the platform's exact `public_ingress`
policy. CORS configuration and an app-declared route alone do not make a path
public. Ordinary routes remain Bearer authenticated.

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
   and `request.state.auth_outcome == "public_ingress"`. The handler must verify
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
workflow against the backend template, then move the tested commit through the
canonical lifecycle:

```bash
mainsequence code-repository sync -m "Release Command Center API"
mainsequence code-repository resources list --filter resource_type=fastapi
```

Verify that:

- code-repository sync used the intended Git branch
- the workflow declaration identifies the tested FastAPI source path
- resource discovery found that path at the exact deployed commit
- Django resolved and built the exact image for the workflow event
- the resulting release kind is `fastapi`
- any declared `public_ingress` pair matches a mounted FastAPI handler
- compute and spot settings are intentional

Enable automatic deployment in the repository workflow when the route paths,
resource path, and frontend contracts are stable enough for future repository
events to promote exact images.

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
