# Resource releases

`ResourceRelease` is the Python adapter for deployed FastAPI, Harness Agent,
and static-site releases. It is available through the public SDK import:

```python
from mainsequence.client import ResourceRelease
```

## Find a release by exact name

```python
releases = ResourceRelease.filter(
    name="Shared MetaTables",
    release_kind="fastapi",
)
```

`name` is an exact match. The existing string normalizer trims surrounding
whitespace; substring and `name__in` lookups are not supported. Ordinary
`filter`, `iter_filter`, and filtered `get` retain the current
[CodeRepositoryBranch context](context.md). Names can repeat across branches
and release kinds.

If exactly one release is expected in the current scope:

```python
release = ResourceRelease.get(
    name="Shared MetaTables",
    release_kind="fastapi",
)
```

`get` raises `DoesNotExist` for no match and `ApiError` for multiple matches.
Filtered `get` returns the collection row. Fetch its UID directly when full
release details are required.

## Find a shared deployment owned by another repository

A shared MetaTables API can belong to a different CodeRepository than its
consumer. Adding `name` to an ordinary query does not change the consuming
process's branch scope. For intentional administrative discovery, use the
existing explicit collection path with the deployment's owning branch:

```python
matches = ResourceRelease.filter_admin(
    name="Shared MetaTables",
    code_repository_branch_uid="<OWNING_CODE_REPOSITORY_BRANCH_UID>",
    release_kind="fastapi",
)

if len(matches) != 1:
    raise ValueError("Expected exactly one visible Shared MetaTables release")

release = ResourceRelease.get(pk=matches[0].uid)
```

`iter_filter_admin` provides the same explicit scope while iterating collection
pages. These methods do not inject the consuming checkout's branch or expand
object permissions or an authenticated runtime's Environment boundary.

## Use a known release UID

```python
release = ResourceRelease.get(pk="<RESOURCE_RELEASE_UID>")
```

The public UID avoids display-name ambiguity and performs a detail lookup.
For FastAPI runtime routing and readiness, use `release.resolve_runtime_access()`
and the backend-returned routing information. Do not construct a deployment
hostname from the name or UID.

## Supported collection filters

The SDK exposes these filters through `ResourceRelease.FILTERSET_FIELDS`:

| Field | Supported lookups | Meaning |
| --- | --- | --- |
| `name` | exact | Shared release display name |
| `uid` | exact, `in` | Public release UID |
| `code_repository_branch_uid` | exact | Owning CodeRepositoryBranch UID |
| `resource__uid` | exact, `in` | Runtime repository-resource UID |
| `related_job__uid` | exact, `in` | Runtime backing Job UID |
| `release_kind` | exact, `in` | `fastapi`, `harness_agent`, or `static_site` |
| `mcp_available` | exact | Whether the active revision advertises an MCP endpoint |

An exact lookup uses the field directly, such as `name="Shared MetaTables"`.
An `in` lookup uses the suffix and a sequence, such as
`release_kind__in=["fastapi", "static_site"]`. Unsupported filter names fail
locally before an HTTP request is sent.

## MCP endpoints

A FastAPI release can serve MCP at its URL plus `/mcp`. `mcp_enabled` is the
desired capability, applied by the release's next successful deployment, and
`mcp_connection` describes the endpoint its active revision advertises, or is
`None`:

```python
from mainsequence.client import ResourceRelease

for release in ResourceRelease.filter(mcp_available=True):
    connection = release.mcp_connection
    print(release.name, connection.url, connection.transport)
```

`mcp_connection` carries `url`, `transport` (`streamable-http`),
`resource_metadata_url`, `organization_environment_uid` and
`active_revision_uid`. Availability is an advertised capability, not a health
check. A response without these fields reads as `False` and `None`. To serve
MCP from a FastAPI application, see
[Serving MCP](../fastapi/index.md#serving-mcp).
