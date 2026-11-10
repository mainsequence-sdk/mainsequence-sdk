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

## Call a release

A release can be scaled to zero, and a request to it is then held until a
replica starts. `request` keeps that start out of your timeout:

```python
release = ResourceRelease.by_uid("<RESOURCE_RELEASE_UID>")
response = release.request("POST", "/v1/prices", timeout=60, json={"symbol": "ABC"})
response.raise_for_status()
```

`by_uid` makes a handle from the UID without contacting the platform, unlike
`get(pk=...)`, which fetches the release's details each time it runs. `request`
itself asks the platform only on the first call in the process, when the
release has shown no sign of serving within its idle window, when its token is
about to expire, and after a timeout.

In asynchronous code use `await release.arequest(...)`, which runs the same
call on a worker thread.

The call first makes sure the release is ready. It asks the platform, which
also starts the release, and waits while the answer is `waking`. Only then is
the request sent, to the URL and with the token the platform returned, so
`timeout` measures the application's own work. `timeout` is a total in seconds
or a `(connect, read)` tuple. Other keyword arguments (`json`, `params`,
`data`, `headers`) go to `requests`. The response is returned whatever its
status. `path` is a path on the release; the SDK sets `Authorization`.

| Argument | Meaning |
| --- | --- |
| `timeout` | Required. How long the application's work may take once it is running. |
| `ready_timeout` | How long a start may take. Default: twice the start time the platform expects, at least 30 seconds; or `MAINSEQUENCE_RELEASE_READY_TIMEOUT_SECONDS` (300) when the platform states none. |
| `idempotent` | `True` allows one more send, only after a timeout that happened while the release was starting. Default `False`: the request is sent once. |

### What the process remembers

The SDK keeps one in-memory cache per process, `release_runtime_cache`, with
the platform's last answer for each release called, when the release last
answered a request, and the last start this process waited for. While a
release is known warm, `request` skips the platform call. A release is warm
when its token is still valid and either the platform reports
`min_instances >= 1`, or the release answered within its idle window less a
margin (the larger of 10 seconds and 20% of the window). The window is the
platform's `idle_timeout_seconds`, or `MAINSEQUENCE_RELEASE_IDLE_TIMEOUT_SECONDS`
(60) when the platform states none.

The cache is not shared between processes and is not the truth: another
caller may keep a release warm, and a deployment restarts it. Do not build
another one. Read it with `runtime_status()`, which never contacts the
platform and never starts the release:

```python
status = release.runtime_status()
status.state                    # "warm", "cold" or "unknown"
status.last_cold_start_at       # when the last start this process waited for began
status.last_cold_start_seconds  # how long it took
status.process_started_at       # when the release's process started, as it reported
```

`min_instances`, `idle_timeout_seconds` and `expected_cold_start_seconds` are
`None` until the platform returns a `scaling` object in its answer.

### Failures

Each is a `ReleaseCallError` with `resource_release_uid`, `phase`
(`readiness` or `request`), `timeout_seconds` and `runtime_phase`.

| Error | Raised when | Sent? |
| --- | --- | --- |
| `ReleaseReadinessTimeoutError` | `ready_timeout` ended while the release was starting. | No |
| `ReleaseUnavailableError` | The platform answered that the release cannot take requests. | No |
| `ReleaseDeadlineTooShortError` | The caller of the current request will not wait long enough for the release to start. | No |
| `ReleaseColdStartTimeoutError` | `timeout` ended and the platform then shows the release starting. | Yes |
| `ReleaseRequestTimeoutError` | `timeout` ended on a serving release. | Yes |

After a request times out, the SDK asks the platform once to tell the last two
apart. A start that finished before that question is recognised only when the
platform returns `scaling.serving_since`; without it, such a call reads as
`ReleaseRequestTimeoutError`.

`wait_for_runtime_access()` runs the same wait without sending a request, for
code that only needs the platform's answer. Concurrent first calls to a cold
release share one wait.

### Starting several releases

`prewarm_releases([release_a, release_b])` asks the platform to start each,
in parallel, and returns at once. It accepts releases or UIDs and never
raises. Call it from an application's lifespan with the releases it depends
on, so a chain of cold releases costs about the slowest start instead of the
sum.

### Deadlines across calls

`request` sends `Mainsequence-Request-Timeout-Ms`, the time it will still
wait. An application with `install_request_identity` reads it from a verified
caller, and calls made with `request` inside that handler are bounded by what
remains and send the remainder on. A call that cannot fit the target's
expected start in that time fails with `ReleaseDeadlineTooShortError` before
anything is sent. Without the header nothing is bounded.

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

## Who can use a release

A release's access is decided by its policies:

- `viewer`: see the release's configuration, revisions and logs, and call it:
  its API, site or MCP endpoint;
- `user`: see the release and call it, without its logs;
- `editor`: also change its configuration (scaling, concurrency, timeouts,
  CORS, public paths, MCP), activate static deployments, delete it, manage what
  its workload can reach, and see who has access;
- `owner`: also decide who has access.

A call reaches the application as you: the application receives your identity
and decides what you may do in it, and its logs can contain other callers'
request data. `resolve_runtime_access()` and `wait_for_runtime_access()` need
the release's `invoke` action, which every policy holds. The release's creator
holds `owner`; repository workflows create releases as the Organization's
automation identity, which holds no policy on them. Organization admins hold
every action, and turning on acting for the requester is in no policy: only
they do it.

The token that `resolve_runtime_access()` returns in `access` belongs to the
login that asked for it. Asked with your Main Sequence login
(`mainsequence login` or a token pair), it stops working within at most 60
seconds after you log out or that login session is revoked, even before it
expires. Asked inside a hosted application with its own credential, it lasts
until it expires. Either way the platform checks again, at least every 60
seconds, that the caller may still call the release.

A release also takes the policies of the branch that declares it: `reader` on
the branch gives `viewer`, and `editor` or `owner` gives `editor`, whether the
branch policy is held on the branch or passed down from its repository or a
Project. `get_access()` shows each of those as a `computed` row with `source`
`inherited`. A release that backs an Agent follows the Agent instead, and
branch access does not reach it: the Agent's `viewer` and `operator` give
`viewer`, its `editor` and `owner` give `editor`, and its `user` gives nothing.
Such a release takes no policy of its own: `attach_policy()` raises
`BadRequestError` whose `payload` has `code` `sharing_managed_by_owner`, so
give access on the Agent; see [Agents](agents.md). A release's backing Job
follows the release.

Read and change the policies with the access methods on `ResourceRelease`:

```python
from mainsequence.client import ResourceRelease

release = ResourceRelease.get(pk=release_uid)

for row in release.get_access().computed:
    print(row.source, row.policy, row.parent_policy)

binding = release.attach_policy("user", principal_type="team", principal_uid=team_uid)
print(release.get_my_access().allowed_actions)
```

The access methods address the release by its UID alone and take no
Environment; with a workload's runtime credentials they reach only the releases
of that workload's Environment. A refused change raises
`PermissionDeniedError`, whose `payload` names the `required_action` and the
`granting_policies`. A repository workflow file's `access` entry for a release
gives its workload the policy it names, such as `policy: user`. `mainsequence
access policies --type resource-releases` lists the policies with their
actions, and [Who Has Access to an Object](object_access.md) describes every
method and model.

From the command line:

```bash
mainsequence code-repository resources access list <RELEASE_UID>
mainsequence code-repository resources access grant <RELEASE_UID> user --user <USER_UID>
mainsequence code-repository resources access revoke <RELEASE_UID> <BINDING_UID>
mainsequence code-repository resources access explain <RELEASE_UID> resource-releases.invoke
```

See [Resource releases](../../cli/index.md#resource-releases) in the CLI
reference.
