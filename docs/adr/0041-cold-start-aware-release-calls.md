# ADR 0041: Cold-start-aware calls between releases

Date: 2026-11-10

Status: Proposed. The SDK's part is implemented and runs on defaults; see
[Implementation](#implementation). The platform changes in
[section 1](#1-the-platform-states-a-releases-scaling-facts) and
[section 6](#6-a-deadline-travels-with-the-call) are requests to the platform,
not observations of it, and nothing here was tried against a deployed release.

Owners: Main Sequence SDK maintainers.

Related: [ADR 0029](0029-a2a-message-send-only.md) (Agent message send),
[ADR 0038](0038-project-mcp-on-fastapi-releases.md) (MCP on FastAPI releases),
[ADR 0040](0040-caller-context-on-releases-calls-and-runs.md) (context that
travels with a call), ms-tau-sdk ADR 0022 (readiness before MCP).

## Context

FastAPI releases, MCP endpoints and Agents run as Knative services, and many
scale to zero. A project built from several of them calls in a chain, for
example API A → API B → API C → Agent P, each hop an HTTP request with its own
timeout.

A caller cannot tell a request that hangs from a request that waits for the
target to start. A request to a release at zero replicas is held until a
replica serves, so a call that takes 60 seconds warm can take 360 seconds when
the target needs 300 seconds to start. The caller's 60-second timeout then
fires on a healthy call. In a chain the starts add up, and the outermost
caller's timeout must cover every start below it.

The platform already answers the readiness question. At SDK revision
`origin/main` of 2026-11-10:

- `POST …/resource-release/<uid>/resolve-runtime-access/` returns
  `runtime_access.state` (`ready`, `waking`, `unavailable`), `can_request` and
  `retry_after_ms`, and `runtime_presence` with a `phase` (`idle`,
  `provisioning`, `pulling_image`, `starting`, `serving`, …), desired and
  actual replicas, and the wake operation's `requested_at` and `deadline_at`.
  The SDK reads it as `ResourceReleaseRuntimeAccess`.
- `ResourceRelease.wait_for_runtime_access()` polls that endpoint until the
  release can take a request. Nothing calls it for an application's own HTTP
  calls: an application that calls another release with `requests` or `httpx`
  gets no readiness handling.
- `AgentSession` message send has its own loop over the transient states
  `checking`, `starting`, `waking`, `updating`, with a 60-second access cache.
- ms-tau-sdk has a third loop, for MCP applications and credential hydrate
  releases, with a readiness limit (`TAU_MCP_APPLICATION_READY_TIMEOUT_SECONDS`,
  default 120 seconds) separate from the tool's execution limit. It does not
  depend on this SDK and talks to the platform over `httpx`.

So the same wait is written three times, with three default limits (600, 120
and per-turn), and the most common case, one release calling another, has
none. The answer also lacks what a caller needs to choose a limit: the SDK's
model carries no minimum instance count, no idle window and no expected start
time.

## Decision

Readiness is asked of the platform, never guessed by the caller. The SDK
provides one routine for calling a release, which waits for readiness under
its own limit and only then starts the request's limit, and one process cache
that makes the routine cheap on a warm path. Other projects follow the same
contract.

### 1. The platform states a release's scaling facts

Requested of the platform: `resolve-runtime-access` and the release detail
carry a `scaling` object.

| Field | Meaning |
| --- | --- |
| `min_instances` | Replicas kept when idle. `0` means the release scales to zero. |
| `idle_timeout_seconds` | How long the last replica stays after its last request. |
| `expected_cold_start_seconds` | The platform's measured time from wake request to serving, a high percentile over recent wakes of the active revision. `null` until measured. |
| `serving_since` | When the oldest current replica began serving. `null` at zero replicas. |

`min_instances` and `idle_timeout_seconds` are release configuration, changed
by the release's `editor` like its other scaling settings. The SDK reads all
four tolerantly ([ADR 0032](0032-tolerant-response-reading.md)): a response
without `scaling` reads as unknown, and the routine below then uses its
defaults.

Requested as well: a read of the same answer that does not wake the release,
for showing state without starting anything.

### 2. One routine calls a release

`ResourceRelease.request(method, path, *, timeout, ready_timeout=None,
idempotent=False, **kwargs)` and its asynchronous twin `arequest` are the way
an application calls another release. `ResourceRelease.by_uid(uid)` makes the
handle to call them on without fetching the release, so no line of the
documented pattern asks the platform unconditionally. `arequest` runs `request` on a worker
thread: the SDK's transport is `requests`, and a second transport is not worth
a dependency. Each call:

1. **Resolves access.** From the cache of [section 3](#3-a-process-cache-of-release-runtime-state)
   when the release is known warm, otherwise from `resolve-runtime-access`,
   which also wakes the release.
2. **Waits while `waking`.** It follows `retry_after_ms`, at least 100 ms
   apart, under the readiness limit. A state other than `ready` or `waking`
   ends the call at once.
3. **Sends the request** to the returned `rpc_url` with the returned token,
   under `timeout`. `timeout` starts here, so it measures the target's work
   and nothing else.

The readiness limit is `ready_timeout` when given; otherwise twice
`expected_cold_start_seconds`, no less than 30 seconds; otherwise
`MAINSEQUENCE_RELEASE_READY_TIMEOUT_SECONDS`, default 300.

`wait_for_runtime_access()` stays for code that wants access without sending
a request, and runs the same wait and fills the same cache. Concurrent first
calls to a cold release share one wait.

Agent message send keeps its own budget rule: waiting for a starting Agent
runtime spends the call's own timeout, as [ADR 0029](0029-a2a-message-send-only.md)
decided and its tests hold. It sends the deadline header of
[section 6](#6-a-deadline-travels-with-the-call). Giving it a separate
readiness limit would reverse that decision and is left to its owners.

### 3. A process cache of release runtime state

`mainsequence.client.release_runtime_cache` is one in-memory, thread-safe
cache per process, keyed by release UID. An entry holds the access grant and
its expiry, the `scaling` facts, the time of the last successful response from
the release, and the last cold start this process observed (when it began and
how long it took).

The routine skips step 1's platform call when the entry's token is still valid
and the release is known warm:

- `min_instances >= 1`, or
- the last sign that it was serving, a response below 500 or a `ready` answer
  from the platform, is younger than `idle_timeout_seconds` minus a margin
  (the larger of 10 seconds and 20% of the window). Without the platform's
  value the window is `MAINSEQUENCE_RELEASE_IDLE_TIMEOUT_SECONDS`, default 60.

Otherwise it asks the platform. The cache is an optimisation and never the
truth: another caller may have kept the target warm, a deployment may have
restarted it, and this process may be one of several replicas. A wrong "warm"
costs one held request, which [section 4](#4-failures-name-their-phase)
classifies. A wrong "cold" costs one platform call.

The cache is not persisted and not shared between processes. Applications do
not write their own: `ResourceRelease.runtime_status()` returns the entry
(`warm`, `cold` or `unknown`, the scaling facts, the last observed cold start)
for code that wants to decide something from it, such as showing "starting"
to a user.

### 4. Failures name their phase

| Error | Raised when |
| --- | --- |
| `ReleaseReadinessTimeoutError` | The readiness limit ended while the release was `waking`. Carries the limit, the last `phase` and the wake's `requested_at`. |
| `ReleaseUnavailableError` | The platform answered a terminal state. |
| `ReleaseColdStartTimeoutError` | `timeout` ended and the platform then reports the release `waking` or below one serving replica: the request was held by a start, not by the application. |
| `ReleaseRequestTimeoutError` | `timeout` ended and the release is serving: the application was slow or hung. |

After a request timeout the routine asks the platform once to tell the last
two apart, and forgets that the release was warm. A start that finished before
that question is recognised by `serving_since` being later than the send;
without `scaling` such a call reads as `ReleaseRequestTimeoutError`. It sends the request again only when
the caller passed `idempotent=True`, once, after waiting for readiness. A call
that may have reached the application is never replayed otherwise, as
[ADR 0038](0038-project-mcp-on-fastapi-releases.md) already holds for tool
calls.

### 5. The target reports its own start

`install_request_identity` adds two headers to the HTTP responses of a
verified caller, never to public ingress, preflight or refused requests:
`Mainsequence-Process-Started-At` (RFC 3339) and
`Mainsequence-Cold-Start: true` on the first such request a process serves.
The routine records the start time in the cache entry, and logs
`release.call.cold_start` with the target's UID and the measured wait when it
waited for a start. This is
how a project learns when each dependency last started and how long it took,
from its own logs.

### 6. A deadline travels with the call

An HTTP header `Mainsequence-Request-Timeout-Ms` carries the time the caller
will still wait, as a duration, so clocks need not agree.

- `install_request_identity` reads it into the request's scope.
- `request` inside that scope reduces its limits to what remains, forwards the
  remainder, and fails before sending, with `ReleaseDeadlineTooShortError`,
  when what remains is below `expected_cold_start_seconds` for a cold target.
  The error names the target, the remainder and the expected start, so the
  outermost caller learns which hop its limit could not cover.
- Without the header nothing is reduced.

Requested of the platform: the gateway forwards the header unchanged on
release requests and Agent Task dispatch, as it forwards `baggage` and
`traceparent` ([ADR 0040](0040-caller-context-on-releases-calls-and-runs.md)).
The value is advisory. The platform neither enforces nor trusts it.

### 7. Dependencies start together

`prewarm_releases(uids)` resolves access for each release without waiting, so
they start in parallel. An application calls it from its FastAPI lifespan with
the releases it depends on, and may call it at the start of a request that
will reach several. A chain of three cold releases then costs about the
slowest start, not the sum.

### 8. Long work is a Task, not a held request

A call whose warm duration plus the chain's starts can pass the gateway's
request limit is submitted and polled, as Agent message send already is
([ADR 0029](0029-a2a-message-send-only.md)). The routine does not hide this:
`ReleaseDeadlineTooShortError` and the cold-start timeout are the signals to
change the shape of the call.

### 9. A scaffold skill teaches it

A new scaffold skill, `application_surfaces/release_calls`, tells an agent
building a project:

- call another release only through `ResourceRelease.request` / `arequest`,
  never a hand-built URL, a stored token or a bare HTTP client;
- pass `timeout` as the target's own work time, and leave readiness to the
  routine;
- never keep a private "last used" clock or readiness cache: read
  `runtime_status()`;
- declare dependencies once and prewarm them in the lifespan;
- mark a call `idempotent=True` only when replaying it is safe;
- move a call that exceeds its deadline to submit-and-poll.

The API surfaces skill and the orchestration and releases skill link to it.

## Changes in each project

| Project | Change |
| --- | --- |
| Platform (Django, gateway) | The `scaling` object and its configuration; measuring cold starts; a read that does not wake; forwarding `Mainsequence-Request-Timeout-Ms`. Knative's own request and progress limits must exceed a release's expected start. |
| mainsequence-sdk | Sections 2 to 7 and 9; the Resource releases and FastAPI knowledge pages; Agent message send on the shared wait and cache. |
| ms-tau-sdk | It does not depend on this SDK, so it implements the same contract over `httpx`: its two readiness loops (MCP applications, credential hydrate releases) become one, with the limit rule of section 2, the cache rule of section 3 and the error phases of section 4, and its runtime forwards and honours the deadline header. Recorded there as an amendment to its ADR 0022. |
| Command Center and other HTTP clients | Read `scaling` to show "starting" rather than an error. No contract change. |

## Consequences

- A caller's `timeout` means the same thing warm and cold.
- A warm call costs no extra platform request. A cold or unknown one costs one.
- Three wait loops and three default limits become one rule.
- A project can answer, from its logs, which dependency was cold, when it
  started and how long that took.
- The readiness wait holds a worker of the caller while it lasts. Prewarming
  and submit-and-poll are the remedies. The routine does not queue.
- Until the platform returns `scaling`, the routine runs on defaults: every
  call to a release that showed no sign of serving in the last 50 seconds asks
  the platform first.

## Alternatives considered

- **The caller tracks last use and infers cold starts.** Rejected as the
  source of truth, for the reasons in section 3. Kept as the cache's
  warm-path rule.
- **Raise every timeout to cover the worst start.** Rejected: a hung
  application is then found after minutes, and chains multiply the padding.
- **Keep every release at one instance.** Rejected as a rule: it is the cost
  that scale to zero removes. It stays a per-release setting, which
  `min_instances` makes visible to callers.
- **Retry on timeout.** Rejected as a default: a held request may have
  reached the application. Opt-in through `idempotent=True`.
- **Carry the deadline in `baggage`.** Rejected: the platform gives baggage no
  meaning ([ADR 0040](0040-caller-context-on-releases-calls-and-runs.md)), and
  a deadline changes at every hop.
- **ms-tau-sdk depends on this SDK for the routine.** Not decided here. The
  contract is small enough to implement twice, and the dependency would bring
  `requests` and the ORM into the runtime.

## Implementation

In the SDK, on defaults until the platform returns `scaling`:

- `mainsequence/client/release_calls.py`: the routine, the cache
  (`release_runtime_cache`), `ReleaseRuntimeStatus`, the five errors and
  `prewarm_releases`. `ResourceRelease.request`, `arequest` and
  `runtime_status` call it.
- `ResourceReleaseScaling`, read as the optional `scaling` of
  `ResourceReleaseRuntimeAccess`.
- `mainsequence/_request_deadline.py` and `install_request_identity`: the
  deadline header in and out, and the two start headers.
- The scaffold skill `application_surfaces/release_calls`, the "Call a
  release" section of the Resource releases guide and "Start time and
  deadlines" in the FastAPI guide.
- Tests: `tests/client/test_release_calls.py` and one in
  `tests/test_fastapi_request_identity.py`, against a stand-in for the
  platform and a local HTTP server.

Not done:

- No call was made to a deployed release. Whether the gateway forwards
  `Mainsequence-Request-Timeout-Ms`, and passes the two response headers back,
  is unverified.
- Agent message send still spends its own timeout on a starting runtime (see
  section 2). It sends the deadline header and nothing else changed.
- The platform is unchanged. ms-tau-sdk follows the contract in its own code,
  recorded as an amendment to its ADR 0022.

## Open questions

1. Does the platform already hold `min_instances` and the idle window per
   release, and can it expose them without a migration?
2. Which percentile and window define `expected_cold_start_seconds`, and is
   it per revision or per release?
3. Should the platform start a release's declared dependencies when it wakes
   the release, making section 7 automatic?
4. The header names of sections 5 and 6.
