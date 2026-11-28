# ADR 0041: Cold-start-aware calls between releases

Date: 2026-11-10

Status: Proposed. The SDK's part is implemented and runs on defaults; see
[Implementation](#implementation). The platform changes in
[section 1](#1-the-platform-states-a-releases-scaling-facts) and
[section 6](#6-a-deadline-travels-with-the-call) are requests to the platform,
not observations of it, and nothing here was tried against a deployed release.
From 9.0.45 a library's own HTTP client can call a release the same way; see
[Amendment 2026-11-27](#amendment-2026-11-27-a-call-a-librarys-own-client-sends).
From 9.0.46 the platform's retry hint, not the state, decides whether a call
waits; see
[Amendment 2026-11-27](#amendment-2026-11-27-the-retry-hint-decides-the-wait).
From 9.0.46 such a library lets the SDK's `ReleaseCallError` reach its caller
unchanged; see
[Amendment 2026-11-27: a library lets the SDK's errors through](#amendment-2026-11-27-a-library-lets-the-sdks-errors-through).

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
2. **Waits while the platform asks it to.** It follows `retry_after_ms`, at
   least 100 ms apart, under the readiness limit. An answer without a retry
   hint ends the call at once. (Amended 2026-11-27: the hint decides, not the
   state; see
   [the amendment](#amendment-2026-11-27-the-retry-hint-decides-the-wait).)
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
| `ReleaseUnavailableError` | The platform answered that the release cannot take requests, with no retry hint. `ReleaseNotDeployedError`, a subclass, when it has no active revision. |
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
- From 9.0.46 an application reads and narrows the same time with the public
  `mainsequence.client.remaining_request_seconds()` and `request_deadline()`;
  see [Amendment 2026-11-27: the remaining time is public](#amendment-2026-11-27-the-remaining-time-is-public).

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
- move a call that exceeds its deadline to submit-and-poll;
- let `ReleaseCallError` reach the caller unchanged, so the caller still learns
  whether anything was sent; a library's own errors are for its own failures
  and the target's answers (added 2026-11-27; see
  [the amendment](#amendment-2026-11-27-a-library-lets-the-sdks-errors-through)).

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

## Amendment 2026-11-27: a call a library's own client sends

Issue #265. A client library that keeps its own `requests` or `httpx` client,
for its connection pool, its streaming or its test seams, could not call a
release the way `request` does. The caller's deadline helpers were private, so
its call was not bounded by the caller's remaining time and passed none on.
`wait_for_runtime_access()` asks the platform on every call and never reuses
the access this process keeps, and only `request` records answers in
`release_runtime_cache`. Such a library asked the platform before every call,
or kept the token and let a release that went idle start inside its own
timeout, and `runtime_status()` never showed its calls. Since 9.0.44 it could
make each call a span with `dependency_call` (ADR 0042), but nothing more.

Decision, from 9.0.45:

- **`ResourceRelease.call(method, path, *, timeout, ready_timeout=None)`**
  returns a `ReleaseCall`, used with `with` or `async with`, that prepares one
  request the library's client sends. Entering does what `request` does before
  it sends: the kept access while the release is warm and its token fresh,
  otherwise the platform is asked and the call waits under the readiness limit
  of section 2, and the caller's remaining time is checked
  (`ReleaseDeadlineTooShortError`). It then exposes `url`, `timeout` (cut to
  the caller's remaining time), `headers` (`Authorization`, the deadline
  header of section 6, and the span's `traceparent` and `baggage`) and
  `reused_access`.
- **Leaving does what `request` does after it sends.** The library passes its
  response to `record()`. An answer below 500 is recorded as a sign that the
  release serves. A `401` to the kept access, an exception in the block or a
  block that recorded nothing invalidates the kept access, so the next call
  asks the platform. The `dependency.call.*` line is written.
- **Two things stay with the library.** It makes the send, so it resends after
  a refused kept token (`reused_access` says when that applies), and a timeout
  is its own client's exception: the call does not ask the platform whether a
  start held it, as `request` does. The next call asks, and `runtime_status()`
  shows the platform's answer.
- **`request` and `call` share one access path**, `_access_for_call`, so they
  reuse each other's warm access and wait for a cold release together.
- **Not done:** making `_request_deadline` public, a `release=` argument on
  `dependency_call` (it would tie instrumentation to the release cache), and a
  pooled or streaming `request`.

Tests: `tests/client/test_release_calls.py` covers the deadline and its header
inside a caller's deadline, a spent deadline, warm reuse and the idle window,
a refused kept token, an exception or no answer, a server error, the record of
a process start, `runtime_status()` and a later `request` seeing a library's
answers, `async with`, and a library's own `requests.Session` reusing one
connection to a local server.

## Amendment 2026-11-27: the remaining time is public

Issue #270. Section 6 gave the caller's remaining time to the SDK's own calls
only: `request`, and from 9.0.45 `call` and A2A sends, read it through the
private `mainsequence._request_deadline`. An application's own work, such as a
FastAPI release that bounds each SQL statement, could not read it, and could
not apply its own limit to the release calls it makes inside a request.

Decision, from 9.0.46:

- **`mainsequence.client.remaining_request_seconds()`** returns the seconds
  the current request's caller will still wait: zero or less once it has
  passed, and `None` without a limit (no header, a public request, a Job run,
  a script).
- **`mainsequence.client.request_deadline(seconds)`** bounds the block by
  `seconds` or by the caller's remaining time, whichever is shorter. It never
  extends a limit, and `None` changes nothing. The SDK's calls inside the
  block, and `remaining_request_seconds()`, see the shorter limit.
- **They are the functions the SDK uses**, exported from `mainsequence.client`,
  not from `mainsequence.server.fastapi`: the value bounds client calls and is
  read by code that is not a FastAPI app, and it reads `None` outside a
  request instead of failing.
- **Not made public:** the header name and its formatting. A library sending
  to a release uses `ResourceRelease.call`, which sends the header.

The value lives in a context variable: FastAPI's thread pool, `asyncio` tasks
and `asyncio.to_thread` carry it, and a plain `threading.Thread` or a
`concurrent.futures` executor does not.

Tests: `tests/test_fastapi_request_identity.py` reads and narrows the time in
a sync and an async handler, with and without the header;
`tests/client/test_release_calls.py` bounds a release call and a `request`
by a narrower `request_deadline`.

## Amendment 2026-11-27: a library lets the SDK's errors through

Issue #267. The skill of [section 9](#9-a-scaffold-skill-teaches-it) and the
guides described the five `ReleaseCallError` types and what to do about each,
but gave a library that calls a release no rule for them. One client library
caught them and raised its own `RuntimeError`, "did not become available" or
"is unavailable", for what the SDK reports as `ReleaseReadinessTimeoutError`
or `ReleaseUnavailableError`. Its caller could no longer tell whether the
request was sent, which decides whether repeating a write is safe, and lost
the release's UID and phase and the guidance tied to each type.

Decision, from 9.0.46:

- **Code that calls a release lets `ReleaseCallError` reach its caller
  unchanged**, through `request` or `release.call(...)`. A library does not
  catch these errors to raise its own or fold them into its own error class.
  Its own errors are for its own failures and for the target's answers, such
  as a `404` or `409`.
- **Context is added, not substituted**: `error.add_note(...)` and a bare
  `raise`. A broad `except` (`Exception`, `ApiError`, `TimeoutError`,
  `OSError`, `RuntimeError`) is preceded by `except ReleaseCallError: raise`.
- **"Sent" stays apart from "not sent" with `release.call`.** Entering the
  block raises only the three errors that mean nothing was sent. An exception
  the library's own client raises inside the block means the request may have
  been sent, and is never turned into an error that says nothing was.
- The `release_calls` skill states the rule, in its general rules and for a
  library's send method, and its Verify section checks it. The "Failures"
  section of the Resource releases guide and the Caller context guide's
  `ResourceRelease.call` section say the same.
- **Not done:** no change to the SDK's code. The SDK cannot stop a library
  catching an exception; the rule lives in the guidance an agent reads when it
  writes one. `tests/test_scaffold_skills.py` fails if the rule leaves the
  skill.

## Amendment 2026-11-27: the retry hint decides the wait

Issue #269. The wait of [section 2](#2-one-routine-calls-a-release) waited
only while `runtime_access.state` was `waking`, and a `TransportError` from the
platform ask ended it. A client with its own readiness loop could not move to
`ResourceRelease.call`: it waited through a rollout and retried a failed ask,
and the SDK did neither.

The platform settled the contract. A release that serves stays `ready`
through a rollout, whatever `runtime_presence.phase` says. The one temporary
`unavailable` left, evidence the platform is still recording, is to carry
`retry_after_ms` and the notice code `runtime_evidence_pending`; that is agreed
but not yet deployed. Every other refusal carries no hint.
The rule for a client: `can_request` sends; otherwise a `retry_after_ms` asks
again after that long, within the client's own budget; otherwise it stops and
shows the notice. Neither the phase nor the notice code decides.

Decision, from 9.0.46:

- **`wait_for_release` asks again whenever the answer carries
  `retry_after_ms`**, under the readiness limit and the caller's deadline, and
  stops at once on an answer without it, `redeploying` included, and `waking`
  too (amended for 9.0.47: the SDK codes against the current platform, which
  gives every `waking` answer a hint). `ReleaseReadinessTimeoutError` names the last state and the platform's notice.
- **A failed ask is made again.** A `TransportError` from
  `resolve-runtime-access` is asked again after 2 seconds, within the same
  limits, up to three times in a row; the third is raised. The POST is safe to
  repeat: it only asks, and wakes the release.
- **`ReleaseNotDeployedError(ReleaseUnavailableError)`** when the answer's
  `routing.active_revision_uid` is `None`: the release has no revision to wake.
  It is read from routing, not from the `not_deployed` phase, which also covers
  an active revision whose Service vanished.
- **AgentSession message send follows the same rule.** Its loop also asks
  again when a state outside its transient set carries `retry_after_ms`.
- `request`, `call` and `wait_for_runtime_access()` share the wait, so all
  three change. Tests: `tests/client/test_release_calls.py` and
  `tests/client/test_a2a_request_budget.py`.
- **Not done:** a failed AgentSession platform ask is not retried.

## Amendment 2026-11-27: a call for the request's person

Issue #272. A FastAPI application serving a person's request could call another
FastAPI release only as itself. The platform now accepts the request's caller
assertion on `resolve-runtime-access` and gives a token that names the calling
application as the caller and the person as the requester.

Decision, from 9.0.47:

- **A per-call option.** `request`, `arequest`, `call`,
  `resolve_runtime_access` and `wait_for_runtime_access` take
  `act_for_requester=True`. A block would make every call inside it, including
  a library's, act for the person; the option keeps that choice on the call
  (see ADR-0036's amendment of the same date).
- **The assertion goes only to the platform**, on `resolve-runtime-access`, as
  `X-MainSequence-Caller-Assertion`, and only to the configured platform
  endpoint. The called release never receives it.
- **The person's token is never the release's kept access** of
  [section 3](#3-a-process-cache-of-release-runtime-state). It is kept in the
  request's own scope, per release, until the token or the request ends. The
  platform's facts about the release (presence, scaling, whether it serves)
  still go to the process cache. A delegated call never shares another call's
  cold-start wait, so no token crosses between requests.
- **Every ask for the person sends the assertion again:** while waiting for a
  start, after a timeout and after a refused token. None falls back to the
  application's own access.
- **The wait ends at the assertion's expiry.** The assertion lives at most 300
  seconds; a wait whose next ask would come after it raises
  `ReleaseRequesterError` with `expired=True`, before the platform refuses it.
- **Errors.** `403` `requester_binding_invalid` is `ReleaseRequesterError`, a
  `ReleaseCallError` with `phase == "readiness"`: nothing was sent. It is never
  retried as the application. The option raises `RequestIdentityError` before
  any platform call outside an authenticated request, without an assertion
  (local mode, WebSockets) and in a requester-bound request.
- **Not done:** detecting a platform that does not serve this contract. The SDK
  codes against the current platform contract only.
