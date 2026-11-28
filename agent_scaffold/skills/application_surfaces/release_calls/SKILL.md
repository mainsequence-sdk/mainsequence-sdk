---
name: mainsequence-release-calls
description: Call one Main Sequence release (FastAPI API, MCP application or Agent-backing release) from another with the SDK's cold-start-aware routine. Use when an application, Job or Agent tool makes HTTP requests to another release, when choosing timeouts for such calls, when a call times out and the target may have been scaled to zero, when a chain of releases (API to API to Agent) needs deadlines, prewarming or a "starting" state, or when a client library sends its own requests to a release (its own requests or httpx client) with ResourceRelease.call, so they keep the caller's trace, deadline and the process's release access, or when deciding what code or a library does with a ReleaseCallError.
---

# Calls Between Main Sequence Releases

Releases scale to zero. A request to a release with no replica is held until
one starts, which can take minutes. From the caller's side a held request and
a hung application look the same, so a timeout chosen for the application's
work fires on a healthy call.

The SDK solves this in one place. Use it; do not build your own.

## Read First

1. <https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/infrastructure/resource_releases/>,
   section "Call a release".
2. <https://mainsequence-sdk.github.io/mainsequence-sdk/knowledge/server/caller_context/>,
   sections "Traces" and "Calls a library sends itself".
3. ADR 0041, "Cold-start-aware calls between releases", and ADR 0042, "The
   SDK's hops in a platform trace", in the SDK's `docs/adr`.

If the installed SDK has no `ResourceRelease.request`, report the missing
capability. Do not replace it with a hand-written wait loop. If it has no
`ResourceRelease.call`, a library with its own HTTP client cannot call a
release the way `request` does: report that, and do not copy the SDK's private
code.

## The one way to call a release

```python
from mainsequence.client import ResourceRelease

prices = ResourceRelease.by_uid("<RESOURCE_RELEASE_UID>")

response = prices.request("POST", "/v1/prices", timeout=60, json={"symbol": "ABC"})
response.raise_for_status()
```

`by_uid` makes a handle from the UID and contacts nothing. Do not use
`ResourceRelease.get(pk=...)` to call a release: it asks the platform for the
release's details every time it runs.

In asynchronous code, such as a FastAPI handler:

```python
response = await prices.arequest("POST", "/v1/prices", timeout=60, json={"symbol": "ABC"})
```

What the routine does:

1. Makes sure the release is ready. It asks the platform, which also starts
   the release, unless this process already knows the release is warm.
2. Waits while the release starts, under a separate readiness limit.
3. Sends the request. `timeout` starts only now.
4. Makes the call a span in the caller's trace: the request carries
   `traceparent` and the caller's `baggage`, and one `dependency.call.completed`
   or `dependency.call.failed` log line is written with `dependency_type`
   `release` and `dependency_name` the release's UID.

Never keep the release's URL or token yourself: the routine obtains and renews
both. A client library that must keep its own HTTP client uses
`release.call(...)`, which obtains them the same way; see "A client library
with its own HTTP client" below.

`request` asks the platform only when it must: on the first call in the
process, when the release has shown no sign of serving within its idle window,
when its token is about to expire, and after a timeout. With steady traffic it
goes straight to the target.

The target sees the call as coming from your application's own identity.

## Rules

- **`timeout` is the target's work time.** Pass how long the target's own
  work may take when it is already running. Never add the start time to it.
- **Leave readiness to the routine.** Pass `ready_timeout` only when the
  product has a real limit on how long a start may take. The default follows
  the start time the platform measures, or 300 seconds
  (`MAINSEQUENCE_RELEASE_READY_TIMEOUT_SECONDS`).
- **No private clocks or caches.** Do not record "last used" times, ping
  endpoints to keep a release awake, or cache readiness. The SDK keeps one
  cache per process. To decide something from it, read
  `release.runtime_status()`.
- **No bare HTTP clients for release calls.** Not `requests`, not `httpx`,
  not a URL built from a name or UID. The only exception is a client library
  that keeps its own client, and then every send is made inside
  `release.call(...)`, as described below.
- **`idempotent=True` only when a repeat is safe.** With it, a request that
  timed out while the target was starting is sent once more. Without it,
  nothing is ever sent twice. A read is safe. A write is safe only when the
  target deduplicates it.
- **Do not catch and retry timeouts yourself.** The errors below say what
  happened; a blind retry can run the target's work twice.
- **Let `ReleaseCallError` through.** Code that calls a release, with
  `request` or `release.call`, lets the errors below reach its caller
  unchanged. Never catch one to raise an error of your own, and never fold
  them into a library's error class: the error says whether anything was
  sent, carries the release's UID and phase, and the timeout ones are
  `TimeoutError`, and a replacement loses all of that. Your own errors are
  for your own failures and for the target's answers, such as mapping its
  `404` or `409`. Broader clauses catch them too: `except Exception` and
  `except ApiError` catch all five (a `ReleaseCallError` is an `ApiError`),
  `except TimeoutError` and `except OSError` the timeout ones, and
  `except RuntimeError` `ReleaseUnavailableError`, so put
  `except ReleaseCallError: raise` before such a clause. To say what the call
  was for, add a note and re-raise the same exception:

  ```python
  from mainsequence.client import ReleaseCallError

  try:
      response = prices.request("GET", "/v1/prices", timeout=30)
  except ReleaseCallError as error:
      error.add_note("reading prices for the risk report")
      raise
  ```

## Errors

All are `ReleaseCallError`, importable from `mainsequence.client`. The timeout
ones are also `TimeoutError`.

| Error | Meaning | What to do |
| --- | --- | --- |
| `ReleaseReadinessTimeoutError` | The release was still starting when the readiness limit ended. Nothing was sent. | Report "starting, try again". Check the release's deployment if it repeats. |
| `ReleaseUnavailableError` | The platform says the release cannot take requests and gave no retry hint. Nothing was sent. | Report it. Do not retry in a loop. |
| `ReleaseNotDeployedError` | A `ReleaseUnavailableError`: the release has no active revision yet. Nothing was sent. | Report "not deployed yet". It needs a deployment, not a retry. |
| `ReleaseRequesterError` | A call with `act_for_requester=True` was refused for the person (`expired` is `False`), or the person's assertion ran out first (`True`). Nothing was sent. | Never retry as the application. Refused: the caller is not a person, or may not use the target. Expired: the work outlived the person's request. |
| `ReleaseDeadlineTooShortError` | Your own caller will not wait long enough for the target to start. Nothing was sent. | The outermost caller's limit is too short for this chain, or the work should be submit-and-poll. |
| `ReleaseColdStartTimeoutError` | The request was sent and timed out while the target was starting. | Safe to repeat only for an idempotent call. |
| `ReleaseRequestTimeoutError` | The request was sent and timed out on a running target. | The target is slow or hung. Read its logs. |

Each carries `resource_release_uid`, `phase` (`readiness` or `request`),
`timeout_seconds` and, when known, `runtime_phase`.

An HTTP error status is not an exception: the response is returned.

## Calling for the person a request serves

On a route people call, pass `act_for_requester=True` to `request`,
`arequest` or `call` when the target must act for that person, for example
to apply the person's own access:

```python
response = billing.request("GET", "/v1/invoices", timeout=30, act_for_requester=True)
```

- The target sees the person in `User.get_requester()` and your application in
  `User.get_logged_user()`.
- Only inside a person's direct request: it raises `RequestIdentityError`
  outside an authenticated request, in local mode, on a WebSocket and in a
  requester-bound request, and the platform refuses a workload caller with
  `ReleaseRequesterError`. Do not use it on routes workloads call, or for
  background work: the person's assertion lasts at most 300 seconds.
- Per call, never a block: the option goes on each call that should act for the
  person, so no other call, and no library's call, does so by accident.
- The person's token is kept only for the rest of the request, never for the
  process.

## A client library with its own HTTP client

Use `request` whenever you can. A client library for an API on a release may
instead need its own `requests` or `httpx` client: one connection pool for its
whole life, streamed responses, or its own test seam. It then sends each call
inside `release.call(...)`, which does around the library's send what
`request` does around its own. Without it, every call breaks the trace, skips
the caller's deadline, and asks the platform every time or misses a release
that went idle.

Put the block in the one method through which the library sends:

```python
import requests

from mainsequence.client import ResourceRelease


class PricesClient:
    def __init__(self, release_uid: str, session: requests.Session | None = None):
        self.release = ResourceRelease.by_uid(release_uid)
        self.session = session or requests.Session()  # kept: one connection pool

    def _send(self, method: str, path: str, *, timeout: float, **kwargs) -> requests.Response:
        with self.release.call(method, path, timeout=timeout) as call:
            headers = {**call.headers, **(kwargs.pop("headers", None) or {})}
            response = self.session.request(
                method, call.url, headers=headers, timeout=call.timeout, **kwargs
            )
            call.record(response)
            return response
```

What the block does:

- **Entering** reuses the address and token this process keeps while the
  release is warm and its token fresh, or asks the platform, which starts the
  release, and waits while it starts. It raises `ReleaseReadinessTimeoutError`,
  `ReleaseUnavailableError` or `ReleaseDeadlineTooShortError` with nothing
  sent, as `request` does.
- **`call.url`** is the platform's address followed by `path`;
  **`call.timeout`** is `timeout` cut to the caller's remaining time;
  **`call.headers`** hold `Authorization`, `Mainsequence-Request-Timeout-Ms`,
  `traceparent` and `baggage`.
- **Leaving** records an answer below 500, so `runtime_status()` and later
  calls see it; after a `401` to the kept access (`call.reused_access`), an
  exception, or no recorded response, the next call asks the platform. It
  writes the `dependency.call.*` line.

Rules for the block:

- **Pass `call.url`, `call.headers` and `call.timeout` to your client**, and
  `call.record(response)` as soon as the answer arrives, whatever its status.
  Add your own headers after `call.headers`.
- **One send, one block.** Leave the block after the body is read; with
  `httpx`, read a streamed response inside it. A request sent again is a new
  block. `async with release.call(...)` works in asynchronous code.
- **Let the SDK's errors through, and keep "sent" apart from "not sent".**
  Entering the block raises only the three errors that mean nothing was
  sent; let them reach your caller unchanged. An exception your client raises
  inside the block means the request may have been sent: never map it to an
  error that says nothing was sent, and do not repeat a write after it.
- **Resend after a refused token yourself.** When the answer is `401` and
  `call.reused_access` is `True`, a request that is safe to repeat may be sent
  once more, in a new block, which asks the platform for a new token.
- **A timeout is your client's exception.** Do not retry it blindly: the next
  call asks the platform, and `release.runtime_status()` shows whether the
  release was starting.
- **Do not keep `call.url` or the token** between blocks, and do not copy the
  SDK's private code (`_transport`, `_request_deadline`, `release_calls`
  internals) or write `dependency.call.*` lines yourself.

For a platform call that is not to a release, such as one to an Agent runtime,
`mainsequence.instrumentation.dependency_call` makes the span alone; see the
Caller context guide.

## Show state without calling

```python
status = prices.runtime_status()
status.state                    # "warm", "cold" or "unknown"
status.last_cold_start_at       # when the last observed start began
status.last_cold_start_seconds  # how long it took
status.expected_cold_start_seconds  # from the platform, or None
```

`runtime_status()` never contacts the platform and never starts the release.
`unknown` is a normal answer for a release this process has not called
recently.

## Start dependencies together

Declare the releases an application depends on once, and start them when the
application starts:

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI
from mainsequence.client import ResourceRelease, prewarm_releases

PRICES = ResourceRelease.by_uid("<PRICES_RELEASE_UID>")
RISK = ResourceRelease.by_uid("<RISK_RELEASE_UID>")


@asynccontextmanager
async def lifespan(app: FastAPI):
    prewarm_releases([PRICES, RISK])
    yield


app = FastAPI(lifespan=lifespan)
```

`prewarm_releases` returns at once and never raises. A chain of cold releases
then costs about the slowest start, not the sum. Call it at the start of a
request too when the request will reach several releases one after another.

## Deadlines across a chain

An application with `install_request_identity(app)` reads how long its own
caller will still wait. Calls made with `request` or `release.call` inside that
handler are bounded by it, and what remains is sent on to the next release.
Nothing is needed in application code for those calls.

The application's own work is bounded only if it asks. Use the public
functions from `mainsequence.client`, never `mainsequence._request_deadline`:

```python
from mainsequence.client import remaining_request_seconds, request_deadline

with request_deadline(60):  # the API's own limit, cut by its caller's
    remaining = remaining_request_seconds()  # never None inside the block
    rows = database.execute(sql, timeout=min(15, remaining))
    response = prices.request("POST", "/v1/enrich", timeout=30, json=rows)
```

- `remaining_request_seconds()` is `None` when no caller set a limit, and zero
  or less once the time has passed: stop then, the caller no longer waits.
- `request_deadline(seconds)` only shortens; the release calls inside it see
  the shorter limit and send it on.
- A plain `threading.Thread` or `concurrent.futures` executor does not carry
  the deadline; run such work with `contextvars.copy_context().run(...)`.

The limit travels in the `Mainsequence-Request-Timeout-Ms` header. A hop that
does not receive the header is bounded only by its own `timeout`, so do not
rely on the chain's limit to stop work: keep each call's `timeout` honest.

Set the limit once, at the outermost caller, as that caller's `timeout`. It
must cover the whole chain's work plus one start.

## When to stop holding a request

Change the call to submit-and-poll when its warm work time plus a start can
pass a few minutes, or when `ReleaseDeadlineTooShortError` or
`ReleaseColdStartTimeoutError` recur:

- For an Agent, use the A2A skill
  (`.agents/skills/mainsequence/a2a_sdk_execution/SKILL.md`) with a Task
  response.
- For an API, expose one endpoint that accepts the work and returns an id,
  and one that reports its state. Call both through `request`.

## Verify

- A test calls the code with the target answering `waking` and then `ready`,
  and asserts the request is sent once, after readiness.
- A test with the target answering `waking` past `ready_timeout` asserts
  that the code's caller receives `ReleaseReadinessTimeoutError` itself, not
  an error of the library's, and that nothing was sent.
- No module imports `requests` or `httpx` to reach a release, except a client
  library's single send method, and that method sends inside
  `release.call(...)`.
- No module stores a release's `rpc_url` or token.
- No module catches a `ReleaseCallError` to raise a different exception, and
  every broad `except` around a release call (`Exception`, `ApiError`,
  `TimeoutError`, `OSError`, `RuntimeError`) is preceded by
  `except ReleaseCallError: raise`.
- For a client library: a test sends calls to a local server inside a known
  span and a caller deadline, and asserts the received `traceparent` has that
  span's trace id and a new span id, the received
  `Mainsequence-Request-Timeout-Ms` is within the deadline, two calls in a row
  share one connection, and two calls to a warm release ask the platform once.
- Logs show `release.call.cold_start` with the target's UID when a start was
  waited for.
