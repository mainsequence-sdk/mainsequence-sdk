---
name: mainsequence-release-calls
description: Call one Main Sequence release (FastAPI API, MCP application or Agent-backing release) from another with the SDK's cold-start-aware routine. Use when an application, Job or Agent tool makes HTTP requests to another release, when choosing timeouts for such calls, when a call times out and the target may have been scaled to zero, or when a chain of releases (API to API to Agent) needs deadlines, prewarming or a "starting" state.
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
2. ADR 0041, "Cold-start-aware calls between releases", in the SDK's
   `docs/adr`.

If the installed SDK has no `ResourceRelease.request`, report the missing
capability. Do not replace it with a hand-written wait loop.

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

Never keep the release's URL or token yourself: the routine obtains and renews
both.

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
  not a URL built from a name or UID.
- **`idempotent=True` only when a repeat is safe.** With it, a request that
  timed out while the target was starting is sent once more. Without it,
  nothing is ever sent twice. A read is safe. A write is safe only when the
  target deduplicates it.
- **Do not catch and retry timeouts yourself.** The errors below say what
  happened; a blind retry can run the target's work twice.

## Errors

All are `ReleaseCallError`, importable from `mainsequence.client`. The timeout
ones are also `TimeoutError`.

| Error | Meaning | What to do |
| --- | --- | --- |
| `ReleaseReadinessTimeoutError` | The release was still starting when the readiness limit ended. Nothing was sent. | Report "starting, try again". Check the release's deployment if it repeats. |
| `ReleaseUnavailableError` | The platform says the release cannot take requests. Nothing was sent. | Report it. Do not retry in a loop. |
| `ReleaseDeadlineTooShortError` | Your own caller will not wait long enough for the target to start. Nothing was sent. | The outermost caller's limit is too short for this chain, or the work should be submit-and-poll. |
| `ReleaseColdStartTimeoutError` | The request was sent and timed out while the target was starting. | Safe to repeat only for an idempotent call. |
| `ReleaseRequestTimeoutError` | The request was sent and timed out on a running target. | The target is slow or hung. Read its logs. |

Each carries `resource_release_uid`, `phase` (`readiness` or `request`),
`timeout_seconds` and, when known, `runtime_phase`.

An HTTP error status is not an exception: the response is returned.

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
caller will still wait. Calls made with `request` inside that handler are
bounded by it, and what remains is sent on to the next release. Nothing is
needed in application code.

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
  `ReleaseReadinessTimeoutError` and that nothing was sent.
- No module imports `requests` or `httpx` to reach a release.
- No module stores a release's `rpc_url` or token.
- Logs show `release.call.cold_start` with the target's UID when a start was
  waited for.
