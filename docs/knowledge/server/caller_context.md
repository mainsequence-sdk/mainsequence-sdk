# Caller context

The [caller assertion](caller_assertions.md) tells an application **who is
calling**. Caller context tells it **who the call is for**.

A service user, such as an
[external workload](../infrastructure/users_and_access.md#external-workloads)
that serves its own users, can send its own context with a call to Main
Sequence: which of its users the call is for, and its own information about the
call. It sends that context in the W3C [`baggage`](https://www.w3.org/TR/baggage/)
header. The W3C [`traceparent`](https://www.w3.org/TR/trace-context/) travels
with it and ties the work to one trace; the context itself is the baggage.

Main Sequence carries the context to the applications that run on it (FastAPI
releases, Job runs and Agents) and records it as the caller's claim. It never
uses it to authenticate, authorize, admit or bill a call, defines no keys, and
builds no feature on it. Applications use it to make their own rules and
filters, for example to return only the data of the person a call is for.

## Keys

Each member of the header is one key and its value:

```text
baggage: mainsequence.<application>.<name>=<value>,mainsequence.<application>.<name>=<value>
```

- Only keys of the form `mainsequence.<application>.<name>` are recorded and
  carried into the work a call creates, such as Job runs and Agent Tasks. Other
  keys stop there, and W3C properties (anything after a member's `;`) are
  dropped. Values are kept as received. Give every key that an application on
  Main Sequence must receive this form.
- The application chooses its segment and its names, with no registry and no
  approval. Main Sequence gives no key a meaning and treats every key the same
  way. Two applications that choose the same segment collide only on calls that
  cross both, and a collision grants nothing in Main Sequence.
- What the keys and values mean, including whether one refers to a person, is
  up to whoever designs the application.
- Main Sequence never parses a value. Encode a `,`, `;`, `=` or space inside a
  value with percent-encoding, as W3C Baggage requires; OpenTelemetry's
  propagators do it for you.
- **Values are public by contract.** Baggage is passed on downstream, to third
  parties too, and Main Sequence records it. Never put a secret, a credential
  or a proof in it.

## Trust

Anyone who can call an application can send it any context. Context is a
claim, and it is worth exactly as much as the application's trust in the caller
that delivered it. Main Sequence never signs it and never checks a value.

- **Believe context only from callers you trust to vouch for it**, checked
  against the verified caller at the hop where you read it. At a FastAPI
  release, that caller is `User.get_logged_user()`.
- **From any other caller, ignore it**, or treat it as that caller's own
  unverified claim.
- **Trust is per hop.** An application that receives a call from another
  application trusts that application to have checked the original caller. It
  does not see, and does not need, the original caller's identity.
- **Whoever may run a Job may set its runs' context.** A run carries the
  context of the request that created it, so a Job whose code makes rules from
  that context gives run permission only to service users it trusts; see
  [Who can use a Job](../infrastructure/scheduling_jobs.md#who-can-use-a-job).

What Main Sequence guarantees:

- at a release or an Agent, the context arrives next to the authenticated
  caller, as received;
- the context of a Job run or an Agent Task is exactly the context of the
  request that created it, never merged with another request's and never
  changed afterwards;
- no Main Sequence decision reads it.

It does not guarantee that any value is true.

Caller context is not a requester. It names no Main Sequence User and grants
nothing; an application that acts for a Main Sequence person does so only
through [requester-bound access](../infrastructure/users_and_access.md#applications-that-act-for-their-requester).

## Where context arrives

| Where the application runs | What it receives |
| --- | --- |
| A FastAPI release | The request's `baggage` and `traceparent` headers, on every request, next to the caller assertion. Never as environment variables: each request brings its own. |
| A Job run | `BAGGAGE` and `TRACEPARENT` environment variables, in W3C header format, when the run was created by a request that carried context. |
| An Agent Task | `baggage` and `traceparent` headers on the Task's dispatch signal. |

Work keeps the context of the request that created it: a Job run or an Agent
Task started by a call with context carries that context. A Job, its schedule,
a release or an Agent never stores one, so a scheduled run, or anything the
platform starts on its own, has no context.

A call a Job run makes to Main Sequence with its run credentials and no
`baggage` header is recorded with the run's own context, so the work it creates
inherits that context even when the run's code does not pass it on. A header,
when the run sends one, wins.

## Missing context

**Missing context means no external user.** Fail closed: refuse what needs a
person, and never fall back to broader access. Context can be missing because
the caller sent none, because it was malformed or too large, or because an
older SDK on the way did not pass it on. Context from a caller you do not trust
counts as missing too.

## Limits

The limits are those of OpenTelemetry's W3C baggage parser:

- a header over 8192 bytes is ignored whole;
- a member over 4096 bytes is skipped;
- at most 180 members are read.

A malformed member is skipped. Bad baggage never fails a request: what cannot
be read counts as missing. Keep context to a few short keys.

## With the SDK

The SDK gives no key a meaning and adds no accessor of its own: application
code reads the context with OpenTelemetry's standard API,
[`opentelemetry.baggage`](https://opentelemetry-python.readthedocs.io/en/latest/api/baggage.html).

- **FastAPI releases.** `install_request_identity(app)` makes each request's
  `baggage` the OpenTelemetry context of that request, next to the caller it
  verifies, and the request's own span its current span: the span the
  platform's launcher names for the request, or, without one (in local
  development, for example), the request's `traceparent` as its parent. See
  [Caller context](../fastapi/index.md#caller-context) on the FastAPI page.
- **Job runs.** `import mainsequence` restores `TRACEPARENT` and `BAGGAGE` as
  the process's OpenTelemetry context. See
  [Caller context in a run](../infrastructure/scheduling_jobs.md#caller-context-in-a-run).
- **Calls the SDK makes to the platform**, to the Main Sequence API, to
  releases, to Agents and for inference, carry the current `baggage` and a
  `traceparent` that names the call's own span (see [Traces](#traces)). A Job
  run or an Agent Task that a release starts while it handles a request
  therefore receives that request's context. A header you set yourself wins.
  Without a current context, a call carries no baggage and starts a new trace.
  A request to any other address carries neither header.
- **Calls your own code sends** to the platform with its own HTTP client, as a
  client library for an API on a release does, carry the same headers and
  write the same line when each send is wrapped in `ResourceRelease.call`, for
  a release, or in `dependency_call`, for another platform call; see
  [Calls a library sends itself](#calls-a-library-sends-itself).
- **Log lines** written through the SDK's logger carry the current context's
  `mainsequence.` keys as a `baggage` field, and its `trace_id`, `span_id` and
  `parent_span_id`.
- **Agents** built with the Agents SDK receive a Task's context on its dispatch
  signal; the Agents SDK documentation describes how their code reads it.

## Traces

A trace is the set of runtime log lines that share one W3C trace id. Each hop
writes one line that is its span, with `trace_id`, `span_id`, `parent_span_id`
and `duration_ms`: the platform writes those of the gateway, a release's
request, a Job run and an Agent's work, and the SDK writes one for each call it
makes to the platform, and for each call your own code wraps in
[`ResourceRelease.call` or `dependency_call`](#calls-a-library-sends-itself).
Spans are log lines only: the SDK exports nothing over OTLP and sets no
OpenTelemetry provider.

**The current span.** In a FastAPI release it is the request's own span, as
the platform's launcher names it; in a Job run it is the run's span, from
`TRACEPARENT`. It is a non-recording span: it has ids, and nothing records it.
A span your own OpenTelemetry instrumentation records wins.

**Every log line** written as JSON through the SDK's logger carries the current
span's `trace_id` and `span_id` at the top level, and `null` without a current
span. `parent_span_id` is set only for a span your own instrumentation
records; the hop's own line names the parent of the release's or run's span.
The nested `span` object, set only for a recording span, is still written next
to them; it is deprecated and will be removed in a later minor release.

**Each call the SDK makes to the platform** gets a new span id, a child of the
current span, sent as its `traceparent`, and writes one log line when it ends:

| Field | Value |
| --- | --- |
| `event` | `dependency.call.completed` when an answer arrived, whatever its status; `dependency.call.failed` when none did. |
| `trace_id`, `span_id` | The call's own span. |
| `parent_span_id` | The current span; `null` when the call started a new trace. |
| `dependency_type` | `api`, `release`, `agent` or `inference`. |
| `dependency_name` | What was called: `mainsequence_backend` for `api` and `inference`, the release's uid for `release`, the Agent session's uid for `agent`. |
| `http_method`, `route` | The method, and the path without its query, with every UUID replaced by `{uid}`. For a release or an Agent, the path on the release or the runtime. |
| `status_code` | The answer's status; `null` without an answer. |
| `duration_ms` | How long the call took, its retries included. |
| `outcome` | `success` for a status below 400, `rejected` for 400 to 499, `failed` for 500 and above or without an answer. |
| `error_type` | On `dependency.call.failed` only: the exception's class name, such as `ConnectionError` or `ReadTimeout`. |

These are the platform's own dependency-call field names and outcome values, so
the line is kept and read like the platform's own. The line never holds a URL,
a query string, a header or a body. A completed
call is written at `info` level and a failed one at `warning`. An API or
inference call is one span with its retries and its resend after a refreshed
credential; a release call replayed after a start, or an A2A message sent again
after a refused credential, is one span for each send.

**Reading a trace.** The log routes take an exact `trace_id`, 32 lowercase
hexadecimal characters, with the same access rules as the logs themselves; see
[Reading a trace](../infrastructure/owner_observability.md#reading-a-trace).

**Your own export.** An application that wants its own OpenTelemetry export
configures it itself, with the standard `OTEL_*` variables or
`opentelemetry-instrument`; the SDK no longer sets a provider before it, so the
provider it sets is the one used. `TracerInstrumentator().build_tracer()` and
`mainsequence.instrumentation.tracer` return a tracer from that provider, and a
no-op one while none is set.

## Calls a library sends itself

A library that sends requests to the platform with its own HTTP client makes
each of them a span itself. The usual case is a client library for an API that
runs as a release, which keeps a `requests` or `httpx` client for its
connection pool, its streaming or its test seams.

Without the span, such a call carries no trace context: the release's request
lines start a new trace, the release receives no caller context, and the
caller's logs have no line for the call. With it, the call is a child of the
current span, exactly as the SDK's own calls are, and writes the same
`dependency.call.completed` or `dependency.call.failed` line
([fields](#traces)).

When you need no client of your own, call the release with
[`ResourceRelease.request`](../infrastructure/resource_releases.md#call-a-release)
instead: it does everything below for you.

### A call to a release: `ResourceRelease.call`

`release.call(method, path, *, timeout, ready_timeout=None)` prepares one
request your client sends to the release, and does around it what `request`
does around its own:

```python
with release.call("GET", "/v1/prices", timeout=30) as call:
    response = client.get(call.url, headers=call.headers, timeout=call.timeout)
    call.record(response)
```

| When | What happens |
| --- | --- |
| Entering the block | It takes the address and token this process keeps while the release is warm and its token fresh. Otherwise it asks the platform, which starts the release, and waits while it starts, under `ready_timeout` as `request` does. |
| `call.url` | The address the platform gave, followed by `path`. |
| `call.timeout` | `timeout`, in seconds, cut to the time the current request's caller will still wait. Pass it to your client. |
| `call.headers` | `Authorization` with the release's token, `Mainsequence-Request-Timeout-Ms` with `call.timeout`, and `traceparent` and `baggage` naming the call's own span. Send them with the request; add your own headers after them. |
| `call.reused_access` | `True` when the kept address and token were used. |
| `call.record(response)` | Records the answer, from any response with `status_code` and `headers` (`requests`, `httpx`). |
| Leaving the block | An answer below status 500 is a sign that the release serves: `runtime_status()` shows it, and later calls, `request`'s too, reuse the access. A `401` to the kept access, an exception in the block, or a block that recorded no response makes the next call ask the platform. The `dependency.call.*` line is written. |

Entering raises what `request` raises before it sends, with nothing sent:
`ReleaseReadinessTimeoutError`, `ReleaseUnavailableError` or
`ReleaseDeadlineTooShortError`. What the send raises is your client's own
exception. `async with release.call(...)` works the same way; the wait for a
start runs on a worker thread.

Two things `request` does stay with the library, because it makes the send:

- **A refused kept token.** `request` sends once more after a `401` to the kept
  access. With `call`, when `response.status_code == 401` and
  `call.reused_access`, send a request that is safe to repeat once more, in a
  new block: that block asks the platform for a new token.
- **Telling a start from a hang.** After a timeout, `request` asks the platform
  whether a start held the request. With `call`, the timeout is your client's
  exception, the next call asks the platform, and
  `release.runtime_status()` shows what the platform last said.

**In a library**, put the block in the one method through which the library
sends, so every call, present and future, gets it:

```python
import requests

from mainsequence.client import ResourceRelease


class PricesClient:
    """A client for the Prices API release, with one connection pool for its life."""

    def __init__(self, release_uid: str, session: requests.Session | None = None):
        self.release = ResourceRelease.by_uid(release_uid)
        self.session = session or requests.Session()

    def _send(self, method: str, path: str, *, timeout: float, **kwargs) -> requests.Response:
        with self.release.call(method, path, timeout=timeout) as call:
            headers = {**call.headers, **(kwargs.pop("headers", None) or {})}
            response = self.session.request(
                method, call.url, headers=headers, timeout=call.timeout, **kwargs
            )
            call.record(response)
            return response

    def price(self, symbol: str) -> dict:
        response = self._send("GET", "/v1/prices", params={"symbol": symbol}, timeout=30)
        response.raise_for_status()
        return response.json()
```

With `httpx`, the block wraps the same send, and a streamed response is read
inside it, so the span covers the whole body:

```python
async with release.call("GET", "/v1/prices/export", timeout=300) as call:
    async with client.stream(
        "GET", call.url, headers=call.headers, timeout=call.timeout
    ) as response:
        call.record(response)
        async for chunk in response.aiter_bytes():
            sink.write(chunk)
```

### Any other platform call: `dependency_call`

For a call that is not to a release, such as one to the Main Sequence API or
an Agent runtime, `mainsequence.instrumentation.dependency_call` makes the
span alone:

```python
from mainsequence.instrumentation import dependency_call

with dependency_call("agent", "POST", "/api/a2a/v1/message:send", name=session_uid) as call:
    response = client.post(url, headers={**call.headers(), **auth}, json=message)
    call.status_code = response.status_code
```

`dependency_call(dependency_type, method, path, *, name=None)` returns a
`DependencyCall`, used as a context manager:

| Argument | Meaning |
| --- | --- |
| `dependency_type` | `release` or `agent` for a call to an address the platform gave, `api` or `inference` for a call to the origin of `MAINSEQUENCE_ENDPOINT`. These are `mainsequence.instrumentation.DEPENDENCY_TYPES`; another value raises `ValueError`. For a release, prefer `ResourceRelease.call`, which also manages the access and the deadline. |
| `method` | The HTTP method. |
| `path` | The path the request adds to that address, such as `/v1/prices`, not the address's own path. It is written as `route`, without its query and with every UUID replaced by `{uid}`. |
| `name` | The release's uid for `release`, the Agent session's uid for `agent`; required for both. It is written as `dependency_name`. `api` and `inference` write `mainsequence_backend`. |

**The rules**, for `dependency_call` and `ResourceRelease.call` alike:

- **One send, one block.** Open the block just before the request is sent and
  leave it after the response's body is read, so `duration_ms` covers the
  exchange. A retry your client makes inside the block belongs to that span; a
  request your code sends again is a new block.
- **Send the block's headers with that request.** They hold `traceparent`,
  which names the call's own span, and the current `baggage`. A header of your
  own with the same name wins, so add yours after them.
- **Record the answer** as soon as it arrives, whatever its status
  (`call.status_code = ...` for `dependency_call`, `call.record(response)` for
  `ResourceRelease.call`). The block writes `dependency.call.completed` with
  it, and `dependency.call.failed` when it raises or when no answer was
  recorded: `error_type` is the exception's class name, or `NoResponse`.
- **Only to the platform.** Send the headers only to the origin of
  `MAINSEQUENCE_ENDPOINT` and to addresses the platform gave. The baggage is
  the caller's context: it must not reach a third party, and the SDK cannot
  check where your client sends.
- **Your client stays yours.** The block sends nothing. Your client's pool,
  retries and streaming are unchanged, and no OpenTelemetry configuration is
  needed: the line is written through the SDK's logger, as the SDK's own lines
  are. Outside any context, as in a script, the call starts a new trace.

**Testing a library.** Send its calls to a local server that records request
headers, inside a known span: the `traceparent` it receives has that span's
trace id and a new span id. For a release call, also check that two calls in
a row to a warm release ask the platform once, and that the
`Mainsequence-Request-Timeout-Ms` it receives is no more than its caller's
remaining time. The SDK's lines go to `mainsequence.logconf.logger`; replace it
in a test to read the line's `span_id`, `parent_span_id` and
`dependency_name`.

## Example

Advisor is an invented application: a service outside Main Sequence, connected
as the external workload `advisor-backend`, that calls the FastAPI release
`portfolio-api` for its own users. It chooses two keys:

- `mainsequence.advisor.person`: Advisor's opaque id for the person;
- `mainsequence.advisor.request`: Advisor's id for the request.

**1. `advisor-backend` sends its context** with each call it makes as its
external workload, signed in with
[workload identity](../infrastructure/auth.md#workload-identity-auth):

```text
traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
baggage: mainsequence.advisor.person=p-7f3a9c,mainsequence.advisor.request=r-0042
```

With OpenTelemetry, the service sets the keys on its context and its
propagator writes the headers:

```python
from opentelemetry import baggage, propagate

context = baggage.set_baggage("mainsequence.advisor.person", "p-7f3a9c")
context = baggage.set_baggage("mainsequence.advisor.request", "r-0042", context=context)
headers = {}
propagate.inject(headers, context=context)  # W3C baggage, and traceparent when tracing
```

**2. `portfolio-api` checks its caller, then reads the person.** It believes
an Advisor person only when the verified caller is `advisor-backend`'s workload
User, whose UID it read from a call that workload made:

```python
from fastapi import FastAPI, HTTPException
from opentelemetry import baggage

from mainsequence.client import User
from mainsequence.server.fastapi import install_request_identity

ADVISOR_BACKEND = "<workload User UID of advisor-backend>"

app = FastAPI()
install_request_identity(app)


def advisor_person() -> str | None:
    """The Advisor person this call is for, or None."""
    if User.get_logged_user().uid != ADVISOR_BACKEND:
        return None  # only advisor-backend vouches for Advisor people
    return baggage.get_baggage("mainsequence.advisor.person")


@app.get("/portfolio")
def portfolio():
    person = advisor_person()
    if person is None:
        raise HTTPException(status_code=403, detail="This operation needs an Advisor person.")
    return load_portfolio(person)  # that person's data, and nothing else
```

A call from any other caller gets no Advisor person, whatever its baggage says.
A call from `advisor-backend` without context gets none either.

**3. The work it starts keeps the context.** While it handles the request,
`portfolio-api` runs the Job `portfolio-rebalance` with `job.run_job()`. The
SDK passes the request's context on, so the run starts with `BAGGAGE` and
`TRACEPARENT`:

```python
import mainsequence  # restores the run's TRACEPARENT and BAGGAGE
from opentelemetry import baggage

person = baggage.get_baggage("mainsequence.advisor.person")
if person is None:
    raise SystemExit("This run needs an Advisor person.")  # a scheduled run has none
```

Run permission on `portfolio-rebalance` is given to `portfolio-api`'s workload
and to no other service user, so the run can trust its context.

**4. Main Sequence records the context as a claim** on the work it carried, and
no Main Sequence decision reads it.
