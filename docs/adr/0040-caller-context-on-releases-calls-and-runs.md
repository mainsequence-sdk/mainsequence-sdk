# ADR 0040: Caller context on release requests, SDK calls and Job runs

Date: 2026-11-07

Status: Accepted 2026-11-07 and implemented in the SDK; see
[Implementation](#implementation).

Owners: Main Sequence SDK maintainers.

Related: [ADR-0036](0036-request-scoped-logged-user.md) (the request-scoped
caller), the [Caller context](../knowledge/server/caller_context.md) page,
[W3C Baggage](https://www.w3.org/TR/baggage/),
[W3C Trace Context](https://www.w3.org/TR/trace-context/).

## Context

A service user, such as an external workload that serves its own users, can
send its own context with a call to Main Sequence in the W3C `baggage` header:
which of its users the call is for, and its own information about the call.
The platform carries that context to the applications that run on it and
records it. It never authenticates, authorizes, admits or bills on it, defines
no keys and builds no feature on it. Applications make their own rules and
filters from it. Tracing is a secondary benefit.

The platform's side of the contract, as an application sees it:

- **Keys.** Only members named `mainsequence.<application>.<name>` are carried
  to Job runs and Agent Tasks and recorded. Other keys and W3C properties
  (after a member's `;`) are dropped. Values are kept as received.
- **Limits.** OpenTelemetry's `W3CBaggagePropagator` limits: a header over 8192
  bytes is ignored whole, a member over 4096 bytes is skipped, at most 180
  members are read.
- **Releases.** The gateway forwards `baggage` and `traceparent` on every
  request, next to the caller assertion, whose claims do not change. Context
  never travels inside the assertion.
- **Job runs.** A run created by a request that carried context starts with
  `BAGGAGE` and `TRACEPARENT` environment variables in W3C header format.
  Scheduled and platform-started runs have none.
- **Run calls.** A call a run makes with its run credentials and no `baggage`
  header is recorded with the run's stored context. A header, when sent, wins.
- **Agent Tasks** get `baggage` and `traceparent` headers on their dispatch
  signal. That side belongs to the Agents SDK.

The trust rule belongs to the application: believe context only from callers
it trusts to vouch for it, checked against the verified caller at the hop where
it reads it; ignore it from anyone else; treat missing context as no external
user and fail closed; trust per hop. The platform never signs the context.
Whoever may run a Job may set its runs' context.

What the SDK did before this decision, read from source at `e6c75d25`:

- No HTTP instrumentation. `TracerInstrumentator.get_telemetry_carrier()`
  injected `traceparent` only, and no SDK code called it, so the SDK's calls
  carried neither header.
- `install_request_identity` verified the caller and read no trace header.
- Nothing read `TRACEPARENT` or `BAGGAGE`.
- `import mainsequence` sets an OpenTelemetry SDK `TracerProvider` when none is
  set; the SDK starts no span of its own.

## Decision

Each part is additive. An application on an SDK without it keeps working and
carries no context. No route, request body or response changes. The SDK
treats every baggage key alike and gives none a meaning.

### 1. Release requests

`install_request_identity` makes each request's headers the OpenTelemetry
context of that request, attached around the application and detached when it
returns, fails or is cancelled.

- **Baggage is exactly the request's own.** The request's context starts from
  the current context with its baggage cleared, then adds the request's
  `baggage` header. A process-wide context, or another request's, never leaks
  into a request.
- **Only next to a verified caller.** A request that the integration
  authenticated (signed HTTP, local Bearer, WebSocket ticket) gets its baggage.
  A public-ingress or preflight request, which has no verified caller to vouch
  for it, gets none.
- **Parsed as the platform parses it.** The whole-header limit is checked
  first, in bytes. W3C properties are then dropped from each member before the
  values are decoded, so an encoded `;` inside a value survives and each value
  reads as the platform records it. `W3CBaggagePropagator` then applies its
  per-member and member-count limits. Without this step OpenTelemetry's
  parser keeps a member's properties inside its value and would pass them on
  percent-encoded as part of the value.
- **Every member is kept**, not only `mainsequence.` keys: this is
  OpenTelemetry's context, and the application may use other keys of its own.
  Only logs filter by the prefix (decision 4).
- **The trace parent** is the request's `traceparent`, unless instrumentation
  already records a span for the request, which is then kept.
- **Bad input never fails a request.** Malformed members are skipped. The
  parser's warnings quote the header, so the SDK drops them while it parses,
  and only then.

Repeated `baggage` headers are joined with `,`, as HTTP combines repeated
fields.

### 2. Calls the SDK makes

`get_telemetry_carrier()` injects both W3C headers of the current context,
`traceparent` and `baggage`, with the W3C propagators themselves rather than
the globally configured one, because the platform's contract is W3C.

The carrier is added per request, never to a shared session's headers, at the
SDK's calls to Main Sequence and to applications on it:

- `make_request`, which every client model uses;
- `request_within_budget`, which A2A sends and A2A Task reads and cancels use;
- the inference client.

A header the caller sets wins. Without a current context the carrier is empty
and the request is exactly as before. Token exchanges and refreshes, key
discovery and cloud identity endpoints get no context: they are not calls for
anyone. Neither does the Job start-up state read, which the platform records
with the run's own context anyway.

### 3. Process start

`import mainsequence`, right after the credential bootstrap and before the
first log line or call, attaches `TRACEPARENT` and `BAGGAGE`, when either is
set, as the importing thread's OpenTelemetry context, parsed as in decision 1.
The platform sets them only on Job runs; reading them wherever they are set is
OpenTelemetry's environment carrier convention, and lets a developer reproduce
a run locally.

Python threads do not inherit context variables, so a thread a Job starts
itself begins without the context unless it runs in a copy of it. The
documentation says so. The platform's run-call rule still records such a
thread's calls with the run's context.

A release process must not take a process-wide context; decision 1 clears it
for every request, so even a release started with these variables gives each
request only its own baggage.

### 4. Logs

A structlog processor adds the current context's `mainsequence.` keys as a
`baggage` field to every line written through the SDK's logger, structlog and
standard-library lines alike, at the moment the line is written. A line with
no such key gets no field, and a line that already has a `baggage` field keeps
its own.

### 5. Reading

Application code reads the context with OpenTelemetry's standard API,
`opentelemetry.baggage`. The SDK adds no accessor and no key name.

### 6. Names

The only names this decision adds are the ones already approved for the
platform's contract: the environment variables `TRACEPARENT` and `BAGGAGE`
and the log field `baggage`. Every new function, class and variable is
private. No version, route or contract changes.

## Consequences

- A FastAPI release can apply its own rules for the users of a service it
  trusts, and the Job runs and Agent Tasks it starts receive the same context.
- The strength of every such rule is the application's check of its caller.
  The SDK documents the trust rule and cannot enforce it.
- An application that skips the check can be given any context by anyone
  allowed to call it, but never gains anything in Main Sequence.
- A local shell that sets `BAGGAGE` for another tool passes that tool's keys on
  from SDK processes it starts. The platform drops keys outside
  `mainsequence.`, and logs show only those.
- Context propagates to the tasks a request starts, which keep it after the
  request ends, as OpenTelemetry context does; the caller identity does not.

## Alternatives considered

- **OpenTelemetry's ASGI and `requests` instrumentation packages.** Not added
  as dependencies: they would forward context to every host the application
  calls and start spans the application did not ask for. An application that
  installs them keeps working: a span they start for the request before the SDK
  reads it is kept, and only the baggage is replaced.
- **A Main Sequence accessor or key constants.** Rejected: the SDK gives no
  key a meaning, and OpenTelemetry's API is what applications already know.
- **Keeping only `mainsequence.` keys in the request's context.** Rejected:
  the context is OpenTelemetry's, and other keys may be the application's own.
- **Setting the headers on the shared session.** Rejected: one session serves
  many requests at once, so a request could send another's context.
- **Falling back to the process environment when a request or thread has no
  context.** Rejected: missing context must stay missing, and in a release a
  process-wide context would give every request the same external user.
- **Baggage on public-ingress requests.** Rejected: no verified caller can
  vouch for it.

## Implementation

- `mainsequence/instrumentation/utils.py`: `get_telemetry_carrier()` and the
  private parsing, request, environment and log helpers.
- `mainsequence/server/fastapi.py`: the request context in the request
  identity middleware.
- `mainsequence/client/utils.py`, `mainsequence/_transport.py`,
  `mainsequence/client/inference.py`: the outbound carrier.
- `mainsequence/__init__.py`: the environment carriers at import.
- `mainsequence/logconf.py`: the log processor.
- `tests/test_caller_context.py` covers each decision, including two invented
  applications' keys carried alike, a request without baggage inside a process
  that has some, two concurrent requests with their own context, the parser
  limits, and an `import mainsequence` subprocess that restores a run's
  carriers for its calls and logs.
