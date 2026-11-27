# ADR 0042: The SDK's hops in a platform trace


Date: 2026-11-26

Status: Accepted 2026-11-26, by the owner's instruction to implement it, and
implemented in the SDK in 9.0.42; see [What was built](#what-was-built). The
event's field names changed in 9.0.43; see
[Amendment 2026-11-27](#amendment-2026-11-27-the-platforms-field-names). From
9.0.44 code outside the SDK can make its own calls spans; see
[Amendment 2026-11-27: calls a library sends itself](#amendment-2026-11-27-calls-a-library-sends-itself).

Owners: Main Sequence SDK maintainers.

Related: [ADR 0040](0040-caller-context-on-releases-calls-and-runs.md)
(caller context), the [Caller context](../knowledge/server/caller_context.md)
page, the [owner observability](../knowledge/infrastructure/owner_observability.md)
page, [W3C Trace Context](https://www.w3.org/TR/trace-context/).

## Context

The platform's side, as an application sees it:

- **A trace is a set of log events.** The platform's runtime log events that
  share one W3C trace id form a trace. Each boundary writes one terminal
  event: the gateway, the release's request handler, a Job run, an Agent's
  turn, model call and tool call. That event is the hop's span, with its
  `trace_id`, `span_id`, `parent_span_id` and `duration_ms`.
- **Where the events are kept.** The platform keeps them with the runtime
  logs, in the cloud account that ran the work. It serves them through the
  existing log routes, which gain an exact `trace_id` filter, with the same
  access rules as logs.
- **A release request.** The platform's launcher writes the request's
  terminal event and puts that request's `trace_id` and `span_id` in the ASGI
  scope, under `scope["state"]`.
- **A Job run.** Every run starts with `TRACEPARENT` naming the run's own
  span. A run created with caller context continues that trace. A scheduled
  run is the root of a new trace.
- **No export.** The platform exports no spans over OTLP and sets no `OTEL_*`
  variable. Spans are log lines.

What the SDK does today, read from source at `ce06e203`:

- **Import-time tracer.** `import mainsequence` calls
  `TracerInstrumentator.build_tracer()`. When no provider is set, it sets the
  global OpenTelemetry `TracerProvider` with `service.name=tdag` and no span
  processor, so every span opened in the process is recorded and dropped.
  OpenTelemetry refuses a second global provider, so an application that sets
  its own after importing the SDK is ignored.
- **Crash with `OTLP_ENDPOINT`.** With `OTLP_ENDPOINT` set, the import raises
  `AttributeError: 'TracerInstrumentator' object has no attribute
  'agent_host'`.
- **No `OTEL_*` variables.** They are not read, because `Resource(...)` is
  used instead of `Resource.create()`.
- **Requests forward the caller's span.** `install_request_identity` attaches
  the inbound `traceparent` as a remote parent and opens no span. Every call
  the SDK makes while handling the request therefore sends the caller's
  `traceparent` unchanged. The next hop records the release's caller as its
  parent, and the release itself drops out of the tree.
- **Log lines lose the ids.** The `plain` JSON log formatter nests the ids
  under `span` and writes `"span": null` unless a span is recording. A
  request with only a remote parent, or a Job run with only `TRACEPARENT`,
  therefore logs no trace id at all.
- **Platform calls leave no record.** The SDK's own calls to the platform
  (the API, release calls, A2A sends, inference) write no event of their own.
- **The carrier reaches any URL.** The carrier is added to every request
  `make_request` sends, whatever its URL.

## Decision

### 1. The SDK never sets a global TracerProvider

- **Import.** The import-time `build_tracer()` call is removed.
- **`build_tracer()` stays for compatibility.** It returns
  `trace.get_tracer("mainsequence")` from whatever provider is set. It never
  sets a provider, never adds a processor and never reads `OTLP_ENDPOINT`.
  That removes the crash.
- **Applications.** An application that wants its own OpenTelemetry export
  configures it itself, with the standard `OTEL_*` variables or
  `opentelemetry-instrument`, and the SDK no longer stands in its way.
  Libraries that open spans through the SDK (data-node updaters, `mcp`) get a
  no-op tracer when no provider is set, which is cheaper than today's
  recorded-and-dropped spans.

### 2. The current span is the hop's own span

- **FastAPI.** The request-identity middleware reads `scope["state"]["trace_id"]`
  and `scope["state"]["span_id"]` and attaches them as the current span. That
  is a non-recording span context, not a remote parent.
  - The inbound `baggage` is attached as today (ADR 0040).
  - Without that state, for example in local development without the
    platform launcher, the middleware falls back to today's remote parent.
  - A span that is already recording, opened by an application's own
    instrumentation, still wins.
- **Job runs.** `TRACEPARENT` already names the run's span, so attaching it
  at import (ADR 0040) stays as it is. The run's calls become its children.

### 3. A call to the platform is a span

For each request the SDK sends to a platform destination (the Main Sequence
API, a release call, an A2A send, get or cancel, an inference call), the SDK:

1. mints a span id and sends `traceparent` with the current trace id and that
   span id, plus `baggage` as today;
2. writes one log event, `dependency.call.completed` or
   `dependency.call.failed`, with:
   - `trace_id`, `span_id`, and `parent_span_id` (the current span);
   - `dependency_kind`: `api`, `release`, `agent` or `inference`;
   - the target release or Agent uid when the SDK knows it;
   - `http_method`, and `route` (the URL path with every UUID replaced by
     `{uid}`);
   - `status_code`, `duration_ms`, `outcome`, and `error_type` on failure.

URLs with their query strings, headers and bodies are never written.

A call made while no context is attached, such as a script outside a run,
starts a new trace.

### 4. Log lines carry the ids at the top level

Every structured log line carries `trace_id`, `span_id` and `parent_span_id`
as top-level fields, taken from the current span context whether or not it is
recording. The nested `span` object is written next to them for one minor
release and then removed. The change is listed in the CHANGELOG.

### 5. The carrier goes only to the platform

`traceparent` and `baggage` are sent only to these destinations:

- the origin of `MAINSEQUENCE_ENDPOINT`;
- the release and Agent addresses the platform gave the SDK.

A request `make_request` sends to any other origin carries neither header. In
practice the SDK sends nothing else today, so this only states the rule. It
also replaces the need for a "call without context" helper.

### 6. Nothing to flush

The events are log lines written synchronously to stdout, the stream the pods
already log to. The SDK adds no exporter, no exit hook and no SIGTERM
handler. `opentelemetry-api` is the only OpenTelemetry package the SDK needs.
Whether `opentelemetry-sdk` and `opentelemetry-exporter-otlp` move to an
optional extra is a separate packaging decision, because applications may
rely on getting them through the SDK.

### 7. Reading traces

`get_logs()` and `search_logs()` accept `trace_id` and pass it to the
platform's log routes. The rows already have `trace_id`, `span_id`,
`parent_span_id`, `duration_ms` and `occurred_at`. A trace is the rows of one
`trace_id`, built into a tree by `parent_span_id`. A span whose parent is
missing (an owner the caller cannot see, or a Job run) hangs under its owner.
`OwnerLogRow` does not change.

## Consequences

- An application that set its own `TracerProvider` before importing the SDK
  keeps it. One that set it after importing now gets it, instead of having it
  silently ignored.
- Data-node updaters that call `build_tracer()` once per run no longer risk
  adding a processor on each call.
- The SDK writes one extra log line for each platform call it makes.

## Implementation

1. Remove the import-time tracer and make `build_tracer()` inert. Fix the
   crash. Add a test that `import mainsequence` with `OTLP_ENDPOINT` set
   leaves the provider untouched.
2. Adopt the launcher's request span in the middleware, and write top-level
   log ids.
3. Write the platform-call events, propagate the minted span id, and limit
   the carrier to platform destinations.
4. Add `trace_id` to `get_logs()` and `search_logs()`, released together with
   the platform's filter (wire contract).
5. Update docs: `caller_context.md`, `owner_observability.md`,
   `scheduling_jobs.md` (threads), and the CHANGELOG.

## What was built

In 9.0.42, as decided above, with these refinements read from the code:

- **`tracer` is still exported.** `mainsequence.instrumentation.tracer` is
  kept for code that imports it, now `trace.get_tracer("mainsequence")`: a
  proxy that uses whatever provider the application sets, now or later. It
  configures nothing. The tracer is named `mainsequence` instead of `tdag`.
  `TracerInstrumentator` keeps all its methods; `get_current_trace_id()` now
  also reads a non-recording span instead of raising.
- **The launcher's ids are checked.** They are adopted only when `trace_id` is
  32 and `span_id` 16 lowercase hexadecimal characters, neither all zeros;
  otherwise the request falls back to its `traceparent`. The adopted span keeps
  the inbound flags and `tracestate` when it is in the same trace, and is
  sampled otherwise. Public and preflight requests adopt it too: the ids are
  the launcher's, not the caller's.
- **`parent_span_id` on ordinary log lines** is set only for a recording span.
  A non-recording span carries no parent in OpenTelemetry, and guessing it
  from the inbound `traceparent` could be wrong; the hop's own event, written
  by the platform, names the parent of a request's or run's span.
- **The events' values.** `dependency.call.completed` is written when an
  answer arrived, whatever its status, at `info`; `dependency.call.failed` when
  none did, at `warning`, with `error_type` set to the exception's class name
  (`NoResponse` if there is none). `outcome` is `ok` below status 400 and
  `error` otherwise. An API or inference call is one span with its retries and
  its resend after a refreshed credential; a release call replayed after a
  start, or an A2A message sent again after a refused credential, is one span
  for each send. A field a log call sets itself wins over the current
  context's ids.
- **The target of an `agent` call** is `target_agent_session_uid`: what the
  SDK knows when it sends an A2A message, gets or cancels a task is the
  session, not always the Agent. A `release` call has `target_release_uid`.
- **`route` for releases and Agents** is the path the SDK appended to the
  address the platform gave (`/v1/prices`, `/api/a2a/v1/message:send`), not the
  address's own path, which is opaque and server-issued. Query strings are
  dropped from every route.
- **Platform destinations.** A `make_request` call is a platform call when its
  scheme, host and port equal those of the endpoint the SDK uses
  (`MAINSEQUENCE_ENDPOINT`). `request_within_budget` carries the carrier and
  writes an event only when its caller passes the call as a `dependency`,
  which release calls and A2A calls do. Inference calls the API. Requests the
  SDK sends with `requests` directly (credential exchange and refresh, the
  logger's start-up state) were never given the carrier and still are not.
- **A call outside any context** now carries a `traceparent` that starts a new
  trace; before, it carried no header. It still carries no baggage.

Tests: `tests/test_platform_trace.py` and `tests/test_instrumentation.py`, with
the caller-context, release-call, A2A and inference tests updated.

Not verified: no call was made to a deployed platform. The launcher's
`scope["state"]` ids, every run's `TRACEPARENT` and the log routes' `trace_id`
filter are the platform's side and were not available to test against.

## Amendment 2026-11-27: the platform's field names

Section 3 named the event's fields `dependency_kind`, `target_release_uid` and
`target_agent_session_uid`, with `outcome` `ok` or `error`, and 9.0.42 wrote
them so. The platform's runtime log contract already defines dependency-call
fields, and one of its log stores keeps only the fields it knows, so an event
with the 9.0.42 names reached its reader without saying what it called. From
9.0.43 the event uses the platform's names and values:

- `dependency_type`: the kind, `api`, `release`, `agent` or `inference`, as
  `dependency_kind` held it;
- `dependency_name`: what was called: `mainsequence_backend` for `api` and
  `inference`, the release's uid for `release`, the Agent session's uid for
  `agent`;
- `outcome`: `success` below status 400, `rejected` from 400 to 499, and
  `failed` from 500 or without an answer, the values of the platform's own
  request events.

`trace_id`, `span_id`, `parent_span_id`, `http_method`, `route`,
`status_code`, `duration_ms` and `error_type` are unchanged.
`dependency_kind`, `target_release_uid` and `target_agent_session_uid` are no
longer written, with no compatibility period: 9.0.42 was released for hours
only. `OwnerLogRow` now declares the dependency-call fields the platform's
rows return (`dependency_type`, `dependency_name`, `dependency_operation`,
`dependency_attempt`, `dependency_duration_ms`, `dependency_outcome`,
`dependency_status_code`, `dependency_error_type`, `circuit_breaker_state`),
so a trace reader gets them typed.

## Amendment 2026-11-27: calls a library sends itself

Issue #263. A client library for an API on a release typically finds the
release with the public `ResourceRelease.resolve_runtime_access()` and sends
its calls with its own `requests` or `httpx` client, for its connection pool,
its streaming or its test seams. Section 3 made only the SDK's own sends spans,
through the private `_DependencyCall`, so such a library's calls carried no
trace context: the release's request lines started a new trace, the release
received no caller context, and the caller's logs had no line for the call.
`ResourceRelease.request` does make the span, but opens a connection for each
call, refuses `stream` and takes no client of the caller's.

Decision, from 9.0.44:

- **A public call span.** `mainsequence.instrumentation.dependency_call`,
  given `dependency_type`, `method`, `path` and, for a release or an Agent,
  `name`, returns the span the SDK uses for its own calls, now public as
  `DependencyCall`. Its `headers()` are the
  carrier to send; the library sets `status_code` when an answer arrives;
  leaving the `with` block writes the section 3 event, with the field names of
  the amendment above. The library keeps its own client, pool, retries and
  streaming.
- **Its arguments are checked.** `dependency_type` is one of
  `DEPENDENCY_TYPES` (`api`, `release`, `agent`, `inference`), and `release`
  and `agent` need `name`, the target's uid, so every line names what was
  called. `path` is the path the library adds to the platform's address, which
  becomes `route` as in section 3.
- **Section 5 covers these calls.** The platform destinations are the same:
  the origin of `MAINSEQUENCE_ENDPOINT` and the release and Agent addresses the
  platform gave, now also when the library, not the SDK, sends to an address
  it obtained through the SDK's public API. The SDK cannot see where a library
  sends, so the rule is stated in `dependency_call`'s documentation, the
  [Caller context](../knowledge/server/caller_context.md#calls-a-library-sends-itself)
  guide and the `release_calls` skill, and keeping to it is the library's
  part.
- **No transport is added.** A release-bound session that would also wait for
  a start and renew the token (the issue's second shape) and a pooled,
  streaming `ResourceRelease.request` (its third) are not part of this
  decision. A library that keeps its own client waits and renews with
  `wait_for_runtime_access()` and `access["expires_at"]`.

Tests: `tests/test_platform_trace.py` sends a library's calls through
`dependency_call` with its own `requests.Session` to a local HTTP/1.1 server,
inside a release request's span, and checks that the received `traceparent`
is a child of that span whose id is the written line's `span_id`, and that two
calls in a row came over one connection.
