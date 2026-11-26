# ADR 0042: The SDK's hops in a platform trace


Date: 2026-11-26

Status: Proposed. Not accepted, not implemented.

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

## Implementation (after acceptance)

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
