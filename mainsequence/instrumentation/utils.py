import logging
import os
import re
import secrets
import time
from contextvars import ContextVar
from urllib.parse import urlsplit

import structlog
from opentelemetry import baggage, trace
from opentelemetry import context as otel_context
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.trace import (
    NonRecordingSpan,
    SpanContext,
    TraceFlags,
    get_current_span,
    get_tracer,
)
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator


def is_port_in_use(port: int, agent_host: str) -> bool:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((agent_host, port)) == 0


class TracerInstrumentator:
    __doc__ = """
        Reads and passes on the current OpenTelemetry context. It never
        configures OpenTelemetry: that belongs to the application (ADR 0042).
    """

    def build_tracer(self) -> trace.Tracer:
        """A tracer from whatever provider is set; a no-op one while none is.

        Kept for compatibility. It never sets a provider, never adds a span
        processor and never reads ``OTLP_ENDPOINT`` (ADR 0042).
        """
        return get_tracer("mainsequence")

    def get_current_trace_id(self):
        current_span = get_current_span()
        return format(current_span.get_span_context().trace_id, "032x")

    def get_telemetry_carrier(self):
        """The W3C ``traceparent`` and ``baggage`` headers of the current context.

        Empty when the current context has neither. The SDK sends them on its
        calls to Main Sequence and to the applications on it (ADR 0040).
        """
        telemetry_carrier = {}
        TraceContextTextMapPropagator().inject(carrier=telemetry_carrier)
        W3CBaggagePropagator().inject(carrier=telemetry_carrier)
        return telemetry_carrier

    def append_attribute_to_current_span(self, attribute_key, attribute_value):
        current_span = get_current_span()
        current_span.set_attribute(attribute_key, attribute_value)


def add_otel_trace_context(logger, method_name, event_dict):
    """
    Enrich log records with OpenTelemetry trace context (trace_id, span_id).

    ``trace_id``, ``span_id`` and ``parent_span_id`` are top-level fields of
    every line, from the current span context whether or not it is recording,
    and ``None`` without one; a field the log call sets wins (ADR 0042). Only a
    recording span knows its parent. The nested ``span`` object is written as
    before, for one more minor release.
    """
    span = trace.get_current_span()
    ctx = span.get_span_context()
    recording = span.is_recording()
    parent = getattr(span, "parent", None) if recording else None
    ids = {
        "trace_id": format(ctx.trace_id, "032x") if ctx.is_valid else None,
        "span_id": format(ctx.span_id, "016x") if ctx.is_valid else None,
        "parent_span_id": format(parent.span_id, "016x") if parent else None,
    }
    for key, value in ids.items():
        event_dict.setdefault(key, value)

    if not recording:
        event_dict["span"] = None
        return event_dict

    event_dict["span"] = ids

    return event_dict


class OTelJSONRenderer(structlog.processors.JSONRenderer):
    """
    A custom JSON renderer that injects OTel trace/span fields
    immediately before serializing to JSON.
    """

    def __call__(self, logger, method_name, event_dict):
        # 1) Grab the current active span from OpenTelemetry
        event_dict = add_otel_trace_context(logger, method_name, event_dict)

        # 3) Now call the base JSONRenderer to produce final JSON
        return super().__call__(logger, method_name, event_dict)


# Caller context (ADR 0040): the W3C ``traceparent`` and ``baggage`` an
# application receives and passes on. The SDK carries every baggage key the
# same way and gives none of them a meaning.

_parsing_baggage: ContextVar[bool] = ContextVar("mainsequence_parsing_baggage", default=False)


class _QuietBaggageParser(logging.Filter):
    """Drop the parser's warnings while the SDK parses: they quote the header."""

    def filter(self, record):
        return not _parsing_baggage.get()


logging.getLogger("opentelemetry.baggage.propagation").addFilter(_QuietBaggageParser())


def _with_baggage_header(header, context):
    """``context`` with the members of one W3C ``baggage`` header added.

    The parser's limits apply: a header over 8192 bytes is ignored whole, a
    member over 4096 bytes is skipped and at most 180 members are read. W3C
    properties, after a member's ``;``, are dropped, as the platform drops them,
    so each value reads as the platform records it. Malformed input is skipped
    and never raises.
    """
    if not header or len(header.encode()) > W3CBaggagePropagator._MAX_HEADER_LENGTH:
        return context
    # Properties follow an unencoded ';', so an encoded ';' inside a value survives.
    members = ",".join(member.split(";", 1)[0] for member in header.split(","))
    token = _parsing_baggage.set(True)
    try:
        return W3CBaggagePropagator().extract({"baggage": members}, context=context)
    except Exception:
        return context
    finally:
        _parsing_baggage.reset(token)


_TRACE_ID = re.compile(r"[0-9a-f]{32}")
_SPAN_ID = re.compile(r"[0-9a-f]{16}")


def _launcher_span(state, inbound):
    """The request's own span, as the platform's launcher names it in ``scope["state"]``.

    ``None`` unless ``trace_id`` and ``span_id`` are valid W3C ids. The inbound
    span's flags and ``tracestate`` are kept when it is in the same trace.
    """
    trace_id, span_id = (state or {}).get("trace_id"), (state or {}).get("span_id")
    if not (
        isinstance(trace_id, str)
        and isinstance(span_id, str)
        and _TRACE_ID.fullmatch(trace_id)
        and _SPAN_ID.fullmatch(span_id)
        and int(trace_id, 16)
        and int(span_id, 16)
    ):
        return None
    same_trace = inbound.is_valid and inbound.trace_id == int(trace_id, 16)
    return SpanContext(
        int(trace_id, 16),
        int(span_id, 16),
        is_remote=False,
        trace_flags=inbound.trace_flags if same_trace else TraceFlags(TraceFlags.SAMPLED),
        trace_state=inbound.trace_state if same_trace else None,
    )


def _request_context(carrier, *, with_baggage, state=None):
    """The OpenTelemetry context of one inbound request, from its headers.

    Its baggage is exactly the request's ``baggage`` header, or none without
    ``with_baggage``: never the process's or another request's. Its current
    span is the request's own span that the platform's launcher put in
    ``state`` (ADR 0042), as a non-recording span. Without that, its trace
    parent is the request's ``traceparent``. A span that instrumentation
    already records for the request wins over both.
    """
    context = baggage.clear(context=otel_context.get_current())
    if with_baggage:
        context = _with_baggage_header(carrier.get("baggage"), context)
    if not trace.get_current_span(context).is_recording():
        context = TraceContextTextMapPropagator().extract(carrier, context=context)
        own = _launcher_span(state, trace.get_current_span(context).get_span_context())
        if own is not None:
            context = trace.set_span_in_context(NonRecordingSpan(own), context)
    return context


def _attach_environment_context():
    """Attach a Job run's ``TRACEPARENT`` and ``BAGGAGE`` as the process's context.

    The platform starts a run created by a request that carried caller context
    with both variables, in W3C header format. Without either, nothing changes.
    The context is attached to the calling thread, as any OpenTelemetry context.
    """
    traceparent = os.environ.get("TRACEPARENT")
    baggage_header = os.environ.get("BAGGAGE")
    if not traceparent and not baggage_header:
        return
    context = _with_baggage_header(
        baggage_header, baggage.clear(context=otel_context.get_current())
    )
    if traceparent:
        context = TraceContextTextMapPropagator().extract(
            {"traceparent": traceparent}, context=context
        )
    otel_context.attach(context)


def _add_log_baggage(logger, method_name, event_dict):
    """Add the current context's ``mainsequence.`` baggage keys as ``baggage``."""
    if "baggage" not in event_dict:
        members = {
            key: str(value)
            for key, value in baggage.get_all().items()
            if key.startswith("mainsequence.")
        }
        if members:
            event_dict["baggage"] = members
    return event_dict


# Calls to the platform (ADR 0042): each is a span, written as one log event.

_UUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")


def _route(path):
    """A URL's path, without its query, with every UUID replaced by ``{uid}``."""
    return _UUID.sub("{uid}", urlsplit(path).path or "/")


# What ``dependency_name`` holds for a call to the Main Sequence API.
_PLATFORM_API = "mainsequence_backend"


def _outcome(status_code):
    """The platform's outcome of a call: ``status_code`` is ``None`` without an answer."""
    if status_code is None or status_code >= 500:
        return "failed"
    return "rejected" if status_code >= 400 else "success"


def _random_id(bits):
    while True:
        value = secrets.randbits(bits)
        if value:
            return value


class DependencyCall:
    """One call to a platform destination, as a span (ADR 0042).

    The SDK makes one for each call it sends; code that sends its own requests
    makes one with :func:`dependency_call`, which checks its arguments. The
    span is a child of the current span, or the root of a new trace when there
    is none. :meth:`headers` is the carrier to send with it: ``traceparent``
    naming this span, and the current ``baggage``. Leaving the ``with`` block
    writes one log event: ``dependency.call.completed`` once ``status_code`` is
    set, else ``dependency.call.failed``. The event never holds a URL, a query
    string, a header or a body: ``route`` is ``path`` without its query and with
    every UUID replaced by ``{uid}``.

    The event uses the platform's dependency-call fields: ``dependency_type``
    is ``kind`` (``api``, ``release``, ``agent`` or ``inference``) and
    ``dependency_name`` is what was called: ``name``, the target's uid for a
    release or an Agent, or ``mainsequence_backend`` for the API and
    inference. ``outcome`` is ``success`` below status 400, ``rejected`` from
    400 to 499, and ``failed`` from 500 or without an answer. One instance is
    one span, whatever retries happen inside it.
    """

    def __init__(self, kind, method, path, *, name=None):
        current = trace.get_current_span().get_span_context()
        self.parent_span_id = current.span_id if current.is_valid else None
        self.span = SpanContext(
            current.trace_id if current.is_valid else _random_id(128),
            _random_id(64),
            is_remote=False,
            trace_flags=current.trace_flags if current.is_valid else TraceFlags(TraceFlags.SAMPLED),
            trace_state=current.trace_state if current.is_valid else None,
        )
        self.fields = {
            "dependency_type": kind,
            "dependency_name": str(name) if name is not None else _PLATFORM_API,
            "http_method": method.upper(),
            "route": _route(path),
        }
        self.status_code = None
        self.error = None
        self.started = time.perf_counter()

    def headers(self):
        """``traceparent`` with this span's id, and the current ``baggage``."""
        context = trace.set_span_in_context(NonRecordingSpan(self.span))
        carrier = {}
        TraceContextTextMapPropagator().inject(carrier, context=context)
        W3CBaggagePropagator().inject(carrier, context=context)
        return carrier

    def __enter__(self):
        self.started = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            self._write(exc if exc is not None else self.error)
        except Exception:
            pass  # The event never fails the call.
        return False

    def _write(self, error):
        from mainsequence import logconf

        log = logconf.logger
        if log is None:
            return
        failed = error is not None or self.status_code is None
        fields = {
            "trace_id": format(self.span.trace_id, "032x"),
            "span_id": format(self.span.span_id, "016x"),
            "parent_span_id": (
                format(self.parent_span_id, "016x") if self.parent_span_id else None
            ),
            **self.fields,
            "status_code": self.status_code,
            "duration_ms": round((time.perf_counter() - self.started) * 1000, 3),
            "outcome": _outcome(None if failed else self.status_code),
        }
        if failed:
            fields["error_type"] = type(error).__name__ if error is not None else "NoResponse"
            log.warning("dependency.call.failed", **fields)
        else:
            log.info("dependency.call.completed", **fields)


DEPENDENCY_TYPES = ("api", "release", "agent", "inference")


def dependency_call(dependency_type, method, path, *, name=None):
    """A call your own HTTP client sends to the platform, as a span (ADR 0042).

    The span and the log event are the ones the SDK makes for its own calls, for
    code that sends a request itself: a client library that keeps its own
    ``requests`` or ``httpx`` client, its connection pool or its streaming::

        with dependency_call("release", "GET", "/v1/prices", name=release_uid) as call:
            response = client.get(
                rpc_url + "/v1/prices",
                headers={**call.headers(), "Authorization": f"Bearer {token}"},
            )
            call.status_code = response.status_code

    ``dependency_type`` is ``release`` or ``agent`` for a call to an address the
    platform gave, such as the ``rpc_url`` of
    ``ResourceRelease.resolve_runtime_access``, with ``name`` the release's or
    the Agent session's uid; it is ``api`` or ``inference`` for a call to the
    origin of ``MAINSEQUENCE_ENDPOINT``. ``path`` is the path the request adds
    to that address. Only its path is written, without its query and with every
    UUID replaced by ``{uid}``.

    Send :meth:`DependencyCall.headers` with the request, and only to those
    addresses: they carry the current ``baggage``. Merge your own headers after
    them, so one of yours with the same name wins. Set ``status_code`` when an
    answer arrives, whatever its status. Leaving the ``with`` block writes
    ``dependency.call.completed`` with that status, or ``dependency.call.failed``
    when no status was set or the block raised. Leave it after the response's
    body is read, so that ``duration_ms`` covers it. One block is one span: a
    retry inside it belongs to it, and a call sent again is a new block.
    """
    if dependency_type not in DEPENDENCY_TYPES:
        raise ValueError(f"dependency_type must be one of {', '.join(DEPENDENCY_TYPES)}.")
    if not isinstance(method, str) or not method.strip():
        raise ValueError("method must be an HTTP method, such as GET.")
    if not isinstance(path, str) or not path:
        raise ValueError("path must be the path the request is sent to.")
    if dependency_type in ("release", "agent") and name in (None, ""):
        raise ValueError(
            "A release or agent call needs name: the release's or the Agent session's uid."
        )
    return DependencyCall(dependency_type, method.strip(), path, name=name)
