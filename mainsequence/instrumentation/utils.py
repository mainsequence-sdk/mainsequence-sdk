import logging
import os
from contextvars import ContextVar

import structlog
from opentelemetry import baggage, trace
from opentelemetry import context as otel_context
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.sdk.trace import (
    TracerProvider,
)
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import (
    ProxyTracerProvider,
    get_current_span,
    get_tracer,
    get_tracer_provider,
    set_tracer_provider,
)
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator


def is_port_in_use(port: int, agent_host: str) -> bool:
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((agent_host, port)) == 0


class TracerInstrumentator:
    __doc__ = """
        Main instrumentator class controlls building and exporting of traces 
    """

    def build_tracer(self) -> TraceContextTextMapPropagator:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import SERVICE_NAME, Resource

        provider = get_tracer_provider()
        if isinstance(provider, ProxyTracerProvider):
            resource = Resource(attributes={SERVICE_NAME: "tdag"})
            provider = TracerProvider(resource=resource)
            set_tracer_provider(provider)

        end_point = os.environ.get("OTLP_ENDPOINT")

        if end_point is not None:
            otlp_exporter = OTLPSpanExporter(endpoint=end_point)
            if is_port_in_use(4317, agent_host=self.agent_host):
                get_tracer_provider().add_span_processor(BatchSpanProcessor(otlp_exporter))
            else:
                get_tracer_provider().add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        tracer = get_tracer("tdag")
        return tracer

    def get_current_trace_id(self):
        current_span = get_current_span()
        return format(current_span.context.trace_id, "032x")

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
    """
    span = trace.get_current_span()
    if not span.is_recording():
        event_dict["span"] = None
        return event_dict

    ctx = span.get_span_context()
    parent = getattr(span, "parent", None)

    event_dict["span"] = {
        "span_id": format(ctx.span_id, "016x"),
        "trace_id": format(ctx.trace_id, "032x"),
        "parent_span_id": None if not parent else format(parent.span_id, "016x"),
    }

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


def _request_context(carrier, *, with_baggage):
    """The OpenTelemetry context of one inbound request, from its headers.

    Its baggage is exactly the request's ``baggage`` header, or none without
    ``with_baggage``: never the process's or another request's. Its trace
    parent is the request's ``traceparent``, unless instrumentation already
    records a span for the request.
    """
    context = baggage.clear(context=otel_context.get_current())
    if with_baggage:
        context = _with_baggage_header(carrier.get("baggage"), context)
    if not trace.get_current_span(context).is_recording():
        context = TraceContextTextMapPropagator().extract(carrier, context=context)
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
