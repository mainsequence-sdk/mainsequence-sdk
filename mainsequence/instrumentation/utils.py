import os

import structlog
from opentelemetry import trace
from opentelemetry.sdk.trace import (
    TracerProvider,
)
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import (
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

        resource = Resource(attributes={SERVICE_NAME: "tdag"})
        set_tracer_provider(TracerProvider(resource=resource))

        end_point = os.environ.get("OTLP_ENDPOINT")

        if end_point is not None:
            otlp_exporter = OTLPSpanExporter(endpoint=end_point)
            if is_port_in_use(4317, agent_host=self.agent_host) == True:
                get_tracer_provider().add_span_processor(BatchSpanProcessor(otlp_exporter))
            else:
                get_tracer_provider().add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        tracer = get_tracer("tdag")
        return tracer

    def get_current_trace_id(self):
        current_span = get_current_span()
        return format(current_span.context.trace_id, "032x")

    def get_telemetry_carrier(self):
        prop = TraceContextTextMapPropagator()
        telemetry_carrier = {}
        prop.inject(carrier=telemetry_carrier)
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
