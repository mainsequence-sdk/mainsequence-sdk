from .utils import OTelJSONRenderer, TracerInstrumentator, add_otel_trace_context

tracer_instrumentator = TracerInstrumentator()
tracer = tracer_instrumentator.build_tracer()

__all__ = [
    "OTelJSONRenderer",
    "TracerInstrumentator",
    "add_otel_trace_context",
    "tracer",
    "tracer_instrumentator",
]
