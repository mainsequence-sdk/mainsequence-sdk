from opentelemetry import trace as _trace

from .utils import OTelJSONRenderer, TracerInstrumentator, add_otel_trace_context

tracer_instrumentator = TracerInstrumentator()
# A tracer from whatever provider the application sets, now or later. Importing
# the SDK configures no OpenTelemetry (ADR 0042).
tracer = _trace.get_tracer("mainsequence")

__all__ = [
    "OTelJSONRenderer",
    "TracerInstrumentator",
    "add_otel_trace_context",
    "tracer",
    "tracer_instrumentator",
]
