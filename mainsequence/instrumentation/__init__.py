from opentelemetry import trace as _trace

from .utils import (
    DEPENDENCY_TYPES,
    DependencyCall,
    OTelJSONRenderer,
    TracerInstrumentator,
    add_otel_trace_context,
    dependency_call,
)

tracer_instrumentator = TracerInstrumentator()
# A tracer from whatever provider the application sets, now or later. Importing
# the SDK configures no OpenTelemetry (ADR 0042).
tracer = _trace.get_tracer("mainsequence")

__all__ = [
    "DEPENDENCY_TYPES",
    "DependencyCall",
    "OTelJSONRenderer",
    "TracerInstrumentator",
    "add_otel_trace_context",
    "dependency_call",
    "tracer",
    "tracer_instrumentator",
]
