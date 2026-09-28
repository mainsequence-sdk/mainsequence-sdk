"""Optional OpenTelemetry integration; importing the SDK does not configure tracing."""

from .utils import OTelJSONRenderer, TracerInstrumentator, add_otel_trace_context


def setup_tracing():
    """Configure and return a tracer when the application explicitly opts in."""
    return TracerInstrumentator().build_tracer()


__all__ = ["OTelJSONRenderer", "TracerInstrumentator", "add_otel_trace_context", "setup_tracing"]
