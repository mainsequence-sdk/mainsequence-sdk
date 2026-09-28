import builtins

import pytest

from mainsequence.instrumentation import utils


def test_build_tracer_does_not_override_existing_provider(monkeypatch):
    monkeypatch.delenv("OTLP_ENDPOINT", raising=False)
    existing_provider = object()
    tracer = object()

    original_import = builtins.__import__

    def without_exporter(name, *args, **kwargs):
        if name.startswith("opentelemetry.exporter"):
            raise AssertionError("The exporter must not load without an OTLP endpoint")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_exporter)

    monkeypatch.setattr(utils, "get_tracer_provider", lambda: existing_provider)
    monkeypatch.setattr(utils, "get_tracer", lambda name: tracer)

    def fail_set_tracer_provider(provider):
        raise AssertionError("set_tracer_provider should not be called")

    monkeypatch.setattr(utils, "set_tracer_provider", fail_set_tracer_provider)

    assert utils.TracerInstrumentator().build_tracer() is tracer


def test_build_tracer_uses_configured_otlp_endpoint(monkeypatch):
    from opentelemetry.exporter.otlp.proto.grpc import trace_exporter
    from opentelemetry.sdk.trace import export

    class Provider:
        def __init__(self):
            self.processors = []

        def add_span_processor(self, processor):
            self.processors.append(processor)

    provider = Provider()
    tracer = object()
    monkeypatch.setenv("OTLP_ENDPOINT", "http://localhost:4317")
    monkeypatch.setattr(utils, "get_tracer_provider", lambda: provider)
    monkeypatch.setattr(utils, "get_tracer", lambda name: tracer)
    monkeypatch.setattr(trace_exporter, "OTLPSpanExporter", lambda endpoint: endpoint)
    monkeypatch.setattr(export, "BatchSpanProcessor", lambda exporter: ("batch", exporter))

    assert utils.TracerInstrumentator().build_tracer() is tracer
    assert provider.processors == [("batch", "http://localhost:4317")]


def test_tracing_setup_explains_missing_optional_dependency(monkeypatch):
    original_import = builtins.__import__

    def without_tracing_sdk(name, *args, **kwargs):
        if name.startswith("opentelemetry.sdk"):
            raise ModuleNotFoundError("No module named 'opentelemetry.sdk'", name="opentelemetry.sdk")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", without_tracing_sdk)
    with pytest.raises(ImportError, match=r"mainsequence\[tracing\]"):
        utils.TracerInstrumentator().build_tracer()
