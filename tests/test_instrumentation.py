"""The SDK configures no OpenTelemetry of its own (ADR 0042)."""

import os
import subprocess
import sys
from pathlib import Path

from opentelemetry import trace

from mainsequence.instrumentation import utils


def test_build_tracer_never_sets_a_provider_and_never_reads_otlp_endpoint(monkeypatch):
    def fail(*args, **kwargs):
        raise AssertionError("build_tracer must not configure OpenTelemetry")

    monkeypatch.setattr(trace, "set_tracer_provider", fail)
    monkeypatch.setenv("OTLP_ENDPOINT", "http://collector.invalid:4317")
    provider = trace.get_tracer_provider()

    tracer = utils.TracerInstrumentator().build_tracer()

    assert trace.get_tracer_provider() is provider
    # With no provider set it is a no-op tracer: its spans record nothing.
    with tracer.start_as_current_span("work") as span:
        assert not span.is_recording()


def test_get_current_trace_id_reads_a_non_recording_span():
    assert utils.TracerInstrumentator().get_current_trace_id() == "0" * 32


def _run(script, tmp_path, **variables):
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("MAINSEQUENCE_", "MAIN_SEQUENCE_", "JOB_RUN_UID", "OTEL_"))
        and key not in {"TRACEPARENT", "BAGGAGE"}
    }
    environment.update(
        PYTHONPATH=str(Path(__file__).resolve().parents[1]),
        LOGGER_FILE_PATH="/dev/stdout",
        TDAG_ROOT_PATH=str(tmp_path),
        **variables,
    )
    return subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
    )


_OFFLINE_CLI = """
import sys
import types
config = types.ModuleType("mainsequence.cli.config")
config.backend_url = lambda: ""
config.get_tokens = lambda: {}
cli = types.ModuleType("mainsequence.cli")
cli.__path__ = []
cli.config = config
sys.modules[cli.__name__] = cli
sys.modules[config.__name__] = config
"""


def test_importing_the_sdk_with_otlp_endpoint_leaves_the_provider_untouched(tmp_path):
    result = _run(
        _OFFLINE_CLI
        + """
from opentelemetry import trace
import mainsequence
from mainsequence.instrumentation import tracer
print("PROVIDER", type(trace.get_tracer_provider()).__name__)
""",
        tmp_path,
        OTLP_ENDPOINT="http://collector.invalid:4317",
    )

    assert result.returncode == 0, result.stderr
    assert "PROVIDER ProxyTracerProvider" in result.stdout.splitlines()


def test_an_application_provider_set_after_import_is_the_one_used(tmp_path):
    result = _run(
        _OFFLINE_CLI
        + """
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
import mainsequence
from mainsequence.instrumentation import tracer, tracer_instrumentator
provider = TracerProvider()
trace.set_tracer_provider(provider)
print("SAME", trace.get_tracer_provider() is provider)
with tracer.start_as_current_span("work") as span:
    print("RECORDING", span.is_recording())
print("BUILT", tracer_instrumentator.build_tracer().start_span("x").is_recording())
""",
        tmp_path,
    )

    assert result.returncode == 0, result.stderr
    lines = result.stdout.splitlines()
    assert {"SAME True", "RECORDING True", "BUILT True"} <= set(lines)
