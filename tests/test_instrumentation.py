from mainsequence.instrumentation import utils


def test_build_tracer_does_not_override_existing_provider(monkeypatch):
    existing_provider = object()
    tracer = object()

    monkeypatch.setattr(utils, "get_tracer_provider", lambda: existing_provider)
    monkeypatch.setattr(utils, "get_tracer", lambda name: tracer)

    def fail_set_tracer_provider(provider):
        raise AssertionError("set_tracer_provider should not be called")

    monkeypatch.setattr(utils, "set_tracer_provider", fail_set_tracer_provider)

    assert utils.TracerInstrumentator().build_tracer() is tracer
