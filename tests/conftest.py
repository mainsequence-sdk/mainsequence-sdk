import os
import sys

import pytest

from tests._support import RETIRED_RUNTIME_CONTEXT_ENV_NAMES, reset_sdk_modules


@pytest.fixture
def isolated_sdk_imports():
    """Restore package objects after tests that deliberately stub SDK imports."""
    snapshot = {
        name: module
        for name, module in sys.modules.items()
        if name == "mainsequence" or name.startswith("mainsequence.")
    }
    yield
    reset_sdk_modules()
    sys.modules.update(snapshot)


@pytest.fixture
def clean_runtime_context_environment(monkeypatch):
    for name in RETIRED_RUNTIME_CONTEXT_ENV_NAMES:
        monkeypatch.delenv(name, raising=False)
    yield
    for name in RETIRED_RUNTIME_CONTEXT_ENV_NAMES:
        os.environ.pop(name, None)
