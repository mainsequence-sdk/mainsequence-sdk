"""Small helpers shared by otherwise independent test domains."""

import base64
import importlib
import json
import sys
import types
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_REMOVED_DOMAIN_TOKEN = "PRO" + "JECT"
RETIRED_RUNTIME_CONTEXT_ENV_NAMES = (
    "MAIN_SEQUENCE_" + _REMOVED_DOMAIN_TOKEN + "_UID",
    "MAIN_SEQUENCE_" + _REMOVED_DOMAIN_TOKEN + "_BRANCH_UID",
    "MAINSEQUENCE_REPOSITORY_BRANCH",
    "MAIN_SEQUENCE_ORGANIZATION_" + _REMOVED_DOMAIN_TOKEN + "_ENVIRONMENT_UID",
)


def reset_sdk_modules() -> None:
    for name in tuple(sys.modules):
        if name == "mainsequence" or name.startswith("mainsequence."):
            sys.modules.pop(name, None)


def load_sdk_submodule(module_name: str):
    """Import a submodule without executing package bootstrap code."""
    reset_sdk_modules()
    package_root = REPOSITORY_ROOT / "mainsequence"
    package = types.ModuleType("mainsequence")
    package.__path__ = [str(package_root)]
    sys.modules["mainsequence"] = package

    if module_name.startswith("mainsequence.client."):
        client = types.ModuleType("mainsequence.client")
        client.__path__ = [str(package_root / "client")]
        sys.modules["mainsequence.client"] = client

    return importlib.import_module(module_name)


def jwt_with_expiry(expiry: int | None) -> str:
    claims = {} if expiry is None else {"exp": expiry}
    body = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"e30.{body}.signature"
