from __future__ import annotations

import builtins
import importlib
import pathlib
import sys
import types


def _reset_mainsequence_modules() -> None:
    for name in tuple(sys.modules):
        if name == "mainsequence" or name.startswith("mainsequence."):
            sys.modules.pop(name, None)


def _seed_mainsequence_packages() -> None:
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    pkg_root = repo_root / "mainsequence"
    client_pkg_root = pkg_root / "client"

    pkg = types.ModuleType("mainsequence")
    pkg.__path__ = [str(pkg_root)]
    sys.modules["mainsequence"] = pkg

    client_pkg = types.ModuleType("mainsequence.client")
    client_pkg.__path__ = [str(client_pkg_root)]
    sys.modules["mainsequence.client"] = client_pkg


def test_core_client_import_does_not_require_duckdb(monkeypatch):
    _reset_mainsequence_modules()
    _seed_mainsequence_packages()

    real_import = builtins.__import__

    def _guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name in {
            "duckdb",
            "pyarrow",
            "mainsequence.client.data_sources_interfaces.duckdb",
        }:
            raise ModuleNotFoundError(name=name)
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)

    module = importlib.import_module("mainsequence.client.models_tdag")

    assert hasattr(module, "DynamicTableDataSource")


def test_duckdb_helper_points_to_local_data_extra(monkeypatch):
    _reset_mainsequence_modules()
    _seed_mainsequence_packages()

    real_import = builtins.__import__

    def _guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name in {
            "duckdb",
            "pyarrow",
            "mainsequence.client.data_sources_interfaces.duckdb",
        }:
            raise ModuleNotFoundError(name=name)
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _guarded_import)

    module = importlib.import_module("mainsequence.client.data_sources_interfaces")

    try:
        module.get_duckdb_interface_class()
    except ModuleNotFoundError as exc:
        assert "mainsequence[local-data]" in str(exc)
    else:
        raise AssertionError("Expected optional local-data dependency error")
