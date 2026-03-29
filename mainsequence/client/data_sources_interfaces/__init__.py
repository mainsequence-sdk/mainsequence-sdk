from __future__ import annotations


def get_duckdb_interface_class():
    try:
        from .duckdb import DuckDBInterface
    except ModuleNotFoundError as exc:
        missing = (exc.name or "").split(".")[0]
        if missing in {"duckdb", "pyarrow"}:
            raise ModuleNotFoundError(
                "DuckDB local-data support requires the optional dependency group "
                "`mainsequence[local-data]`."
            ) from exc
        raise

    return DuckDBInterface


__all__ = ["get_duckdb_interface_class"]
