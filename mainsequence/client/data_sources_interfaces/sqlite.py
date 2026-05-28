from __future__ import annotations

import datetime
import os
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd

from mainsequence.logconf import logger as base_logger

from ..dtype_codec import (
    TIMESTAMP_TZ,
    pandas_dtype_to_token,
    token_to_backend_type,
    token_to_pandas_series,
)
from ..utils import UniqueIdentifierRangeMap


def get_logger():
    base_logger.bind(sub_application="sqlite_interface")
    return base_logger


logger = get_logger()


class SQLiteInterface:
    """
    Persist/serve configured-index DataFrames in a local SQLite database file.
    """

    def __init__(self, db_path: str | Path | None = None):
        from mainsequence.tdag.config import TDAG_DATA_PATH

        default_path = os.getenv(
            "SQLITE_PATH",
            os.path.join(f"{TDAG_DATA_PATH}", "sqlite"),
        )
        raw_path = Path(str(db_path or default_path)).expanduser()
        if raw_path.suffix in {".db", ".sqlite", ".sqlite3"}:
            db_file = raw_path
        else:
            db_file = raw_path / "mainsequence.sqlite"

        db_file.parent.mkdir(parents=True, exist_ok=True)
        self.db_file = db_file
        self.db_path = str(db_file)
        self.con = sqlite3.connect(str(db_file))
        self.con.row_factory = sqlite3.Row
        self.con.execute("PRAGMA foreign_keys = ON")
        self.con.execute("PRAGMA journal_mode = WAL")

    @staticmethod
    def _qident(name: str) -> str:
        return '"' + str(name).replace('"', '""') + '"'

    @staticmethod
    def _to_utc_timestamp(value: Any) -> pd.Timestamp | None:
        if value is None or pd.isna(value):
            return None
        ts = pd.to_datetime(value, utc=True)
        if not isinstance(ts, pd.Timestamp):
            ts = pd.Timestamp(ts, tz="UTC")
        elif ts.tz is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return ts

    @classmethod
    def _to_time_storage(cls, value: Any) -> str | None:
        ts = cls._to_utc_timestamp(value)
        if ts is None:
            return None
        fraction = (ts.microsecond * 1000) + ts.nanosecond
        return ts.strftime("%Y-%m-%dT%H:%M:%S") + f".{fraction:09d}Z"

    @classmethod
    def _datetime_param(cls, value: Any) -> str | None:
        return cls._to_time_storage(value)

    @staticmethod
    def _sqlite_type(series: pd.Series, *, is_time_index: bool = False) -> str:
        token = (
            TIMESTAMP_TZ
            if is_time_index
            else pandas_dtype_to_token(series.dtype, remote=False, allow_naive_datetime=True)
        )
        return token_to_backend_type(token, "sqlite")

    @staticmethod
    def _sqlite_value(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, dict | list | tuple | set):
            raise ValueError(f"Unsupported SQLite local-data value type: {type(value)!r}")
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        if isinstance(value, pd.Timestamp | datetime.datetime | datetime.date):
            return SQLiteInterface._to_time_storage(value)
        if hasattr(value, "item"):
            value = value.item()
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int | float | str):
            return value
        raise ValueError(f"Unsupported SQLite local-data value type: {type(value)!r}")

    def _table_columns(self, table: str) -> dict[str, str]:
        rows = self.con.execute(f"PRAGMA table_info({self._qident(table)})").fetchall()
        return {row["name"]: row["type"] for row in rows}

    def _ensure_table(
        self,
        *,
        table: str,
        df: pd.DataFrame,
        index_names: list[str],
        time_index_name: str,
    ) -> None:
        table_columns = self._table_columns(table)
        if not table_columns:
            columns_sql = []
            for column in df.columns:
                col_type = self._sqlite_type(
                    df[column],
                    is_time_index=column == time_index_name,
                )
                columns_sql.append(f"{self._qident(column)} {col_type}")
            unique_sql = ", ".join(self._qident(name) for name in index_names)
            sql = (
                f"CREATE TABLE IF NOT EXISTS {self._qident(table)} "
                f"({', '.join(columns_sql)}, UNIQUE ({unique_sql}))"
            )
            self.con.execute(sql)
            return

        for column in df.columns:
            if column in table_columns:
                continue
            col_type = self._sqlite_type(
                df[column],
                is_time_index=column == time_index_name,
            )
            self.con.execute(
                f"ALTER TABLE {self._qident(table)} "
                f"ADD COLUMN {self._qident(column)} {col_type}"
            )

        missing_index_columns = [name for name in index_names if name not in self._table_columns(table)]
        if missing_index_columns:
            raise ValueError(
                "SQLite table is missing configured index columns. "
                f"Missing: {missing_index_columns}"
            )

    @staticmethod
    def _validate_index(index_names: list[str], time_index_name: str) -> None:
        if time_index_name not in index_names:
            raise ValueError(
                f"time_index_name {time_index_name!r} must be present in index_names {index_names!r}"
            )

    def table_exists(self, table: str) -> bool:
        row = self.con.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
            (table,),
        ).fetchone()
        return row is not None

    def list_tables(self) -> list[str]:
        rows = self.con.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall()
        return [row["name"] for row in rows]

    def drop_table(self, table: str) -> None:
        self.con.execute(f"DROP TABLE IF EXISTS {self._qident(table)}")
        self.con.commit()

    def upsert(
        self,
        df: pd.DataFrame,
        table: str,
        *,
        index_names: list[str],
        time_index_name: str,
        **_: Any,
    ) -> None:
        self._validate_index(index_names, time_index_name)
        if df.empty:
            logger.warning(f"Attempted to upsert an empty DataFrame to table '{table}'. Skipping.")
            return

        missing_index_columns = [name for name in index_names if name not in df.columns]
        if missing_index_columns:
            raise ValueError(
                "SQLite upsert requires every configured index column in the DataFrame. "
                f"Missing: {missing_index_columns}"
            )

        df = df.copy()
        df[time_index_name] = df[time_index_name].map(self._to_time_storage)
        df = df.drop_duplicates(subset=index_names, keep="last")

        with self.con:
            self._ensure_table(
                table=table,
                df=df,
                index_names=index_names,
                time_index_name=time_index_name,
            )

            columns = list(df.columns)
            insert_columns = ", ".join(self._qident(column) for column in columns)
            placeholders = ", ".join("?" for _ in columns)
            conflict_columns = ", ".join(self._qident(column) for column in index_names)
            value_columns = [column for column in columns if column not in index_names]
            if value_columns:
                update_sql = ", ".join(
                    f"{self._qident(column)} = excluded.{self._qident(column)}"
                    for column in value_columns
                )
                conflict_sql = f"DO UPDATE SET {update_sql}"
            else:
                conflict_sql = "DO NOTHING"

            sql = (
                f"INSERT INTO {self._qident(table)} ({insert_columns}) "
                f"VALUES ({placeholders}) "
                f"ON CONFLICT ({conflict_columns}) {conflict_sql}"
            )
            rows = [
                tuple(self._sqlite_value(row[column]) for column in columns)
                for _, row in df.iterrows()
            ]
            self.con.executemany(sql, rows)

    def constrain_read(
        self,
        table: str,
        *,
        start: datetime.datetime | None = None,
        end: datetime.datetime | None = None,
        time_index_name: str,
        index_names: list[str],
        dimension_filters: dict[str, list[Any]] | None = None,
        index_coordinates: list[dict[str, Any]] | None = None,
        dimension_range_map: list[dict[str, Any]] | None = None,
        ids: list[str] | None = None,
        unique_identifier_range_map: dict[str, dict[str, Any]] | None = None,
        max_rows: int | None = None,
        now: datetime.datetime | None = None,
    ) -> tuple[datetime.datetime | None, datetime.datetime | None, list[dict[str, Any]] | None, dict[str, Any]]:
        self._validate_index(index_names, time_index_name)
        identity_dimensions = [name for name in index_names if name != time_index_name]
        uses_legacy_unique_identifier = identity_dimensions == ["unique_identifier"]

        if ids is not None and unique_identifier_range_map is not None:
            raise ValueError("Cannot provide both 'ids' and 'unique_identifier_range_map'.")
        if ids is not None and not uses_legacy_unique_identifier:
            raise ValueError("Legacy 'ids' reads are valid only for unique_identifier tables.")
        if unique_identifier_range_map is not None and not uses_legacy_unique_identifier:
            raise ValueError(
                "Legacy 'unique_identifier_range_map' reads are valid only for "
                "unique_identifier tables."
            )
        if unique_identifier_range_map is not None and dimension_range_map is not None:
            raise ValueError(
                "Cannot provide both 'unique_identifier_range_map' and 'dimension_range_map'."
            )

        normalized_dimension_range_map = (
            None if dimension_range_map is None else [dict(item) for item in dimension_range_map]
        )
        if ids and normalized_dimension_range_map is None:
            normalized_dimension_range_map = [
                {
                    "coordinate": {"unique_identifier": uid},
                    "start_date": start,
                    "start_date_operand": ">=",
                    "end_date": end or now,
                    "end_date_operand": "<=",
                }
                for uid in ids
            ]
        if unique_identifier_range_map is not None:
            normalized_dimension_range_map = [
                {"coordinate": {"unique_identifier": uid}, **dict(info)}
                for uid, info in unique_identifier_range_map.items()
            ]

        diagnostics = {
            "limited": False,
            "reason": "sqlite_no_metadata_row_limit",
            "max_rows": max_rows,
            "dimension_filters": dimension_filters is not None,
            "index_coordinates": index_coordinates is not None,
        }
        return start, end, normalized_dimension_range_map, diagnostics

    def read(
        self,
        table: str,
        *,
        start: datetime.datetime | None = None,
        end: datetime.datetime | None = None,
        great_or_equal: bool = True,
        less_or_equal: bool = True,
        index_names: list[str],
        time_index_name: str,
        dimension_filters: dict[str, list[Any]] | None = None,
        index_coordinates: list[dict[str, Any]] | None = None,
        dimension_range_map: list[dict[str, Any]] | None = None,
        ids: list[str] | None = None,
        columns: list[str] | None = None,
        unique_identifier_range_map: UniqueIdentifierRangeMap | None = None,
        column_range_descriptor: dict[str, UniqueIdentifierRangeMap] | None = None,
        **_: Any,
    ) -> pd.DataFrame:
        self._validate_index(index_names, time_index_name)
        identity_dimensions = [name for name in index_names if name != time_index_name]
        uses_legacy_unique_identifier = identity_dimensions == ["unique_identifier"]

        if ids is not None and unique_identifier_range_map is not None:
            raise ValueError("Cannot provide both 'ids' and 'unique_identifier_range_map'.")
        if ids is not None and not uses_legacy_unique_identifier:
            raise ValueError("Legacy 'ids' reads are valid only for unique_identifier tables.")
        if unique_identifier_range_map is not None and not uses_legacy_unique_identifier:
            raise ValueError(
                "Legacy 'unique_identifier_range_map' reads are valid only for "
                "unique_identifier tables."
            )
        if unique_identifier_range_map is not None and dimension_range_map is not None:
            raise ValueError(
                "Cannot provide both 'unique_identifier_range_map' and 'dimension_range_map'."
            )
        if column_range_descriptor is not None:
            raise NotImplementedError("SQLite column_range_descriptor reads are not supported.")

        if ids:
            dimension_filters = dict(dimension_filters or {})
            if "unique_identifier" in dimension_filters:
                raise ValueError("Cannot provide both 'ids' and a unique_identifier filter.")
            dimension_filters["unique_identifier"] = list(ids)
        if unique_identifier_range_map is not None:
            dimension_range_map = [
                {"coordinate": {"unique_identifier": uid}, **dict(info)}
                for uid, info in unique_identifier_range_map.items()
            ]

        if not self.table_exists(table):
            logger.warning(f"Table '{table}' does not exist in {self.db_path}.")
            return pd.DataFrame()

        existing_columns = self._table_columns(table)
        projected_columns = (
            list(existing_columns.keys())
            if columns is None
            else list(dict.fromkeys([*index_names, *columns]))
        )
        missing_columns = [column for column in projected_columns if column not in existing_columns]
        if missing_columns:
            logger.warning(
                f"Columns '{missing_columns}' are not present in table '{table}'. returning an empty DF"
            )
            return pd.DataFrame()

        select_sql = ", ".join(self._qident(column) for column in projected_columns)
        sql_parts = [f"SELECT {select_sql} FROM {self._qident(table)}"]
        where_clauses: list[str] = []
        params: list[Any] = []

        def validate_dimension_name(name: str) -> None:
            if name not in identity_dimensions:
                raise ValueError(
                    f"Unknown identity dimension {name!r}; expected one of {identity_dimensions!r}"
                )

        if start is not None:
            start_operator = ">=" if great_or_equal else ">"
            where_clauses.append(f"{self._qident(time_index_name)} {start_operator} ?")
            params.append(self._datetime_param(start))
        if end is not None:
            end_operator = "<=" if less_or_equal else "<"
            where_clauses.append(f"{self._qident(time_index_name)} {end_operator} ?")
            params.append(self._datetime_param(end))
        if dimension_filters:
            for dimension, values in dimension_filters.items():
                validate_dimension_name(dimension)
                value_list = list(values)
                if not value_list:
                    where_clauses.append("FALSE")
                    continue
                placeholders = ", ".join("?" for _ in value_list)
                where_clauses.append(f"{self._qident(dimension)} IN ({placeholders})")
                params.extend(value_list)
        if index_coordinates:
            coordinate_conditions = []
            for coordinate in index_coordinates:
                parts = []
                for dimension, value in coordinate.items():
                    validate_dimension_name(dimension)
                    parts.append(f"{self._qident(dimension)} IS ?")
                    params.append(value)
                if parts:
                    coordinate_conditions.append(f"({' AND '.join(parts)})")
            if coordinate_conditions:
                where_clauses.append(f"({' OR '.join(coordinate_conditions)})")
        if dimension_range_map:
            range_conditions = []
            for date_info in dimension_range_map:
                coordinate = date_info.get("coordinate") or {}
                range_parts = []
                range_params = []
                for dimension, value in coordinate.items():
                    validate_dimension_name(dimension)
                    range_parts.append(f"{self._qident(dimension)} IS ?")
                    range_params.append(value)
                if date_info.get("start_date") is not None:
                    s_op = date_info.get("start_date_operand", ">=")
                    range_parts.append(f"{self._qident(time_index_name)} {s_op} ?")
                    range_params.append(self._datetime_param(date_info["start_date"]))
                if date_info.get("end_date") is not None:
                    e_op = date_info.get("end_date_operand", "<=")
                    range_parts.append(f"{self._qident(time_index_name)} {e_op} ?")
                    range_params.append(self._datetime_param(date_info["end_date"]))
                if range_parts:
                    range_conditions.append(f"({' AND '.join(range_parts)})")
                params.extend(range_params)
            if range_conditions:
                where_clauses.append(f"({' OR '.join(range_conditions)})")

        if where_clauses:
            sql_parts.append("WHERE " + " AND ".join(where_clauses))
        order_by = ", ".join(self._qident(name) for name in index_names)
        sql_parts.append(f"ORDER BY {order_by}")
        query = " ".join(sql_parts)

        df = pd.read_sql_query(query, self.con, params=params)
        if not df.empty and time_index_name in df.columns:
            df[time_index_name] = token_to_pandas_series(
                df[time_index_name],
                TIMESTAMP_TZ,
                is_time_index=True,
            )
        return df

    def time_index_minima(
        self,
        table: str,
        *,
        index_names: list[str],
        time_index_name: str,
    ) -> tuple[pd.Timestamp | None, dict[Any, pd.Timestamp | None]]:
        self._validate_index(index_names, time_index_name)
        if not self.table_exists(table):
            return None, {}

        qtime = self._qident(time_index_name)
        global_min_raw = self.con.execute(
            f"SELECT MIN({qtime}) AS min_val FROM {self._qident(table)}"
        ).fetchone()["min_val"]
        global_min = pd.to_datetime(global_min_raw, utc=True) if global_min_raw is not None else None

        identity_dimensions = [name for name in index_names if name != time_index_name]
        if not identity_dimensions:
            return global_min, {}

        dimension_select = ", ".join(self._qident(name) for name in identity_dimensions)
        rows = self.con.execute(
            f"""
            SELECT {dimension_select}, MIN({qtime}) AS min_val
            FROM {self._qident(table)}
            GROUP BY {dimension_select}
            """
        ).fetchall()

        per_coordinate: dict[Any, pd.Timestamp | None] = {}
        for row in rows:
            values = [row[name] for name in identity_dimensions]
            key = values[0] if len(values) == 1 else tuple(values)
            min_value = row["min_val"]
            per_coordinate[key] = (
                pd.to_datetime(min_value, utc=True) if min_value is not None else None
            )
        return global_min, per_coordinate
