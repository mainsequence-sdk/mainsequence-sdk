from __future__ import annotations

import datetime
import logging
from typing import Any, ClassVar, Literal
from uuid import UUID

import pandas as pd
import structlog.contextvars as cvars

from mainsequence.client.dtype_codec import TIMESTAMP_TZ, token_to_pandas_series
from mainsequence.client.metatables import MetaTable, TimeIndexMetaTable, TimeIndexTableUpdate
from mainsequence.client.utils import DoesNotExist
from mainsequence.logconf import logger
from mainsequence.project_context import resolve_organization_environment_uid


def _required_uid(value: Any, *, context: str) -> str:
    if isinstance(value, dict):
        uid = value.get("uid")
    else:
        uid = getattr(value, "uid", None)
    if uid in (None, ""):
        raise ValueError(f"{context} requires a canonical TimeIndexMetaTable uid.")
    return str(uid)


def _data_source_uid(value: Any, *, context: str) -> str:
    if isinstance(value, dict):
        uid = value.get("data_source_uid")
        data_source = value.get("data_source")
    else:
        uid = getattr(value, "data_source_uid", None)
        data_source = getattr(value, "data_source", None)
    if uid not in (None, ""):
        return str(uid)
    if isinstance(data_source, dict):
        uid = data_source.get("uid")
    elif isinstance(data_source, str | UUID):
        uid = data_source
    else:
        uid = getattr(data_source, "uid", None)
    if uid in (None, ""):
        raise ValueError(f"{context} requires TimeIndexMetaTable data-source identity.")
    return str(uid)


def _validate_environment(value: TimeIndexMetaTable, *, context: str) -> None:
    table_environment_uid = getattr(value, "organization_environment_uid", None)
    if table_environment_uid in (None, ""):
        return
    current_environment_uid = resolve_organization_environment_uid(context)
    if str(table_environment_uid) != str(current_environment_uid):
        raise ValueError(
            f"{context} cannot reference a TimeIndexMetaTable from another "
            "Organization Environment."
        )


class TimeIndexTableReader:
    """Read operations for one already-resolved time-index table."""

    def __init__(self, *, output_table: TimeIndexMetaTable):
        if not isinstance(output_table, TimeIndexMetaTable):
            raise TypeError(
                "TimeIndexTableReader output_table must be a TimeIndexMetaTable; "
                f"got {type(output_table).__name__}."
            )
        _required_uid(output_table, context="TimeIndexTableReader")
        _data_source_uid(output_table, context="TimeIndexTableReader")
        _validate_environment(output_table, context="TimeIndexTableReader")
        self.output_table = output_table

    def get_last_observation(
        self,
        *,
        dimension_filters: dict[str, list[Any]] | None = None,
        index_coordinates: list[dict[str, Any]] | None = None,
        dimension_range_map: list[dict[str, Any]] | None = None,
    ) -> Any:
        return self.output_table.get_last_observation(
            dimension_filters=dimension_filters,
            index_coordinates=index_coordinates,
            dimension_range_map=dimension_range_map,
        )

    def get_df_between_dates(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        frame = self.output_table.get_data_between_dates_from_api(*args, **kwargs)
        if frame.empty:
            return frame

        time_index_name, index_names, column_dtypes_map = (
            self.output_table._require_time_indexed_table_contract()
        )
        frame[time_index_name] = token_to_pandas_series(
            frame[time_index_name],
            TIMESTAMP_TZ,
            is_time_index=True,
        )
        requested_columns = kwargs.get("columns")
        column_filter = requested_columns or column_dtypes_map.keys()
        for column_name in column_filter:
            if column_name not in frame.columns:
                continue
            column_type = column_dtypes_map.get(column_name)
            if column_type is None:
                continue
            frame[column_name] = token_to_pandas_series(
                frame[column_name],
                column_type,
                is_time_index=column_name == time_index_name,
            )
        return frame.set_index(index_names)


class TimeIndexTableAccessMixin:
    """Shared read helpers for updater outputs and read-only table references."""

    @property
    def table_accessor(self) -> Any:
        raise NotImplementedError

    def __repr__(self) -> str:
        return self.__class__.__name__

    def get_last_observation(
        self,
        *,
        dimension_filters: dict[str, list[Any]] | None = None,
        index_coordinates: list[dict[str, Any]] | None = None,
        dimension_range_map: list[dict[str, Any]] | None = None,
    ) -> Any:
        return self.table_accessor.get_last_observation(
            dimension_filters=dimension_filters,
            index_coordinates=index_coordinates,
            dimension_range_map=dimension_range_map,
        )

    def get_df_between_dates(
        self,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        great_or_equal: bool = True,
        less_or_equal: bool = True,
        dimension_filters: dict[str, list[Any]] | None = None,
        index_coordinates: list[dict[str, Any]] | None = None,
        dimension_range_map: list[dict[str, Any]] | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        return self.table_accessor.get_df_between_dates(
            start_date=start_date,
            end_date=end_date,
            great_or_equal=great_or_equal,
            less_or_equal=less_or_equal,
            dimension_filters=dimension_filters,
            index_coordinates=index_coordinates,
            dimension_range_map=dimension_range_map,
            columns=columns,
        )

    def get_logger_context_variables(self) -> dict[str, Any]:
        context = {
            "dependency_kind": self.dependency_kind,
            "output_table_uid": self.output_table_uid,
            "data_source_uid": self.data_source_uid,
        }
        update_hash = getattr(self, "update_hash", None)
        if update_hash not in (None, ""):
            context["update_hash"] = str(update_hash)
        return context

    @property
    def logger(self) -> logging.Logger:
        if not hasattr(self, "_logger"):
            cvars.bind_contextvars(**self.get_logger_context_variables())
            self._logger = logger
        return self._logger

    @staticmethod
    def set_context_in_logger(logger_context: dict[str, Any]) -> None:
        for key, value in logger_context.items():
            logger.bind(**{key: value})

    def unbind_context_variables_from_logger(self) -> None:
        cvars.unbind_contextvars(*self.get_logger_context_variables().keys())


class TimeIndexTableRef(TimeIndexTableAccessMixin):
    """Read-only dependency reference to one canonical TimeIndexMetaTable."""

    dependency_kind: ClassVar[Literal["time_index_table_ref"]] = "time_index_table_ref"

    def __init__(self, *, output_table: TimeIndexMetaTable):
        if not isinstance(output_table, TimeIndexMetaTable):
            raise TypeError(
                "TimeIndexTableRef output_table must be a TimeIndexMetaTable; "
                f"got {type(output_table).__name__}."
            )
        self.output_table = output_table
        self._output_table_uid = _required_uid(output_table, context="TimeIndexTableRef")
        self._data_source_uid = _data_source_uid(output_table, context="TimeIndexTableRef")
        self._reader = TimeIndexTableReader(output_table=output_table)

    @property
    def output_table_uid(self) -> str:
        return self._output_table_uid

    @property
    def time_index_meta_table_uid(self) -> str:
        return self.output_table_uid

    @property
    def data_source_uid(self) -> str:
        return self._data_source_uid

    @property
    def reader(self) -> TimeIndexTableReader:
        return self._reader

    @property
    def table_accessor(self) -> TimeIndexTableReader:
        return self.reader

    @classmethod
    def from_uid(cls, time_index_meta_table_uid: str) -> TimeIndexTableRef:
        if time_index_meta_table_uid in (None, ""):
            raise ValueError("TimeIndexTableRef.from_uid requires a table uid.")
        return cls(output_table=TimeIndexMetaTable.get(uid=str(time_index_meta_table_uid)))

    @classmethod
    def from_meta_table(cls, meta_table: MetaTable) -> TimeIndexTableRef:
        uid = _required_uid(meta_table, context="TimeIndexTableRef.from_meta_table")
        if isinstance(meta_table, TimeIndexMetaTable):
            return cls(output_table=meta_table)
        return cls.from_uid(uid)

    @classmethod
    def from_identifier(cls, identifier: str) -> TimeIndexTableRef:
        if identifier in (None, ""):
            raise ValueError("TimeIndexTableRef.from_identifier requires an identifier.")
        return cls(output_table=TimeIndexMetaTable.get(identifier=str(identifier)))

    @classmethod
    def from_physical_identity(
        cls,
        *,
        schema: str,
        table_name: str,
        data_source_uid: str | None = None,
    ) -> TimeIndexTableRef:
        if schema in (None, "") or table_name in (None, ""):
            raise ValueError(
                "TimeIndexTableRef.from_physical_identity requires schema and table_name."
            )
        filters: dict[str, Any] = {
            "physical_schema": str(schema),
            "physical_table_name": str(table_name),
            "limit": 2,
        }
        if data_source_uid not in (None, ""):
            filters["data_source__uid"] = str(data_source_uid)
        matches = list(TimeIndexMetaTable.filter_by_body(**filters))
        exact_matches = [
            match
            for match in matches
            if str(getattr(match, "physical_schema", "")) == str(schema)
            and str(getattr(match, "physical_table_name", "")) == str(table_name)
            and (
                data_source_uid in (None, "")
                or _data_source_uid(
                    match,
                    context="TimeIndexTableRef.from_physical_identity",
                )
                == str(data_source_uid)
            )
        ]
        if not exact_matches:
            raise DoesNotExist(
                f"No TimeIndexMetaTable found for physical identity {schema!r}.{table_name!r}."
            )
        if len(exact_matches) != 1:
            raise ValueError(
                "TimeIndexTableRef.from_physical_identity is ambiguous; provide "
                "data_source_uid so exactly one TimeIndexMetaTable is selected."
            )
        return cls(output_table=exact_matches[0])

    @classmethod
    def from_table_update(cls, table_update: TimeIndexTableUpdate) -> TimeIndexTableRef:
        output_table = table_update.output_table
        if isinstance(output_table, str | UUID):
            return cls.from_uid(str(output_table))
        return cls.from_meta_table(output_table)

    def get_update_statistics(self) -> Any:
        return self.output_table.get_data_updates()


__all__ = [
    "TimeIndexTableAccessMixin",
    "TimeIndexTableReader",
    "TimeIndexTableRef",
]
