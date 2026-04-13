from __future__ import annotations

from typing import Any

from mainsequence.client import (
    DynamicTableDataSource,
    SimpleTableStorage,
    SimpleTableUpdate,
)
from mainsequence.client.models_simple_tables import SimpleTableUpdateDetails

from ..base_persist_managers import BasePersistManager
from .models import SimpleTable


class SimpleTablePersistManager(BasePersistManager):
    STORAGE_CLASS = SimpleTableStorage
    UPDATE_CLASS = SimpleTableUpdate
    UPDATE_DETAILS_CLASS = SimpleTableUpdateDetails
    UPDATE_GET_OR_NONE_DATASOURCE_LOOKUP = "remote_table__data_source__id"
    UPDATE_CREATE_STORAGE_LOOKUP = "remote_table"

    def __init__(
        self,
        data_source: DynamicTableDataSource,
        update_hash: str,
        description: str | None = None,
        class_name: str | None = None,
        data_node_storage: dict | None = None,
        data_node_update: SimpleTableUpdate | None = None,
        simple_table_schema: type[SimpleTable] | None = None,
        resolved_simple_table_schema: dict[str, Any] | None = None,
        configuration: Any = None,
    ):
        self.simple_table_schema = simple_table_schema
        self.resolved_simple_table_schema = resolved_simple_table_schema
        self.configuration = configuration
        super().__init__(
            data_source=data_source,
            update_hash=update_hash,
            description=description,
            class_name=class_name,
            data_node_storage=data_node_storage,
            data_node_update=data_node_update,
        )
    #Todo: this repeats a method that ius called in a upstream code flow but returns different things we most be carefule
    @classmethod
    def get_from_data_type(
        cls, data_source: DynamicTableDataSource, *args, **kwargs
    ) -> SimpleTablePersistManager:
        return cls(data_source=data_source, *args, **kwargs)

    @staticmethod
    def _build_storage_get_or_create_kwargs(
        *,
        storage_hash: str,
        remote_configuration: dict,
        data_source: DynamicTableDataSource,
        time_serie_source_code_git_hash: str,
        time_serie_source_code: str,
        build_configuration_json_schema: dict,
        open_to_public: bool,
        namespace: str | None = None,
        schema: dict[str, Any] | None = None,
        source_class_name: str | None = None,
    ) -> dict[str, Any]:
        kwargs = BasePersistManager._build_storage_get_or_create_kwargs(
            storage_hash=storage_hash,
            remote_configuration=remote_configuration,
            data_source=data_source,
            time_serie_source_code_git_hash=time_serie_source_code_git_hash,
            time_serie_source_code=time_serie_source_code,
            build_configuration_json_schema=build_configuration_json_schema,
            open_to_public=open_to_public,
            namespace=namespace,
        )
        kwargs.pop("time_serie_source_code_git_hash")
        kwargs.pop("time_serie_source_code")

        kwargs["source_code_git_hash"] = time_serie_source_code_git_hash
        kwargs["source_code"] = time_serie_source_code

        if schema is not None:
            kwargs["schema"] = schema
        if source_class_name is not None:
            kwargs["source_class_name"] = source_class_name
        return kwargs

    def _get_storage_get_or_create_extra_kwargs(self) -> dict[str, Any]:
        schema = None
        if self.simple_table_schema is not None:
            schema = (
                self.resolved_simple_table_schema
                if self.resolved_simple_table_schema is not None
                else self.simple_table_schema.schema().to_canonical_dict()
            )

        return {
            "schema": schema,
            "source_class_name": self.class_name,
        }

    def _build_update_get_or_create_kwargs(
        self,
        *,
        storage_hash: str,
        local_configuration: dict | None = None,
        open_to_public: bool = False,
    ) -> dict[str, Any]:
        storage_reference = storage_hash
        if self.data_node_storage is not None and not isinstance(self.data_node_storage, dict):
            storage_reference = self.data_node_storage.id

        kwargs = dict(
            update_hash=self.update_hash,
            build_configuration=local_configuration,
            data_source_id=self.data_source.id,
            remote_table=storage_reference,
        )
        kwargs["open_for_everyone"] = open_to_public
        return kwargs

    def _should_refresh_update_when_remote_exists(self) -> bool:
        return self._data_node_update_cached is None and self._data_node_storage_cached is None

    def insert_records(
        self,
        records: list[SimpleTable],
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> list[SimpleTable]:
        return self.persist_records(
            records,
            overwrite=False,
            timeout=timeout,
        )

    def upsert_records(
        self,
        records: list[SimpleTable],
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> list[SimpleTable]:
        return self.persist_records(
            records,
            overwrite=True,
            timeout=timeout,
        )

    def persist_records(
        self,
        records: list[SimpleTable],
        *,
        overwrite: bool = False,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> list[SimpleTable]:
        del timeout
        payload = [
            self.simple_table_schema.serialize_record_payload(record)
            for record in records
        ]
        self.UPDATE_CLASS.insert_records_into_table(
            data_node_update_id=self.data_node_update.id,
            records=payload,
            overwrite=overwrite,
        )
        return records

    def delete(
        self,
        record_or_id: SimpleTable | Any,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> None:
        record_id = record_or_id if isinstance(record_or_id, int) else getattr(record_or_id, "id", None)
        if record_id is None:
            raise ValueError("delete(...) requires a record id or a record with an 'id' field value.")
        SimpleTableStorage.delete_records_from_table(
            data_node_storage_id=self.data_node_storage.id,
            records_ids=[record_id],
            timeout=timeout,
        )

PersistManager = SimpleTablePersistManager

__all__ = ["PersistManager", "SimpleTablePersistManager"]
