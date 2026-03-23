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
    UPDATE_GET_OR_NONE_DATASOURCE_LOOKUP = "simple_table__data_source__id"
    UPDATE_CREATE_STORAGE_LOOKUP = "simple_table"

    def __init__(
        self,
        data_source: DynamicTableDataSource,
        update_hash: str,
        description: str | None = None,
        class_name: str | None = None,
        data_node_storage: dict | None = None,
        data_node_update: SimpleTableUpdate | None = None,
        simple_table_schema: type[SimpleTable] | None = None,
        configuration: Any = None,
    ):
        self.simple_table_schema = simple_table_schema
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

    def _build_storage_get_or_create_kwargs(
        self,
        *,
        storage_hash: str,
        remote_configuration: dict,
        data_source: DynamicTableDataSource,
        time_serie_source_code_git_hash: str,
        time_serie_source_code: str,
        build_configuration_json_schema: dict,
        open_to_public: bool,
    ) -> dict[str, Any]:
        kwargs = super()._build_storage_get_or_create_kwargs(
            storage_hash=storage_hash,
            remote_configuration=remote_configuration,
            data_source=data_source,
            time_serie_source_code_git_hash=time_serie_source_code_git_hash,
            time_serie_source_code=time_serie_source_code,
            build_configuration_json_schema=build_configuration_json_schema,
            open_to_public=open_to_public,
        )
        if self.simple_table_schema is not None:
            kwargs.update(
                schema=self.simple_table_schema.schema().to_canonical_dict(),
                schema_fingerprint=self.simple_table_schema.schema_fingerprint(),
                physical_name=self.simple_table_schema.physical_name(),
                source_class_name=self.class_name,
            )
        return kwargs

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
            simple_table=storage_reference,
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
        del timeout
        payload = [record.model_dump(mode="python") for record in records]
        self.UPDATE_CLASS.insert_data_into_table(
            data_node_update_id=self.data_node_update.id,
            records=payload,
            overwrite=False,
        )
        return records

    def upsert_records(
        self,
        records: list[SimpleTable],
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> list[SimpleTable]:
        del timeout
        payload = [record.model_dump(mode="python") for record in records]
        self.UPDATE_CLASS.insert_data_into_table(
            data_node_update_id=self.data_node_update.id,
            records=payload,
            overwrite=True,
        )
        return records

    def delete(
        self,
        record_or_id: SimpleTable | Any,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> None:
        SimpleTableStorage.delete_record(record_or_id, timeout=timeout)


PersistManager = SimpleTablePersistManager

__all__ = ["PersistManager", "SimpleTablePersistManager"]
