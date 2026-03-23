from __future__ import annotations

import datetime
import json
from collections.abc import Sequence
from decimal import Decimal
from enum import Enum
from typing import Any, ClassVar

from pydantic import BaseModel

import mainsequence.client as ms_client

from ..configuration_models import BaseConfiguration
from ..data_nodes import build_operations
from ..data_nodes.data_nodes import DataNode
from ..data_nodes.persist_managers import (
    get_data_node_source_code,
    get_data_node_source_code_git_hash,
)
from ..pydantic_metadata import serialize_pydantic_model, strip_pydantic_hash_exclusions
from .models import SimpleTable
from .persist_managers import SimpleTablePersistManager


class SimpleTableUpdaterConfiguration(BaseConfiguration):
    """
    Base class for simple-table updater configuration.

    Fields participate in the update hash by default. Mark a field with
    ``json_schema_extra={"update_only": True}`` to exclude it from the
    storage hash, or ``json_schema_extra={"runtime_only": True}`` to
    exclude it from both hashes.
    """

    def serialized_configuration(self) -> dict[str, Any]:
        serialized = _serialize_simple_table_value(self)
        if not isinstance(serialized, dict):
            raise TypeError("SimpleTableUpdaterConfiguration must serialize to a dictionary.")
        return serialized

    def update_configuration(self) -> dict[str, Any]:
        return strip_pydantic_hash_exclusions(
            self.serialized_configuration(),
            for_storage_hash=False,
        )

    def storage_configuration(self) -> dict[str, Any]:
        return strip_pydantic_hash_exclusions(
            self.serialized_configuration(),
            for_storage_hash=True,
        )


def _serialize_simple_table_value(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return serialize_pydantic_model(
            value,
            serialize_field=_serialize_simple_table_value,
        )

    if isinstance(value, datetime.datetime):
        return value.isoformat()

    if isinstance(value, datetime.date):
        return value.isoformat()

    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, Enum):
        return value.value

    if isinstance(value, list):
        return [_serialize_simple_table_value(item) for item in value]

    if isinstance(value, tuple):
        return {
            "__type__": "tuple",
            "items": [_serialize_simple_table_value(item) for item in value],
        }

    if isinstance(value, set):
        serialized_items = [_serialize_simple_table_value(item) for item in value]
        return {
            "__type__": "set",
            "items": sorted(serialized_items, key=lambda item: json.dumps(item, sort_keys=True)),
        }

    if isinstance(value, dict):
        return {key: _serialize_simple_table_value(item) for key, item in value.items()}

    return value


_ANNOTATION_DTYPE_MAP: dict[Any, str] = {
    bool: "bool",
    int: "int64",
    float: "float64",
    Decimal: "float64",
    datetime.datetime: "datetime64[ns, UTC]",
    datetime.date: "string",
    str: "string",
}


class SimpleTableUpdater(DataNode):
    DATA_NODE_UPDATE_CLASS = ms_client.SimpleTableUpdate
    SIMPLE_TABLE_SCHEMA: ClassVar[type[SimpleTable] | None] = None

    def __init__(
        self,
        configuration: SimpleTableUpdaterConfiguration,
        *,
        hash_namespace: str | None = None,
        test_node: bool = False,
    ):
        simple_table_schema = self.SIMPLE_TABLE_SCHEMA
        if not isinstance(simple_table_schema, type) or not issubclass(simple_table_schema, SimpleTable):
            raise TypeError(
                f"{self.__class__.__name__} must define SIMPLE_TABLE_SCHEMA as a SimpleTable subclass"
            )

        self.simple_table_schema = simple_table_schema
        super().__init__(
            config=configuration,
            hash_namespace=hash_namespace,
            test_node=test_node,
        )

    def _initialize_configuration(self, init_kwargs: dict) -> None:
        """
        Build the updater configuration and hashes using a resolved simple-table schema.

        Foreign-key declarations are authored against ``SimpleTable`` classes, but
        the hashed/backend schema must point to canonical backend table storages.
        This method therefore:

        - ensures a data source is available for storage resolution,
        - resolves every foreign-key target to its canonical ``SimpleTableStorage.id``,
        - injects the resolved schema into the hashed init kwargs,
        - stores the resolved schema on the updater so the persist manager can
          reuse the exact same payload during backend registration.
        """
        data_source = self._ensure_data_source_for_schema_resolution()
        resolved_schema = self._build_resolved_simple_table_schema(data_source=data_source)
        self.resolved_simple_table_schema = resolved_schema

        init_kwargs["simple_table_schema"] = resolved_schema
        init_kwargs["time_series_class_import_path"] = {
            "module": self.__class__.__module__,
            "qualname": self.__class__.__qualname__,
        }

        config = build_operations.create_config(
            kwargs=init_kwargs,
            ts_class_name=self.__class__.__name__,
        )

        for field_name, value in config.__dict__.items():
            setattr(self, field_name, value)

    def _ensure_data_source_for_schema_resolution(self) -> Any:
        if getattr(self, "_data_source", None) is None:
            self.set_data_source()
        return self.data_source

    @staticmethod
    def _resolved_schema_storage_hash(resolved_schema: dict[str, Any]) -> str:
        _, storage_hash = build_operations.hash_signature({"simple_table_schema": resolved_schema})
        return f"simpletable_{storage_hash}".lower()

    def _build_foreign_table_storage_kwargs(
        self,
        *,
        schema_cls: type[SimpleTable],
        resolved_schema: dict[str, Any],
        data_source: Any,
    ) -> dict[str, Any]:
        return {
            "storage_hash": self._resolved_schema_storage_hash(resolved_schema),
            "build_configuration": {"simple_table_schema": resolved_schema},
            "data_source": data_source.model_dump(),
            "build_configuration_json_schema": {},
            "open_to_public": False,
            "source_code_git_hash": get_data_node_source_code_git_hash(schema_cls),
            "source_code": get_data_node_source_code(schema_cls),
            "schema": resolved_schema,
            "source_class_name": schema_cls.__name__,
        }

    def _resolve_simple_table_storage(
        self,
        *,
        schema_cls: type[SimpleTable],
        data_source: Any,
        resolved_schema_cache: dict[type[SimpleTable], dict[str, Any]],
        resolved_storage_cache: dict[type[SimpleTable], ms_client.SimpleTableStorage],
        resolution_stack: set[type[SimpleTable]],
    ) -> ms_client.SimpleTableStorage:
        cached_storage = resolved_storage_cache.get(schema_cls)
        if cached_storage is not None:
            return cached_storage

        if schema_cls in resolution_stack:
            cycle = " -> ".join(cls.__name__ for cls in [*resolution_stack, schema_cls])
            raise ValueError(f"Cyclic simple-table foreign-key resolution is not supported: {cycle}")

        resolution_stack.add(schema_cls)
        try:
            resolved_schema = self._resolve_simple_table_schema_dict(
                schema_cls=schema_cls,
                data_source=data_source,
                resolved_schema_cache=resolved_schema_cache,
                resolved_storage_cache=resolved_storage_cache,
                resolution_stack=resolution_stack,
            )
            storage = ms_client.SimpleTableStorage.get_or_create(
                **self._build_foreign_table_storage_kwargs(
                    schema_cls=schema_cls,
                    resolved_schema=resolved_schema,
                    data_source=data_source,
                )
            )
            resolved_storage_cache[schema_cls] = storage
            return storage
        finally:
            resolution_stack.remove(schema_cls)

    def _resolve_simple_table_schema_dict(
        self,
        *,
        schema_cls: type[SimpleTable],
        data_source: Any,
        resolved_schema_cache: dict[type[SimpleTable], dict[str, Any]],
        resolved_storage_cache: dict[type[SimpleTable], ms_client.SimpleTableStorage],
        resolution_stack: set[type[SimpleTable]],
    ) -> dict[str, Any]:
        cached_schema = resolved_schema_cache.get(schema_cls)
        if cached_schema is not None:
            return cached_schema

        table_schema = schema_cls.schema()
        resolved_schema = json.loads(json.dumps(table_schema.to_canonical_dict()))

        for index, field_spec in enumerate(table_schema.fields):
            if field_spec.foreign_key is None:
                continue

            target_storage = self._resolve_simple_table_storage(
                schema_cls=field_spec.foreign_key.target,
                data_source=data_source,
                resolved_schema_cache=resolved_schema_cache,
                resolved_storage_cache=resolved_storage_cache,
                resolution_stack=resolution_stack,
            )
            resolved_schema["fields"][index]["foreign_key"]["target"] = target_storage.id

        resolved_schema_cache[schema_cls] = resolved_schema
        return resolved_schema

    def _build_resolved_simple_table_schema(self, data_source: Any) -> dict[str, Any]:
        resolved_schema_cache: dict[type[SimpleTable], dict[str, Any]] = {}
        resolved_storage_cache: dict[type[SimpleTable], ms_client.SimpleTableStorage] = {}
        return self._resolve_simple_table_schema_dict(
            schema_cls=self.simple_table_schema,
            data_source=data_source,
            resolved_schema_cache=resolved_schema_cache,
            resolved_storage_cache=resolved_storage_cache,
            resolution_stack=set(),
        )

    def _set_local_persist_manager(
        self,
        update_hash: str,
        data_node_update: None | dict = None,
    ) -> None:
        self._local_persist_manager = SimpleTablePersistManager(
            update_hash=update_hash,
            class_name=self.__class__.__name__,
            data_node_update=data_node_update,
            data_source=self.data_source,
            simple_table_schema=self.simple_table_schema,
            resolved_simple_table_schema=getattr(self, "resolved_simple_table_schema", None),
            configuration=self.config,
        )

    def dependencies(self) -> dict[str, DataNode]:
        return {}

    def hashes(self) -> tuple[str, str]:
        return self.update_hash, self.storage_hash

    def get_column_metadata(self) -> list[ms_client.ColumnMetaData] | None:
        columns: list[ms_client.ColumnMetaData] = []
        for field_spec in self.simple_table_schema.field_specs():
            dtype = _ANNOTATION_DTYPE_MAP.get(field_spec.annotation, "string")
            label = field_spec.name.replace("_", " ").title()
            columns.append(
                ms_client.ColumnMetaData(
                    column_name=field_spec.name,
                    dtype=dtype,
                    label=label,
                    description=label,
                )
            )
        return columns

    def _set_update_statistics(
        self,
        update_statistics: ms_client.BaseUpdateStatistics | None,
    ) -> ms_client.BaseUpdateStatistics | None:
        self.update_statistics = update_statistics
        return update_statistics

    def _normalize_update_records(
        self,
        update_result: Any,
    ) -> tuple[list[SimpleTable], bool]:
        overwrite = False
        records = update_result

        if (
            isinstance(update_result, tuple)
            and len(update_result) == 2
            and isinstance(update_result[1], bool)
        ):
            records, overwrite = update_result

        if not isinstance(records, Sequence) or isinstance(records, (str, bytes, bytearray)):
            raise TypeError(
                f"{self} update(...) must return a sequence of {self.simple_table_schema.__name__} "
                "instances, or a tuple of (records, overwrite)."
            )

        validated_records = list(records)
        invalid_record = next(
            (
                record
                for record in validated_records
                if not isinstance(record, self.simple_table_schema)
            ),
            None,
        )
        if invalid_record is not None:
            raise TypeError(
                f"{self} update(...) must return {self.simple_table_schema.__name__} instances; "
                f"received {type(invalid_record).__name__}."
            )

        return validated_records, overwrite

    def _execute_local_update(
        self,
        historical_update: Any,
    ) -> Any:
        del historical_update

        self.logger.debug(f"Calculating update for {self}...")
        update_result = self.update()
        if update_result is None:
            raise Exception(
                f" {self} update(...) method needs to return {self.simple_table_schema.__name__} "
                "instances or (instances, overwrite)"
            )

        records, overwrite = self._normalize_update_records(update_result)

        if len(records) == 0:
            self.logger.warning(f"No new data returned from update for {self}.")
            return records

        self.logger.info(f"Persisting {len(records)} new rows for {self}.")
        self.local_persist_manager.persist_records(
            records=records,
            overwrite=overwrite,
        )
        self.logger.info(f"Successfully updated {self} with {len(records)} records.")
        return records

    def insert_records(
        self, records: list[SimpleTable | dict[str, Any]]
    ) -> list[SimpleTable]:
        table_model = self.simple_table_schema
        validated = [
            record if isinstance(record, table_model) else table_model.model_validate(record)
            for record in records
        ]
        self.local_persist_manager.insert_records(validated)
        return validated

    def upsert_records(
        self, records: list[SimpleTable | dict[str, Any]]
    ) -> list[SimpleTable]:
        table_model = self.simple_table_schema
        validated = [
            record if isinstance(record, table_model) else table_model.model_validate(record)
            for record in records
        ]
        self.local_persist_manager.upsert_records(validated)
        return validated

    def delete_record(
        self,
        record_or_id: SimpleTable | Any,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> None:
        self.local_persist_manager.delete(record_or_id, timeout=timeout)
