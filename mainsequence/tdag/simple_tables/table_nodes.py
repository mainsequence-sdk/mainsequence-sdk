from __future__ import annotations

import datetime
import json
from decimal import Decimal
from enum import Enum
from typing import Any, ClassVar

from pydantic import BaseModel

import mainsequence.client as ms_client

from ..configuration_models import BaseConfiguration
from ..data_nodes import build_operations
from ..data_nodes.data_nodes import DataNode
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
        init_kwargs["simple_table_schema"] = self.simple_table_schema.schema().to_canonical_dict()
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
