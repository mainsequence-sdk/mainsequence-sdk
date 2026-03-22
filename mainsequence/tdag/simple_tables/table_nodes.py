from __future__ import annotations

import datetime
import hashlib
import json
import logging
from abc import ABC, abstractmethod
from decimal import Decimal
from enum import Enum
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict

from ..pydantic_metadata import serialize_pydantic_model, strip_pydantic_hash_exclusions
from .models import Index, SimpleTable
from .persist_managers import SimpleTablePersistManager


class SimpleTableUpdaterConfiguration(BaseModel):
    """
    Base class for simple-table node configuration.

    Fields participate in the update hash by default. Mark a field with
    ``json_schema_extra={"update_only": True}`` to exclude it from the
    storage hash, or ``json_schema_extra={"runtime_only": True}`` to
    exclude it from both hashes.
    """

    model_config = ConfigDict(extra="forbid")

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


class BaseNode(ABC):
    """
    Minimal node abstraction local to `simple_tables`.

    This intentionally avoids any `DataNode` / time-series backend coupling while
    still preserving a familiar node lifecycle:
    - declared dependencies
    - `run()` that updates the dependency tree first
    """

    def __init__(self):
        self.logger = logging.getLogger(
            f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        )

    def dependencies(self) -> dict[str, BaseNode]:
        return {}

    def run(self, *, update_tree: bool = True) -> Any:
        seen: set[int] = set()
        return self._run_with_seen(update_tree=update_tree, seen=seen)

    def _run_with_seen(self, *, update_tree: bool, seen: set[int]) -> Any:
        node_id = id(self)
        if node_id in seen:
            return None
        seen.add(node_id)

        if update_tree:
            for dependency in self.dependencies().values():
                dependency._run_with_seen(update_tree=True, seen=seen)

        return self.update()

    @abstractmethod
    def update(self) -> Any:
        raise NotImplementedError


class SimpleTableUpdater(BaseNode):
    SIMPLE_TABLE_SCHEMA: ClassVar[type[SimpleTable] | None] = None

    def __init__(
        self,
        configuration: SimpleTableUpdaterConfiguration | None = None,
    ):
        super().__init__()
        simple_table_schema = self.SIMPLE_TABLE_SCHEMA
        if not isinstance(simple_table_schema, type) or not issubclass(simple_table_schema, SimpleTable):
            raise TypeError(
                f"{self.__class__.__name__} must define SIMPLE_TABLE_SCHEMA as a SimpleTable subclass"
            )

        self.simple_table_schema = simple_table_schema
        self.configuration = configuration or SimpleTableUpdaterConfiguration()
        self.persist_manager = SimpleTablePersistManager(
            simple_table_schema=self.simple_table_schema,
            configuration=self.configuration,
            class_name=self.__class__.__name__,
        )

    def schema_configuration(self) -> dict[str, Any]:
        return self.simple_table_schema.schema().to_canonical_dict()

    def update_configuration(self) -> dict[str, Any]:
        return {
            "simple_table_schema": self.schema_configuration(),
            "configuration": self.configuration.update_configuration(),
        }

    def storage_configuration(self) -> dict[str, Any]:
        return {
            "simple_table_schema": self.schema_configuration(),
            "configuration": self.configuration.storage_configuration(),
        }

    def hashes(self) -> tuple[str, str]:
        update_configuration = self.update_configuration()
        storage_configuration = self.storage_configuration()
        return (
            hashlib.md5(json.dumps(update_configuration, sort_keys=True).encode()).hexdigest(),
            hashlib.md5(json.dumps(storage_configuration, sort_keys=True).encode()).hexdigest(),
        )

    @classmethod
    def _field_has_index_marker(cls, field_info: Any) -> bool:
        return any(
            isinstance(item, Index)
            for item in tuple(getattr(field_info, "metadata", ()) or ())
        )

    def get_index_field_names(self) -> list[str]:
        return [
            field_name
            for field_name, field_info in self.simple_table_schema.model_fields.items()
            if self._field_has_index_marker(field_info)
        ]

    def persist_records(self, records: list[SimpleTable]) -> Any:
        return self.persist_manager.insert_records(records)

    def insert_records(
        self, records: list[SimpleTable | dict[str, Any]]
    ) -> list[SimpleTable]:
        table_model = self.simple_table_schema
        validated = [
            record if isinstance(record, table_model) else table_model.model_validate(record)
            for record in records
        ]
        self.persist_records(validated)
        return validated

    def upsert_records(
        self, records: list[SimpleTable | dict[str, Any]]
    ) -> list[SimpleTable]:
        table_model = self.simple_table_schema
        validated = [
            record if isinstance(record, table_model) else table_model.model_validate(record)
            for record in records
        ]
        return self.persist_manager.upsert_records(validated)

    def delete_record(
        self,
        record_or_id: SimpleTable | Any,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> None:
        self.persist_manager.delete(record_or_id, timeout=timeout)
