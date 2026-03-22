from __future__ import annotations

import logging
from typing import Any

from .models import SimpleTable


class SimpleTablePersistManager:
    """
    Minimal persistence manager for simple-table nodes.

    The manager keeps the node-facing lifecycle small while delegating the
    actual HTTP behavior to the `SimpleTable` client mixin methods.
    """

    def __init__(
        self,
        *,
        simple_table_schema: type[SimpleTable],
        configuration: Any = None,
        class_name: str | None = None,
    ):
        self.simple_table_schema = simple_table_schema
        self.configuration = configuration
        self.class_name = class_name
        self.logger = logging.getLogger(
            f"{self.__class__.__module__}.{self.__class__.__qualname__}"
        )

    @property
    def table_schema(self):
        return self.simple_table_schema.schema()

    @property
    def schema_fingerprint(self) -> str:
        return self.table_schema.fingerprint

    @property
    def physical_name(self) -> str:
        return self.table_schema.physical_name

    def insert_records(
        self,
        records: list[SimpleTable],
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> list[SimpleTable]:
        return self.simple_table_schema.insert_records(records, timeout=timeout)

    def upsert_records(
        self,
        records: list[SimpleTable],
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> list[SimpleTable]:
        return self.simple_table_schema.upsert_records(records, timeout=timeout)

    def delete(
        self,
        record_or_id: SimpleTable | Any,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> None:
        if isinstance(record_or_id, self.simple_table_schema):
            record_or_id.delete(timeout=timeout)
            return
        self.simple_table_schema.delete_by_id(record_or_id, timeout=timeout)
