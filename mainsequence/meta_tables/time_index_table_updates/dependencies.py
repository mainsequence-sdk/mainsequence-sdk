from __future__ import annotations

from mainsequence.meta_tables.time_index_table_refs import TimeIndexTableRef

from .updaters import TimeIndexTableUpdater

type TableDependency = TimeIndexTableUpdater | TimeIndexTableRef

__all__ = ["TableDependency"]
