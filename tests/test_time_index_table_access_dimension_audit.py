from __future__ import annotations

import os
from typing import Any

import pandas as pd
import pytest

os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh")

import mainsequence.client.metatables as client_metatables
import mainsequence.meta_tables.time_index_table_refs as table_refs
from mainsequence.client.metatables import TimeIndexMetaTable, TimeIndexTableUpdate
from mainsequence.client.utils import DoesNotExist
from mainsequence.meta_tables import TimeIndexTableRef
from mainsequence.meta_tables.time_index_table_refs import (
    TimeIndexTableAccessMixin,
    TimeIndexTableReader,
)


def _table(
    *,
    uid: str = "table-uid",
    data_source_uid: str = "data-source-uid",
    identifier: str = "prices",
    physical_schema: str = "public",
    physical_table_name: str = "prices_table",
) -> TimeIndexMetaTable:
    return TimeIndexMetaTable.model_construct(
        uid=uid,
        data_source_uid=data_source_uid,
        identifier=identifier,
        physical_schema=physical_schema,
        physical_table_name=physical_table_name,
    )


class _FakeAccessor:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.last_calls: list[dict[str, Any]] = []

    def get_df_between_dates(self, **kwargs: Any) -> str:
        self.calls.append(kwargs)
        return "ok"

    def get_last_observation(self, **kwargs: Any) -> str:
        self.last_calls.append(kwargs)
        return "latest"


class _Readable(TimeIndexTableAccessMixin):
    dependency_kind = "test"
    output_table_uid = "table-uid"
    data_source_uid = "data-source-uid"

    def __init__(self, accessor: _FakeAccessor) -> None:
        self._accessor = accessor

    @property
    def table_accessor(self) -> _FakeAccessor:
        return self._accessor


def test_access_mixin_repr_is_stable() -> None:
    assert repr(_Readable(_FakeAccessor())) == "_Readable"


def test_table_ref_requires_resolved_time_index_meta_table() -> None:
    with pytest.raises(TypeError, match="must be a TimeIndexMetaTable"):
        TimeIndexTableRef(output_table=object())  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="canonical TimeIndexMetaTable uid"):
        TimeIndexTableRef(output_table=_table(uid=""))

    with pytest.raises(ValueError, match="data-source identity"):
        TimeIndexTableRef(output_table=_table(data_source_uid=""))


def test_table_ref_rejects_cross_environment_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        table_refs,
        "resolve_organization_environment_uid",
        lambda operation: "current-environment-uid",
    )
    table = _table()
    table.organization_environment_uid = "other-environment-uid"

    with pytest.raises(ValueError, match="another Organization Environment"):
        TimeIndexTableRef(output_table=table)


def test_table_ref_has_table_identity_and_no_executable_surface() -> None:
    table = _table()
    ref = TimeIndexTableRef(output_table=table)

    assert repr(ref) == "TimeIndexTableRef"
    assert ref.output_table is table
    assert ref.output_table_uid == "table-uid"
    assert ref.time_index_meta_table_uid == "table-uid"
    assert ref.data_source_uid == "data-source-uid"
    assert isinstance(ref.reader, TimeIndexTableReader)
    for forbidden_name in ("run", "update", "update_hash", "scheduler", "set_relation_tree"):
        assert not hasattr(ref, forbidden_name)


def test_table_ref_from_uid_uses_canonical_table_model(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    table = _table()

    def fake_get(**kwargs: Any) -> TimeIndexMetaTable:
        captured.update(kwargs)
        return table

    monkeypatch.setattr(TimeIndexMetaTable, "get", staticmethod(fake_get))

    ref = TimeIndexTableRef.from_uid("table-uid")

    assert captured == {"uid": "table-uid"}
    assert ref.output_table is table


def test_table_ref_from_identifier_uses_canonical_table_model(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    table = _table()

    def fake_get(**kwargs: Any) -> TimeIndexMetaTable:
        captured.update(kwargs)
        return table

    monkeypatch.setattr(TimeIndexMetaTable, "get", staticmethod(fake_get))

    ref = TimeIndexTableRef.from_identifier("prices")

    assert captured == {"identifier": "prices"}
    assert ref.output_table is table


def test_table_ref_from_meta_table_uses_canonical_uid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table = _table()
    generic_table = client_metatables.MetaTable.model_construct(uid="table-uid")
    monkeypatch.setattr(
        TimeIndexMetaTable,
        "get",
        staticmethod(lambda **kwargs: table if kwargs == {"uid": "table-uid"} else None),
    )

    ref = TimeIndexTableRef.from_meta_table(generic_table)

    assert ref.output_table is table


def test_table_ref_from_physical_identity_requires_an_exact_match(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table = _table()
    captured: dict[str, Any] = {}

    def fake_filter_by_body(**kwargs: Any) -> list[TimeIndexMetaTable]:
        captured.update(kwargs)
        return [table]

    monkeypatch.setattr(TimeIndexMetaTable, "filter_by_body", staticmethod(fake_filter_by_body))

    ref = TimeIndexTableRef.from_physical_identity(
        schema="public",
        table_name="prices_table",
        data_source_uid="data-source-uid",
    )

    assert ref.output_table is table
    assert captured == {
        "physical_schema": "public",
        "physical_table_name": "prices_table",
        "data_source__uid": "data-source-uid",
        "limit": 2,
    }

    monkeypatch.setattr(TimeIndexMetaTable, "filter_by_body", staticmethod(lambda **_: []))
    with pytest.raises(DoesNotExist, match="No TimeIndexMetaTable"):
        TimeIndexTableRef.from_physical_identity(schema="public", table_name="missing")

    duplicate = _table(uid="duplicate-uid")
    monkeypatch.setattr(
        TimeIndexMetaTable,
        "filter_by_body",
        staticmethod(lambda **_: [table, duplicate]),
    )
    with pytest.raises(ValueError, match="ambiguous"):
        TimeIndexTableRef.from_physical_identity(
            schema="public",
            table_name="prices_table",
        )


def test_table_ref_from_table_update_resolves_uid(monkeypatch: pytest.MonkeyPatch) -> None:
    table = _table()
    monkeypatch.setattr(
        TimeIndexMetaTable,
        "get",
        staticmethod(lambda **kwargs: table if kwargs == {"uid": "table-uid"} else None),
    )
    update = TimeIndexTableUpdate.model_construct(output_table="table-uid")

    assert TimeIndexTableRef.from_table_update(update).output_table is table


def test_access_mixin_forwards_explicit_dimension_contract() -> None:
    accessor = _FakeAccessor()
    readable = _Readable(accessor)
    range_map = [{"coordinate": {"entity_uid": "BTC"}, "start_date": "2026-05-01T00:00:00Z"}]

    assert (
        readable.get_df_between_dates(
            dimension_filters={"entity_uid": ["BTC", "ETH"]},
            dimension_range_map=range_map,
        )
        == "ok"
    )
    assert accessor.calls == [
        {
            "start_date": None,
            "end_date": None,
            "great_or_equal": True,
            "less_or_equal": True,
            "dimension_filters": {"entity_uid": ["BTC", "ETH"]},
            "index_coordinates": None,
            "dimension_range_map": range_map,
            "columns": None,
        }
    ]

    assert (
        readable.get_last_observation(dimension_filters={"account_uid": ["account-a"]}) == "latest"
    )
    assert accessor.last_calls == [
        {
            "dimension_filters": {"account_uid": ["account-a"]},
            "index_coordinates": None,
            "dimension_range_map": None,
        }
    ]


def test_reader_delegates_to_resolved_table(monkeypatch: pytest.MonkeyPatch) -> None:
    table = _table()
    expected = pd.DataFrame()
    monkeypatch.setattr(
        TimeIndexMetaTable,
        "get_data_between_dates_from_api",
        lambda self, *args, **kwargs: expected,
    )
    reader = TimeIndexTableReader(output_table=table)

    assert reader.get_df_between_dates() is expected
