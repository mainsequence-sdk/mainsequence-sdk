from __future__ import annotations

import pytest

from mainsequence.client import metatables as models_metatables


def _table_update_without_uid() -> models_metatables.TimeIndexTableUpdate:
    return models_metatables.TimeIndexTableUpdate(
        uid=None,
        update_hash="prices_daily",
        build_configuration={
            "configuration_schema_version": 2,
            "table_updater_class_import_path": {
                "module": "tests.test_update_uid_guards",
                "qualname": "ExampleUpdater",
            },
        },
        output_table="time-index-table-storage-uid",
    )


@pytest.mark.parametrize(
    ("method_name", "args", "kwargs"),
    [
        ("set_start_of_execution", (), {}),
        ("set_end_of_execution", (), {"table_update_run_uid": "table-update-run-uid"}),
        ("set_last_update_index_time_from_update_stats", (), {}),
        ("get_all_dependencies_update_priority", (), {}),
        ("verify_if_direct_dependencies_are_updated", (), {}),
    ],
)
def test_table_update_public_endpoints_require_uid_before_request(
    monkeypatch,
    method_name: str,
    args: tuple,
    kwargs: dict,
):
    def _fail_make_request(**_kwargs):
        raise AssertionError("make_request should not be called when uid is missing")

    monkeypatch.setattr(models_metatables, "make_request", _fail_make_request)
    update = _table_update_without_uid()

    with pytest.raises(ValueError, match="TimeIndexTableUpdate must have a uid"):
        getattr(update, method_name)(*args, **kwargs)
