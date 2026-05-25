from __future__ import annotations

import pytest

from mainsequence.client import models_tdag


def _data_node_update_without_uid() -> models_tdag.DataNodeUpdate:
    return models_tdag.DataNodeUpdate(
        uid=None,
        update_hash="prices_daily",
        build_configuration={},
        data_node_storage="data-node-storage-uid",
    )


@pytest.mark.parametrize(
    ("method_name", "args", "kwargs"),
    [
        ("add_tags", (["curated"],), {}),
        ("set_start_of_execution", (), {}),
        ("set_end_of_execution", (), {"historical_update_id": 1}),
        ("set_last_update_index_time_from_update_stats", (), {}),
        ("get_all_dependencies_update_priority", (), {}),
        ("verify_if_direct_dependencies_are_updated", (), {}),
    ],
)
def test_data_node_update_public_endpoints_require_uid_before_request(
    monkeypatch,
    method_name: str,
    args: tuple,
    kwargs: dict,
):
    def _fail_make_request(**_kwargs):
        raise AssertionError("make_request should not be called when uid is missing")

    monkeypatch.setattr(models_tdag, "make_request", _fail_make_request)
    update = _data_node_update_without_uid()

    with pytest.raises(ValueError, match="DataNodeUpdate must have a uid"):
        getattr(update, method_name)(*args, **kwargs)
