import os

os.environ.setdefault("MAINSEQUENCE_ACCESS_TOKEN", "test-access-token")
os.environ.setdefault("MAINSEQUENCE_REFRESH_TOKEN", "test-refresh-token")

from mainsequence.client.models_simple_tables import SimpleTableStorage
from mainsequence.client.models_tdag import DataNodeStorage, DataNodeUpdate, DataNodeUpdateDetails


def test_data_node_update_accepts_local_time_serie_update_details_in_run_configuration():
    payload = {
        "id": 44,
        "update_hash": "issue-44-update-hash",
        "data_node_storage": 1,
        "build_configuration": {},
        "run_configuration": {
            "update_schedule": "*/1 * * * *",
            "local_time_serie_update_details": 8053,
        },
        "update_details": {
            "related_table": 44,
            "run_configuration": {
                "update_schedule": "*/1 * * * *",
                "local_time_serie_update_details": 8053,
            },
        },
    }

    update = DataNodeUpdate(**payload)

    assert update.run_configuration is not None
    assert update.run_configuration.local_time_serie_update_details == 8053
    assert isinstance(update.update_details, DataNodeUpdateDetails)
    assert update.update_details.run_configuration is not None
    assert update.update_details.run_configuration.local_time_serie_update_details == 8053


def test_data_node_storage_accepts_namespace():
    storage = DataNodeStorage(
        id=12,
        storage_hash="prices_storage_hash",
        namespace="pytest_case_123",
        data_source=1,
        source_class_name="PricesNode",
        creation_date="2026-04-13T00:00:00Z",
    )

    assert storage.namespace == "pytest_case_123"


def test_simple_table_storage_accepts_namespace():
    storage = SimpleTableStorage(
        id=15,
        storage_hash="orders_storage_hash",
        namespace="pytest_case_456",
        data_source={"id": 1},
        schema={"model": "tests.OrderRow", "fields": []},
        source_class_name="OrderUpdater",
    )

    assert storage.namespace == "pytest_case_456"
