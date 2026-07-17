import ast
import datetime
import json
import pathlib
import uuid
from types import SimpleNamespace

import pytest
from pydantic import ConfigDict, Field

import mainsequence.client.agent_runtime_models as agent_models_mod
import mainsequence.client.base as base_mod
import mainsequence.client.models_foundry as models_foundry_mod
import mainsequence.client.models_helpers as models_helpers_mod
import mainsequence.client.models_user as models_user_mod
from mainsequence.client.base import BaseObjectOrm, BasePydanticModel, ShareableObjectMixin


class _IdRef:
    def __init__(self, value: int):
        self.id = value


class _UidRef:
    def __init__(self, value: str):
        self.uid = value


class DemoFilterModel(BaseObjectOrm):
    FILTERSET_FIELDS = {
        "name": ["exact", "contains", "in"],
        "parent__id": ["exact", "in"],
        "active": ["isnull"],
    }
    FILTER_VALUE_NORMALIZERS = {
        "parent__id": "id",
    }


class DemoDestroyModel(BaseObjectOrm):
    DESTROY_QUERY_PARAMS = {
        "full_delete_selected": "bool",
        "override_protection": "bool",
    }

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        return "https://backend.test/demo"

    @classmethod
    def build_session(cls):
        return object()


class DemoReadModel(BaseObjectOrm):
    FILTERSET_FIELDS = {
        "id": ["exact"],
    }
    FILTER_VALUE_NORMALIZERS = {
        "id": "id",
    }
    READ_QUERY_PARAMS = {
        "include_relations_detail": "bool",
    }

    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.payload = kwargs

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        return "https://backend.test/demo-read"

    @classmethod
    def build_session(cls):
        return object()


class DemoShareableModel(ShareableObjectMixin, BaseObjectOrm):
    def __init__(self, uid: str, object_id: int | None = None):
        self.uid = uid
        self.id = object_id

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        return "https://backend.test/demo-shareable"

    @classmethod
    def build_session(cls):
        return object()


class DemoIdOnlyResource(ShareableObjectMixin, BaseObjectOrm):
    def __init__(self, object_id: int):
        self.id = object_id

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        return "https://backend.test/demo-shareable"

    @classmethod
    def build_session(cls):
        return object()


class DemoPatchModel(BasePydanticModel, BaseObjectOrm):
    id: int
    label: str | None = None

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        return "https://backend.test/demo-patch"

    @classmethod
    def build_session(cls):
        return object()


class DemoAliasedPatchModel(BasePydanticModel, BaseObjectOrm):
    model_config = ConfigDict(populate_by_name=True)

    id: int
    schema_payload: dict | None = Field(default=None, alias="schema")

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        return "https://backend.test/demo-patch"

    @classmethod
    def build_session(cls):
        return object()


def test_job_run_status_uses_status_detail_endpoint(monkeypatch):
    job_run = models_helpers_mod.JobRun(
        uid="4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
        name="demo-run",
        unique_identifier="jobrun_501",
    )

    captured: dict[str, object] = {}

    class _FakeResponse:
        status_code = 200

        def json(self):
            return {"message": "Job status updated to RUNNING."}

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out):
        captured["url"] = url
        captured["r_type"] = r_type
        captured["payload"] = payload
        return _FakeResponse()

    monkeypatch.setattr(models_helpers_mod, "make_request", _fake_make_request)

    payload = job_run.job_run_status(status="RUNNING", git_hash="abc123", timeout=30)

    assert payload == {"message": "Job status updated to RUNNING."}
    assert captured["r_type"] == "POST"
    assert captured["payload"] == {"status": "RUNNING", "git_hash": "abc123"}
    assert str(captured["url"]).endswith(
        "/orm/api/pods/job-run/4c1d77c8-8a42-42b8-a9c1-06be9a336e5d/status/"
    )


def test_job_run_filters_are_uid_based():
    normalized = models_helpers_mod.JobRun._normalize_filter_kwargs(
        {
            "uid": " 4c1d77c8-8a42-42b8-a9c1-06be9a336e5d ",
            "job__uid__in": [
                " ab6a5d50-8a3e-4f0d-a9bb-7e84180bd50e ",
            ],
        }
    )

    assert normalized == {
        "uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
        "job__uid__in": ["ab6a5d50-8a3e-4f0d-a9bb-7e84180bd50e"],
    }
    with pytest.raises(ValueError, match="job__id"):
        models_helpers_mod.JobRun._normalize_filter_kwargs({"job__id": [501]})


def test_job_run_deserializes_uid_payload_without_id():
    job_run = models_helpers_mod.JobRun(
        uid="4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
        name="demo-run",
        unique_identifier="jobrun_501",
        job_uid="ab6a5d50-8a3e-4f0d-a9bb-7e84180bd50e",
        job_name="daily-training-job",
        status="RUNNING",
        cpu_request="1",
        cpu_limit="2",
        memory_request="4Gi",
        memory_limit="8Gi",
        gpu_request="1",
        gpu_type="nvidia-l4",
        command_args=["sync"],
    )

    dumped = job_run.model_dump()
    assert dumped["uid"] == "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d"
    assert dumped["job_uid"] == "ab6a5d50-8a3e-4f0d-a9bb-7e84180bd50e"
    assert "id" not in dumped


def test_normalize_filter_kwargs_coerces_supported_values():
    normalized = DemoFilterModel._normalize_filter_kwargs(
        {
            "name__contains": "  momentum  ",
            "name__in": [" growth ", "value"],
            "parent__id__in": [1, _IdRef(2), {"id": 3}],
            "active__isnull": "true",
        }
    )

    assert normalized == {
        "name__contains": "momentum",
        "name__in": ["growth", "value"],
        "parent__id__in": [1, 2, 3],
        "active__isnull": True,
    }


def test_project_image_accepts_creation_date():
    from mainsequence.client.models_foundry import ProjectImage

    image = ProjectImage(
        uid="f3cb8477-df47-49cb-a151-80b746fb1243",
        project_repo_hash="abc123",
        related_project_uid="5a28020a-0f1b-47ee-aab8-334286234bea",
        base_image=None,
        is_ready=False,
        creation_date="2026-04-07T09:00:00Z",
    )

    assert image.creation_date == datetime.datetime(
        2026,
        4,
        7,
        9,
        0,
        tzinfo=datetime.UTC,
    )


def test_data_node_storage_normalizes_namespace_filters():
    from mainsequence.client.metatables import TimeIndexMetaTable

    normalized = TimeIndexMetaTable._normalize_filter_kwargs(
        {
            "namespace__contains": "  pytest  ",
            "namespace__in": [" alpha ", "beta"],
            "namespace__isnull": "false",
        }
    )

    assert normalized == {
        "namespace__contains": "pytest",
        "namespace__in": ["alpha", "beta"],
        "namespace__isnull": False,
    }


def test_data_node_storage_normalizes_data_source_uid_filters():
    from mainsequence.client.metatables import TimeIndexMetaTable

    uid = uuid.UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd")

    normalized = TimeIndexMetaTable._normalize_filter_kwargs(
        {
            "data_source__uid": {"uid": uid},
            "data_source__uid__in": [{"uid": uid}],
        }
    )

    assert normalized == {
        "data_source__uid": str(uid),
        "data_source__uid__in": [str(uid)],
    }


def test_data_node_storage_rejects_data_source_id_filter():
    from mainsequence.client.metatables import TimeIndexMetaTable

    with pytest.raises(ValueError, match="Unsupported TimeIndexMetaTable filter"):
        TimeIndexMetaTable._normalize_filter_kwargs({"data_source__id": {"id": 7}})


def test_include_relations_detail_is_only_data_node_update_read_param():
    from mainsequence.client.metatables import DataNodeUpdate, TimeIndexMetaTable

    assert "include_relations_detail" in DataNodeUpdate.READ_QUERY_PARAMS
    assert "include_relations_detail" not in (TimeIndexMetaTable.READ_QUERY_PARAMS or {})

    filter_kwargs, read_query_kwargs = TimeIndexMetaTable._split_filter_and_read_query_kwargs(
        {"include_relations_detail": True}
    )

    assert read_query_kwargs == {}
    assert filter_kwargs == {"include_relations_detail": True}
    with pytest.raises(ValueError, match="Unsupported TimeIndexMetaTable filter"):
        TimeIndexMetaTable._normalize_filter_kwargs(filter_kwargs)


def test_data_node_storage_delete_after_date_posts_tail_delete(monkeypatch):
    from mainsequence.client import metatables as models_metatables

    captured = {}

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "ok": True,
                "dynamic_table_id": 714,
                "deleted_count": 123,
                "table_empty": False,
                "stats": {
                    "last_time_index_value": "2026-03-31T23:59:00Z",
                    "earliest_index_value": "2024-01-01T00:00:00Z",
                    "multi_index_stats": None,
                    "multi_index_column_stats": None,
                },
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(models_metatables, "make_request", _fake_make_request)
    monkeypatch.setattr(
        models_metatables.TimeIndexMetaTable, "build_session", classmethod(lambda cls: object())
    )

    storage = models_metatables.TimeIndexMetaTable(
        uid="714",
        storage_hash="prices_hash",
        management_mode="platform_managed",
        physical_table_name="prices_hash",
        data_source=1,
        source_class_name="PricesNode",
        creation_date="2026-04-01T00:00:00Z",
        time_indexed_profile=models_metatables.TimeIndexedProfile(
            related_table_uid="714",
            time_index_name="time_index",
            index_names=["time_index", "entity_uid"],
            column_dtypes_map={
                "time_index": "datetime64[ns, UTC]",
                "entity_uid": "object",
                "value": "float64",
            },
            storage_layout={
                "time_index": "time_index",
                "identity_dimensions": ["entity_uid"],
            },
            physical_index_plan={
                "uniqueness": {"columns": ["time_index", "entity_uid"]},
            },
        ),
    )

    result = storage.delete_after_date(
        "2026-04-01T00:00:00Z",
        dimension_filters={"entity_uid": ["AAPL", "MSFT"]},
        timeout=30,
    )

    assert result["ok"] is True
    assert result["deleted_count"] == 123
    assert captured == {
        "r_type": "POST",
        "url": f"{models_metatables.TimeIndexMetaTable.get_object_url()}/714/delete_after_date/",
        "payload": {
            "json": {
                "after_date": "2026-04-01T00:00:00Z",
                "dimension_filters": {"entity_uid": ["AAPL", "MSFT"]},
            }
        },
        "timeout": 30,
    }


def test_data_node_storage_delete_after_date_accepts_index_coordinates(monkeypatch):
    from mainsequence.client import metatables as models_metatables

    captured = {}

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {"ok": True, "dynamic_table_id": 714, "deleted_count": 1}

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["payload"] = payload
        return FakeResponse()

    monkeypatch.setattr(models_metatables, "make_request", _fake_make_request)
    monkeypatch.setattr(
        models_metatables.TimeIndexMetaTable, "build_session", classmethod(lambda cls: object())
    )

    storage = models_metatables.TimeIndexMetaTable(
        uid="714",
        storage_hash="prices_hash",
        management_mode="platform_managed",
        physical_table_name="prices_hash",
        data_source=1,
        source_class_name="PricesNode",
        creation_date="2026-04-01T00:00:00Z",
        time_indexed_profile=models_metatables.TimeIndexedProfile(
            related_table_uid="714",
            time_index_name="time_index",
            index_names=["time_index", "entity_uid"],
            column_dtypes_map={
                "time_index": "datetime64[ns, UTC]",
                "entity_uid": "object",
                "value": "float64",
            },
            storage_layout={
                "time_index": "time_index",
                "identity_dimensions": ["entity_uid"],
            },
            physical_index_plan={
                "uniqueness": {"columns": ["time_index", "entity_uid"]},
            },
        ),
    )

    storage.delete_after_date(
        datetime.datetime(2026, 4, 1, 0, 0, tzinfo=datetime.UTC),
        index_coordinates=[{"entity_uid": "AAPL"}],
    )

    assert captured["payload"] == {
        "json": {
            "after_date": "2026-04-01T00:00:00+00:00",
            "index_coordinates": [{"entity_uid": "AAPL"}],
        }
    }


def test_data_node_storage_run_query_posts_plain_text_sql(monkeypatch):
    from mainsequence.client import metatables as models_metatables

    captured = {}
    session = SimpleNamespace(headers={"Content-Type": "application/json"})

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "ok": True,
                "query_id": "abc123",
                "dynamic_table_id": 714,
                "results": [{"column_a": "value", "column_b": 10}],
                "truncated": False,
                "max_rows": 1000,
                "row_count": 1,
                "error": None,
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["headers"] = dict(s.headers)
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(models_metatables, "make_request", _fake_make_request)
    monkeypatch.setattr(
        models_metatables.TimeIndexMetaTable, "build_session", classmethod(lambda cls: session)
    )

    storage = models_metatables.TimeIndexMetaTable(
        uid="714",
        storage_hash="prices_hash",
        management_mode="platform_managed",
        physical_table_name="prices_hash",
        data_source=1,
        source_class_name="PricesNode",
        creation_date="2026-04-01T00:00:00Z",
    )

    result = storage.run_query("SELECT * FROM my_table LIMIT 100", timeout=30)

    assert result["ok"] is True
    assert result["dynamic_table_id"] == 714
    assert captured == {
        "headers": {"Content-Type": "text/plain"},
        "r_type": "POST",
        "url": f"{models_metatables.TimeIndexMetaTable.get_object_url()}/714/run_query/",
        "payload": {"data": "SELECT * FROM my_table LIMIT 100"},
        "timeout": 30,
    }
    assert session.headers == {"Content-Type": "application/json"}


def test_meta_table_run_query_posts_json_sql(monkeypatch):
    from mainsequence.client import metatables as models_metatables

    captured = {}
    session = SimpleNamespace(headers={"Content-Type": "application/json"})

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "ok": True,
                "query_id": "abc123",
                "meta_table_uid": "b14db80b-64b7-4390-8483-5377510de505",
                "results": [{"column_a": "value", "column_b": 10}],
                "truncated": False,
                "max_rows": 1000,
                "row_count": 1,
                "error": None,
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["headers"] = dict(s.headers)
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(models_metatables, "make_request", _fake_make_request)
    monkeypatch.setattr(
        models_metatables.MetaTable, "build_session", classmethod(lambda cls: session)
    )

    meta_table = models_metatables.MetaTable(
        uid="b14db80b-64b7-4390-8483-5377510de505",
        data_source_uid="data-source-uid",
        storage_hash="asset_storage",
        management_mode="platform_managed",
        physical_table_name="asset_storage",
    )

    result = meta_table.run_query("SELECT * FROM asset LIMIT 100", timeout=30)

    assert result["ok"] is True
    assert result["meta_table_uid"] == "b14db80b-64b7-4390-8483-5377510de505"
    assert captured == {
        "headers": {"Content-Type": "application/json"},
        "r_type": "POST",
        "url": (
            f"{models_metatables.MetaTable.get_object_url()}/"
            "b14db80b-64b7-4390-8483-5377510de505/run_query/"
        ),
        "payload": {"json": "SELECT * FROM asset LIMIT 100"},
        "timeout": 30,
    }
    assert session.headers == {"Content-Type": "application/json"}


def test_data_node_storage_run_query_returns_structured_error_envelope(monkeypatch):
    from mainsequence.client import metatables as models_metatables

    session = SimpleNamespace(headers={})

    class FakeResponse:
        status_code = 400
        content = b'{"ok": false}'

        @staticmethod
        def json():
            return {
                "ok": False,
                "query_id": "abc123",
                "dynamic_table_id": 714,
                "results": [],
                "truncated": False,
                "max_rows": 0,
                "row_count": 0,
                "error": {
                    "kind": "validation_error",
                    "message": "Only SELECT/WITH/EXPLAIN queries are allowed.",
                    "retryable": False,
                    "sqlstate": None,
                },
            }

    monkeypatch.setattr(models_metatables, "make_request", lambda **_kwargs: FakeResponse())
    monkeypatch.setattr(
        models_metatables.TimeIndexMetaTable, "build_session", classmethod(lambda cls: session)
    )

    storage = models_metatables.TimeIndexMetaTable(
        uid="714",
        storage_hash="prices_hash",
        management_mode="platform_managed",
        physical_table_name="prices_hash",
        data_source=1,
        source_class_name="PricesNode",
        creation_date="2026-04-01T00:00:00Z",
    )

    result = storage.run_query("DELETE FROM my_table")
    assert result["ok"] is False
    assert result["error"]["kind"] == "validation_error"


def test_data_node_update_normalizes_related_table_namespace_filters():
    from mainsequence.client.metatables import DataNodeUpdate

    normalized = DataNodeUpdate._normalize_filter_kwargs(
        {
            "related_table__namespace__contains": "  pytest  ",
            "related_table__namespace__in": [" alpha ", "beta"],
            "related_table__namespace__isnull": "false",
        }
    )

    assert normalized == {
        "related_table__namespace__contains": "pytest",
        "related_table__namespace__in": ["alpha", "beta"],
        "related_table__namespace__isnull": False,
    }


def test_data_node_update_accepts_uid_update_lookup_filters():
    from mainsequence.client.metatables import DataNodeUpdate

    uid = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"

    normalized = DataNodeUpdate._normalize_filter_kwargs(
        {
            "update_hash": " weights_daily ",
            "remote_table__uid": {"uid": uid},
            "remote_table__data_source__uid": {"uid": uid},
        }
    )

    assert normalized == {
        "update_hash": "weights_daily",
        "remote_table__uid": uid,
        "remote_table__data_source__uid": uid,
    }


def test_data_node_update_rejects_data_source_id_filter():
    from mainsequence.client.metatables import DataNodeUpdate

    with pytest.raises(ValueError, match="Unsupported DataNodeUpdate filter"):
        DataNodeUpdate._normalize_filter_kwargs({"remote_table__data_source__id": {"id": 7}})


def test_meta_table_normalizes_data_source_uid_filters():
    from mainsequence.client.metatables import MetaTable

    uid = "ffffffff-ffff-4fff-8fff-ffffffffffff"

    normalized = MetaTable._normalize_filter_kwargs(
        {
            "data_source__uid": {"uid": uid},
            "data_source__uid__in": [{"uid": uid}],
        }
    )

    assert normalized == {
        "data_source__uid": uid,
        "data_source__uid__in": [uid],
    }


def test_meta_table_rejects_data_source_id_filter():
    from mainsequence.client.metatables import MetaTable

    with pytest.raises(ValueError, match="Unsupported MetaTable filter"):
        MetaTable._normalize_filter_kwargs({"data_source__id": {"id": 7}})


def test_normalize_filter_kwargs_rejects_unsupported_filters():
    with pytest.raises(ValueError, match="Unsupported DemoFilterModel filter"):
        DemoFilterModel._normalize_filter_kwargs({"unsupported": 1})


def test_normalize_destroy_kwargs_coerces_supported_values():
    normalized = DemoDestroyModel._normalize_destroy_kwargs(
        {
            "full_delete_selected": "true",
            "override_protection": False,
        }
    )

    assert normalized == {
        "full_delete_selected": "true",
        "override_protection": "false",
    }


def test_destroy_by_uid_uses_query_params(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 204

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    DemoDestroyModel.destroy_by_uid(
        "uid-7",
        full_delete_selected=True,
        override_protection="false",
        timeout=30,
    )

    assert captured == {
        "r_type": "DELETE",
        "url": "https://backend.test/demo/uid-7/",
        "payload": {
            "params": {
                "full_delete_selected": "true",
                "override_protection": "false",
            }
        },
        "timeout": 30,
    }


def test_iter_filter_merges_read_query_params(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {"results": [], "next": None}

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    results = list(
        DemoReadModel.iter_filter(
            id=7,
            include_relations_detail=True,
            timeout=12,
        )
    )

    assert results == []
    assert captured == {
        "r_type": "GET",
        "url": "https://backend.test/demo-read/",
        "payload": {"params": {"id": 7, "include_relations_detail": "true"}},
        "timeout": 12,
    }


def test_get_by_uid_normalizes_read_query_params(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {"id": 9}

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    result = DemoReadModel.get_by_uid("uid-9", include_relations_detail=False, timeout=8)

    assert isinstance(result, DemoReadModel)
    assert result.id == 9
    assert captured == {
        "r_type": "GET",
        "url": "https://backend.test/demo-read/uid-9/",
        "payload": {"params": {"include_relations_detail": "false"}},
        "timeout": 8,
    }


def test_patch_by_uid_raises_with_context_for_unmapped_response_fields(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "id": 9,
                "schema": {"name": "customers"},
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    with pytest.raises(ValueError) as exc_info:
        DemoPatchModel.patch_by_uid("uid-9", _into=DemoPatchModel(id=9), label="patched")

    assert str(exc_info.value) == (
        "Failed to apply PATCH response to DemoPatchModel at field 'schema'. "
        "Response fragment: {'schema': {'name': 'customers'}}. "
        'Original error: "DemoPatchModel" object has no field "schema"'
    )
    assert captured == {
        "r_type": "PATCH",
        "url": "https://backend.test/demo-patch/uid-9/",
        "payload": {"json": {"label": "patched"}},
        "timeout": None,
    }


def test_patch_by_uid_updates_aliased_field_on_existing_instance(monkeypatch):
    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "id": 9,
                "schema": {"name": "customers"},
            }

    monkeypatch.setattr(
        base_mod,
        "make_request",
        lambda *args, **kwargs: FakeResponse(),
    )

    instance = DemoAliasedPatchModel(id=9)
    patched = DemoAliasedPatchModel.patch_by_uid("uid-9", _into=instance)

    assert patched is instance
    assert instance.schema_payload == {"name": "customers"}


def test_shareable_action_posts_user_id(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200
        content = b'{"detail":"ok"}'

        @staticmethod
        def json():
            return {"detail": "ok"}

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    response = DemoShareableModel(11).add_to_edit(_IdRef(7), timeout=15)

    assert response == {"detail": "ok"}
    assert captured == {
        "r_type": "POST",
        "url": "https://backend.test/demo-shareable/11/add-to-edit/",
        "payload": {"json": {"user_id": 7}},
        "timeout": 15,
    }


def test_shareable_action_returns_empty_dict_on_no_content(monkeypatch):
    class FakeResponse:
        status_code = 200
        content = b""

    monkeypatch.setattr(base_mod, "make_request", lambda **kwargs: FakeResponse())

    response = DemoShareableModel(9).remove_from_view(3)

    assert response == {}


def test_id_only_resource_patch_does_not_route_by_integer_id(monkeypatch):
    def _unexpected_request(**kwargs):
        raise AssertionError("id-only resource should not make a PATCH request")

    monkeypatch.setattr(base_mod, "make_request", _unexpected_request)

    instance = DemoPatchModel(id=9)

    with pytest.raises(ValueError, match="non-empty uid"):
        instance.patch(label="patched")


def test_id_only_resource_delete_does_not_route_by_integer_id(monkeypatch):
    def _unexpected_request(**kwargs):
        raise AssertionError("id-only resource should not make a DELETE request")

    monkeypatch.setattr(base_mod, "make_request", _unexpected_request)

    with pytest.raises(ValueError, match="non-empty uid"):
        DemoIdOnlyResource(9).delete()


def test_id_only_resource_detail_action_does_not_route_by_integer_id(monkeypatch):
    def _unexpected_request(**kwargs):
        raise AssertionError("id-only resource should not make a detail action request")

    monkeypatch.setattr(base_mod, "make_request", _unexpected_request)

    with pytest.raises(ValueError, match="non-empty uid"):
        DemoIdOnlyResource(9).can_view()


def test_shareable_team_action_posts_team_id(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200
        content = b'{"detail":"ok"}'

        @staticmethod
        def json():
            return {"detail": "ok"}

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    response = DemoShareableModel(11).add_team_to_view(_IdRef(5), timeout=21)

    assert response == {"detail": "ok"}
    assert captured == {
        "r_type": "POST",
        "url": "https://backend.test/demo-shareable/11/add-team-to-view/",
        "payload": {"json": {"team_id": 5}},
        "timeout": 21,
    }


def test_shareable_can_view_parses_permission_state(monkeypatch):
    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "object_id": 11,
                "object_type": "tdag.constant",
                "access_level": "view",
                "users": [
                    {
                        "id": 7,
                        "first_name": "Jose",
                        "last_name": "Ambrosino",
                        "username": "jose@main-sequence.io",
                        "email": "jose@main-sequence.io",
                        "phone_number": None,
                    }
                ],
                "teams": [],
            }

    monkeypatch.setattr(base_mod, "make_request", lambda **kwargs: FakeResponse())

    access_state = DemoShareableModel(11).can_view()

    assert access_state.object_id == 11
    assert access_state.access_level == "view"
    assert len(access_state.users) == 1
    assert access_state.users[0].id == 7
    assert access_state.users[0].email == "jose@main-sequence.io"
    assert access_state.teams == []


def test_shareable_can_edit_parses_permission_state(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "object_id": 15,
                "object_type": "tdag.secret",
                "access_level": "edit",
                "users": [
                    {
                        "id": 9,
                        "first_name": "Ana",
                        "last_name": "Smith",
                        "username": "ana@example.com",
                        "email": "ana@example.com",
                        "phone_number": "+43123456789",
                    }
                ],
                "teams": [
                    {
                        "id": 5,
                        "name": "Research",
                        "description": "Research team",
                        "member_count": 4,
                    }
                ],
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    access_state = DemoShareableModel(15).list_users_can_edit(timeout=20)

    assert access_state.object_id == 15
    assert access_state.access_level == "edit"
    assert len(access_state.users) == 1
    assert access_state.users[0].id == 9
    assert access_state.teams[0].name == "Research"
    assert access_state.teams[0].member_count == 4
    assert captured == {
        "r_type": "GET",
        "url": "https://backend.test/demo-shareable/15/can-edit/",
        "payload": {},
        "timeout": 20,
    }


def test_team_uses_user_api_team_endpoint():
    assert models_user_mod.Team.get_object_url().endswith("/user/api/team")


def test_user_team_and_organization_filters_use_uid_references():
    team_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    org_uid = "8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"
    user_uid = "fdf409f7-d16f-4f71-986b-9057db6c7eca"

    team_filters = models_user_mod.Team._normalize_filter_kwargs(
        {
            "uid": {"uid": team_uid},
            "uid__in": [_UidRef(team_uid)],
            "organization_uid": {"uid": org_uid},
            "is_active": "true",
        }
    )
    assert team_filters == {
        "uid": team_uid,
        "uid__in": [team_uid],
        "organization_uid": org_uid,
        "is_active": True,
    }

    user_filters = models_user_mod.User._normalize_filter_kwargs(
        {
            "uid": {"uid": user_uid},
            "uid__in": [_UidRef(user_uid)],
            "email__contains": "main-sequence.io",
        }
    )
    assert user_filters == {
        "uid": user_uid,
        "uid__in": [user_uid],
        "email__contains": "main-sequence.io",
    }

    with pytest.raises(ValueError, match="Unsupported Team filter"):
        models_user_mod.Team._normalize_filter_kwargs({"id": 11})


def test_team_list_members_uses_team_members_endpoint(monkeypatch):
    captured = {}
    team_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    user_uid = "fdf409f7-d16f-4f71-986b-9057db6c7eca"

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return [
                {
                    "id": 21,
                    "uid": user_uid,
                    "first_name": "Ana",
                    "last_name": "Smith",
                    "username": "ana@example.com",
                    "email": "ana@example.com",
                }
            ]

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    team = models_user_mod.Team(id=11, uid=team_uid, name="Platform")
    members = team.list_members(timeout=12)

    assert len(members) == 1
    assert members[0].id == 21
    assert members[0].uid == user_uid
    assert members[0].phone_number is None
    assert captured == {
        "r_type": "GET",
        "url": f"{models_user_mod.Team.get_object_url()}/{team_uid}/members/",
        "payload": {},
        "timeout": 12,
    }


def test_team_manage_members_posts_bulk_membership_payload(monkeypatch):
    captured = {}
    team_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    user_uid_1 = "fdf409f7-d16f-4f71-986b-9057db6c7eca"
    user_uid_2 = "ac9e221d-1cd6-464c-a253-e302754872c1"

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "team_id": 11,
                "team_uid": team_uid,
                "member_count": 4,
                "selected": 2,
                "added": 2,
                "removed": 0,
                "skipped": 0,
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    team = models_user_mod.Team(id=11, uid=team_uid, name="Platform")
    result = team.add_members([_UidRef(user_uid_1), {"uid": user_uid_2}], timeout=18)

    assert result.team_id == 11
    assert result.team_uid == team_uid
    assert result.member_count == 4
    assert result.added == 2
    assert captured == {
        "r_type": "POST",
        "url": f"{models_user_mod.Team.get_object_url()}/{team_uid}/manage-members/",
        "payload": {"json": {"action": "add", "user_uids": [user_uid_1, user_uid_2]}},
        "timeout": 18,
    }


def test_user_org_team_models_deserialize_current_backend_uid_payloads():
    org_uid = "8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"
    user_uid = "fdf409f7-d16f-4f71-986b-9057db6c7eca"
    team_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    organization_payload = {
        "uid": org_uid,
        "name": "Main Sequence",
        "url": "https://backend.test",
        "organization_domain": "main-sequence.io",
        "identity_platform_tenant_id": None,
        "has_pending_invoices": False,
    }

    organization = models_user_mod.Organization.model_validate(organization_payload)
    assert organization.uid == org_uid
    assert "id" not in organization.model_dump()

    current_user = models_user_mod.User.model_validate(
        {
            "id": 4,
            "uid": user_uid,
            "username": "jose",
            "email": "jose@main-sequence.io",
            "profile_picture": None,
            "last_login": None,
            "api_request_limit": 10000,
            "mfa_enabled": False,
            "organization": organization_payload,
            "plan": None,
            "groups": [],
            "phone_number": None,
            "organization_teams": [],
            "is_active": True,
            "date_joined": "2026-01-01T00:00:00Z",
        }
    )
    assert current_user.uid == user_uid
    assert current_user.user_permissions == []

    user = models_user_mod.User.model_validate(
        {
            "id": 4,
            "uid": user_uid,
            "username": "jose",
            "email": "jose@main-sequence.io",
            "first_name": "Jose",
            "last_name": "Ambrosino",
            "profile_picture": None,
            "phone_number": None,
            "organization": organization_payload,
            "is_verified": True,
            "blocked_access": False,
            "api_request_limit": 10000,
            "mfa_enabled": False,
            "requires_password_change": False,
            "identity_platform_uid": None,
            "active_plan_type": None,
            "is_active": True,
            "date_joined": "2026-01-01T00:00:00Z",
            "last_login": None,
            "groups": [],
            "user_permissions": [],
            "organization_teams": [],
        }
    )
    assert user.uid == user_uid
    assert user.id == 4
    assert "id" not in user.model_dump()

    team = models_user_mod.Team.model_validate(
        {
            "id": 11,
            "uid": team_uid,
            "name": "Platform",
            "description": "Platform team",
            "is_active": True,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-02T00:00:00Z",
            "organization": organization_payload,
            "created_by": {
                "id": 4,
                "uid": user_uid,
                "username": "jose",
                "email": "jose@main-sequence.io",
                "first_name": "Jose",
                "last_name": "Ambrosino",
            },
            "member_count": 1,
            "members": [
                {
                    "id": 4,
                    "uid": user_uid,
                    "username": "jose",
                    "email": "jose@main-sequence.io",
                    "first_name": "Jose",
                    "last_name": "Ambrosino",
                }
            ],
        }
    )
    assert team.uid == team_uid
    assert team.created_by.uid == user_uid
    assert team.members[0].uid == user_uid


def test_user_get_by_uid_uses_user_uid_detail_route(monkeypatch):
    captured = {}
    user_uid = "fdf409f7-d16f-4f71-986b-9057db6c7eca"

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "id": 4,
                "uid": user_uid,
                "username": "jose",
                "email": "jose@main-sequence.io",
                "date_joined": "2026-01-01T00:00:00Z",
                "is_active": True,
                "api_request_limit": 10000,
                "mfa_enabled": False,
                "groups": [],
                "user_permissions": [],
                "organization_teams": [],
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    user = models_user_mod.User.get_by_uid(user_uid, timeout=9)

    assert user.uid == user_uid
    assert captured == {
        "r_type": "GET",
        "url": f"{models_user_mod.User.get_object_url()}/{user_uid}/",
        "payload": {"params": {}},
        "timeout": 9,
    }


def test_agent_runtime_models_deserialize_backend_uid_payloads():
    agent_uid = "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    user_uid = "fdf409f7-d16f-4f71-986b-9057db6c7eca"
    service_uid = "ac9e221d-1cd6-464c-a253-e302754872c1"

    agent = agent_models_mod.Agent.model_validate(
        {
            "uid": agent_uid,
            "name": "Research Copilot",
            "agent_type": "custom",
            "description": "Research assistant.",
            "agent_card": {"name": "Research Copilot"},
            "llm_provider": "openai",
            "llm_model": "gpt-5.4",
            "llm_thinking": "medium",
            "runtime_config": {"temperature": 0},
            "configuration": {"mode": "analysis"},
            "last_session_at": "2026-01-01T00:00:00Z",
            "has_agent_service": True,
            "agent_service_uid": service_uid,
            "agent_service_automatic_deployment": True,
        }
    )
    assert agent.uid == agent_uid
    assert agent.has_agent_service is True
    assert agent.agent_service_uid == service_uid
    assert agent.agent_service_automatic_deployment is True

    search_result = agent_models_mod.AgentSemanticSearchResult.model_validate(
        {
            "uid": agent_uid,
            "name": "Research Copilot",
            "agent_type": "custom",
            "description": "Research assistant.",
            "semantic_score": 0.91,
            "text_score": 0.74,
            "combined_score": 0.85,
        }
    )
    assert search_result.uid == agent_uid

    session = agent_models_mod.AgentSession.model_validate(
        {
            "uid": session_uid,
            "agent_uid": agent_uid,
            "agent_name": "Research Copilot",
            "agent_type": "custom",
            "created_by_user_uid": user_uid,
            "parent_session_uid": None,
            "name": "Research follow-up",
            "status": "running",
            "runtime_state": "running",
            "working": True,
            "started_at": "2026-01-01T00:00:00Z",
            "ended_at": None,
            "llm_provider": "openai",
            "llm_model": "gpt-5.4",
            "llm_thinking": "medium",
            "engine_name": "codex",
            "runtime_config_snapshot": {"temperature": 0},
            "error_detail": "",
            "thread_id": "thread-123",
            "session_metadata": {"origin": "test"},
            "bound_handle": {
                "uid": "44444444-4444-4444-8444-444444444444",
                "handle_unique_id": "delegated-handle-1",
                "owner_user_uid": user_uid,
                "is_locked": False,
            },
        }
    )
    assert session.uid == session_uid
    assert session.agent_uid == agent_uid
    assert session.name == "Research follow-up"

    service = agent_models_mod.CodingAgentService.model_validate(
        {
            "uid": service_uid,
            "agent_uid": agent_uid,
            "agent_type": "project-executor",
            "scope": {"kind": "project", "project_uid": "project-uid"},
            "is_ready": True,
            "image_drift": {"has_drift": False, "checks": []},
            "related_job_uid": "job-uid",
            "service_runtime_uid": "runtime-uid",
            "automatic_deployment": True,
        }
    )
    assert service.uid == service_uid
    assert service.agent_uid == agent_uid
    assert service.agent_type == "project-executor"
    assert service.scope["project_uid"] == "project-uid"
    assert service.service_runtime_uid == "runtime-uid"
    assert service.knative_service_runtime_uid == "runtime-uid"


def test_agent_session_runtime_access_uses_session_uid_route(monkeypatch):
    captured = {}
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "coding_agent_service_id": "svc-12",
                "coding_agent_id": "agent-rt-77",
                "mode": "token",
                "rpc_url": "https://runtime.main-sequence.app/rpc",
                "token": "tok-secret",
                "is_ready": True,
                "service_runtime_uid": "70c6efb9-8e80-4051-ad3a-f432b2c37f5a",
                "image_drift": {
                    "agent_kind": "astro_orchestrator",
                    "available": True,
                    "has_drift": False,
                    "autoheal_available": False,
                    "autoheal_message": "No automatic drift repair is needed.",
                    "checks": [
                        {
                            "key": "orchestrator_image",
                            "label": "Orchestrator image",
                            "status": "match",
                            "has_drift": False,
                            "matches": True,
                            "reason": "match",
                            "message": "The runtime image matches the catalog image.",
                            "autoheal_supported": True,
                            "autoheal_mode": "request_driven_runtime_sync",
                            "autoheal_message": "No runtime repair is needed.",
                            "expected_image_uri": "registry.example/astro@sha256:expected",
                            "actual_image_uri": "registry.example/astro@sha256:expected",
                            "expected_commit_hash": "",
                            "actual_commit_hash": "",
                        }
                    ],
                    "detail": None,
                    "catalog_state": {
                        "image_prefix": "astro",
                        "tag": "latest",
                        "ttl_seconds": 3600,
                        "last_synced_at": "2026-07-17T10:00:00+00:00",
                        "status": "fresh",
                        "fresh": True,
                        "refresh_required": False,
                        "detail": "",
                        "catalog_image_registry_id": 11,
                        "catalog_image_registry_uid": "registry-uid",
                        "catalog_image_id": 42,
                        "latest_pinned_uri": "registry.example/astro@sha256:expected",
                        "age_seconds": 12.5,
                    },
                },
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(agent_models_mod, "make_request", _fake_make_request)

    access = agent_models_mod.AgentSession.resolve_runtime_access(session_uid, timeout=11)

    assert access.coding_agent_service_id == "svc-12"
    assert access.is_ready is True
    assert access.service_runtime_uid == "70c6efb9-8e80-4051-ad3a-f432b2c37f5a"
    assert access.knative_service_runtime_uid == "70c6efb9-8e80-4051-ad3a-f432b2c37f5a"
    assert access.image_drift is not None
    assert access.image_drift.agent_kind == "astro_orchestrator"
    assert access.image_drift.checks[0].key == "orchestrator_image"
    assert access.image_drift.catalog_state is not None
    assert access.image_drift.catalog_state["refresh_required"] is False
    assert captured == {
        "r_type": "POST",
        "url": f"{agent_models_mod.AgentSession.get_object_url()}/{session_uid}/resolve_runtime_access/",
        "payload": {"json": {}},
        "timeout": 11,
    }


def test_agent_session_runtime_access_accepts_minimal_image_drift(monkeypatch):
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "coding_agent_service_id": "39",
                "coding_agent_id": "astro-orchestrator",
                "mode": "token",
                "rpc_url": "https://runtime.main-sequence.app/rpc",
                "token": "tok-secret",
                "is_ready": True,
                "knative_service_runtime_uid": "70c6efb9-8e80-4051-ad3a-f432b2c37f5a",
                "image_drift": {
                    "has_drift": False,
                    "detail": None,
                },
                "reconciliation": {
                    "queued": False,
                    "reason": "not_required",
                },
            }

    monkeypatch.setattr(agent_models_mod, "make_request", lambda **kwargs: FakeResponse())

    access = agent_models_mod.AgentSession.resolve_runtime_access(session_uid, timeout=11)

    assert access.image_drift is not None
    assert access.image_drift.has_drift is False
    assert access.image_drift.detail is None
    assert access.model_dump()["reconciliation"] == {
        "queued": False,
        "reason": "not_required",
    }


def test_agent_session_send_a2a_message_posts_standard_contract(monkeypatch):
    captured = {"resolve_count": 0, "runtime": {}}
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"

    agent_models_mod.AgentSession.clear_cached_runtime_access(session_uid)

    class FakeResolveResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "rpc_url": "https://runtime.main-sequence.app/rpc",
                "token": "tok-secret",
                "expires_at": "2999-01-01T00:00:00Z",
            }

    class FakeRuntimeResponse:
        status_code = 200
        headers = {"Content-Type": "application/a2a+json"}
        text = ""

        @staticmethod
        def json():
            return {
                "message": {
                    "messageId": "msg-runtime-output",
                    "role": "ROLE_AGENT",
                    "contextId": session_uid,
                    "parts": [{"text": "I can analyze workspaces."}],
                }
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["resolve_count"] += 1
        captured["resolve"] = {
            "r_type": r_type,
            "url": url,
            "payload": payload,
            "timeout": time_out,
        }
        return FakeResolveResponse()

    def _fake_post(url, *, headers, data, timeout):
        captured["runtime"] = {
            "url": url,
            "headers": headers,
            "data": data,
            "timeout": timeout,
        }
        return FakeRuntimeResponse()

    monkeypatch.setattr(agent_models_mod, "make_request", _fake_make_request)
    monkeypatch.setattr(agent_models_mod.requests, "post", _fake_post)
    monkeypatch.setattr(
        agent_models_mod.uuid,
        "uuid4",
        lambda: "00000000-0000-4000-8000-000000000001",
    )

    payload = agent_models_mod.AgentSession.send_a2a_message(
        session_uid,
        message="What can this agent do?",
        timeout=15,
    )

    assert captured["resolve_count"] == 1
    assert captured["resolve"] == {
        "r_type": "POST",
        "url": f"{agent_models_mod.AgentSession.get_object_url()}/{session_uid}/resolve_runtime_access/",
        "payload": {"json": {}},
        "timeout": 15,
    }
    assert payload["message"]["parts"] == [{"text": "I can analyze workspaces."}]
    assert captured["runtime"]["url"] == (
        "https://runtime.main-sequence.app/rpc/api/a2a/v1/message:send"
    )
    assert captured["runtime"]["headers"]["Content-Type"] == "application/a2a+json"
    assert captured["runtime"]["headers"]["Accept"] == "application/a2a+json"
    assert captured["runtime"]["headers"]["Authorization"] == "Bearer tok-secret"
    request_body = json.loads(captured["runtime"]["data"])
    assert request_body == {
        "message": {
            "messageId": "msg-00000000-0000-4000-8000-000000000001",
            "role": "ROLE_USER",
            "contextId": session_uid,
            "parts": [{"text": "What can this agent do?"}],
        },
        "configuration": {
            "acceptedOutputModes": ["text/plain"],
            "returnImmediately": False,
        },
    }
    assert "omit_reasoning" not in captured["runtime"]["data"]


def test_agent_session_send_a2a_message_posts_strict_dictionary_contract(monkeypatch):
    captured = {"resolve_count": 0}
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"

    agent_models_mod.AgentSession.clear_cached_runtime_access(session_uid)

    class FakeResolveResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "rpc_url": "https://runtime.main-sequence.app/rpc",
                "token": "tok-secret",
            }

    class FakeRuntimeResponse:
        status_code = 200
        headers = {"Content-Type": "application/a2a+json"}

        @staticmethod
        def json():
            return {
                "message": {
                    "messageId": "msg-runtime-output",
                    "role": "ROLE_AGENT",
                    "contextId": session_uid,
                    "parts": [{"text": '{"ok": true}'}],
                }
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["resolve_count"] += 1
        return FakeResolveResponse()

    def _fake_post(url, *, headers, data, timeout):
        captured["data"] = data
        return FakeRuntimeResponse()

    monkeypatch.setattr(agent_models_mod, "make_request", _fake_make_request)
    monkeypatch.setattr(agent_models_mod.requests, "post", _fake_post)
    monkeypatch.setattr(
        agent_models_mod.uuid,
        "uuid4",
        lambda: "00000000-0000-4000-8000-000000000002",
    )

    agent_models_mod.AgentSession.send_a2a_message(
        session_uid,
        message="Return a JSON dictionary with keys ok, answer, and example.",
        strict_dictionary=True,
        json_repair_attempts=3,
    )

    assert captured["resolve_count"] == 1
    request_body = json.loads(captured["data"])
    assert request_body["configuration"]["acceptedOutputModes"] == ["application/json"]
    assert request_body["metadata"] == {
        "https://mainsequence.ai/a2a/extensions/output-contract/v1": {
            "response_format": {
                "type": "dictionary",
                "strict": True,
            },
            "jsonRepairAttempts": 3,
        }
    }
    assert "omit_reasoning" not in captured["data"]


def test_agent_session_send_a2a_message_refreshes_access_and_reuses_body(monkeypatch):
    captured = {"resolve_count": 0, "runtime_bodies": []}
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    agent_models_mod.AgentSession.clear_cached_runtime_access(session_uid)

    class FakeResolveResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            captured["resolve_count"] += 1
            return {
                "rpc_url": "https://runtime.main-sequence.app/rpc",
                "token": f"tok-secret-{captured['resolve_count']}",
            }

    class FakeUnauthorizedResponse:
        status_code = 401
        headers = {"Content-Type": "application/a2a+json"}
        text = '{"error":"unauthorized"}'

        @staticmethod
        def json():
            return {"error": "unauthorized"}

    class FakeRuntimeResponse:
        status_code = 200
        headers = {"Content-Type": "application/a2a+json"}
        text = ""

        @staticmethod
        def json():
            return {
                "message": {
                    "messageId": "msg-runtime-output",
                    "role": "ROLE_AGENT",
                    "contextId": session_uid,
                    "parts": [{"text": "Done."}],
                }
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        return FakeResolveResponse()

    def _fake_post(url, *, headers, data, timeout):
        captured["runtime_bodies"].append(data)
        captured.setdefault("tokens", []).append(headers["Authorization"])
        if len(captured["runtime_bodies"]) == 1:
            return FakeUnauthorizedResponse()
        return FakeRuntimeResponse()

    monkeypatch.setattr(agent_models_mod, "make_request", _fake_make_request)
    monkeypatch.setattr(agent_models_mod.requests, "post", _fake_post)

    payload = agent_models_mod.AgentSession.send_a2a_message(
        session_uid,
        message="Retry safely.",
        message_id="msg-client-retry-1",
    )

    assert payload["message"]["parts"] == [{"text": "Done."}]
    assert captured["resolve_count"] == 2
    assert captured["runtime_bodies"][0] == captured["runtime_bodies"][1]
    request_body = json.loads(captured["runtime_bodies"][0])
    assert request_body["message"]["messageId"] == "msg-client-retry-1"
    assert captured["tokens"] == ["Bearer tok-secret-1", "Bearer tok-secret-2"]


def test_agent_get_or_create_session_posts_new_contract(monkeypatch):
    captured = {}
    agent_uid = "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    parent_session_uid = "33333333-3333-4333-8333-333333333333"
    user_uid = "fdf409f7-d16f-4f71-986b-9057db6c7eca"
    agent = agent_models_mod.Agent(
        uid=agent_uid,
        name="Research Copilot",
        agent_type="custom",
        description="Research assistant.",
        agent_card=None,
        llm_provider="openai",
        llm_model="gpt-5.4",
        llm_thinking="medium",
    )

    class FakeResponse:
        status_code = 201
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "uid": session_uid,
                "agent_uid": agent_uid,
                "agent_name": "Research Copilot",
                "agent_type": "custom",
                "created_by_user_uid": user_uid,
                "parent_session_uid": parent_session_uid,
                "name": "Quarterly portfolio review",
                "status": "running",
                "runtime_state": "running",
                "working": True,
                "started_at": "2026-01-01T00:00:00Z",
                "ended_at": None,
                "llm_provider": "openai",
                "llm_model": "gpt-5.4",
                "llm_thinking": "",
                "engine_name": "codex",
                "runtime_config_snapshot": {},
                "error_detail": "",
                "thread_id": "",
                "session_metadata": {},
                "bound_handle": {
                    "uid": "44444444-4444-4444-8444-444444444444",
                    "handle_unique_id": "portfolio-review-q2-2026",
                    "owner_user_uid": user_uid,
                    "is_locked": False,
                },
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(agent_models_mod, "make_request", _fake_make_request)

    session = agent.get_or_create_session(
        handle_unique_id="portfolio-review-q2-2026",
        name="Quarterly portfolio review",
        parent_session_uid=parent_session_uid,
        llm_provider="openai",
        llm_model="gpt-5.4",
        llm_thinking="",
        timeout=13,
    )

    assert session.uid == session_uid
    assert session.name == "Quarterly portfolio review"
    assert session.parent_session_uid == parent_session_uid
    assert captured == {
        "r_type": "POST",
        "url": (
            f"{agent_models_mod.Agent.get_object_url()}/{agent_uid}/"
            "sessions/get_or_create_session/"
        ),
        "payload": {
            "json": {
                "handle_unique_id": "portfolio-review-q2-2026",
                "name": "Quarterly portfolio review",
                "parent_session_uid": parent_session_uid,
                "llm_provider": "openai",
                "llm_model": "gpt-5.4",
                "llm_thinking": "",
            }
        },
        "timeout": 13,
    }


def test_agent_get_or_create_session_by_uid_sends_only_session_uid(monkeypatch):
    captured = {}
    agent_uid = "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    agent = agent_models_mod.Agent(
        uid=agent_uid,
        name="Research Copilot",
        agent_type="custom",
        description="Research assistant.",
        agent_card=None,
        llm_provider="openai",
        llm_model="gpt-5.4",
        llm_thinking="medium",
    )

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "uid": session_uid,
                "agent_uid": agent_uid,
                "agent_name": "Research Copilot",
                "agent_type": "custom",
                "name": "Existing session",
                "status": "running",
                "llm_provider": "openai",
                "llm_model": "gpt-5.4",
                "llm_thinking": "",
                "bound_handle": None,
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(agent_models_mod, "make_request", _fake_make_request)

    session = agent.get_or_create_session(session_uid=session_uid, timeout=9)

    assert session.uid == session_uid
    assert captured == {
        "r_type": "POST",
        "url": (
            f"{agent_models_mod.Agent.get_object_url()}/{agent_uid}/"
            "sessions/get_or_create_session/"
        ),
        "payload": {"json": {"session_uid": session_uid}},
        "timeout": 9,
    }


def _class_base_names_from_source(path: pathlib.Path) -> dict[str, list[str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    out: dict[str, list[str]] = {}
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            base_names: list[str] = []
            for base in node.bases:
                if isinstance(base, ast.Name):
                    base_names.append(base.id)
                elif isinstance(base, ast.Attribute):
                    base_names.append(base.attr)
            out[node.name] = base_names
    return out


def test_shareable_models_keep_shareable_object_mixin():
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    models_foundry_bases = _class_base_names_from_source(
        repo_root / "mainsequence" / "client" / "models_foundry.py"
    )
    models_helpers_bases = _class_base_names_from_source(
        repo_root / "mainsequence" / "client" / "models_helpers.py"
    )

    expected = {
        "Artifact": models_foundry_bases,
        "Bucket": models_foundry_bases,
        "Project": models_foundry_bases,
        "Constant": models_foundry_bases,
        "Secret": models_foundry_bases,
        "ResourceRelease": models_helpers_bases,
    }

    for class_name, source_bases in expected.items():
        assert class_name in source_bases, f"{class_name} class not found"
        assert "ShareableObjectMixin" in source_bases[class_name], (
            f"{class_name} must inherit ShareableObjectMixin"
        )


def test_secret_constant_bucket_artifact_accept_uid_identity_payloads():
    secret = models_foundry_mod.Secret(uid="11111111-1111-4111-8111-111111111111", name="API_KEY")
    constant = models_foundry_mod.Constant(
        uid="22222222-2222-4222-8222-222222222222",
        name="APP__MODE",
        value="production",
    )
    bucket = models_foundry_mod.Bucket(
        uid="33333333-3333-4333-8333-333333333333",
        name="default_bucket",
    )
    artifact = models_foundry_mod.Artifact(
        uid="44444444-4444-4444-8444-444444444444",
        name="report.pdf",
        bucket_name="default_bucket",
        bucket_uid="33333333-3333-4333-8333-333333333333",
        content="https://signed.example/report.pdf",
        creation_date=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
    )

    assert secret.uid == "11111111-1111-4111-8111-111111111111"
    assert constant.uid == "22222222-2222-4222-8222-222222222222"
    assert bucket.uid == "33333333-3333-4333-8333-333333333333"
    assert artifact.uid == "44444444-4444-4444-8444-444444444444"


def test_team_uses_permission_managed_object_mixin():
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    models_user_bases = _class_base_names_from_source(
        repo_root / "mainsequence" / "client" / "models_user.py"
    )

    assert "Team" in models_user_bases, "Team class not found"
    assert "PermissionManagedObjectMixin" in models_user_bases["Team"], (
        "Team must inherit PermissionManagedObjectMixin"
    )
    assert "ShareableObjectMixin" not in models_user_bases["Team"], (
        "Team should not inherit ShareableObjectMixin directly"
    )
