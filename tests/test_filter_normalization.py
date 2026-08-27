import ast
import datetime
import json
import pathlib
import uuid
from types import SimpleNamespace

import pytest
from pydantic import ConfigDict, Field, ValidationError

import mainsequence.client.agent_runtime_models as agent_models_mod
import mainsequence.client.base as base_mod
import mainsequence.client.metatables as models_metatables_mod
import mainsequence.client.models_foundry as models_foundry_mod
import mainsequence.client.models_helpers as models_helpers_mod
import mainsequence.client.models_user as models_user_mod
import mainsequence.project_context as project_context
from mainsequence.client.base import BaseObjectOrm, BasePydanticModel, ShareableObjectMixin

PROJECT_UID = "1d0530c0-65d1-4db0-856b-dc29d8260a09"
PROJECT_BRANCH_UID = "5a28020a-0f1b-47ee-aab8-334286234bea"
ENVIRONMENT_UID = "58218213-5e4e-43de-a5bd-6757f4e1c8f6"


@pytest.fixture(autouse=True)
def _resolved_project_context(monkeypatch):
    project_context._reset_project_runtime_context()
    source = project_context.GitProjectSourceContext(
        repository_root=pathlib.Path.cwd().resolve(),
        canonical_repository_identity="github.com/mainsequence-sdk/filters",
        repository_branch="main",
        repository_ref="refs/heads/main",
        commit_sha="a" * 40,
    )
    monkeypatch.setattr(
        project_context,
        "_resolve_git_source_context",
        lambda project_dir: source,
    )
    project_context.get_project_runtime_context(
        _project_branch_context_loader=lambda resolved_source: SimpleNamespace(
            canonical_repository_identity=(resolved_source.canonical_repository_identity),
            repository_branch=resolved_source.repository_branch,
            repository_ref=resolved_source.repository_ref,
            commit_sha=resolved_source.commit_sha,
            project_branch=SimpleNamespace(
                uid=PROJECT_BRANCH_UID,
                project_uid=PROJECT_UID,
                repository_branch=resolved_source.repository_branch,
                organization_environment_uid=ENVIRONMENT_UID,
                metatables_data_source=None,
            ),
        ),
    )
    yield
    project_context._reset_project_runtime_context()


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
    def __init__(self, uid: str, internal_id: int | None = None):
        self.uid = uid
        self.id = internal_id

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        return "https://backend.test/demo-shareable"

    @classmethod
    def build_session(cls):
        return object()


class DemoIdOnlyResource(ShareableObjectMixin, BaseObjectOrm):
    def __init__(self, internal_id: int):
        self.id = internal_id

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
        project_uid=None,
        project_name=None,
        project_branch_uid=None,
        project_branch_name=None,
        organization_environment_uid=ENVIRONMENT_UID,
        runtime_image_uid="6cfdb152-923e-45b9-a150-c4541c68b0d1",
        runtime_image_digest="sha256:" + "b" * 64,
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
        "/api/v1/job-runs/4c1d77c8-8a42-42b8-a9c1-06be9a336e5d/status/"
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


def test_job_run_uid_filters_send_canonical_query_parameters(monkeypatch):
    captured_params = []

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"results": [], "next": None}

    def _fake_make_request(**kwargs):
        captured_params.append(kwargs["payload"]["params"])
        return FakeResponse()

    monkeypatch.setattr(
        models_helpers_mod.JobRun,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    first_uid = "ab6a5d50-8a3e-4f0d-a9bb-7e84180bd50e"
    second_uid = "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da"

    assert models_helpers_mod.JobRun.filter(job__uid=first_uid) == []
    assert models_helpers_mod.JobRun.filter(job__uid__in=[first_uid, second_uid]) == []
    assert captured_params == [
        {"job__uid": first_uid},
        {"job__uid__in": f"{first_uid},{second_uid}"},
    ]


def test_job_run_deserializes_uid_payload_without_id():
    job_run = models_helpers_mod.JobRun(
        uid="4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
        name="demo-run",
        unique_identifier="jobrun_501",
        job_uid="ab6a5d50-8a3e-4f0d-a9bb-7e84180bd50e",
        job_name="daily-training-job",
        project_uid="1d0530c0-65d1-4db0-856b-dc29d8260a09",
        project_name="market-data-service",
        project_branch_uid="5a28020a-0f1b-47ee-aab8-334286234bea",
        project_branch_name="main",
        organization_environment_uid=ENVIRONMENT_UID,
        status="RUNNING",
        cpu_request="1",
        cpu_limit="2",
        memory_request="4Gi",
        memory_limit="8Gi",
        gpu_request="1",
        gpu_type="nvidia-l4",
        runtime_image_uid="6cfdb152-923e-45b9-a150-c4541c68b0d1",
        runtime_image_digest="sha256:" + "b" * 64,
        command_args=["sync"],
    )

    dumped = job_run.model_dump()
    assert dumped["uid"] == "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d"
    assert dumped["job_uid"] == "ab6a5d50-8a3e-4f0d-a9bb-7e84180bd50e"
    assert dumped["organization_environment_uid"] == ENVIRONMENT_UID
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
        related_project_branch_uid="5a28020a-0f1b-47ee-aab8-334286234bea",
        base_image=None,
        build_error=False,
        is_ready=False,
        source_provenance={
            "verification_state": "unverified",
        },
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


def _project_image_response(*, uid: str, build_error: bool, is_ready: bool = False):
    return {
        "uid": uid,
        "project_repo_hash": "abc123abc123abc123abc123abc123abc123abcd",
        "related_project_branch_uid": "5a28020a-0f1b-47ee-aab8-334286234bea",
        "base_image": None,
        "tags": [],
        "build_error": build_error,
        "is_ready": is_ready,
        "source_provenance": {
            "verification_state": "verified",
            "project_uid": "1d0530c0-65d1-4db0-856b-dc29d8260a09",
            "project_branch_uid": "5a28020a-0f1b-47ee-aab8-334286234bea",
            "git_repository_uid": "3c2113e7-40ba-4d8c-ad65-51ca236c3b0c",
            "repository_branch": "main",
            "repository_ref": "refs/heads/main",
            "commit_sha": "abc123abc123abc123abc123abc123abc123abcd",
            "source_archive_sha256": "a" * 64,
            "build_context_checksum": "b" * 64,
            "base_image_uid": "c3ddb792-a3c0-428c-b5cf-31ed99dad10f",
            "output_image_digest": "sha256:" + "c" * 64,
        },
        "creation_date": "2026-04-07T09:00:00Z",
    }


@pytest.mark.parametrize("build_error", [False, True])
def test_project_image_accepts_boolean_build_error(build_error):
    image = models_foundry_mod.ProjectImage.model_validate(
        _project_image_response(
            uid="f3cb8477-df47-49cb-a151-80b746fb1243",
            build_error=build_error,
        )
    )

    assert image.build_error is build_error


@pytest.mark.parametrize("build_error", [False, True])
def test_project_image_create_accepts_boolean_build_error(monkeypatch, build_error):
    payload = _project_image_response(
        uid="f3cb8477-df47-49cb-a151-80b746fb1243",
        build_error=build_error,
    )

    class FakeResponse:
        status_code = 202

        @staticmethod
        def json():
            return payload

    monkeypatch.setattr(
        models_foundry_mod.ProjectImage,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(models_foundry_mod, "make_request", lambda **kwargs: FakeResponse())

    image = models_foundry_mod.ProjectImage.create(
        project_repo_hash=payload["project_repo_hash"],
        related_project_branch_uid=payload["related_project_branch_uid"],
    )

    assert image.build_error is build_error


@pytest.mark.parametrize("build_error", [False, True])
def test_project_image_get_accepts_boolean_build_error(monkeypatch, build_error):
    payload = _project_image_response(
        uid="f3cb8477-df47-49cb-a151-80b746fb1243",
        build_error=build_error,
    )

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return payload

    monkeypatch.setattr(
        models_foundry_mod.ProjectImage,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(base_mod, "make_request", lambda **kwargs: FakeResponse())

    image = models_foundry_mod.ProjectImage.get(pk=payload["uid"])

    assert image.build_error is build_error


def test_project_image_filter_accepts_boolean_build_error(monkeypatch):
    payloads = [
        _project_image_response(
            uid="f3cb8477-df47-49cb-a151-80b746fb1243",
            build_error=False,
        ),
        _project_image_response(
            uid="39dc72dc-d905-43af-8012-d67c026c2970",
            build_error=True,
        ),
    ]

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"results": payloads, "next": None}

    monkeypatch.setattr(
        models_foundry_mod.ProjectImage,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(base_mod, "make_request", lambda **kwargs: FakeResponse())

    images = models_foundry_mod.ProjectImage.filter(
        related_project_branch_uid="5a28020a-0f1b-47ee-aab8-334286234bea"
    )

    assert [image.build_error for image in images] == [False, True]


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


def test_data_node_storage_does_not_expose_environment_filters():
    from mainsequence.client.metatables import TimeIndexMetaTable

    uid = uuid.UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")

    with pytest.raises(ValueError, match="Unsupported TimeIndexMetaTable filter"):
        TimeIndexMetaTable._normalize_filter_kwargs({"organization_environment_uid": {"uid": uid}})


@pytest.mark.parametrize(
    "model_class",
    [
        models_metatables_mod.MetaTable,
        models_metatables_mod.TimeIndexMetaTable,
    ],
)
def test_meta_table_collection_sends_canonical_environment_query_params(
    monkeypatch,
    model_class,
):
    captured = {}

    class FakeResponse:
        status_code = 200
        content = b'{"results": [], "next": null}'

        @staticmethod
        def json():
            return {"results": [], "next": None}

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured.update(
            {
                "r_type": r_type,
                "payload": payload,
                "timeout": time_out,
            }
        )
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)
    monkeypatch.setattr(model_class, "build_session", classmethod(lambda cls: object()))

    rows = model_class.filter(timeout=17)

    assert rows == []
    assert captured == {
        "r_type": "GET",
        "payload": {
            "params": {
                "organization_environment_uid": ENVIRONMENT_UID,
            }
        },
        "timeout": 17,
    }

    with pytest.raises(ValueError, match="cannot override SDK-resolved context"):
        model_class.filter(organization_environment_uid=("11111111-1111-4111-8111-111111111111"))


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
        data_source={
            "uid": "data-source-uid",
            "data_source_uid": "data-source-uid",
            "class_type": "timescale_db",
        },
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
        "url": f"{models_metatables.TimeIndexMetaTable.get_object_url()}/714/delete-after-date/",
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
        data_source={
            "uid": "data-source-uid",
            "data_source_uid": "data-source-uid",
            "class_type": "timescale_db",
        },
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
        data_source={
            "uid": "data-source-uid",
            "data_source_uid": "data-source-uid",
            "class_type": "timescale_db",
        },
        source_class_name="PricesNode",
        creation_date="2026-04-01T00:00:00Z",
    )

    result = storage.run_query("SELECT * FROM my_table LIMIT 100", timeout=30)

    assert result["ok"] is True
    assert result["dynamic_table_id"] == 714
    assert captured == {
        "headers": {"Content-Type": "text/plain"},
        "r_type": "POST",
        "url": f"{models_metatables.TimeIndexMetaTable.get_object_url()}/714/run-query/",
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
            "b14db80b-64b7-4390-8483-5377510de505/run-query/"
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
        data_source={
            "uid": "data-source-uid",
            "data_source_uid": "data-source-uid",
            "class_type": "timescale_db",
        },
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


def test_meta_table_does_not_expose_environment_filters():
    from mainsequence.client.metatables import MetaTable

    uid = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"

    with pytest.raises(ValueError, match="Unsupported MetaTable filter"):
        MetaTable._normalize_filter_kwargs({"organization_environment_uid": {"uid": uid}})


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


def test_shareable_action_posts_user_uid(monkeypatch):
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

    user_uid = "8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"
    response = DemoShareableModel(11).add_to_edit(_UidRef(user_uid), timeout=15)

    assert response == {"detail": "ok"}
    assert captured == {
        "r_type": "POST",
        "url": "https://backend.test/demo-shareable/11/add-to-edit/",
        "payload": {"json": {"user_uid": user_uid}},
        "timeout": 15,
    }


def test_shareable_action_returns_empty_dict_on_no_content(monkeypatch):
    class FakeResponse:
        status_code = 200
        content = b""

    monkeypatch.setattr(base_mod, "make_request", lambda **kwargs: FakeResponse())

    response = DemoShareableModel(9).remove_from_view("8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123")

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


def test_shareable_team_action_posts_team_uid(monkeypatch):
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

    team_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    response = DemoShareableModel(11).add_team_to_view(_UidRef(team_uid), timeout=21)

    assert response == {"detail": "ok"}
    assert captured == {
        "r_type": "POST",
        "url": "https://backend.test/demo-shareable/11/add-team-to-view/",
        "payload": {"json": {"team_uid": team_uid}},
        "timeout": 21,
    }


def test_shareable_can_view_parses_permission_state(monkeypatch):
    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "object_uid": "11111111-1111-4111-8111-111111111111",
                "object_type": "tdag.constant",
                "access_level": "view",
                "users": [
                    {
                        "id": 7,
                        "uid": "8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123",
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

    assert access_state.object_uid == "11111111-1111-4111-8111-111111111111"
    assert access_state.access_level == "view"
    assert len(access_state.users) == 1
    assert access_state.users[0].uid == "8f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"
    assert not hasattr(access_state.users[0], "id")
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
                "object_uid": "22222222-2222-4222-8222-222222222222",
                "object_type": "tdag.secret",
                "access_level": "edit",
                "users": [
                    {
                        "id": 9,
                        "uid": "9f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123",
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
                        "uid": "3f1cc452-43ec-49cb-b2ba-87dbac164d29",
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

    assert access_state.object_uid == "22222222-2222-4222-8222-222222222222"
    assert access_state.access_level == "edit"
    assert len(access_state.users) == 1
    assert access_state.users[0].uid == "9f5d6b54-2f5e-4a8b-bb10-0b17f3f4c123"
    assert access_state.teams[0].uid == "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    assert access_state.teams[0].name == "Research"
    assert access_state.teams[0].member_count == 4
    assert captured == {
        "r_type": "GET",
        "url": "https://backend.test/demo-shareable/15/can-edit/",
        "payload": {},
        "timeout": 20,
    }


def test_shareable_access_state_rejects_removed_internal_identity():
    removed_field = "_".join(("object", "id"))
    payload = {
        "object_uid": "33333333-3333-4333-8333-333333333333",
        "object_type": "tdag.constant",
        "access_level": "view",
        "users": [],
        "teams": [],
        removed_field: 17,
    }

    with pytest.raises(ValidationError, match=removed_field):
        models_user_mod.ShareableAccessState.model_validate(payload)


@pytest.mark.parametrize(
    ("resource_cls", "accessor_name", "access_level"),
    [
        (models_metatables_mod.MetaTable, "can_view", "view"),
        (models_metatables_mod.TimeIndexMetaTable, "can_view", "view"),
        (models_foundry_mod.Constant, "can_edit", "edit"),
    ],
)
def test_shareable_resources_parse_canonical_public_identity(
    monkeypatch,
    resource_cls,
    accessor_name,
    access_level,
):
    resource_uid = "44444444-4444-4444-8444-444444444444"
    resource = resource_cls.model_construct(uid=resource_uid)
    monkeypatch.setattr(
        resource_cls,
        "_request_detail_action",
        lambda self, **kwargs: {
            "object_uid": resource_uid,
            "object_type": type(self).__name__,
            "access_level": access_level,
            "users": [],
            "teams": [],
        },
    )

    access_state = getattr(resource, accessor_name)()

    assert access_state.object_uid == resource_uid
    assert access_state.access_level == access_level


def test_shareable_access_state_accepts_nullable_canonical_identity():
    access_state = models_user_mod.ShareableAccessState(
        object_uid=None,
        object_type="tdag.constant",
        access_level="view",
    )

    assert access_state.object_uid is None


def test_team_uses_user_api_team_endpoint():
    assert models_user_mod.Team.get_object_url().endswith("/api/v1/teams")


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
    assert members[0].uid == user_uid
    assert not hasattr(members[0], "id")
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
    production_environment_uid = "00000000-0000-4000-8000-000000000002"
    user_uid = "fdf409f7-d16f-4f71-986b-9057db6c7eca"
    team_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    organization_payload = {
        "uid": org_uid,
        "name": "Main Sequence",
        "url": "https://backend.test",
        "organization_domain": "main-sequence.io",
        "identity_platform_tenant_id": None,
        "has_pending_invoices": False,
        "production_environment_uid": production_environment_uid,
    }

    organization = models_user_mod.Organization.model_validate(organization_payload)
    assert organization.uid == org_uid
    assert organization.production_environment_uid == production_environment_uid
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
    assert current_user.organization.production_environment_uid == production_environment_uid
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
    project_branch_uid = "9d81d63f-b8c9-404d-9f1a-5f2ad29dbf16"
    environment_uid = "22222222-2222-4222-8222-222222222222"

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
            "project_branch_uid": project_branch_uid,
            "repository_branch": "main",
            "organization_environment_uid": environment_uid,
            "organization_environment_name": "production",
        }
    )
    assert agent.uid == agent_uid
    assert agent.has_agent_service is True
    assert agent.agent_service_uid == service_uid
    assert agent.agent_service_automatic_deployment is True
    assert agent.project_branch_uid == project_branch_uid
    assert agent.repository_branch == "main"
    assert agent.organization_environment_uid == environment_uid
    assert agent.organization_environment_name == "production"
    assert agent.a2a_profile.supported_response_kinds == [agent_models_mod.A2AResponseKind.MESSAGE]

    search_result = agent_models_mod.AgentSemanticSearchResult.model_validate(
        {
            "uid": agent_uid,
            "name": "Research Copilot",
            "agent_type": "custom",
            "description": "Research assistant.",
            "project_branch_uid": project_branch_uid,
            "repository_branch": "main",
            "organization_environment_uid": environment_uid,
            "organization_environment_name": "production",
            "semantic_score": 0.91,
            "text_score": 0.74,
            "combined_score": 0.85,
        }
    )
    assert search_result.uid == agent_uid
    assert search_result.project_branch_uid == project_branch_uid
    assert search_result.repository_branch == "main"
    assert search_result.organization_environment_uid == environment_uid
    assert search_result.organization_environment_name == "production"
    assert search_result.a2a_profile.default_response_kind == (
        agent_models_mod.A2AResponseKind.MESSAGE
    )

    session = agent_models_mod.AgentSession.model_validate(
        {
            "uid": session_uid,
            "agent_uid": agent_uid,
            "agent_name": "Research Copilot",
            "agent_type": "custom",
            "harness": "tau",
            "harness_protocol": "tau-session-v1",
            "harness_version": "0.3.1",
            "created_by_user_uid": user_uid,
            "parent_session_uid": None,
            "name": "Research follow-up",
            "status": "running",
            "runtime_state": "running",
            "working": True,
            "started_at": "2026-01-01T00:00:00Z",
            "ended_at": None,
            "is_archived": False,
            "archived_at": None,
            "llm_provider": "openai",
            "llm_model": "gpt-5.4",
            "llm_thinking": "medium",
            "active_provider": "openai",
            "active_model": "gpt-5.4",
            "active_thinking": "medium",
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
            "observability": {
                "application_logs_url": f"/api/v1/agent-sessions/{session_uid}/logs/",
                "resource_usage_url": None,
                "deployment_runs_url": None,
                "sessions_url": None,
            },
            "runtime_capabilities": {
                "tau_runtime_bootstrap": "v1",
                "tau_resume_snapshot": "v1",
                "tau_activity_sequence": "v1",
                "tau_turn_commit": "v1",
            },
        }
    )
    assert session.uid == session_uid
    assert session.agent_uid == agent_uid
    assert session.name == "Research follow-up"
    assert session.harness is agent_models_mod.AgentHarnessKind.TAU
    assert session.harness_protocol is agent_models_mod.AgentHarnessProtocol.TAU_SESSION_V1
    assert session.harness_version == "0.3.1"
    assert session.is_archived is False
    assert session.active_model == "gpt-5.4"
    assert session.observability.application_logs_url.endswith(f"/{session_uid}/logs/")
    assert session.runtime_capabilities == {
        "tau_runtime_bootstrap": "v1",
        "tau_resume_snapshot": "v1",
        "tau_activity_sequence": "v1",
        "tau_turn_commit": "v1",
    }

    service = agent_models_mod.CodingAgentService.model_validate(
        {
            "uid": service_uid,
            "harness": "tau",
            "agent_uid": agent_uid,
            "agent_type": "project-executor",
            "scope": {
                "kind": "project_branch",
                "project_branch_uid": "project-branch-uid",
            },
            "is_ready": True,
            "image_drift": {"has_drift": False, "checks": []},
            "llm_provider": "openai",
            "llm_model": "gpt-5.4",
            "llm_thinking": "medium",
            "cpu_request": "250m",
            "cpu_limit": "1000m",
            "memory_request": "512Mi",
            "memory_limit": "2Gi",
            "gpu_request": None,
            "gpu_type": None,
            "spot": False,
            "related_job_uid": "job-uid",
            "service_runtime_uid": "runtime-uid",
            "automatic_deployment": True,
            "automatic_redeployment_policy": {
                "tag_regex": None,
                "policy_revision": 1,
            },
        }
    )
    assert service.uid == service_uid
    assert service.agent_uid == agent_uid
    assert service.agent_type == "project-executor"
    assert service.harness is agent_models_mod.AgentHarnessKind.TAU
    assert service.scope["project_branch_uid"] == "project-branch-uid"
    assert service.llm_model == "gpt-5.4"
    assert service.cpu_request == "250m"
    assert service.spot is False
    assert service.service_runtime_uid == "runtime-uid"
    assert service.automatic_redeployment_policy is not None
    assert service.automatic_redeployment_policy.policy_revision == 1


def test_agent_scope_projection_is_required_but_nullable():
    payload = {
        "name": "Astro Orchestrator",
        "llm_thinking": "medium",
        "repository_branch": None,
        "organization_environment_uid": None,
        "organization_environment_name": None,
    }

    agent = agent_models_mod.Agent.model_validate(payload)

    assert agent.repository_branch is None
    assert agent.organization_environment_uid is None
    assert agent.organization_environment_name is None

    missing_projection = dict(payload)
    missing_projection.pop("repository_branch")
    with pytest.raises(ValidationError, match="repository_branch"):
        agent_models_mod.Agent.model_validate(missing_projection)


def test_agent_filter_sends_environment_read_scope_and_parses_projection(monkeypatch):
    captured = {}
    environment_uid = uuid.UUID("22222222-2222-4222-8222-222222222222")

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "results": [
                    {
                        "uid": "e0e75693-4110-464c-93e0-82c7fd9c9a23",
                        "name": "Project Executor",
                        "llm_thinking": "medium",
                        "project_branch_uid": "9d81d63f-b8c9-404d-9f1a-5f2ad29dbf16",
                        "repository_branch": "main",
                        "organization_environment_uid": str(environment_uid),
                        "organization_environment_name": "production",
                    }
                ],
                "next": None,
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured.update(
            {
                "r_type": r_type,
                "url": url,
                "payload": payload,
                "timeout": time_out,
            }
        )
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)
    monkeypatch.setattr(
        agent_models_mod.Agent,
        "build_session",
        classmethod(lambda cls: object()),
    )

    agents = agent_models_mod.Agent.filter(
        organization_environment_uid=environment_uid,
        agent_type="project-executor",
        timeout=11,
    )

    assert captured == {
        "r_type": "GET",
        "url": f"{agent_models_mod.Agent.get_object_url()}/",
        "payload": {
            "params": {
                "agent_type": "project-executor",
                "organization_environment_uid": str(environment_uid),
            }
        },
        "timeout": 11,
    }
    assert len(agents) == 1
    assert agents[0].repository_branch == "main"
    assert agents[0].organization_environment_uid == str(environment_uid)


def test_agent_semantic_search_sends_environment_scope_and_parses_projection(monkeypatch):
    captured = {}
    environment_uid = uuid.UUID("22222222-2222-4222-8222-222222222222")

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return [
                {
                    "uid": "e0e75693-4110-464c-93e0-82c7fd9c9a23",
                    "name": "Project Executor",
                    "agent_type": "project-executor",
                    "description": "Project coding agent.",
                    "project_branch_uid": "9d81d63f-b8c9-404d-9f1a-5f2ad29dbf16",
                    "repository_branch": "main",
                    "organization_environment_uid": str(environment_uid),
                    "organization_environment_name": "production",
                    "semantic_score": 0.91,
                    "text_score": 0.74,
                    "combined_score": 0.85,
                }
            ]

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured.update(
            {
                "r_type": r_type,
                "url": url,
                "payload": payload,
                "timeout": time_out,
            }
        )
        return FakeResponse()

    monkeypatch.setattr(agent_models_mod, "make_request", _fake_make_request)
    monkeypatch.setattr(
        agent_models_mod.Agent,
        "build_session",
        classmethod(lambda cls: object()),
    )

    results = agent_models_mod.Agent.semantic_search(
        "project coding",
        organization_environment_uid=environment_uid,
        limit=7,
        timeout=13,
    )

    assert captured == {
        "r_type": "POST",
        "url": f"{agent_models_mod.Agent.get_object_url()}/semantic-search/",
        "payload": {
            "json": {
                "organization_environment_uid": str(environment_uid),
                "q": "project coding",
                "limit": 7,
            }
        },
        "timeout": 13,
    }
    assert len(results) == 1
    assert results[0].project_branch_uid == "9d81d63f-b8c9-404d-9f1a-5f2ad29dbf16"
    assert results[0].organization_environment_name == "production"


def test_agent_session_filter_supports_archive_history_query(monkeypatch):
    captured = {}
    user_uid = "3a715c8d-da66-452a-b6cb-ffbb696ef121"

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"count": 0, "next": None, "previous": None, "results": []}

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured.update(
            {
                "r_type": r_type,
                "url": url,
                "payload": payload,
                "timeout": time_out,
            }
        )
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    sessions = agent_models_mod.AgentSession.filter(
        created_by_user_uid=user_uid,
        is_archived=False,
        ordering="-started_at",
        limit=20,
        offset=0,
        timeout=7,
    )

    assert sessions == []
    assert captured == {
        "r_type": "GET",
        "url": f"{agent_models_mod.AgentSession.get_object_url()}/",
        "payload": {
            "params": {
                "created_by_user_uid": user_uid,
                "is_archived": False,
                "ordering": "-started_at",
                "limit": "20",
                "offset": "0",
            }
        },
        "timeout": 7,
    }


def test_agent_session_list_and_detail_parse_runtime_capabilities(monkeypatch):
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    agent_uid = "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    session_payload = {
        "uid": session_uid,
        "agent_uid": agent_uid,
        "agent_name": "Research Copilot",
        "agent_type": "custom",
        "harness": "tau",
        "harness_protocol": "tau-session-v1",
        "harness_version": "0.3.1",
        "name": "Reusable session",
        "status": "running",
        "llm_provider": "openai",
        "llm_model": "gpt-5.4",
        "llm_thinking": "medium",
        "observability": {
            "application_logs_url": f"/api/v1/agent-sessions/{session_uid}/logs/",
            "resource_usage_url": None,
            "deployment_runs_url": None,
            "sessions_url": None,
        },
        "runtime_capabilities": {
            "tau_runtime_bootstrap": "v1",
            "tau_resume_snapshot": "v1",
            "tau_activity_sequence": "v1",
            "tau_turn_commit": "v1",
        },
    }
    responses = iter(
        [
            {"count": 1, "next": None, "previous": None, "results": [dict(session_payload)]},
            dict(session_payload),
        ]
    )

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        def __init__(self, payload):
            self._payload = payload

        def json(self):
            return self._payload

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        return FakeResponse(next(responses))

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    listed = agent_models_mod.AgentSession.filter(agent_uid=agent_uid)
    detailed = agent_models_mod.AgentSession.get(pk=session_uid)

    assert listed[0].runtime_capabilities["tau_turn_commit"] == "v1"
    assert detailed.runtime_capabilities == listed[0].runtime_capabilities
    assert detailed.observability.application_logs_url.endswith(f"/{session_uid}/logs/")

    with pytest.raises(ValidationError, match="unexpected_projection"):
        agent_models_mod.AgentSession.model_validate(
            {**session_payload, "unexpected_projection": "still forbidden"}
        )


def test_agent_session_get_insights_parses_pi_response(monkeypatch):
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    captured = {}

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "has_insights": False,
                "agent_session_uid": session_uid,
                "harness": "pi",
                "harness_protocol": "pi-checkpoint-v1",
                "harness_version": "",
                "checkpoint_version": None,
                "bundle_hash": "",
                "computed_at": None,
                "flushed_at": None,
                "reason": None,
                "insights": {},
                "updated_at": None,
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured.update(
            {
                "r_type": r_type,
                "url": url,
                "payload": payload,
                "timeout": time_out,
            }
        )
        return FakeResponse()

    monkeypatch.setattr(agent_models_mod, "make_request", _fake_make_request)

    insights = agent_models_mod.AgentSession.get_insights(session_uid, timeout=9)

    assert isinstance(insights, agent_models_mod.PiAgentSessionInsights)
    assert insights.has_insights is False
    assert insights.checkpoint_version is None
    assert captured == {
        "r_type": "GET",
        "url": f"{agent_models_mod.AgentSession.get_object_url()}/{session_uid}/insights/",
        "payload": {},
        "timeout": 9,
    }


def test_agent_session_get_insights_parses_tau_response(monkeypatch):
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "has_insights": True,
                "agent_session_uid": session_uid,
                "harness": "tau",
                "harness_protocol": "tau-session-v1",
                "harness_version": "0.3.1",
                "entry_count": 4,
                "last_sequence": 4,
                "active_branch_entry_count": 3,
                "entry_type_counts": {"message": 3, "session_info": 1},
                "title": "Competition analysis",
                "computed_at": "2026-07-26T19:25:00Z",
                "reason": "tau_entries_projection",
                "updated_at": "2026-07-26T19:24:00Z",
                "insights": {
                    "version": 1,
                    "model": {
                        "provider": "openai",
                        "model": "gpt-5.4",
                        "reasoningEffort": "high",
                    },
                    "session": {
                        "agentSessionId": session_uid,
                        "sessionId": "thread-123",
                        "threadId": "thread-123",
                        "status": "running",
                        "startedAt": "2026-07-26T19:00:00Z",
                        "updatedAt": "2026-07-26T19:24:00Z",
                        "lastError": None,
                    },
                    "usage": {
                        "totalMessages": 3,
                        "userMessages": 1,
                        "assistantMessages": 2,
                        "assistantTurns": 2,
                        "toolCalls": 1,
                        "toolResults": 0,
                        "estimatedCostUsd": 0.42,
                        "tokens": {
                            "input": 100,
                            "output": 50,
                            "cacheRead": 10,
                            "cacheWrite": 5,
                            "total": 165,
                        },
                    },
                    "context": {
                        "source": "tau_entries",
                        "status": "reported_by_last_assistant",
                        "tokens": 100,
                        "latestCompaction": None,
                    },
                    "lastTurn": {
                        "completedAt": "2026-07-26T19:24:00Z",
                        "finishReason": "stop",
                        "errorMessage": None,
                        "model": {
                            "provider": "openai",
                            "model": "gpt-5.4",
                        },
                        "tokens": {
                            "input": 60,
                            "output": 20,
                            "cacheRead": 10,
                            "cacheWrite": 5,
                            "total": 95,
                        },
                    },
                },
            }

    monkeypatch.setattr(agent_models_mod, "make_request", lambda **kwargs: FakeResponse())

    insights = agent_models_mod.AgentSession.get_insights(session_uid)

    assert isinstance(insights, agent_models_mod.TauAgentSessionInsights)
    assert insights.entry_count == 4
    assert insights.entry_type_counts == {"message": 3, "session_info": 1}
    assert insights.insights.model.reasoning_effort == "high"
    assert insights.insights.session.agent_session_id == session_uid
    assert insights.insights.usage.tokens.cache_read == 10
    assert insights.insights.last_turn is not None
    assert insights.insights.last_turn.finish_reason == "stop"


def test_agent_session_archive_actions_return_current_session_contract(monkeypatch):
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    requests = []

    class FakeResponse:
        status_code = 200

        def __init__(self, archived):
            self.archived = archived

        def json(self):
            return {
                "uid": session_uid,
                "agent_uid": "9d81d63f-b8c9-404d-9f1a-5f2ad29dbf16",
                "agent_name": "Research Copilot",
                "agent_type": "custom",
                "harness": "tau",
                "harness_protocol": "tau-session-v1",
                "harness_version": "0.3.1",
                "created_by_user_uid": "3a715c8d-da66-452a-b6cb-ffbb696ef121",
                "parent_session_uid": None,
                "name": "Research follow-up",
                "status": "running",
                "started_at": "2026-07-26T19:00:00Z",
                "ended_at": None,
                "is_archived": self.archived,
                "archived_at": "2026-07-26T20:00:00Z" if self.archived else None,
                "llm_provider": "openai",
                "llm_model": "gpt-5.4",
                "llm_thinking": "high",
                "active_provider": "openai",
                "active_model": "gpt-5.4",
                "active_thinking": "high",
                "engine_name": "tau",
                "runtime_config_snapshot": {},
                "error_detail": "",
                "thread_id": "thread-123",
                "session_metadata": {},
                "bound_handle": None,
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        requests.append((r_type, url, payload, time_out))
        return FakeResponse(url.endswith("/archive/"))

    monkeypatch.setattr(agent_models_mod, "make_request", _fake_make_request)

    archived = agent_models_mod.AgentSession.archive_by_uid(session_uid, timeout=5)
    unarchived = archived.unarchive(timeout=6)

    assert archived.is_archived is True
    assert archived.archived_at is not None
    assert unarchived.is_archived is False
    assert unarchived.archived_at is None
    assert requests == [
        (
            "POST",
            f"{agent_models_mod.AgentSession.get_object_url()}/{session_uid}/archive/",
            {},
            5,
        ),
        (
            "POST",
            f"{agent_models_mod.AgentSession.get_object_url()}/{session_uid}/unarchive/",
            {},
            6,
        ),
    ]


def test_resource_release_model_accepts_collection_payload_without_runtime_child_fields():
    release_uid = "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed"
    project_branch_uid = "9d81d63f-b8c9-404d-9f1a-5f2ad29dbf16"

    release = models_helpers_mod.ResourceRelease.model_validate(
        {
            "uid": release_uid,
            "project_branch_uid": project_branch_uid,
            "name": "Competition Analysis",
            "release_kind": "static_site",
            "automatic_deployment": True,
        }
    )

    assert release.uid == release_uid
    assert release.project_branch_uid == project_branch_uid
    assert release.name == "Competition Analysis"
    assert release.release_kind == models_helpers_mod.ResourceReleaseKind.STATIC_SITE
    assert release.resource_uid is None
    assert release.readme_resource_uid is None
    assert release.related_job_uid is None
    assert "subdomain" not in models_helpers_mod.ResourceRelease.model_fields


def test_job_requires_exact_image_and_exposes_automatic_redeployment_state():
    with pytest.raises(ValueError, match="related_image_uid"):
        models_helpers_mod.Job.model_validate(
            {
                "name": "Daily prices",
                "image_status": "ready",
            }
        )

    job = models_helpers_mod.Job.model_validate(
        {
            "uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
            "name": "Daily prices",
            "project_branch_uid": "5a28020a-0f1b-47ee-aab8-334286234bea",
            "related_image_uid": "6cfdb152-923e-45b9-a150-c4541c68b0d1",
            "project_repo_hash": "a" * 40,
            "image_status": "ready",
            "automatic_deployment": True,
            "automatic_redeployment_policy": {
                "tag_regex": "^v[0-9]+$",
                "policy_revision": 4,
            },
        }
    )

    assert job.related_image_uid == "6cfdb152-923e-45b9-a150-c4541c68b0d1"
    assert job.project_repo_hash == "a" * 40
    assert job.image_status == "ready"
    assert job.automatic_deployment is True
    assert job.automatic_redeployment_policy is not None
    assert job.automatic_redeployment_policy.policy_revision == 4


@pytest.mark.parametrize("environment_uid", [None, ENVIRONMENT_UID])
def test_job_accepts_backend_derived_environment_uid(environment_uid):
    job = models_helpers_mod.Job.model_validate(
        {
            "uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
            "name": "Daily prices",
            "project_branch_uid": PROJECT_BRANCH_UID,
            "organization_environment_uid": environment_uid,
            "related_image_uid": "6cfdb152-923e-45b9-a150-c4541c68b0d1",
            "project_repo_hash": "a" * 40,
            "image_status": "ready",
        }
    )

    assert job.organization_environment_uid == environment_uid


def test_job_filter_parses_backend_derived_environment_uid(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return [
                {
                    "uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
                    "name": "Daily prices",
                    "project_branch_uid": PROJECT_BRANCH_UID,
                    "organization_environment_uid": ENVIRONMENT_UID,
                    "related_image_uid": "6cfdb152-923e-45b9-a150-c4541c68b0d1",
                    "project_repo_hash": "a" * 40,
                    "image_status": "ready",
                }
            ]

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", fake_make_request)

    jobs = models_helpers_mod.Job.filter()

    assert jobs[0].organization_environment_uid == ENVIRONMENT_UID
    assert captured["payload"]["params"] == {
        "project_branch_uid": PROJECT_BRANCH_UID,
    }


def test_job_create_requires_exact_image_and_does_not_send_commit(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 201

        @staticmethod
        def json():
            return {
                "uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
                "name": "Daily prices",
                "project_branch_uid": "5a28020a-0f1b-47ee-aab8-334286234bea",
                "related_image_uid": "6cfdb152-923e-45b9-a150-c4541c68b0d1",
                "project_repo_hash": "a" * 40,
                "image_status": "ready",
                "automatic_deployment": True,
                "automatic_redeployment_policy": {
                    "tag_regex": None,
                    "policy_revision": 1,
                },
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured.update({"r_type": r_type, "payload": payload})
        return FakeResponse()

    monkeypatch.setattr(models_helpers_mod, "make_request", _fake_make_request)

    with pytest.raises(ValueError, match="related_image_uid is required"):
        models_helpers_mod.Job.create(
            name="Daily prices",
            project_branch_uid="5a28020a-0f1b-47ee-aab8-334286234bea",
            execution_path="jobs/daily_prices.py",
            cpu_request="1",
            memory_request="2",
        )

    job = models_helpers_mod.Job.create(
        name="Daily prices",
        project_branch_uid="5a28020a-0f1b-47ee-aab8-334286234bea",
        execution_path="jobs/daily_prices.py",
        cpu_request="1",
        memory_request="2",
        automatic_deployment=True,
        automatic_redeployment_policy={"tag_regex": None},
    )

    assert job.automatic_deployment is True
    assert captured["r_type"] == "POST"
    assert captured["payload"]["json"]["automatic_deployment"] is True
    assert captured["payload"]["json"]["automatic_redeployment_policy"] == {"tag_regex": None}
    assert "project_repo_hash" not in captured["payload"]["json"]
    assert "related_image_uid" not in captured["payload"]["json"]

    with pytest.raises(ValueError, match="must be omitted"):
        models_helpers_mod.Job.create(
            name="Invalid auto job",
            project_branch_uid="5a28020a-0f1b-47ee-aab8-334286234bea",
            execution_path="jobs/daily_prices.py",
            related_image_uid="6cfdb152-923e-45b9-a150-c4541c68b0d1",
            cpu_request="1",
            memory_request="2",
            automatic_deployment=True,
        )


def test_job_run_requires_immutable_runtime_image_snapshot():
    payload = {
        "uid": "4c1d77c8-8a42-42b8-a9c1-06be9a336e5d",
        "name": "daily-prices-run",
        "unique_identifier": "jobrun_2026_08_19_abc123",
        "project_uid": "1d0530c0-65d1-4db0-856b-dc29d8260a09",
        "project_name": "market-data-service",
        "project_branch_uid": "5a28020a-0f1b-47ee-aab8-334286234bea",
        "project_branch_name": "main",
        "organization_environment_uid": ENVIRONMENT_UID,
        "commit_hash": "a" * 40,
        "runtime_image_uid": "6cfdb152-923e-45b9-a150-c4541c68b0d1",
        "runtime_image_digest": "sha256:" + "b" * 64,
    }
    run = models_helpers_mod.JobRun.model_validate(payload)

    assert run.runtime_image_uid == payload["runtime_image_uid"]
    assert run.runtime_image_digest == payload["runtime_image_digest"]
    assert run.commit_hash == payload["commit_hash"]
    assert run.project_uid == payload["project_uid"]
    assert run.project_name == payload["project_name"]
    assert run.project_branch_uid == payload["project_branch_uid"]
    assert run.project_branch_name == payload["project_branch_name"]
    assert run.organization_environment_uid == ENVIRONMENT_UID

    missing_project_context = dict(payload)
    missing_project_context.pop("project_uid")
    with pytest.raises(ValueError, match="project_uid"):
        models_helpers_mod.JobRun.model_validate(missing_project_context)

    with pytest.raises(ValueError, match="runtime_image_uid"):
        models_helpers_mod.JobRun.model_validate(
            {
                "name": "bad-run",
                "unique_identifier": "jobrun_bad",
                "project_uid": None,
                "project_name": None,
                "project_branch_uid": None,
                "project_branch_name": None,
                "organization_environment_uid": ENVIRONMENT_UID,
                "runtime_image_digest": "sha256:" + "b" * 64,
            }
        )


def test_resource_release_model_supports_automatic_deployment_payloads():
    release_uid = "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed"
    resource_uid = "857bec7b-dd77-4272-aecd-13fc2138eacc"
    job_uid = "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da"

    release = models_helpers_mod.ResourceRelease.model_validate(
        {
            "uid": release_uid,
            "resource_uid": resource_uid,
            "readme_resource_uid": None,
            "related_job_uid": job_uid,
            "release_kind": "fastapi",
            "automatic_deployment": True,
            "automatic_redeployment_policy": {
                "tag_regex": None,
                "policy_revision": 3,
            },
            "revision_retention_count": 3,
            "active_revision": None,
            "desired_revision": "2a9370a7-c07f-439c-bcd9-629e3e916699",
        }
    )

    assert release.uid == release_uid
    assert release.release_kind == models_helpers_mod.ResourceReleaseKind.FAST_API
    assert release.automatic_deployment is True
    assert release.automatic_redeployment_policy is not None
    assert release.automatic_redeployment_policy.tag_regex is None
    assert release.automatic_redeployment_policy.policy_revision == 3
    assert release.revision_retention_count == 3
    assert release.active_revision is None
    assert release.desired_revision == "2a9370a7-c07f-439c-bcd9-629e3e916699"


def test_resource_release_filter_accepts_canonical_revision_lifecycle_projection(monkeypatch):
    release_uid = "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed"
    desired_revision_uid = "2a9370a7-c07f-439c-bcd9-629e3e916699"
    response_payload = {
        "uid": release_uid,
        "project_branch_uid": PROJECT_BRANCH_UID,
        "name": "Pricing API",
        "release_kind": "fastapi",
        "automatic_deployment": True,
        "automatic_redeployment_policy": {
            "tag_regex": None,
            "policy_revision": 2,
        },
        "revision_retention_count": 3,
        "active_revision": None,
        "desired_revision": desired_revision_uid,
    }

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return [dict(response_payload)]

    monkeypatch.setattr(base_mod, "make_request", lambda **kwargs: FakeResponse())

    releases = models_helpers_mod.ResourceRelease.filter(project_branch_uid=PROJECT_BRANCH_UID)

    assert len(releases) == 1
    assert releases[0].uid == release_uid
    assert releases[0].revision_retention_count == 3
    assert releases[0].active_revision is None
    assert releases[0].desired_revision == desired_revision_uid


def test_legacy_resource_release_deployment_run_model_is_not_exported():
    import mainsequence.client as client_package

    legacy_name = "ResourceReleaseAutomaticDeploymentRun"

    assert not hasattr(models_helpers_mod, legacy_name)
    assert not hasattr(client_package, legacy_name)
    with pytest.raises(KeyError, match=legacy_name):
        models_helpers_mod.get_model_class(legacy_name)


def test_resource_release_create_sends_automatic_deployment(monkeypatch):
    captured = {}
    release_uid = "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed"
    resource_uid = "857bec7b-dd77-4272-aecd-13fc2138eacc"
    image_uid = "6cfdb152-923e-45b9-a150-c4541c68b0d1"

    class FakeResponse:
        status_code = 201

        @staticmethod
        def json():
            return {
                "uid": release_uid,
                "resource_uid": resource_uid,
                "readme_resource_uid": None,
                "related_job_uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
                "release_kind": "fastapi",
                "automatic_deployment": True,
                "automatic_redeployment_policy": {
                    "tag_regex": "^v[0-9]+\\.[0-9]+\\.[0-9]+$",
                    "policy_revision": 1,
                },
                "revision_retention_count": 5,
                "active_revision": "19128ab6-d72f-460c-8525-d758fa92676a",
                "desired_revision": "2a9370a7-c07f-439c-bcd9-629e3e916699",
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(models_helpers_mod, "make_request", _fake_make_request)

    release = models_helpers_mod.ResourceRelease.create(
        resource_uid=resource_uid,
        related_image_uid=image_uid,
        release_kind=models_helpers_mod.ResourceReleaseKind.FAST_API,
        cpu_request="500m",
        memory_request="1Gi",
        automatic_deployment=True,
        revision_retention_count=5,
        timeout=11,
    )

    assert release.automatic_deployment is True
    assert release.automatic_redeployment_policy is not None
    assert release.automatic_redeployment_policy.tag_regex == "^v[0-9]+\\.[0-9]+\\.[0-9]+$"
    assert release.automatic_redeployment_policy.policy_revision == 1
    assert release.revision_retention_count == 5
    assert release.active_revision == "19128ab6-d72f-460c-8525-d758fa92676a"
    assert release.desired_revision == "2a9370a7-c07f-439c-bcd9-629e3e916699"
    assert captured["r_type"] == "POST"
    assert str(captured["url"]).endswith("/api/v1/resource-releases/")
    assert captured["payload"]["json"]["automatic_deployment"] is True
    assert captured["payload"]["json"]["revision_retention_count"] == 5
    assert captured["payload"]["json"]["resource_uid"] == resource_uid
    assert captured["payload"]["json"]["related_image_uid"] == image_uid
    assert captured["timeout"] == 11


@pytest.mark.parametrize("invalid_value", [0, -1, True, 1.5, "3"])
def test_resource_release_create_rejects_invalid_revision_retention_count(
    monkeypatch, invalid_value
):
    monkeypatch.setattr(
        models_helpers_mod,
        "make_request",
        lambda **kwargs: pytest.fail("invalid retention must fail before the request"),
    )

    with pytest.raises(
        ValueError,
        match="revision_retention_count must be a positive integer",
    ):
        models_helpers_mod.ResourceRelease.create(
            resource_uid="857bec7b-dd77-4272-aecd-13fc2138eacc",
            related_image_uid="6cfdb152-923e-45b9-a150-c4541c68b0d1",
            release_kind=models_helpers_mod.ResourceReleaseKind.FAST_API,
            cpu_request="500m",
            memory_request="1Gi",
            revision_retention_count=invalid_value,
        )


def test_project_resource_create_release_uses_related_image_uid(monkeypatch):
    captured = {}
    resource_uid = "857bec7b-dd77-4272-aecd-13fc2138eacc"
    image_uid = "6cfdb152-923e-45b9-a150-c4541c68b0d1"
    release_uid = "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed"
    resource = models_helpers_mod.ProjectResource(
        uid=resource_uid,
        name="analytics_dashboard.py",
        resource_type="dashboard",
        path="dashboards/analytics_dashboard.py",
    )

    class FakeResponse:
        status_code = 201

        @staticmethod
        def json():
            return {
                "uid": release_uid,
                "resource_uid": resource_uid,
                "readme_resource_uid": None,
                "related_job_uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
                "release_kind": "streamlit_dashboard",
                "automatic_deployment": True,
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(models_helpers_mod, "make_request", _fake_make_request)

    release = resource.create_dashboard(
        related_image_uid=image_uid,
        cpu_request="500m",
        memory_request="1Gi",
        automatic_deployment=True,
        timeout=7,
    )

    assert release.uid == release_uid
    assert captured["r_type"] == "POST"
    assert captured["payload"]["json"]["related_image_uid"] == image_uid
    assert captured["payload"]["json"]["automatic_deployment"] is True
    assert captured["timeout"] == 7


@pytest.mark.parametrize(
    "resource_type",
    [
        "configuration",
        "notebook",
        "script",
        "dashboard",
        "agent",
        "fastapi",
        "project_agent_card",
        "markdown",
    ],
)
def test_project_resource_accepts_canonical_backend_resource_types(resource_type):
    resource = models_helpers_mod.ProjectResource.model_validate(
        {
            "uid": "857bec7b-dd77-4272-aecd-13fc2138eacc",
            "project_branch_uid": PROJECT_BRANCH_UID,
            "name": ".agents/agent_card.json",
            "resource_type": resource_type,
            "path": ".agents/agent_card.json",
            "repo_commit_sha": "abc123",
        }
    )

    assert resource.resource_type == resource_type


def test_resource_release_get_accepts_fastapi_cors_allowed_origins(monkeypatch):
    captured = {}
    release_uid = "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed"
    response_payload = {
        "uid": release_uid,
        "resource_uid": "857bec7b-dd77-4272-aecd-13fc2138eacc",
        "readme_resource_uid": None,
        "related_job_uid": "7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da",
        "release_kind": "fastapi",
        "automatic_deployment": True,
        "automatic_redeployment_policy": None,
        "revision_retention_count": 4,
        "active_revision": "19128ab6-d72f-460c-8525-d758fa92676a",
        "desired_revision": None,
        "cors_allowed_origins": [
            "https://app.example.com",
            "https://*.site-dev.main-sequence.app",
        ],
    }

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return dict(response_payload)

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured.update(
            {
                "r_type": r_type,
                "url": url,
                "payload": payload,
                "timeout": time_out,
            }
        )
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    release = models_helpers_mod.ResourceRelease.get(pk=release_uid, timeout=13)

    assert release.release_kind == models_helpers_mod.ResourceReleaseKind.FAST_API
    assert release.revision_retention_count == 4
    assert release.active_revision == "19128ab6-d72f-460c-8525-d758fa92676a"
    assert release.desired_revision is None
    assert release.cors_allowed_origins == response_payload["cors_allowed_origins"]
    assert captured["r_type"] == "GET"
    assert captured["url"].endswith(f"/resource-releases/{release_uid}/")
    assert captured["timeout"] == 13


def test_resource_release_patch_supports_positive_revision_retention_count(monkeypatch):
    captured = {}
    release_uid = "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed"
    release = models_helpers_mod.ResourceRelease(
        uid=release_uid,
        release_kind="fastapi",
        revision_retention_count=3,
    )

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "revision_retention_count": 6,
                "active_revision": None,
                "desired_revision": None,
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured.update({"r_type": r_type, "url": url, "payload": payload})
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    patched = release.patch(revision_retention_count=6)

    assert patched is release
    assert patched.revision_retention_count == 6
    assert captured["r_type"] == "PATCH"
    assert captured["url"].endswith(f"/resource-releases/{release_uid}/")
    assert captured["payload"]["json"] == {"revision_retention_count": 6}


@pytest.mark.parametrize("invalid_value", [None, 0, -1, True, 1.5, "3"])
def test_resource_release_patch_rejects_invalid_revision_retention_count(
    monkeypatch, invalid_value
):
    monkeypatch.setattr(
        base_mod,
        "make_request",
        lambda **kwargs: pytest.fail("invalid retention must fail before the request"),
    )

    with pytest.raises(
        ValueError,
        match="revision_retention_count must be a positive integer",
    ):
        models_helpers_mod.ResourceRelease.patch_by_uid(
            "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed",
            revision_retention_count=invalid_value,
        )


def _resource_release_pipeline_payload(*, running_step: str | None = None):
    keys = (
        "resolve_revision",
        "validate_deployment",
        "resolve_resource",
        "build_project_image",
        "deploy_runtime",
        "verify_runtime_readiness",
    )
    current_index = keys.index(running_step) if running_step else None
    steps = []
    for index, key in enumerate(keys, start=1):
        if current_index is None or index - 1 < current_index:
            step_state = "succeeded"
            outcome = "completed"
        elif index - 1 == current_index:
            step_state = "running"
            outcome = ""
        else:
            step_state = "pending"
            outcome = ""
        steps.append(
            {
                "uid": f"00000000-0000-4000-8000-{index:012d}",
                "sequence": index,
                "key": key,
                "name": key.replace("_", " ").title(),
                "kind": "orchestration",
                "required": True,
                "state": step_state,
                "outcome": outcome,
                "artifact_context": {},
                "started_at": None,
                "finished_at": None,
                "error": None,
            }
        )
    return {
        "key": "resource_release.fastapi.build_and_deploy",
        "version": 1,
        "current_step_key": running_step,
        "steps": steps,
    }


def test_resource_release_deploy_current_version_posts_detail_action(monkeypatch):
    captured = {}
    release_uid = "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed"
    run_uid = "11111111-1111-4111-8111-111111111111"
    release = models_helpers_mod.ResourceRelease(
        uid=release_uid,
        release_kind="fastapi",
        automatic_deployment=True,
    )

    class FakeResponse:
        status_code = 202
        content = b'{"uid": "run"}'

        @staticmethod
        def json():
            return {
                "uid": run_uid,
                "target_type": "resource_release",
                "target": {"uid": release_uid, "name": "analytics-123", "kind": "fastapi"},
                "project_branch_uid": "33333333-3333-4333-8333-333333333333",
                "operation": "build_and_deploy",
                "source": "manual",
                "commit_sha": "a" * 40,
                "configuration_revision": 4,
                "state": "succeeded",
                "outcome": "deployed",
                "pipeline": _resource_release_pipeline_payload(),
                "created_at": "2026-07-19T12:00:00Z",
                "started_at": "2026-07-19T12:00:01Z",
                "finished_at": "2026-07-19T12:00:05Z",
                "revision_context": {},
                "trigger_context": {},
                "artifact_context": {},
                "cleanup_context": {},
                "result": {},
                "builder_image": "",
                "builder_runtime": "",
                "logs": {
                    "state": "available",
                    "url": f"/api/v1/deployment-runs/{run_uid}/logs/",
                    "retention_expires_at": None,
                },
                "error": None,
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["r_type"] = r_type
        captured["url"] = url
        captured["payload"] = payload
        captured["timeout"] = time_out
        return FakeResponse()

    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    run = release.deploy_current_version(timeout=9)

    assert run.uid == run_uid
    assert isinstance(run, models_helpers_mod.DeploymentRun)
    assert run.target.uid == release_uid
    assert run.state == "succeeded"
    assert run.outcome == "deployed"
    assert run.pipeline.current_step_key is None
    assert len(run.pipeline.steps) == 6
    assert run.builder_image == ""
    assert run.builder_runtime == ""
    assert run.logs.state == "available"
    assert run.error is None
    assert captured == {
        "r_type": "POST",
        "url": (
            f"{models_helpers_mod.ResourceRelease.get_object_url()}/{release_uid}/"
            "deploy-current-version/"
        ),
        "payload": {},
        "timeout": 9,
    }


def test_deployment_run_collection_and_detail_use_unified_resource_release_contract(monkeypatch):
    captured = []
    run_uid = "11111111-1111-4111-8111-111111111111"
    release_uid = "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed"
    response_payload = {
        "uid": run_uid,
        "target_type": "resource_release",
        "target": {"uid": release_uid, "name": "analytics-123", "kind": "fastapi"},
        "project_branch_uid": PROJECT_BRANCH_UID,
        "operation": "build_and_deploy",
        "source": "repository_event",
        "commit_sha": "a" * 40,
        "configuration_revision": 4,
        "state": "succeeded",
        "outcome": "deployed",
        "pipeline": _resource_release_pipeline_payload(),
        "created_at": "2026-07-19T12:00:00Z",
        "started_at": "2026-07-19T12:00:01Z",
        "finished_at": "2026-07-19T12:00:05Z",
        "revision_context": {},
        "trigger_context": {},
        "artifact_context": {},
        "cleanup_context": {},
        "result": {},
        "builder_image": "",
        "builder_runtime": "",
        "logs": {
            "state": "available",
            "url": f"/api/v1/deployment-runs/{run_uid}/logs/",
            "retention_expires_at": None,
        },
        "error": None,
    }

    collection_payload = {
        key: value
        for key, value in response_payload.items()
        if key
        not in {
            "revision_context",
            "trigger_context",
            "artifact_context",
            "cleanup_context",
            "result",
            "builder_image",
            "builder_runtime",
        }
    }

    class FakeResponse:
        status_code = 200

        def __init__(self, payload):
            self.payload = payload

        def json(self):
            return self.payload

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured.append({"r_type": r_type, "url": url, "payload": payload})
        if url.rstrip("/") == models_helpers_mod.DeploymentRun.get_object_url():
            return FakeResponse({"results": [dict(collection_payload)], "next": None})
        return FakeResponse(dict(response_payload))

    monkeypatch.setattr(
        models_helpers_mod.DeploymentRun,
        "build_session",
        classmethod(lambda cls: object()),
    )
    monkeypatch.setattr(base_mod, "make_request", _fake_make_request)

    runs = models_helpers_mod.DeploymentRun.filter(
        target_type="resource_release",
        target_uid=release_uid,
    )
    detail = models_helpers_mod.DeploymentRun.get(pk=run_uid)

    assert len(runs) == 1
    assert runs[0].target.uid == release_uid
    assert detail.state == "succeeded"
    assert detail.outcome == "deployed"
    assert len(runs[0].pipeline.steps) == 6
    assert detail.pipeline.current_step_key is None
    assert detail.builder_image == ""
    assert detail.builder_runtime == ""
    assert detail.logs.state == "available"
    assert detail.error is None
    assert captured[0]["payload"] == {
        "params": {
            "project_branch_uid": PROJECT_BRANCH_UID,
            "target_type": "resource_release",
            "target_uid": release_uid,
        }
    }
    assert captured[1]["url"].endswith(f"/deployment-runs/{run_uid}/")


def test_unified_deployment_run_models_and_filters(monkeypatch):
    captured = {}
    run_uid = "11111111-1111-4111-8111-111111111111"
    target_uid = "22222222-2222-4222-8222-222222222222"
    project_branch_uid = "33333333-3333-4333-8333-333333333333"
    run = models_helpers_mod.DeploymentRun.model_validate(
        {
            "uid": run_uid,
            "target_type": "resource_release",
            "target": {"uid": target_uid, "name": "Orders API", "kind": "fastapi"},
            "project_branch_uid": project_branch_uid,
            "operation": "build_and_deploy",
            "source": "repository_event",
            "commit_sha": "a" * 40,
            "configuration_revision": None,
            "state": "running",
            "outcome": "",
            "pipeline": _resource_release_pipeline_payload(running_step="build_project_image"),
            "created_at": "2026-07-19T12:00:00Z",
            "started_at": "2026-07-19T12:00:01Z",
            "finished_at": None,
            "builder_image": "us-docker.pkg.dev/platform/static-builder:latest",
            "builder_runtime": "nodejs22",
            "logs": {
                "state": "available",
                "url": f"/api/v1/deployment-runs/{run_uid}/logs/",
                "retention_expires_at": None,
            },
            "error": None,
        }
    )

    normalized = models_helpers_mod.DeploymentRun._normalize_filter_kwargs(
        {
            "project_branch_uid": f" {project_branch_uid} ",
            "target_type__in": [" resource_release ", " static_site "],
            "state__in": [" running ", " failed "],
        }
    )

    assert run.target.uid == target_uid
    assert run.pipeline.current_step_key == "build_project_image"
    assert run.pipeline.steps[3].state == "running"
    assert run.pipeline.steps[4].state == "pending"
    assert run.builder_image == "us-docker.pkg.dev/platform/static-builder:latest"
    assert run.builder_runtime == "nodejs22"
    assert normalized == {
        "project_branch_uid": project_branch_uid,
        "target_type__in": ["resource_release", "static_site"],
        "state__in": ["running", "failed"],
    }

    class FakeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "run_uid": run_uid,
                "entries": [
                    {
                        "sequence": 1,
                        "timestamp": "2026-07-19T12:00:02Z",
                        "step_uid": None,
                        "source": "orchestrator",
                        "stream": "stdout",
                        "level": "info",
                        "text": "Deployment run entered running",
                    }
                ],
                "sources": [{"source": "orchestrator", "state": "available"}],
                "next_cursor": None,
                "complete": True,
                "retention_expires_at": None,
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured.update(
            {
                "r_type": r_type,
                "url": url,
                "payload": payload,
                "timeout": time_out,
            }
        )
        return FakeResponse()

    monkeypatch.setattr(models_helpers_mod, "make_request", _fake_make_request)

    page = run.get_logs(limit=25, source="orchestrator", timeout=8)

    assert page.complete is True
    assert page.entries[0].source == "orchestrator"
    assert captured == {
        "r_type": "GET",
        "url": f"{models_helpers_mod.DeploymentRun.get_object_url()}/{run_uid}/logs/",
        "payload": {
            "params": {
                "limit": 25,
                "source": "orchestrator",
                "organization_environment_uid": ENVIRONMENT_UID,
            }
        },
        "timeout": 8,
    }


def test_agent_session_runtime_access_uses_session_uid_route(monkeypatch):
    captured = {}
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"

    class FakeResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "coding_agent_service_uid": "7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f",
                "mode": "token",
                "rpc_url": "https://7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f.coding-agent.main-sequence.app/",
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

    assert access.coding_agent_service_uid == "7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f"
    assert "coding_agent_service_id" not in agent_models_mod.AgentSessionRuntimeAccess.model_fields
    assert "coding_agent_id" not in agent_models_mod.AgentSessionRuntimeAccess.model_fields
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
        "url": f"{agent_models_mod.AgentSession.get_object_url()}/{session_uid}/resolve-runtime-access/",
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
                "coding_agent_service_uid": "7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f",
                "mode": "token",
                "rpc_url": "https://7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f.coding-agent.main-sequence.app/",
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
                "coding_agent_service_uid": "7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f",
                "mode": "token",
                "rpc_url": "https://7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f.coding-agent.main-sequence.app/",
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
        "url": f"{agent_models_mod.AgentSession.get_object_url()}/{session_uid}/resolve-runtime-access/",
        "payload": {"json": {}},
        "timeout": 15,
    }
    assert payload.message is not None
    assert payload.message["parts"] == [{"text": "I can analyze workspaces."}]
    assert captured["runtime"]["url"] == (
        "https://7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f.coding-agent.main-sequence.app/"
        "api/a2a/v1/message:send"
    )
    assert captured["runtime"]["headers"]["Content-Type"] == "application/a2a+json"
    assert captured["runtime"]["headers"]["Accept"] == "application/a2a+json"
    assert captured["runtime"]["headers"]["Authorization"] == "Bearer tok-secret"
    assert captured["runtime"]["headers"]["A2A-Extensions"] == (
        agent_models_mod.STANDARD_A2A_RESPONSE_KIND_EXTENSION_URI
    )
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
            "responseKind": "message",
        },
    }
    assert "omit_reasoning" not in captured["runtime"]["data"]


def test_agent_session_send_a2a_task_returns_typed_task(monkeypatch):
    captured = {}
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    agent_models_mod.AgentSession.clear_cached_runtime_access(session_uid)
    agent_models_mod.AgentSession.cache_runtime_access(
        session_uid,
        {
            "mode": "token",
            "rpc_url": "https://runtime.example.test/",
            "token": "tok-secret",
        },
    )

    class FakeRuntimeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "task": {
                    "kind": "task",
                    "id": "task-1",
                    "contextId": session_uid,
                    "status": {"state": "TASK_STATE_SUBMITTED"},
                    "artifacts": [],
                }
            }

    def _fake_post(url, *, headers, data, timeout):
        captured.update(url=url, headers=headers, data=data, timeout=timeout)
        return FakeRuntimeResponse()

    monkeypatch.setattr(agent_models_mod.requests, "post", _fake_post)

    result = agent_models_mod.AgentSession.send_a2a_message(
        session_uid,
        message="Run this asynchronously.",
        response_kind=agent_models_mod.A2AResponseKind.TASK,
        a2a_profile={
            "supported_response_kinds": ["message", "task"],
            "default_response_kind": "message",
        },
    )

    assert result.response_kind is agent_models_mod.A2AResponseKind.TASK
    assert result.task is not None
    assert result.task.id == "task-1"
    assert result.task.context_id == session_uid
    assert json.loads(captured["data"])["configuration"]["responseKind"] == "task"


def test_agent_a2a_profile_requires_message_default_and_support():
    with pytest.raises(ValueError, match="must include 'message'"):
        agent_models_mod.AgentA2AProfile(
            supported_response_kinds=[agent_models_mod.A2AResponseKind.TASK],
            default_response_kind=agent_models_mod.A2AResponseKind.MESSAGE,
        )

    with pytest.raises(ValueError, match="must be 'message'"):
        agent_models_mod.AgentA2AProfile(
            supported_response_kinds=[
                agent_models_mod.A2AResponseKind.MESSAGE,
                agent_models_mod.A2AResponseKind.TASK,
            ],
            default_response_kind=agent_models_mod.A2AResponseKind.TASK,
        )

    with pytest.raises(ValueError, match="extension_uri is not supported"):
        agent_models_mod.AgentA2AProfile(
            response_kind_extension_uri="https://example.test/a2a/response-kind",
        )

    with pytest.raises(ValidationError, match="Input should be 'task'"):
        agent_models_mod.A2ATask.model_validate(
            {
                "kind": "message",
                "id": "task-1",
                "contextId": "session-1",
                "status": {"state": "TASK_STATE_SUBMITTED"},
            }
        )


def test_agent_session_task_send_discovers_message_only_profile_before_runtime(monkeypatch):
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    agent_uid = "e0e75693-4110-464c-a253-e302754872c1"
    discovered = {}

    def _get_session(cls, pk=None, timeout=None, **_filters):
        discovered["session"] = (pk, timeout)
        return SimpleNamespace(agent_uid=agent_uid)

    def _get_agent(cls, pk=None, timeout=None, **_filters):
        discovered["agent"] = (pk, timeout)
        return SimpleNamespace(a2a_profile=agent_models_mod.AgentA2AProfile())

    monkeypatch.setattr(agent_models_mod.AgentSession, "get", classmethod(_get_session))
    monkeypatch.setattr(agent_models_mod.Agent, "get", classmethod(_get_agent))

    with pytest.raises(ValueError, match="is not advertised"):
        agent_models_mod.AgentSession.send_a2a_message(
            session_uid,
            message="Run this asynchronously.",
            response_kind=agent_models_mod.A2AResponseKind.TASK,
            timeout=17,
        )

    assert discovered == {
        "session": (session_uid, 17),
        "agent": (agent_uid, 17),
    }


def test_agent_session_a2a_task_helpers_get_wait_and_cancel(monkeypatch):
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    agent_models_mod.AgentSession.clear_cached_runtime_access(session_uid)
    agent_models_mod.AgentSession.cache_runtime_access(
        session_uid,
        {
            "mode": "token",
            "rpc_url": "https://runtime.example.test/",
            "token": "tok-secret",
        },
    )
    states = iter(["TASK_STATE_WORKING", "TASK_STATE_COMPLETED", "TASK_STATE_CANCELED"])
    calls = []

    class FakeTaskResponse:
        status_code = 200

        def __init__(self, state):
            self.state = state

        def json(self):
            return {
                "task": {
                    "kind": "task",
                    "id": "task-1",
                    "contextId": session_uid,
                    "status": {"state": self.state},
                    "artifacts": [],
                }
            }

    def _fake_request(method, url, *, headers, timeout):
        calls.append((method, url, headers, timeout))
        return FakeTaskResponse(next(states))

    monkeypatch.setattr(agent_models_mod.requests, "request", _fake_request)
    monkeypatch.setattr(agent_models_mod.time, "sleep", lambda _seconds: None)

    completed = agent_models_mod.AgentSession.wait_for_a2a_task(
        session_uid,
        task_id="task-1",
        poll_interval_seconds=0.01,
    )
    canceled = agent_models_mod.AgentSession.cancel_a2a_task(
        session_uid,
        task_id="task-1",
    )

    assert completed.status.state == "TASK_STATE_COMPLETED"
    assert canceled.status.state == "TASK_STATE_CANCELED"
    assert [method for method, *_rest in calls] == ["GET", "GET", "POST"]
    assert calls[-1][1].endswith("/api/a2a/v1/tasks/task-1:cancel")


def test_agent_session_send_a2a_message_reports_unavailable_runtime_without_post(
    monkeypatch,
):
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    agent_models_mod.AgentSession.clear_cached_runtime_access(session_uid)

    class FakeResolveResponse:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {
                "coding_agent_service_uid": None,
                "mode": "unavailable",
                "rpc_url": None,
                "token": None,
                "is_ready": False,
                "detail": "Runtime reconciliation is queued.",
            }

    monkeypatch.setattr(
        agent_models_mod,
        "make_request",
        lambda **kwargs: FakeResolveResponse(),
    )

    def _unexpected_post(*args, **kwargs):
        pytest.fail("A2A HTTP request must not run for unavailable runtime access")

    monkeypatch.setattr(agent_models_mod.requests, "post", _unexpected_post)

    with pytest.raises(
        agent_models_mod.ApiError,
        match=("Coding-agent runtime access is unavailable. Runtime reconciliation is queued."),
    ):
        agent_models_mod.AgentSession.send_a2a_message(
            session_uid,
            message="Do not send this yet.",
        )


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
                "coding_agent_service_uid": "7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f",
                "mode": "token",
                "rpc_url": "https://7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f.coding-agent.main-sequence.app/",
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
                "coding_agent_service_uid": "7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f",
                "mode": "token",
                "rpc_url": "https://7bd86be3-d11a-4ad1-8fe2-9260ccdbca7f.coding-agent.main-sequence.app/",
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

    assert payload.message is not None
    assert payload.message["parts"] == [{"text": "Done."}]
    assert captured["resolve_count"] == 2
    assert captured["runtime_bodies"][0] == captured["runtime_bodies"][1]
    request_body = json.loads(captured["runtime_bodies"][0])
    assert request_body["message"]["messageId"] == "msg-client-retry-1"
    assert captured["tokens"] == ["Bearer tok-secret-1", "Bearer tok-secret-2"]


def test_agent_respond_uses_agent_scoped_sessionless_contract(monkeypatch):
    captured = {}
    agent_uid = "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    agent = agent_models_mod.Agent(
        uid=agent_uid,
        name="Research Copilot",
        agent_type="custom",
        description="Research assistant.",
        agent_card=None,
        llm_provider="openai",
        llm_model="gpt-5.4",
        llm_thinking="medium",
        repository_branch=None,
        organization_environment_uid=None,
        organization_environment_name=None,
    )

    class FakeAccessResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "mode": "token",
                "rpc_url": "https://runtime.example.test",
                "token": "runtime-token",
                "runtime_paths": {
                    "responses": f"/api/agents/{agent_uid}/responses",
                },
            }

    class FakeRuntimeResponse:
        status_code = 200

        @staticmethod
        def json():
            return {
                "message": {
                    "kind": "message",
                    "messageId": "msg-output",
                    "role": "ROLE_AGENT",
                    "parts": [{"data": {"answer": 42}, "mediaType": "application/json"}],
                }
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured["access"] = {
            "r_type": r_type,
            "url": url,
            "payload": payload,
            "timeout": time_out,
        }
        return FakeAccessResponse()

    def _fake_post(url, *, headers, data, timeout, stream):
        captured["runtime"] = {
            "url": url,
            "headers": headers,
            "body": json.loads(data),
            "timeout": timeout,
            "stream": stream,
        }
        return FakeRuntimeResponse()

    monkeypatch.setattr(agent_models_mod, "make_request", _fake_make_request)
    monkeypatch.setattr(agent_models_mod.requests, "post", _fake_post)

    result = agent.respond(
        message="Return an answer.",
        message_id="msg-input",
        strict_dictionary=True,
        provider="anthropic",
        model="claude-sonnet-4-5",
        thinking="high",
        max_output_tokens=512,
        timeout_seconds=30,
        timeout=17,
    )

    assert result.message is not None
    assert result.message["parts"][0]["data"] == {"answer": 42}
    assert captured["access"] == {
        "r_type": "POST",
        "url": f"{agent.get_detail_url()}resolve-runtime-access/",
        "payload": {"json": {}},
        "timeout": 17,
    }
    runtime = captured["runtime"]
    assert runtime["url"] == f"https://runtime.example.test/api/agents/{agent_uid}/responses"
    assert runtime["headers"]["Content-Type"] == "application/a2a+json"
    assert runtime["stream"] is False
    body = runtime["body"]
    assert body["message"] == {
        "messageId": "msg-input",
        "role": "ROLE_USER",
        "parts": [{"text": "Return an answer."}],
    }
    assert "contextId" not in body["message"]
    assert body["metadata"][agent_models_mod.STANDARD_AGENT_INFERENCE_METADATA_KEY] == {
        "provider": "anthropic",
        "model": "claude-sonnet-4-5",
        "thinking": "high",
        "maxOutputTokens": 512,
        "timeoutSeconds": 30,
    }


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
        repository_branch=None,
        organization_environment_uid=None,
        organization_environment_name=None,
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
            f"{agent_models_mod.Agent.get_object_url()}/{agent_uid}/sessions/get-or-create-session/"
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


def test_agent_get_or_create_session_parses_reused_handle_capabilities(monkeypatch):
    agent_uid = "e0e75693-4110-464c-93e0-82c7fd9c9a23"
    session_uid = "3f1cc452-43ec-49cb-b2ba-87dbac164d29"
    handle_unique_id = "tutorial-evaluation-development"
    agent = agent_models_mod.Agent(
        uid=agent_uid,
        name="Research Copilot",
        agent_type="custom",
        description="Research assistant.",
        agent_card=None,
        llm_provider="openai",
        llm_model="gpt-5.4",
        llm_thinking="medium",
        repository_branch=None,
        organization_environment_uid=None,
        organization_environment_name=None,
    )
    captured = {}

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
                "harness": "tau",
                "harness_protocol": "tau-session-v1",
                "harness_version": "0.3.1",
                "name": "Tutorial evaluation",
                "status": "running",
                "llm_provider": "openai",
                "llm_model": "gpt-5.4",
                "llm_thinking": "medium",
                "bound_handle": {
                    "uid": "44444444-4444-4444-8444-444444444444",
                    "handle_unique_id": handle_unique_id,
                    "owner_user_uid": "fdf409f7-d16f-4f71-986b-9057db6c7eca",
                    "is_locked": False,
                },
                "observability": {
                    "application_logs_url": f"/api/v1/agent-sessions/{session_uid}/logs/",
                    "resource_usage_url": None,
                    "deployment_runs_url": None,
                    "sessions_url": None,
                },
                "runtime_capabilities": {
                    "tau_runtime_bootstrap": "v1",
                    "tau_resume_snapshot": "v1",
                    "tau_activity_sequence": "v1",
                    "tau_turn_commit": "v1",
                },
            }

    def _fake_make_request(*, s, loaders, r_type, url, payload, time_out=None):
        captured.update({"r_type": r_type, "url": url, "payload": payload, "timeout": time_out})
        return FakeResponse()

    monkeypatch.setattr(agent_models_mod, "make_request", _fake_make_request)

    session = agent.get_or_create_session(handle_unique_id=handle_unique_id, timeout=13)

    assert session.uid == session_uid
    assert session.bound_handle["handle_unique_id"] == handle_unique_id
    assert session.runtime_capabilities["tau_runtime_bootstrap"] == "v1"
    assert session.observability.application_logs_url.endswith(f"/{session_uid}/logs/")
    assert captured["payload"] == {"json": {"handle_unique_id": handle_unique_id}}
    assert captured["timeout"] == 13


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
        repository_branch=None,
        organization_environment_uid=None,
        organization_environment_name=None,
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
            f"{agent_models_mod.Agent.get_object_url()}/{agent_uid}/sessions/get-or-create-session/"
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
