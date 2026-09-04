from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID, uuid4

import pandas as pd
import pytest

import mainsequence.meta_tables.time_index_table_updates.runner as runner_module
from mainsequence.client import metatables as models_metatables


def _build_configuration() -> dict[str, object]:
    return {
        "configuration_schema_version": 2,
        "table_updater_class_import_path": {
            "module": "tests.test_update_runner_uid_runtime",
            "qualname": "ExampleUpdater",
        },
    }


class _Logger:
    def debug(self, *_args, **_kwargs):
        return None

    def info(self, *_args, **_kwargs):
        return None

    def warning(self, *_args, **_kwargs):
        return None

    def exception(self, *_args, **_kwargs):
        return None


class _TimeIndexTableUpdateManager:
    def __init__(self, table_update):
        self.table_update = table_update
        self.output_table = table_update.output_table
        self.output_metadata = table_update.output_table
        self.patch_calls = []

    def synchronize_table_update(self, table_update=None):
        self.synchronized_with = table_update

    def set_table_update_lazy(self, include_relations_detail=False):
        self.include_relations_detail = include_relations_detail

    def get_update_statistics_for_table(self):
        return {"updated": self.table_update.uid}


def _update(uid: str):
    return SimpleNamespace(
        uid=uid,
        update_hash=f"{uid}-hash",
        output_table=SimpleNamespace(physical_table_name=f"{uid}-storage"),
        NODE_TYPE="time_index_table_update",
        ogm_dependencies_linked=True,
        patch=lambda **_kwargs: None,
    )


def _updater(update=None, depth_df=None, dependencies_df=None):
    update = update or _update("head-uid")
    return SimpleNamespace(
        table_update=update,
        update_manager=_TimeIndexTableUpdateManager(update),
        logger=_Logger(),
        depth_df=depth_df if depth_df is not None else pd.DataFrame(),
        dependencies_df=dependencies_df,
        TABLE_UPDATE_CLASS=None,
        set_relation_tree=lambda **_kwargs: None,
        set_dependencies_df=lambda: None,
        dependencies=lambda: {},
    )


def test_update_runner_generates_execution_trace_id():
    runner = runner_module.UpdateRunner(_updater())

    assert str(UUID(runner.execution_trace_id)) == runner.execution_trace_id


def test_update_runner_pre_update_uses_uid_payloads_and_maps():
    depth_df = pd.DataFrame(
        [
            {
                "update_node_uid": "dep-uid",
                "node_type": "time_index_table_update",
                "update_hash": "dep-hash",
                "physical_table_name": "dep-storage",
            }
        ]
    )
    captured = {}

    class _UpdateClass:
        @staticmethod
        def get_table_updates_and_set_updates(**kwargs):
            captured.update(kwargs)
            return SimpleNamespace(
                state_data={"state": "ok"},
                table_updates=[_update("dep-uid"), _update("head-uid")],
            )

    updater = _updater(depth_df=depth_df, dependencies_df=pd.DataFrame())
    relation_tree_calls = []
    updater.set_relation_tree = lambda **kwargs: relation_tree_calls.append(kwargs)
    updater.TABLE_UPDATE_CLASS = _UpdateClass
    runner = runner_module.UpdateRunner(updater)

    table_updates, state_data = runner._pre_update_routines()

    assert relation_tree_calls == [{}]
    assert captured["update_nodes"] == [
        {
            "uid": "dep-uid",
            "node_type": "time_index_table_update",
            "update_hash": "dep-hash",
            "physical_table_name": "dep-storage",
        },
        {
            "uid": "head-uid",
            "update_hash": "head-uid-hash",
            "physical_table_name": "head-uid-storage",
            "node_type": "time_index_table_update",
        },
    ]
    assert captured["update_details_kwargs"] == {
        "error_on_last_update": False,
        "active_update_status": "Q",
    }
    assert list(table_updates) == ["dep-uid", "head-uid"]
    assert state_data == {"state": "ok"}


def test_update_runner_always_executes_an_admitted_update(monkeypatch):
    updater = _updater()
    start_calls = []
    end_calls = []
    local_update_calls = []
    updater.update_manager.table_update.set_start_of_execution = lambda **kwargs: (
        start_calls.append(kwargs)
        or SimpleNamespace(uid="run-uid", update_statistics=None)
    )
    updater.update_manager.table_update.set_end_of_execution = (
        lambda **kwargs: end_calls.append(kwargs)
    )
    updater.run_post_update_routines = lambda **_kwargs: None

    runner = runner_module.UpdateRunner(updater, execution_trace_id="trace-1")
    monkeypatch.setattr(
        runner,
        "_update_local",
        lambda **kwargs: local_update_calls.append(kwargs) or "updated",
    )

    error, result = runner._start_update(override_update_stats={"state": "current"})

    assert error is False
    assert result == "updated"
    assert start_calls == [{"trace_id": "trace-1"}]
    assert len(local_update_calls) == 1
    assert local_update_calls[0]["table_update_run"].uid == "run-uid"
    assert end_calls == [
        {"table_update_run_uid": "run-uid", "error_on_update": False}
    ]


def test_update_runner_verify_tree_uses_dependency_uids(monkeypatch):
    dependency_update = _update("dep-uid")
    head_update = _update("head-uid")
    patch_calls = []
    head_update.patch = lambda **kwargs: patch_calls.append(kwargs)
    depth_df = pd.DataFrame(
        [
            {
                "update_node_uid": "dep-uid",
                "source_class_name": "ConcreteTimeIndexTableUpdater",
            }
        ]
    )
    updater = _updater(
        update=head_update,
        depth_df=depth_df,
        dependencies_df=depth_df.copy(),
    )
    dependency = SimpleNamespace(
        dependency_kind="table_update",
        table_update=dependency_update,
        update_manager=_TimeIndexTableUpdateManager(dependency_update),
        dependencies=lambda: {},
    )
    updater.dependencies = lambda: {"dependency": dependency}
    runner = runner_module.UpdateRunner(updater)
    executed = {}
    monkeypatch.setattr(
        runner,
        "_execute_dependencies_sequentially",
        lambda dependencies_df, update_map: executed.update(
            {
                "update_node_uids": dependencies_df["update_node_uid"].tolist(),
                "update_map": update_map,
            }
        ),
    )

    runner._verify_tree_is_updated()

    assert patch_calls == []
    assert executed["update_node_uids"] == ["dep-uid"]
    assert list(executed["update_map"]) == ["dep-uid"]


def test_update_runner_verify_tree_self_heals_stale_backend_dependency(monkeypatch):
    dependency_update = _update("dep-uid")
    head_update = _update("head-uid")
    depth_df = pd.DataFrame(
        [
            {
                "update_node_uid": "dep-uid",
                "source_class_name": "ConcreteTimeIndexTableUpdater",
            },
            {
                "update_node_uid": "stale-dep-uid",
                "source_class_name": "OldTimeIndexTableUpdater",
            },
        ]
    )
    updater = _updater(
        update=head_update,
        depth_df=depth_df,
        dependencies_df=depth_df.copy(),
    )
    dependency = SimpleNamespace(
        dependency_kind="table_update",
        table_update=dependency_update,
        update_manager=_TimeIndexTableUpdateManager(dependency_update),
        dependencies=lambda: {},
    )
    updater.dependencies = lambda: {"dependency": dependency}
    runner = runner_module.UpdateRunner(updater)
    relation_tree_calls = []
    updater.set_relation_tree = lambda **kwargs: relation_tree_calls.append(kwargs)
    updater.set_dependencies_df = lambda: setattr(
        updater,
        "dependencies_df",
        pd.DataFrame(
            [
                {
                    "update_node_uid": "dep-uid",
                    "source_class_name": "ConcreteTimeIndexTableUpdater",
                    "update_priority": 0,
                    "number_of_upstreams": 0,
                }
            ]
        ),
    )
    executed = {}
    monkeypatch.setattr(
        runner,
        "_execute_dependencies_sequentially",
        lambda dependencies_df, update_map: executed.update(
            {
                "update_node_uids": dependencies_df["update_node_uid"].tolist(),
                "update_map": update_map,
            }
        ),
    )

    runner._verify_tree_is_updated()

    assert relation_tree_calls == [{"force_rebuild": True}]
    assert executed["update_node_uids"] == ["dep-uid"]
    assert list(executed["update_map"]) == ["dep-uid"]


def test_update_runner_pre_update_self_heals_before_batch_payload():
    dependency_update = _update("dep-uid")
    head_update = _update("head-uid")
    stale_depth_df = pd.DataFrame(
        [
            {
                "update_node_uid": "dep-uid",
                "node_type": "time_index_table_update",
                "update_hash": "dep-hash",
                "physical_table_name": "dep-storage",
            },
            {
                "update_node_uid": "stale-dep-uid",
                "node_type": "time_index_table_update",
                "update_hash": "stale-dep-hash",
                "physical_table_name": "stale-storage",
            },
        ]
    )
    current_depth_df = stale_depth_df.iloc[[0]].copy()
    captured = {}

    class _UpdateClass:
        @staticmethod
        def get_table_updates_and_set_updates(**kwargs):
            captured.update(kwargs)
            return SimpleNamespace(
                state_data={"state": "ok"},
                table_updates=[_update("dep-uid"), _update("head-uid")],
            )

    updater = _updater(
        update=head_update,
        depth_df=stale_depth_df,
        dependencies_df=stale_depth_df.copy(),
    )
    dependency = SimpleNamespace(
        dependency_kind="table_update",
        table_update=dependency_update,
        update_manager=_TimeIndexTableUpdateManager(dependency_update),
        dependencies=lambda: {},
    )
    updater.dependencies = lambda: {"dependency": dependency}
    relation_tree_calls = []
    updater.set_relation_tree = lambda **kwargs: relation_tree_calls.append(kwargs)
    updater.set_dependencies_df = lambda: (
        setattr(updater, "depth_df", current_depth_df),
        setattr(updater, "dependencies_df", current_depth_df.copy()),
    )
    updater.TABLE_UPDATE_CLASS = _UpdateClass
    runner = runner_module.UpdateRunner(updater)

    runner._pre_update_routines()

    assert relation_tree_calls == [{}, {"force_rebuild": True}]
    assert [node["uid"] for node in captured["update_nodes"]] == ["dep-uid", "head-uid"]


def test_update_runner_verify_tree_rejects_persistent_stale_backend_dependency():
    dependency_update = _update("dep-uid")
    head_update = _update("head-uid")
    depth_df = pd.DataFrame(
        [
            {
                "update_node_uid": "dep-uid",
                "source_class_name": "ConcreteTimeIndexTableUpdater",
            },
            {
                "update_node_uid": "stale-dep-uid",
                "source_class_name": "OldTimeIndexTableUpdater",
            },
        ]
    )
    updater = _updater(
        update=head_update,
        depth_df=depth_df,
        dependencies_df=depth_df.copy(),
    )
    dependency = SimpleNamespace(
        dependency_kind="table_update",
        table_update=dependency_update,
        update_manager=_TimeIndexTableUpdateManager(dependency_update),
        dependencies=lambda: {},
    )
    updater.dependencies = lambda: {"dependency": dependency}
    updater.set_dependencies_df = lambda: setattr(updater, "dependencies_df", depth_df.copy())
    runner = runner_module.UpdateRunner(updater)

    try:
        runner._verify_tree_is_updated()
    except runner_module.DependencyUpdateError as exc:
        message = str(exc)
        assert "out of sync" in message
        assert "stale-dep-uid" in message
        assert "already cleared and rebuilt" in message
        assert "refresh_dependency_tree" not in message
    else:
        raise AssertionError("Expected DependencyUpdateError")


def test_sequential_dependency_update_uses_update_node_uid_without_data_source_uid(monkeypatch):
    dependency_update = _update("dep-uid")
    dependency = SimpleNamespace(
        dependency_kind="table_update",
        table_update=dependency_update,
        update_hash="dep-hash",
        update_manager=_TimeIndexTableUpdateManager(dependency_update),
        dependencies=lambda: {},
        logger=_Logger(),
    )
    dependencies_df = pd.DataFrame(
        [
            {
                "update_node_uid": "dep-uid",
                "update_hash": "dep-hash",
                "update_priority": 0,
                "number_of_upstreams": 0,
            }
        ]
    )
    updater = _updater()
    updater.dependencies = lambda: {"dependency": dependency}
    runner = runner_module.UpdateRunner(updater, execution_trace_id="root-trace")
    started = []

    monkeypatch.setattr(
        runner_module.UpdateRunner,
        "_start_update",
        lambda self, **_kwargs: started.append(
            ("update", self.updater.table_update.uid, self.execution_trace_id)
        ),
    )

    runner._execute_dependencies_sequentially(
        dependencies_df=dependencies_df,
        update_map={"dep-uid": {"updater": dependency}},
    )

    assert started == [("update", "dep-uid", "root-trace")]


def test_sequential_dependency_update_rejects_backend_dependency_not_declared():
    dependencies_df = pd.DataFrame(
        [
            {
                "update_node_uid": "stale-dep-uid",
                "update_hash": "stale-dep-hash",
                "update_priority": 0,
                "number_of_upstreams": 0,
            }
        ]
    )
    runner = runner_module.UpdateRunner(_updater())

    try:
        runner._execute_dependencies_sequentially(
            dependencies_df=dependencies_df,
            update_map={},
        )
    except runner_module.DependencyUpdateError as exc:
        assert "not declared by the current TimeIndexTableUpdater.dependencies() graph" in str(exc)
        assert "stale-dep-uid" in str(exc)
    else:
        raise AssertionError("Expected DependencyUpdateError")


def test_table_update_dependency_priority_rejects_noncanonical_uid_columns(monkeypatch):
    class _Response:
        status_code = 200

        @staticmethod
        def json():
            return [
                {
                    "time_index_table_update_uid": "dep-uid",
                    "node_type": "time_index_table_update",
                    "update_hash": "dep-hash",
                }
            ]

    monkeypatch.setattr(models_metatables, "make_request", lambda **_kwargs: _Response())
    update = models_metatables.TimeIndexTableUpdate(
        uid="head-uid",
        update_hash="head-hash",
        build_configuration=_build_configuration(),
        output_table=uuid4(),
    )

    with pytest.raises(ValueError, match="requires canonical update_node_uid"):
        update.get_all_dependencies_update_priority()


def test_table_update_clear_dependencies_deletes_canonical_collection(monkeypatch):
    captured = {}

    class _Response:
        status_code = 200
        content = b'{"ok": true}'

        @staticmethod
        def json():
            return {"ok": True}

    def fake_make_request(**kwargs):
        captured.update(kwargs)
        return _Response()

    monkeypatch.setattr(models_metatables, "make_request", fake_make_request)
    update = models_metatables.TimeIndexTableUpdate(
        uid="head-uid",
        update_hash="head-hash",
        build_configuration=_build_configuration(),
        output_table=uuid4(),
    )

    response = update.clear_dependencies(timeout=12)

    assert response == {"ok": True}
    assert captured["r_type"] == "DELETE"
    assert captured["url"].endswith("/head-uid/dependencies/")
    assert "payload" not in captured
    assert captured["time_out"] == 12
