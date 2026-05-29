from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pandas as pd

from mainsequence.client import models_metatables
from mainsequence.meta_tables.data_nodes import run_operations


class _Logger:
    def debug(self, *_args, **_kwargs):
        return None

    def info(self, *_args, **_kwargs):
        return None


class _PersistManager:
    def __init__(self, data_node_update):
        self.data_node_update = data_node_update
        self.storage_table = data_node_update.data_node_storage
        self.storage_metadata = data_node_update.data_node_storage
        self.patch_calls = []

    def synchronize_data_node_update(self, data_node_update=None):
        self.synchronized_with = data_node_update

    def set_data_node_update_lazy(self, include_relations_detail=False):
        self.include_relations_detail = include_relations_detail


class _Scheduler:
    uid = "scheduler-uid"
    name = "scheduler"

    def __init__(self):
        self.started = False
        self.active_tree_calls = []

    def start_heart_beat(self):
        self.started = True

    def in_active_tree_connect(self, update_node_uids):
        self.active_tree_calls.append(update_node_uids)


def _update(uid: str):
    return SimpleNamespace(
        uid=uid,
        update_hash=f"{uid}-hash",
        data_node_storage=SimpleNamespace(storage_hash=f"{uid}-storage"),
        NODE_TYPE="local_time_serie",
        run_configuration={"uid": uid},
        ogm_dependencies_linked=True,
        patch=lambda **_kwargs: None,
    )


def _time_series(update=None, depth_df=None, dependencies_df=None):
    update = update or _update("head-uid")
    return SimpleNamespace(
        data_node_update=update,
        local_persist_manager=_PersistManager(update),
        logger=_Logger(),
        depth_df=depth_df if depth_df is not None else pd.DataFrame(),
        dependencies_df=dependencies_df,
        _scheduler_tree_connected=False,
        scheduler=None,
        update_details_tree=None,
        DATA_NODE_UPDATE_CLASS=None,
        set_relation_tree=lambda: None,
        set_dependencies_df=lambda: None,
        dependencies=lambda: {},
    )


def test_update_runner_scheduler_assignment_uses_update_uid(monkeypatch):
    captured = {}
    scheduler = _Scheduler()

    def _build_and_assign_to_update_nodes(**kwargs):
        captured.update(kwargs)
        return scheduler

    monkeypatch.setattr(
        run_operations.ms_client.Scheduler,
        "build_and_assign_to_update_nodes",
        _build_and_assign_to_update_nodes,
    )

    runner = run_operations.UpdateRunner(_time_series(), debug_mode=True)
    runner._setup_scheduler()

    assert captured["scheduler_name"] == "DEBUG_head-uid"
    assert captured["update_node_uids"] == ["head-uid"]
    assert "update_nodes_ids" not in captured
    assert scheduler.started is True


def test_update_runner_pre_update_uses_uid_payloads_and_maps():
    depth_df = pd.DataFrame(
        [
            {
                "update_node_uid": "dep-uid",
                "node_type": "local_time_serie",
                "update_hash": "dep-hash",
                "remote_table_hash_id": "dep-storage",
            }
        ]
    )
    scheduler = _Scheduler()
    captured = {}

    class _UpdateClass:
        @staticmethod
        def get_data_nodes_and_set_updates(**kwargs):
            captured.update(kwargs)
            return SimpleNamespace(
                state_data={"state": "ok"},
                data_node_updates=[_update("dep-uid"), _update("head-uid")],
            )

    ts = _time_series(depth_df=depth_df, dependencies_df=pd.DataFrame())
    ts.DATA_NODE_UPDATE_CLASS = _UpdateClass
    runner = run_operations.UpdateRunner(ts)
    runner.scheduler = scheduler

    data_node_updates, state_data = runner._pre_update_routines()

    assert scheduler.active_tree_calls == [["dep-uid", "head-uid"]]
    assert captured["update_nodes"] == [
        {
            "uid": "dep-uid",
            "node_type": "local_time_serie",
            "update_hash": "dep-hash",
            "remote_table_hash_id": "dep-storage",
        },
        {
            "uid": "head-uid",
            "update_hash": "head-uid-hash",
            "remote_table_hash_id": "head-uid-storage",
            "node_type": "local_time_serie",
        },
    ]
    assert captured["update_details_kwargs"]["active_update_scheduler_uid"] == "scheduler-uid"
    assert "active_update_scheduler_id" not in captured["update_details_kwargs"]
    assert list(data_node_updates) == ["dep-uid", "head-uid"]
    assert ts.update_details_tree == {
        "dep-uid": {"uid": "dep-uid"},
        "head-uid": {"uid": "head-uid"},
    }
    assert state_data == {"state": "ok"}


def test_update_runner_verify_tree_uses_dependency_uids(monkeypatch):
    dependency_update = _update("dep-uid")
    head_update = _update("head-uid")
    patch_calls = []
    head_update.patch = lambda **kwargs: patch_calls.append(kwargs)
    depth_df = pd.DataFrame(
        [
            {
                "update_node_uid": "dep-uid",
                "source_class_name": "ConcreteDataNode",
            }
        ]
    )
    ts = _time_series(
        update=head_update,
        depth_df=depth_df,
        dependencies_df=depth_df.copy(),
    )
    ts.dependencies = lambda: {
        "dependency": SimpleNamespace(is_api=False, data_node_update=dependency_update)
    }
    runner = run_operations.UpdateRunner(ts, debug_mode=True)
    executed = {}
    monkeypatch.setattr(
        runner,
        "_execute_sequential_debug_update",
        lambda dependencies_df, update_map: executed.update(
            {
                "update_node_uids": dependencies_df["update_node_uid"].tolist(),
                "update_map": update_map,
            }
        ),
    )

    runner._verify_tree_is_updated(reuse_declared_dependency_instances=False)

    assert patch_calls == []
    assert executed["update_node_uids"] == ["dep-uid"]


def test_data_node_update_dependency_priority_normalizes_uid_columns(monkeypatch):
    class _Response:
        status_code = 200

        @staticmethod
        def json():
            return [
                {
                    "local_time_serie_uid": "dep-uid",
                    "node_type": "local_time_serie",
                    "update_hash": "dep-hash",
                }
            ]

    monkeypatch.setattr(models_metatables, "make_request", lambda **_kwargs: _Response())
    update = models_metatables.DataNodeUpdate(
        uid="head-uid",
        update_hash="head-hash",
        build_configuration={},
        data_node_storage=uuid4(),
    )

    depth_df = update.get_all_dependencies_update_priority()

    assert depth_df["update_node_uid"].tolist() == ["dep-uid"]
    assert "local_time_serie_uid" not in depth_df.columns
