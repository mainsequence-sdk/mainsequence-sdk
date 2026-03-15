import time

import pytest

import mainsequence.client as msc


def test_create_project():

    ds = msc.DynamicTableDataSource.filter(related_resource__status=msc.DataSource.STATUS_AVAILABLE)[0]
    img = msc.ProjectBaseImage.filter()[0]
    org = msc.GithubOrganization.filter()[0]


    project=msc.Project.filter(id=124)

    #todo:loop unitl is_initialized == True
    project = msc.Project.create(
        project_name="demo-project-002",
        data_source=ds,                 # <-- pydantic obj with .id
        default_base_image=img,         # <-- pydantic obj with .id (or None)
        github_org=org,                 # <-- pydantic obj with .id (or None)
        repository_branch="main",
        env_vars={"FOO": "bar"},
    )
    print(project)

def test_agent():

    agent=msc.Agent.filter()
    agent=agent[0]


    agent.query(prompt="Test")


def test_project_data_nodes_updates():
    project = msc.Project.filter()[0]

    updates = []
    poll_interval_s = 2
    timeout_s = 120
    deadline = time.time() + timeout_s

    while not updates and time.time() < deadline:
        updates = project.get_data_nodes_updates()
        if not updates:
            remaining = max(0, int(deadline - time.time()))
            print(
                f"No data node updates yet for project {project.id}. "
                f"Retrying in {poll_interval_s}s (remaining: {remaining}s)..."
            )
            time.sleep(poll_interval_s)

    assert updates, f"No data node updates found for project {project.id} within {timeout_s}s."

    for data_node_update in updates:
        print(data_node_update)


def test_project_image_filter():
    images = msc.ProjectImage.filter()
    if not images:
        pytest.skip("No project images available for filter test.")

    image = images[0]
    project_id = image.related_project.id if hasattr(image.related_project, "id") else image.related_project
    repo_hash = image.project_repo_hash

    filtered_by_project = msc.ProjectImage.filter(related_project__id__in=[project_id])
    assert any(img.id == image.id for img in filtered_by_project)

    filtered_by_hash = msc.ProjectImage.filter(project_repo_hash=repo_hash)
    assert any(img.id == image.id for img in filtered_by_hash)

    filtered_by_hash_in = msc.ProjectImage.filter(project_repo_hash__in=[repo_hash])
    assert any(img.id == image.id for img in filtered_by_hash_in)

    with pytest.raises(ValueError):
        msc.ProjectImage.filter(related_project=project_id)




class DummyRule:
    def __init__(self, payload: dict):
        self.payload = payload

    def model_dump(self):
        return dict(self.payload)


def test_asset_translation_table_get_or_create_creates_when_missing(monkeypatch):
    rules = [
        DummyRule(
            {
                "asset_filter": {"security_market_sector": "Crypto"},
                "markets_time_serie_unique_identifier": "binance_1d_bars",
                "target_exchange_code": "BNCE",
                "default_column_name": "close",
            }
        )
    ]
    captured = {}

    monkeypatch.setattr(msc.AssetTranslationTable, "get_or_none", lambda unique_identifier: None)

    def _create(*, unique_identifier, rules):
        captured["unique_identifier"] = unique_identifier
        captured["rules"] = rules
        return "created-table"

    monkeypatch.setattr(msc.AssetTranslationTable, "create", _create)

    result = msc.AssetTranslationTable.get_or_create("prices_translation_table_1d", rules)

    assert result == "created-table"
    assert captured == {
        "unique_identifier": "prices_translation_table_1d",
        "rules": [rule.model_dump() for rule in rules],
    }


def test_asset_translation_table_get_or_create_adds_rules_when_existing(monkeypatch):
    rules = [
        DummyRule(
            {
                "asset_filter": {"security_market_sector": "Equity"},
                "markets_time_serie_unique_identifier": "alpaca_1d_bars",
                "target_exchange_code": "US",
                "default_column_name": "close",
            }
        )
    ]

    class ExistingTable:
        def __init__(self):
            self.received_rules = None

        def add_rules(self, new_rules):
            self.received_rules = new_rules

    existing = ExistingTable()
    create_called = {"value": False}

    monkeypatch.setattr(
        msc.AssetTranslationTable,
        "get_or_none",
        lambda unique_identifier: existing,
    )

    def _create(**kwargs):
        create_called["value"] = True
        return kwargs

    monkeypatch.setattr(msc.AssetTranslationTable, "create", _create)

    result = msc.AssetTranslationTable.get_or_create("prices_translation_table_1d", rules)

    assert result is existing
    assert existing.received_rules is rules



test_asset_translation_table_get_or_create_creates_when_missing()
#
# test_project_image_filter()
# users=msc.User.filter()
#
# msc.User.get(id=users[0].id,serializer="full")
#
# a=5
