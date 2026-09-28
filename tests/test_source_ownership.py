"""The application owns database resources; platform Secrets remain available."""
import importlib.util

from mainsequence import client
from mainsequence.client.base import BaseObjectOrm
from mainsequence.client.models_foundry import CodeRepositoryBranch, Secret


def test_sdk_has_no_application_database_api():
    assert not hasattr(client, "DataSource")
    assert importlib.util.find_spec("mainsequence.client.models_data_sources") is None
    assert "DataSource" not in BaseObjectOrm.END_POINTS
    assert "metatables_data_source_uid" not in CodeRepositoryBranch.model_fields
    assert callable(Secret.get_by_uid)
