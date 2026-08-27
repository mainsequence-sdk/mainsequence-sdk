import pytest

import mainsequence.client.agent_runtime_models as agent_models
import mainsequence.client.metatables.core as metatable_models
import mainsequence.client.models_foundry as foundry_models
import mainsequence.client.models_helpers as helper_models

ENVIRONMENT_UID_FIELD = "organization_environment_uid"
ENVIRONMENT_NAME_FIELD = "organization_environment_name"


@pytest.mark.parametrize(
    ("serializer_names", "model", "expected_fields"),
    [
        pytest.param(
            ("AgentSerializer",),
            agent_models.Agent,
            {ENVIRONMENT_UID_FIELD, ENVIRONMENT_NAME_FIELD},
            id="agent",
        ),
        pytest.param(
            ("AgentSemanticSearchResultSerializer",),
            agent_models.AgentSemanticSearchResult,
            {ENVIRONMENT_UID_FIELD, ENVIRONMENT_NAME_FIELD},
            id="agent-semantic-search-result",
        ),
        pytest.param(
            ("JobSerializer",),
            helper_models.Job,
            {ENVIRONMENT_UID_FIELD},
            id="job",
        ),
        pytest.param(
            ("JobRunSerializer", "JobRunLightSerializer"),
            helper_models.JobRun,
            {ENVIRONMENT_UID_FIELD},
            id="job-run",
        ),
        pytest.param(
            ("ProjectBranchSerializer",),
            foundry_models.ProjectBranch,
            {ENVIRONMENT_UID_FIELD, ENVIRONMENT_NAME_FIELD},
            id="project-branch",
        ),
        pytest.param(
            ("BucketSerializer",),
            foundry_models.Bucket,
            {ENVIRONMENT_UID_FIELD, ENVIRONMENT_NAME_FIELD},
            id="bucket",
        ),
        pytest.param(
            ("ArtifactSerializer",),
            foundry_models.Artifact,
            {ENVIRONMENT_UID_FIELD, ENVIRONMENT_NAME_FIELD},
            id="artifact",
        ),
        pytest.param(
            ("SecretNameSerializer", "SecretValueSerializer"),
            foundry_models.Secret,
            {ENVIRONMENT_UID_FIELD, ENVIRONMENT_NAME_FIELD},
            id="secret",
        ),
        pytest.param(
            ("ConstantSerializer",),
            foundry_models.Constant,
            {ENVIRONMENT_UID_FIELD, ENVIRONMENT_NAME_FIELD},
            id="constant",
        ),
        pytest.param(
            ("MetaTableSerializer",),
            metatable_models.MetaTable,
            {ENVIRONMENT_UID_FIELD, ENVIRONMENT_NAME_FIELD},
            id="meta-table",
        ),
        pytest.param(
            ("MetaTableSerializer",),
            metatable_models.TimeIndexMetaTable,
            {ENVIRONMENT_UID_FIELD, ENVIRONMENT_NAME_FIELD},
            id="time-index-meta-table",
        ),
        pytest.param(
            ("SchedulerSerializer", "SchedulerMinimalSerializer"),
            metatable_models.Scheduler,
            {ENVIRONMENT_UID_FIELD},
            id="scheduler",
        ),
    ],
)
def test_backend_environment_response_contract_is_declared_by_strict_sdk_model(
    serializer_names,
    model,
    expected_fields,
):
    """Keep SDK response models aligned with ADR-0036/ADR-0037 projections."""
    assert model.model_config.get("extra") == "forbid", serializer_names
    assert expected_fields <= set(model.model_fields), serializer_names


@pytest.mark.parametrize(
    "model",
    [
        agent_models.Agent,
        agent_models.AgentSession,
        agent_models.CodingAgentService,
        helper_models.JobRun,
        helper_models.ResourceRelease,
    ],
)
def test_owner_observability_projection_is_declared_by_strict_sdk_model(model):
    assert model.model_config.get("extra") == "forbid"
    assert "observability" in model.model_fields


def test_agent_session_runtime_capabilities_projection_is_declared_and_typed():
    assert agent_models.AgentSession.model_config.get("extra") == "forbid"
    assert agent_models.AgentSession.model_fields["runtime_capabilities"].annotation == dict[str, str]
