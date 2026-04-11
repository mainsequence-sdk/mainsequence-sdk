from __future__ import annotations

import datetime
from enum import Enum
from typing import Any, ClassVar

from pydantic import Field

from .base import BaseObjectOrm, BasePydanticModel, ShareableObjectMixin
from .exceptions import raise_for_response
from .models_user import UserSummary
from .utils import make_request, serialize_to_json


class AgentStatus(str, Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    ARCHIVED = "archived"


class AgentCapabilityKind(str, Enum):
    SKILL = "skill"
    PROMPT = "prompt"
    EXTENSION = "extension"


class AgentCapabilitySourceType(str, Enum):
    INLINE = "inline"
    REGISTRY = "registry"
    REPOSITORY = "repository"
    API = "api"
    EXTERNAL = "external"


class AgentSessionStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class AgentSessionStepType(str, Enum):
    SESSION_STARTED = "session_started"
    MESSAGE = "message"
    REASONING_STEP = "reasoning_step"
    TOOL_CALL = "tool_call"
    TOOL_RESULT = "tool_result"
    SKILL_INVOKED = "skill_invoked"
    PROMPT_APPLIED = "prompt_applied"
    EXTENSION_USED = "extension_used"
    ARTIFACT_CREATED = "artifact_created"
    SESSION_COMPLETED = "session_completed"
    SESSION_FAILED = "session_failed"


class AgentSessionStepActorType(str, Enum):
    USER = "user"
    AGENT = "agent"
    SYSTEM = "system"
    TOOL = "tool"
    SKILL = "skill"
    PROMPT = "prompt"
    EXTENSION = "extension"


class AgentSessionStepStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class AgentArtifactType(str, Enum):
    TEXT = "text"
    JSON = "json"
    FILE = "file"
    LINK = "link"
    IMAGE = "image"


class Agent(ShareableObjectMixin, BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/v1/agents"

    id: int | None = Field(None, description="Primary key of the agent.")
    name: str = Field(..., description="Human-readable display name for the agent inside the organization.")
    agent_unique_id: str = Field(
        ...,
        description="Client-supplied organization-scoped stable identifier for the agent. Use this for deterministic existence checks and idempotent create flows.",
    )
    description: str = Field("", description="Optional long-form description explaining what the agent is for.")
    status: AgentStatus = Field(
        AgentStatus.DRAFT,
        description="Lifecycle status of the agent definition.",
    )
    labels: list[str] = Field(
        default_factory=list,
        description="Free-form labels used to group or classify agents in the UI.",
    )
    llm_provider: str = Field(
        "",
        description="Optional default model provider for new sessions, for example openai, anthropic, or google. This is only a default on the Agent.",
    )
    llm_model: str = Field(
        "",
        description="Optional default model identifier for new sessions, for example gpt-5.4. This is only a default on the Agent.",
    )
    engine_name: str = Field(
        "",
        description="Optional default logical runtime or orchestrator name for new sessions. Use this to record the higher-level engine wrapper, workflow runtime, or agent runtime implementation that sits above the raw LLM model.",
    )
    runtime_config: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional default runtime configuration for new sessions. Store provider-specific or engine-specific settings here when they do not deserve their own top-level field.",
    )
    configuration: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional agent configuration unrelated to runtime resolution.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary metadata for UI or integration use.",
    )
    last_session_at: datetime.datetime | None = Field(
        None,
        description="Timestamp of the most recent session recorded for this agent.",
    )

    @classmethod
    def get_or_create(cls, timeout=None, **kwargs):
        data = serialize_to_json(kwargs)
        payload = {"json": data}
        url = f"{cls.get_object_url()}/get_or_create/"
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload=payload,
            time_out=timeout,
        )
        if response.status_code not in (200, 201):
            raise_for_response(response, payload=payload)
        return cls(**response.json())

    def start_new_session(self, timeout=None):
        """
        Start a new session for this agent and return the created `AgentSession`.

        This is the preferred client action when you want to begin a fresh agent
        interaction without manually constructing session rows through lower-level
        APIs. The backend resolves the runtime defaults from the agent definition
        and records the resulting session as a first-class `AgentSession`.
        """
        url = f"{self.get_detail_url()}start_new_session/"
        payload: dict[str, Any] = {}
        response = make_request(
            s=self.build_session(),
            loaders=self.LOADERS,
            r_type="POST",
            url=url,
            payload=payload,
            time_out=timeout,
        )
        if response.status_code != 201:
            raise_for_response(response, payload=payload)
        return AgentSession(**response.json())

    def get_latest_session(self, timeout=None):
        """
        Fetch the latest recorded session for this agent.

        Use this when you need the most recent `AgentSession` without listing the
        full session history. The backend returns the newest session associated
        with the agent, including resolved runtime metadata and top-level input
        and output fields.
        """
        url = f"{self.get_detail_url()}get_latest_session/"
        payload: dict[str, Any] = {}
        response = make_request(
            s=self.build_session(),
            loaders=self.LOADERS,
            r_type="POST",
            url=url,
            payload=payload,
            time_out=timeout,
        )
        if response.status_code not in (200, 201):
            raise_for_response(response, payload=payload)
        return AgentSession(**response.json())


class AgentCapability(BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/v1/capabilities"

    id: int | None = Field(None, description="Primary key of the capability.")
    name: str = Field(..., description="Human-readable capability name.")
    kind: AgentCapabilityKind = Field(
        ...,
        description="Capability kind exposed to the agent runtime.",
    )
    description: str = Field("", description="Free-form description of the capability.")
    source_type: AgentCapabilitySourceType = Field(
        AgentCapabilitySourceType.INLINE,
        description="Where the capability definition comes from.",
    )
    source_ref: str = Field(
        "",
        description="External or internal reference to the capability source.",
    )
    content_text: str = Field(
        "",
        description="Inline textual content for prompt-like or inline capabilities.",
    )
    configuration: dict[str, Any] = Field(
        default_factory=dict,
        description="Capability configuration payload.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata associated with the capability.",
    )
    is_active: bool = Field(
        True,
        description="Whether the capability is active and available for use.",
    )
    updated_at: datetime.datetime | None = Field(
        None,
        description="Last update timestamp for the capability.",
    )


class AgentCapabilityBinding(BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/v1/bindings"

    id: int | None = Field(None, description="Primary key of the binding.")
    agent: int | Agent | None = Field(
        None,
        description="Agent linked to the capability binding.",
    )
    capability: int | AgentCapability | None = Field(
        None,
        description="Capability linked to the agent binding.",
    )
    role: AgentCapabilityKind = Field(
        ...,
        description="Role under which the capability is attached to the agent.",
    )
    sort_order: int = Field(
        0,
        description="Ordering index applied when listing agent capabilities.",
    )
    enabled: bool = Field(
        True,
        description="Whether the capability binding is enabled for the agent.",
    )
    configuration: dict[str, Any] = Field(
        default_factory=dict,
        description="Binding-specific configuration payload.",
    )
    updated_at: datetime.datetime | None = Field(
        None,
        description="Last update timestamp for the capability binding.",
    )


class AgentSubagentBinding(BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/v1/subagent-bindings"

    id: int | None = Field(None, description="Primary key of the subagent binding.")
    parent_agent: int | Agent | None = Field(
        None,
        description="Parent agent allowed to spawn the child agent.",
    )
    child_agent: int | Agent | None = Field(
        None,
        description="Child agent available as a subagent.",
    )
    role: str = Field(
        "",
        description="Optional alias or orchestration role used when invoking the child agent from the parent agent.",
    )
    sort_order: int = Field(
        0,
        description="Ordering index applied when listing subagent bindings.",
    )
    enabled: bool = Field(
        True,
        description="Whether the subagent binding is enabled.",
    )
    configuration: dict[str, Any] = Field(
        default_factory=dict,
        description="Binding-specific configuration payload.",
    )
    updated_at: datetime.datetime | None = Field(
        None,
        description="Last update timestamp for the subagent binding.",
    )


class AgentSession(BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/v1/sessions"

    id: int | None = Field(None, description="Primary key of the agent session.")
    agent: int | Agent | None = Field(
        None,
        description="Agent definition used for this session.",
    )
    triggered_by_user: int | UserSummary | None = Field(
        None,
        description="User who triggered the session, as either a user id or expanded user payload.",
    )
    parent_session: int | AgentSession | None = Field(
        None,
        description="Optional parent session when this session was spawned as a subagent execution by another session.",
    )
    root_session: int | AgentSession | None = Field(
        None,
        description="Root session of the session tree. Child and descendant sessions point back to the same root for visualization and querying.",
    )
    spawned_by_step: int | AgentSessionStep | None = Field(
        None,
        description="Session step that spawned this session, when applicable. This links subagent executions back to the exact parent timeline step.",
    )
    status: AgentSessionStatus = Field(
        AgentSessionStatus.PENDING,
        description="Lifecycle status of the session.",
    )
    started_at: datetime.datetime | None = Field(
        None,
        description="Timestamp when the session started.",
    )
    ended_at: datetime.datetime | None = Field(
        None,
        description="Timestamp when the session ended.",
    )
    llm_provider: str = Field(
        ...,
        description="Resolved LLM provider actually used for this session. Unlike Agent defaults, this is intended to be the authoritative runtime record.",
    )
    llm_model: str = Field(
        ...,
        description="Resolved LLM model actually used for this session. Unlike Agent defaults, this is intended to be the authoritative runtime record.",
    )
    engine_name: str = Field(
        ...,
        description="Resolved higher-level runtime or engine actually used for this session. This records the wrapper above the raw model, such as the agent runtime, workflow engine, router, or orchestration layer.",
    )
    runtime_config_snapshot: dict[str, Any] = Field(
        default_factory=dict,
        description="Immutable runtime configuration snapshot used by this session after defaults and overrides were resolved.",
    )
    input_text: str = Field(
        "",
        description="Top-level textual input associated with the session.",
    )
    output_text: str = Field(
        "",
        description="Top-level textual output associated with the session.",
    )
    error_detail: str = Field(
        "",
        description="Error detail captured for failed sessions.",
    )
    external_session_id: str = Field(
        "",
        description="External provider session identifier, if any.",
    )
    runtime_session_id: str = Field(
        "",
        description="Runtime session identifier associated with the session.",
    )
    thread_id: str = Field(
        "",
        description="Conversation or thread identifier associated with the session.",
    )
    usage_summary: dict[str, Any] = Field(
        default_factory=dict,
        description="Usage, cost, or token accounting captured for the session.",
    )
    session_metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional session metadata.",
    )


class AgentSessionCapabilitySnapshot(BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/v1/session-capability-snapshots"

    id: int | None = Field(None, description="Primary key of the session capability snapshot.")
    agent_session: int | AgentSession | None = Field(
        None,
        description="Session associated with this capability snapshot.",
    )
    capability: int | AgentCapability | None = Field(
        None,
        description="Capability referenced by this snapshot, when still available.",
    )
    sequence: int = Field(
        0,
        description="Stable sequence number for rendering capabilities within a session.",
    )
    capability_name: str = Field(
        ...,
        description="Capability name captured at session time.",
    )
    capability_kind: AgentCapabilityKind = Field(
        ...,
        description="Capability kind captured at session time.",
    )
    role: AgentCapabilityKind = Field(
        ...,
        description="Role under which the capability was attached during the session.",
    )
    source_type: AgentCapabilitySourceType = Field(
        AgentCapabilitySourceType.INLINE,
        description="Source type captured at session time.",
    )
    source_ref: str = Field(
        "",
        description="Source reference captured at session time.",
    )
    configuration_snapshot: dict[str, Any] = Field(
        default_factory=dict,
        description="Immutable configuration snapshot taken when the session started.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata associated with the capability snapshot.",
    )


class AgentSessionStep(BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/v1/session-steps"

    id: int | None = Field(None, description="Primary key of the session step.")
    agent_session: int | AgentSession | None = Field(
        None,
        description="Session associated with this step.",
    )
    parent_step: int | AgentSessionStep | None = Field(
        None,
        description="Optional parent step for tree-structured session timelines.",
    )
    sequence: int = Field(
        0,
        description="Stable sequence number for ordering steps within a session.",
    )
    step_type: AgentSessionStepType = Field(
        ...,
        description="Categorization of the step in the session timeline.",
    )
    actor_type: AgentSessionStepActorType = Field(
        AgentSessionStepActorType.SYSTEM,
        description="Actor that originated the step.",
    )
    actor_name: str = Field(
        "",
        description="Human-readable actor name when available.",
    )
    title: str = Field(
        "",
        description="Short title shown in session visualizations.",
    )
    summary: str = Field(
        "",
        description="Longer textual summary of the step.",
    )
    status: AgentSessionStepStatus = Field(
        AgentSessionStepStatus.PENDING,
        description="Execution status of the step.",
    )
    started_at: datetime.datetime | None = Field(
        None,
        description="Timestamp when the step started.",
    )
    ended_at: datetime.datetime | None = Field(
        None,
        description="Timestamp when the step ended.",
    )
    llm_provider: str = Field(
        "",
        description="Optional step-level override for the LLM provider when a particular step runs on a different provider than the session default.",
    )
    llm_model: str = Field(
        "",
        description="Optional step-level override for the LLM model when a particular step runs on a different model than the session default.",
    )
    engine_name: str = Field(
        "",
        description="Optional step-level override for the higher-level runtime or engine when a particular step uses a different orchestration layer.",
    )
    runtime_config_override: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional step-level runtime override merged on top of the session-level runtime snapshot.",
    )
    input_payload: dict[str, Any] = Field(
        default_factory=dict,
        description="Structured input payload captured for the step.",
    )
    output_payload: dict[str, Any] = Field(
        default_factory=dict,
        description="Structured output payload captured for the step.",
    )
    error_detail: str = Field(
        "",
        description="Error detail captured for failed steps.",
    )
    external_step_id: str = Field(
        "",
        description="External provider step identifier, if any.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata associated with the session step.",
    )


class AgentSessionArtifact(BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/v1/session-artifacts"

    id: int | None = Field(None, description="Primary key of the session artifact.")
    agent_session: int | AgentSession | None = Field(
        None,
        description="Session associated with this artifact.",
    )
    step: int | AgentSessionStep | None = Field(
        None,
        description="Optional session step associated with this artifact.",
    )
    artifact_type: AgentArtifactType = Field(
        ...,
        description="Type of artifact produced during the session.",
    )
    name: str = Field(..., description="Human-readable artifact name.")
    mime_type: str = Field(
        "",
        description="Optional MIME type associated with the artifact payload.",
    )
    url: str = Field(
        "",
        description="External or internal URL for retrieving the artifact.",
    )
    content_text: str = Field(
        "",
        description="Textual artifact content, when applicable.",
    )
    content_json: dict[str, Any] | list[Any] | None = Field(
        None,
        description="Structured artifact payload, when applicable.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata associated with the artifact.",
    )


__all__ = [
    "Agent",
    "AgentArtifactType",
    "AgentCapability",
    "AgentCapabilityBinding",
    "AgentCapabilityKind",
    "AgentCapabilitySourceType",
    "AgentSession",
    "AgentSessionArtifact",
    "AgentSessionCapabilitySnapshot",
    "AgentSessionStatus",
    "AgentSessionStep",
    "AgentSessionStepActorType",
    "AgentSessionStepStatus",
    "AgentSessionStepType",
    "AgentStatus",
    "AgentSubagentBinding",
]
