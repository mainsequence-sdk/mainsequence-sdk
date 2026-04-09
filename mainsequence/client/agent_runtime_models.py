from __future__ import annotations

import datetime
from enum import Enum
from typing import Any, ClassVar

from pydantic import Field

from .base import BaseObjectOrm, BasePydanticModel, ShareableObjectMixin


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


class AgentRunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class AgentRunStepType(str, Enum):
    RUN_STARTED = "run_started"
    MESSAGE = "message"
    REASONING_STEP = "reasoning_step"
    TOOL_CALL = "tool_call"
    TOOL_RESULT = "tool_result"
    SKILL_INVOKED = "skill_invoked"
    PROMPT_APPLIED = "prompt_applied"
    EXTENSION_USED = "extension_used"
    ARTIFACT_CREATED = "artifact_created"
    RUN_COMPLETED = "run_completed"
    RUN_FAILED = "run_failed"


class AgentRunStepActorType(str, Enum):
    USER = "user"
    AGENT = "agent"
    SYSTEM = "system"
    TOOL = "tool"
    SKILL = "skill"
    PROMPT = "prompt"
    EXTENSION = "extension"


class AgentRunStepStatus(str, Enum):
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
    ENDPOINT: ClassVar[str] = "agents/agent"

    id: int | None = Field(None, description="Primary key of the agent.")
    name: str = Field(..., description="Human-readable agent name.")
    slugified_name: str = Field(
        ...,
        description="Organization-scoped slug used as the stable identifier for the agent.",
    )
    description: str = Field("", description="Free-form description of the agent.")
    status: AgentStatus = Field(
        AgentStatus.DRAFT,
        description="Lifecycle status of the agent.",
    )
    labels: list[str] = Field(
        default_factory=list,
        description="Labels associated with the agent.",
    )
    configuration: dict[str, Any] = Field(
        default_factory=dict,
        description="Arbitrary runtime configuration for the agent.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata associated with the agent.",
    )
    last_run_at: datetime.datetime | None = Field(
        None,
        description="Timestamp of the most recent run for this agent.",
    )


class AgentCapability(BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/agent-capability"

    id: int | None = Field(None, description="Primary key of the capability.")
    name: str = Field(..., description="Human-readable capability name.")
    slugified_name: str = Field(
        ...,
        description="Organization-scoped slug used as the stable identifier for the capability.",
    )
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
    ENDPOINT: ClassVar[str] = "agents/agent-capability-binding"

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


class AgentRun(BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/agent-run"

    id: int | None = Field(None, description="Primary key of the agent run.")
    agent: int | Agent | None = Field(
        None,
        description="Agent definition used for this run.",
    )
    triggered_by_user: int | UserSummary | None = Field(
        None,
        description="User who triggered the run, as either a user id or expanded user payload.",
    )
    status: AgentRunStatus = Field(
        AgentRunStatus.PENDING,
        description="Lifecycle status of the run.",
    )
    started_at: datetime.datetime | None = Field(
        None,
        description="Timestamp when the run started.",
    )
    ended_at: datetime.datetime | None = Field(
        None,
        description="Timestamp when the run ended.",
    )
    input_text: str = Field(
        "",
        description="Top-level textual input associated with the run.",
    )
    output_text: str = Field(
        "",
        description="Top-level textual output associated with the run.",
    )
    error_detail: str = Field(
        "",
        description="Error detail captured for failed runs.",
    )
    external_run_id: str = Field(
        "",
        description="External provider run identifier, if any.",
    )
    session_id: str = Field(
        "",
        description="Session identifier associated with the run.",
    )
    thread_id: str = Field(
        "",
        description="Conversation or thread identifier associated with the run.",
    )
    usage_summary: dict[str, Any] = Field(
        default_factory=dict,
        description="Usage, cost, or token accounting captured for the run.",
    )
    run_metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional run metadata.",
    )


class AgentRunCapabilitySnapshot(BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/agent-run-capability-snapshot"

    id: int | None = Field(None, description="Primary key of the run capability snapshot.")
    agent_run: int | AgentRun | None = Field(
        None,
        description="Run associated with this capability snapshot.",
    )
    capability: int | AgentCapability | None = Field(
        None,
        description="Capability referenced by this snapshot, when still available.",
    )
    sequence: int = Field(
        0,
        description="Stable sequence number for rendering capabilities within a run.",
    )
    capability_name: str = Field(
        ...,
        description="Capability name captured at run time.",
    )
    capability_kind: AgentCapabilityKind = Field(
        ...,
        description="Capability kind captured at run time.",
    )
    role: AgentCapabilityKind = Field(
        ...,
        description="Role under which the capability was attached during the run.",
    )
    source_type: AgentCapabilitySourceType = Field(
        AgentCapabilitySourceType.INLINE,
        description="Source type captured at run time.",
    )
    source_ref: str = Field(
        "",
        description="Source reference captured at run time.",
    )
    configuration_snapshot: dict[str, Any] = Field(
        default_factory=dict,
        description="Immutable configuration snapshot taken when the run started.",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata associated with the capability snapshot.",
    )


class AgentRunStep(BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/agent-run-step"

    id: int | None = Field(None, description="Primary key of the run step.")
    agent_run: int | AgentRun | None = Field(
        None,
        description="Run associated with this step.",
    )
    parent_step: int | AgentRunStep | None = Field(
        None,
        description="Optional parent step for tree-structured run timelines.",
    )
    sequence: int = Field(
        0,
        description="Stable sequence number for ordering steps within a run.",
    )
    step_type: AgentRunStepType = Field(
        ...,
        description="Categorization of the step in the run timeline.",
    )
    actor_type: AgentRunStepActorType = Field(
        AgentRunStepActorType.SYSTEM,
        description="Actor that originated the step.",
    )
    actor_name: str = Field(
        "",
        description="Human-readable actor name when available.",
    )
    title: str = Field(
        "",
        description="Short title shown in run visualizations.",
    )
    summary: str = Field(
        "",
        description="Longer textual summary of the step.",
    )
    status: AgentRunStepStatus = Field(
        AgentRunStepStatus.PENDING,
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
        description="Additional metadata associated with the run step.",
    )


class AgentRunArtifact(BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/agent-run-artifact"

    id: int | None = Field(None, description="Primary key of the run artifact.")
    agent_run: int | AgentRun | None = Field(
        None,
        description="Run associated with this artifact.",
    )
    step: int | AgentRunStep | None = Field(
        None,
        description="Optional run step associated with this artifact.",
    )
    artifact_type: AgentArtifactType = Field(
        ...,
        description="Type of artifact produced during the run.",
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
    "AgentRun",
    "AgentRunArtifact",
    "AgentRunCapabilitySnapshot",
    "AgentRunStatus",
    "AgentRunStep",
    "AgentRunStepActorType",
    "AgentRunStepStatus",
    "AgentRunStepType",
    "AgentStatus",
]
