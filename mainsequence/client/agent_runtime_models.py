from __future__ import annotations

import datetime
from enum import Enum
from typing import Any, ClassVar, Literal

from pydantic import Field

from .base import BaseObjectOrm, BasePydanticModel, ShareableObjectMixin
from .exceptions import raise_for_response
from .utils import make_request, serialize_to_json


class AgentSessionStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class AgentSemanticSearchResult(BasePydanticModel):
    id: int = Field(..., description="Primary key of the matched agent.")
    name: str = Field(..., description="Human-readable display name of the matched agent.")
    agent_unique_id: str = Field(
        ...,
        description="Organization-scoped stable identifier of the matched agent.",
    )
    description: str = Field(
        "",
        description="Short description returned by semantic search for the matched agent.",
    )
    semantic_score: float = Field(
        ...,
        description="Vector similarity component of the semantic-search ranking.",
    )
    text_score: float = Field(
        ...,
        description="Lexical similarity component of the semantic-search ranking.",
    )
    combined_score: float = Field(
        ...,
        description="Final weighted ranking score returned by the semantic-search endpoint.",
    )


class AgentSessionRuntimeAccess(BasePydanticModel):
    coding_agent_service_id: str = Field(
        ...,
        description="Identifier of the backing coding-agent service that owns the runtime.",
    )
    coding_agent_id: str = Field(
        ...,
        description="Stable coding-agent routing identifier presented to the gateway.",
    )
    mode: Literal["token"] = Field(
        ...,
        description="Runtime access mode. Currently only token-based gateway access is supported.",
    )
    rpc_url: str = Field(
        ...,
        description="Gateway RPC URL that the caller should use to reach the coding-agent runtime.",
    )
    token: str = Field(
        ...,
        description="Bearer token that authorizes calls to the coding-agent gateway.",
    )


class Agent(ShareableObjectMixin, BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/v1/agents"

    id: int | None = Field(None, description="Primary key of the agent.")
    name: str = Field(..., description="Human-readable display name for the agent inside the organization.")
    agent_unique_id: str = Field(
        ...,
        description="Client-supplied organization-scoped stable identifier for the agent. Use this for deterministic existence checks and idempotent create flows.",
    )
    description: str = Field("", description="Optional long-form description explaining what the agent is for.")
    agent_card: dict[str, Any] | None = Field(
        None,
        description="Optional structured agent card payload.",
    )

    llm_thinking:str
    llm_provider: str = Field(
        "",
        description="Optional default model provider for new sessions, for example openai, anthropic, or google. This is only a default on the Agent.",
    )
    llm_model: str = Field(
        "",
        description="Optional default model identifier for new sessions, for example gpt-5.4. This is only a default on the Agent.",
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

    @classmethod
    def semantic_search(
        cls,
        q: str,
        *,
        limit: int = 20,
        timeout=None,
    ) -> list[AgentSemanticSearchResult]:
        """
        Hits:
            POST <object_url>/semantic-search/

        Server behavior:
        - results stay scoped to the caller organization
        - returns ranked lightweight agent search rows, not full Agent records
        """
        q = (q or "").strip()
        if not q:
            raise ValueError("q is required")

        limit = int(limit)
        if limit < 1 or limit > 100:
            raise ValueError("limit must be between 1 and 100")

        body: dict[str, Any] = {
            "q": q,
            "limit": limit,
        }

        payload = {"json": serialize_to_json(body)}
        url = f"{cls.get_object_url()}/semantic-search/"
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload=payload,
            time_out=timeout,
        )
        if response.status_code != 200:
            raise_for_response(response, payload=payload)

        data = response.json()
        if isinstance(data, dict) and isinstance(data.get("results"), list):
            data = data["results"]
        if not isinstance(data, list):
            raise TypeError("semantic-search response must be a list of result rows")

        return [AgentSemanticSearchResult(**item) for item in data]

    def start_new_session(self, timeout=None, **kwargs):
        """
        Start a new session for this agent and return the created `AgentSession`.

        Keyword arguments are sent as session-creation overrides on top of the
        agent defaults, so callers can pass values such as `thread_id`,
        `runtime_session_id`, `external_session_id`,
        `runtime_config_snapshot`, or explicit runtime fields.
        """
        url = f"{self.get_detail_url()}start_new_session/"
        payload: dict[str, Any] = {}
        if kwargs:
            payload["json"] = serialize_to_json(kwargs)
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
        with the agent, including resolved runtime metadata and session-level
        identifiers and metadata.
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


class UserOrchestratorAgentService(BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/v1/user-orchestrator-agent-services"

    id: int | None = Field(None, description="Primary key of the orchestrator agent service.")
    user: int | None = Field(None, description="Owning user id.")
    related_job: int | None = Field(None, description="Backing job id.")
    subdomain: str = Field("", description="Public subdomain for the service.")


class UserProjectExecutorAgentService(BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/v1/project-executor-agent-services"

    id: int | None = Field(None, description="Primary key of the project executor agent service.")
    agent_id: int | None = Field(None, description="Resolved Agent id for the executor service.")
    is_ready: bool = Field(False, description="Whether the executor runtime is currently ready.")
    project: int | None = Field(None, description="Owning project id.")
    related_job: int | None = Field(None, description="Backing job id.")
    subdomain: str = Field("", description="Public subdomain for the service.")


class AgentSession(BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/v1/sessions"

    @classmethod
    def resolve_runtime_access(
        cls,
        agent_session: int | AgentSession,
        *,
        timeout=None,
    ) -> AgentSessionRuntimeAccess:
        """
        Hits:
            POST <object_url>/<session_id>/resolve_runtime_access/

        Server behavior:
        - resolves the coding-agent runtime that owns the session
        - returns the gateway RPC URL plus a bearer token for A2A calls
        """
        if isinstance(agent_session, int):
            session_id = agent_session
        else:
            session_id = getattr(agent_session, "id", None)

        if session_id is None:
            raise ValueError("agent_session must be an AgentSession or a session id")

        payload: dict[str, Any] = {}
        url = f"{cls.get_object_url()}/{int(session_id)}/resolve_runtime_access/"
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload=payload,
            time_out=timeout,
        )
        if response.status_code != 200:
            raise_for_response(response, payload=payload)
        return AgentSessionRuntimeAccess(**response.json())

    id: int | None = Field(None, description="Primary key of the agent session.")
    created_by_user: int | None = Field(
        None,
        description="User id of the actor who created the session record in MainSequence.",
    )
    agent: int | Agent | None = Field(
        None,
        description="Agent definition used for this session.",
    )
    agent_name: str = Field(
        "",
        description="Read-only helper with the agent display name for rendering session results.",
    )
    parent_session: int | AgentSession | None = Field(
        None,
        description="Optional parent session when this session was spawned as a subagent execution by another session.",
    )
    root_session: int | AgentSession | None = Field(
        None,
        description="Root session of the session tree. Child and descendant sessions point back to the same root for visualization and querying.",
    )
    spawned_by_step: int | None = Field(
        None,
        description="Identifier of the session step that spawned this session, when applicable.",
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

__all__ = [
    "Agent",
    "AgentSemanticSearchResult",
    "AgentSessionRuntimeAccess",
    "UserOrchestratorAgentService",
    "UserProjectExecutorAgentService",
    "AgentSession",
    "AgentSessionStatus",
]
