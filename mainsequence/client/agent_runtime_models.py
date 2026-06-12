from __future__ import annotations

import datetime
from enum import Enum
from typing import Any, ClassVar, Literal

from pydantic import Field

from .base import BaseObjectOrm, BasePydanticModel, ShareableObjectMixin
from .exceptions import ApiError, raise_for_response
from .utils import make_request, serialize_to_json


class AgentSessionStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"


class AgentSemanticSearchResult(BasePydanticModel):
    uid: str = Field(..., description="Public UID of the matched agent.")
    name: str = Field(..., description="Human-readable display name of the matched agent.")
    agent_type: str = Field(
        "",
        description="Stable machine-readable runtime or workflow type of the matched agent.",
    )
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
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]] | None] = {
        "uid": ["exact", "in"],
        "agent_unique_id": ["exact"],
        "agent_type": ["exact"],
        "search": ["exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "uid": "uid",
        "uid__in": "uid",
        "agent_unique_id": "str",
        "agent_type": "str",
        "search": "str",
    }

    uid: str | None = Field(None, description="Public UID of the agent resource.")
    name: str = Field(..., description="Human-readable display name for the agent inside the organization.")
    agent_type: str = Field(
        "custom",
        description="Stable machine-readable runtime or workflow classifier for the agent. This is not the display name.",
    )
    agent_unique_id: str = Field(
        ...,
        description="Client-supplied organization-scoped stable identifier for the agent. Use this for deterministic existence checks and idempotent create flows.",
    )
    description: str = Field("", description="Optional long-form description explaining what the agent is for.")
    agent_card: dict[str, Any] | None = Field(
        None,
        description="Optional structured agent card payload.",
    )

    llm_thinking: str
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
    def get_by_agent_unique_id(cls, agent_unique_id: str, *, timeout=None) -> Agent:
        """
        Resolve an Agent by its deterministic organization-scoped key.

        The backend detail route is UID-based, but `agent_unique_id` is the
        user-controlled key used for idempotent agent creation and CLI lookup.
        """
        normalized_agent_unique_id = str(agent_unique_id or "").strip()
        if not normalized_agent_unique_id:
            raise ValueError("agent_unique_id is required")

        candidates = cls.filter(
            timeout=timeout,
            agent_unique_id=normalized_agent_unique_id,
        )
        if not candidates:
            from .utils import DoesNotExist

            raise DoesNotExist(
                f"No {cls.class_name()} found matching agent_unique_id={normalized_agent_unique_id!r}"
            )
        if len(candidates) > 1:
            raise ApiError(
                f"Multiple agents returned for agent_unique_id={normalized_agent_unique_id!r}"
            )
        return candidates[0]

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



    def allocate_a2a_target_session(
        self,
        *,
        caller_agent_session_uid: str | AgentSession,
        handle_unique_id: str | None = None,
        timeout=None,
    ) -> dict[str, Any]:
        """
        Allocate or reuse the delegated target session for this target `Agent`.

        Hits:
            POST <detail_url>allocate-a2a-target-session/

        Request contract:
        - `caller_agent_session_uid` is required and identifies the session that is
          delegating work to this target agent.
        - `handle_unique_id` is optional on the first call.
        - when `handle_unique_id` is omitted, the backend creates a delegated
          `AgentSessionHandle`, generates a new `handle_unique_id`, allocates the target
          session, and returns that `handle_unique_id` in the response.
        - when `handle_unique_id` is provided, the backend reuses the same delegated
          handle and therefore the same target session for retries, reconnects, and
          repeated calls for the same delegated conversation.

        Why this is required for A2A communication:
        - A2A communication needs an idempotent control-plane allocation step so that
          transport retries or stream reconnects do not silently create sibling target
          sessions.
        - `allocate_a2a_target_session()` makes the delegated conversation reusable by
          returning a stable `handle_unique_id` that the caller can keep and send again.

        Response contract:
        - `handle_unique_id`: stable delegated-session reuse key.
        - `agent_session_uid`: allocated target session public UID.
        - `allocation_state`: `created_new` or `reused_existing`.
        - `session`: full canonical backend `AgentSession` payload for the target side.

        Typical usage:
        1. first call: send only `caller_agent_session_uid`
        2. persist the returned `handle_unique_id`
        3. later retries for the same delegated task: resend that `handle_unique_id`
        4. intentionally fresh delegated conversation: omit the old handle and let the
           backend allocate a new one
        """
        if isinstance(caller_agent_session_uid, AgentSession):
            resolved_caller_session_uid = getattr(caller_agent_session_uid, "uid", None)
        else:
            resolved_caller_session_uid = caller_agent_session_uid

        if resolved_caller_session_uid is None:
            raise ValueError(
                "caller_agent_session_uid must be an AgentSession or a session uid"
            )
        resolved_caller_session_uid = type(self)._coerce_filter_uid(
            resolved_caller_session_uid,
            field_name="caller_agent_session_uid",
        )

        resolved_handle_unique_id = str(handle_unique_id or '').strip()

        body = {
            "caller_agent_session_uid": resolved_caller_session_uid,
        }
        if resolved_handle_unique_id:
            body['handle_unique_id'] = resolved_handle_unique_id

        url = f"{self.get_detail_url()}allocate-a2a-target-session/"
        payload = {
            'json': serialize_to_json(body)
        }
        response = make_request(
            s=self.build_session(),
            loaders=self.LOADERS,
            r_type='POST',
            url=url,
            payload=payload,
            time_out=timeout,
        )
        if response.status_code not in (200, 201):
            raise_for_response(response, payload=payload)
        return response.json()

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

    uid: str | None = Field(None, description="Public UID of the orchestrator agent service.")
    agent_uid: str | None = Field(None, description="Public UID of the resolved astro Agent.")
    user_uid: str | None = Field(None, description="Public UID of the owning user.")
    is_ready: bool = Field(False, description="Whether the service runtime is routable.")
    automatic_deployment: bool = Field(False, description="Whether this coding-agent service is eligible for automatic deployment flows.")
    orchestrator_image_has_drift: bool = Field(False, description="Whether the orchestrator image is stale.")
    related_job: Any | None = Field(None, description="Backing job payload or UID when returned by the backend.")
    knative_service_runtime: Any | None = Field(None, description="Backing Knative service runtime payload.")
    subdomain: str = Field("", description="Public subdomain for the service.")


class UserProjectExecutorAgentService(BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/v1/project-executor-agent-services"

    uid: str | None = Field(None, description="Public UID of the project executor service.")
    agent_uid: str | None = Field(None, description="Public UID of the resolved executor Agent.")
    is_ready: bool = Field(False, description="Whether the executor runtime is currently ready.")
    automatic_deployment: bool = Field(False, description="Whether this coding-agent service is eligible for automatic deployment flows.")
    image_drift: dict[str, Any] | None = Field(None, description="Executor image drift status payload.")
    project: Any | None = Field(None, description="Owning project payload or UID when returned by the backend.")
    related_job: Any | None = Field(None, description="Backing job payload or UID when returned by the backend.")
    knative_service_runtime: Any | None = Field(None, description="Backing Knative service runtime payload.")
    subdomain: str = Field("", description="Public subdomain for the service.")


class AgentSession(BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/v1/sessions"
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]] | None] = {
        "uid": ["exact", "in"],
        "status": ["exact"],
        "search": ["exact"],
        "q": ["exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "uid": "uid",
        "uid__in": "uid",
        "status": "str",
        "search": "str",
        "q": "str",
    }

    @classmethod
    def resolve_runtime_access(
        cls,
        agent_session: str | AgentSession,
        *,
        timeout=None,
    ) -> AgentSessionRuntimeAccess:
        """
        Hits:
            POST <object_url>/<session_uid>/resolve_runtime_access/

        Server behavior:
        - resolves the coding-agent runtime that owns the session
        - returns the gateway RPC URL plus a bearer token for A2A calls
        """
        if isinstance(agent_session, AgentSession):
            session_uid = getattr(agent_session, "uid", None)
        else:
            session_uid = agent_session

        if session_uid is None:
            raise ValueError("agent_session must be an AgentSession or a session uid")
        session_uid = cls._coerce_filter_uid(session_uid, field_name="agent_session")

        payload: dict[str, Any] = {}
        url = f"{cls.get_object_url()}/{session_uid}/resolve_runtime_access/"
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

    uid: str | None = Field(None, description="Public UID of the agent session.")
    agent_uid: str | None = Field(None, description="Public UID of the agent definition used for this session.")
    created_by_user_uid: str | None = Field(None, description="Public UID of the actor who created the session.")
    parent_session_uid: str | None = Field(None, description="Public UID of the parent session, if any.")
    created_by_user: int | None = Field(
        None,
        exclude=True,
        description="Legacy internal user id of the actor who created the session record.",
    )
    agent: int | Agent | None = Field(
        None,
        exclude=True,
        description="Agent definition used for this session.",
    )
    agent_name: str = Field(
        "",
        description="Read-only helper with the agent display name for rendering session results.",
    )
    agent_type: str = Field(
        "",
        description="Read-only helper with the canonical machine-readable agent runtime or workflow type.",
    )
    parent_session: int | AgentSession | None = Field(
        None,
        exclude=True,
        description="Optional parent session when this session was spawned as a subagent execution by another session.",
    )
    root_session: int | AgentSession | None = Field(
        None,
        exclude=True,
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
    runtime_state: str = Field("", description="Computed runtime state of the session.")
    working: bool | None = Field(None, description="Whether the backend considers the session actively working.")
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
    llm_thinking: str = Field("", description="Resolved thinking/reasoning setting used for this session.")
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
    bound_handles: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Agent session handles currently bound to this session.",
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
