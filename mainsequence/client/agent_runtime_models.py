from __future__ import annotations

import datetime
import uuid
from typing import Any, ClassVar, Literal

from pydantic import ConfigDict, Field, model_validator

from .base import (
    BaseObjectOrm,
    BasePydanticModel,
    CurrentCodeRepositoryEnvironmentResourceMixin,
    ShareableObjectMixin,
)
from .exceptions import ApiError, raise_for_response
from .observability import (
    EnvironmentLogSearchMixin,
    EnvironmentLogSearchPage,
    LogSearchOutcome,
    LogTime,
    ObservabilityLinks,
    OwnerLogMixin,
    OwnerLogPage,
    OwnerResourceUsageMixin,
    PublicLogLevel,
)
from .utils import make_request, serialize_to_json
from .value_sets import OpenStrEnum, OpenValueSet


class AgentSessionStatus(OpenStrEnum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"












class AgentHarnessKind(OpenStrEnum):
    PI = "pi"
    TAU = "tau"


class AgentHarnessProtocol(OpenStrEnum):
    PI_CHECKPOINT_V1 = "pi-checkpoint-v1"
    TAU_SESSION_V1 = "tau-session-v1"


class AgentRuntimeUpdateRemediation(BasePydanticModel):
    tool: Literal["agent.update_runtime"]


class AgentRuntimeUpdate(BasePydanticModel):
    state: OpenValueSet[
        Literal[
            "current",
            "update_required",
            "updating",
            "update_failed",
            "not_deployed",
            "unknown",
        ]
    ]
    needs_redeploy: bool | None
    remediation: AgentRuntimeUpdateRemediation | None


class AgentSemanticSearchResult(BasePydanticModel):
    uid: str = Field(..., description="Public UID of the matched agent.")
    name: str = Field(..., description="Human-readable display name of the matched agent.")
    description: str = Field(
        "",
        description="Short description returned by semantic search for the matched agent.",
    )
    code_repository_branch_uid: str = Field(
        ...,
        description="Public UID of the CodeRepositoryBranch that owns the matched CodeRepository Coding Agent.",
    )
    repository_branch: str = Field(
        ...,
        description="Repository branch of the matched CodeRepository Coding Agent.",
    )
    organization_environment_uid: str = Field(
        ...,
        description="Public UID of the Organization Environment that scoped the search result.",
    )
    organization_environment_name: str = Field(
        ...,
        description="Name of the Organization Environment that scoped the search result.",
    )
    runtime_update: AgentRuntimeUpdate = Field(
        ...,
        description="Backend-owned runtime-currency state for the matched Agent.",
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


class AgentRuntimeImageDriftCheck(BasePydanticModel):
    key: str = Field(..., description="Machine-readable identifier for the drift check.")
    label: str = Field(..., description="Human-readable label for the drift check.")
    status: str = Field(..., description="Backend status for this drift check.")
    has_drift: bool = Field(..., description="Whether this check detected runtime drift.")
    matches: bool = Field(
        ..., description="Whether the actual runtime state matches the expected state."
    )
    reason: str = Field("", description="Machine-readable explanation for the status.")
    message: str = Field("", description="Human-readable explanation for this drift check.")
    autoheal_supported: bool = Field(
        False,
        description="Whether the backend can automatically repair this specific drift condition.",
    )
    autoheal_mode: str | None = Field(
        None,
        description="Backend repair mode for this check when automatic repair is supported.",
    )
    autoheal_message: str | None = Field(
        None,
        description="Human-readable automatic repair guidance for this check.",
    )
    expected_image_uri: str = Field(
        "", description="Expected runtime image URI, when the check is image-based."
    )
    actual_image_uri: str = Field(
        "", description="Actual runtime image URI, when the check is image-based."
    )
    expected_commit_hash: str = Field(
        "", description="Expected CodeRepository commit hash, when the check is commit-based."
    )
    actual_commit_hash: str = Field(
        "", description="Actual runtime commit hash, when the check is commit-based."
    )


class AgentRuntimeImageDrift(BasePydanticModel):
    agent_kind: str = Field("", description="Runtime family that produced the drift payload.")
    available: bool = Field(True, description="Whether drift information could be resolved.")
    has_drift: bool = Field(
        False, description="Whether any included drift check is currently drifting."
    )
    requires_user_action: bool = Field(
        False,
        description=(
            "Whether at least one current drift condition requires explicit user "
            "action instead of backend autohealing."
        ),
    )
    autoheal_available: bool = Field(
        False,
        description="Whether all currently drifting checks can be repaired automatically by the backend.",
    )
    autoheal_message: str | None = Field(
        None,
        description="Human-readable summary of automatic repair availability.",
    )
    checks: list[AgentRuntimeImageDriftCheck] = Field(
        default_factory=list,
        description="Individual runtime drift checks returned by the backend.",
    )
    detail: str | None = Field(
        None,
        description="Additional backend detail when drift information is unavailable or degraded.",
    )
    catalog_state: dict[str, Any] | None = Field(
        None,
        description="Platform catalog freshness state returned by the backend, when available.",
    )


class AgentRuntimePaths(BasePydanticModel):
    model_config = ConfigDict(extra="allow")

    pass


class AgentRuntimeInteractionNotice(BasePydanticModel):
    model_config = ConfigDict(extra="allow")

    code: str
    severity: str
    title: str
    message: str


class AgentRuntimeInteractionOperation(BasePydanticModel):
    model_config = ConfigDict(extra="allow")

    uid: str
    status: str
    created_at: str
    started_at: str | None = None
    finished_at: str | None = None
    support_reference: str


class AgentRuntimeInteraction(BasePydanticModel):
    model_config = ConfigDict(extra="allow")

    state: OpenValueSet[
        Literal[
            "ready",
            "warning",
            "checking",
            "starting",
            "waking",
            "update_required",
            "updating",
            "update_failed",
            "unavailable",
        ]
    ]
    can_submit: bool
    notice: AgentRuntimeInteractionNotice | None
    operation: AgentRuntimeInteractionOperation | None
    retry_after_ms: int | None = Field(None, ge=0)


class AgentRuntimePresenceReplicas(BasePydanticModel):
    desired: int | None = Field(None, ge=0)
    actual: int | None = Field(None, ge=0)


class AgentRuntimePresenceWake(BasePydanticModel):
    operation_uid: str
    state: OpenValueSet[
        Literal[
            "requested",
            "in_progress",
            "serving",
            "failed",
            "expired",
            "superseded",
        ]
    ]
    requested_at: str
    deadline_at: str


class AgentRuntimePresence(BasePydanticModel):
    phase: OpenValueSet[
        Literal[
            "not_deployed",
            "observing",
            "idle",
            "provisioning",
            "pulling_image",
            "starting",
            "serving",
            "redeploying",
            "failed",
        ]
    ]
    replicas: AgentRuntimePresenceReplicas
    detail: str
    observed_at: str | None
    wake: AgentRuntimePresenceWake | None


class AgentSessionRuntimeAccess(BasePydanticModel):
    model_config = ConfigDict(extra="allow")

    coding_agent_service_uid: str | None = Field(
        None,
        description="Canonical public UID of the coding-agent service that owns the runtime.",
    )
    mode: OpenValueSet[Literal["token", "unavailable"]] = Field(
        "token",
        description="Runtime access mode returned by the backend.",
    )
    rpc_url: str | None = Field(
        None,
        description=(
            "Opaque, server-issued gateway RPC URL for the coding-agent runtime. "
            "Callers must not derive it from tenancy, names, numeric IDs, or subdomains."
        ),
    )
    token: str | None = Field(
        None,
        description="Bearer token that authorizes calls to the coding-agent gateway.",
    )
    expires_at: str | None = Field(
        None,
        description="UTC expiry timestamp for this runtime access token, when returned by the backend.",
    )
    is_ready: bool = Field(
        False,
        description="Whether the resolved coding-agent runtime is currently routable.",
    )
    ready: Any | None = Field(
        None,
        description="Backend readiness metadata returned by runtime access resolution.",
    )
    reconciliation: dict[str, Any] | None = Field(
        None,
        description="Backend reconciliation metadata when runtime access is unavailable or stale.",
    )
    detail: str | None = Field(
        None,
        description="Human-readable runtime access detail returned by the backend.",
    )
    runtime_paths: AgentRuntimePaths = Field(
        default_factory=AgentRuntimePaths,
        description="Runtime-relative paths returned by the control plane.",
    )
    service_runtime_uid: str | None = Field(
        None,
        description="Public UID of the linked service runtime, when the backend has one.",
    )
    knative_service_runtime_uid: str | None = Field(
        None,
        description="Deprecated alias for service_runtime_uid kept for older SDK callers.",
    )
    image_drift: AgentRuntimeImageDrift | None = Field(
        None,
        description="Runtime image drift payload for the resolved coding-agent runtime.",
    )
    runtime_interaction: AgentRuntimeInteraction = Field(
        ...,
        description=(
            "Authoritative backend decision for whether a new runtime message may be submitted."
        ),
    )
    runtime_presence: AgentRuntimePresence = Field(
        ...,
        description="Sanitized diagnostic runtime-presence evidence.",
    )

    @property
    def image_drift_dict(self) -> dict[str, Any] | None:
        if self.image_drift is None:
            return None
        return self.image_drift.model_dump()

    @model_validator(mode="after")
    def _mirror_service_runtime_uid(self) -> AgentSessionRuntimeAccess:
        if self.service_runtime_uid is None and self.knative_service_runtime_uid is not None:
            self.service_runtime_uid = self.knative_service_runtime_uid
        elif self.knative_service_runtime_uid is None and self.service_runtime_uid is not None:
            self.knative_service_runtime_uid = self.service_runtime_uid
        return self








class Agent(
    CurrentCodeRepositoryEnvironmentResourceMixin,
    EnvironmentLogSearchMixin,
    OwnerLogMixin,
    OwnerResourceUsageMixin,
    ShareableObjectMixin,
    BaseObjectOrm,
    BasePydanticModel,
):
    ENDPOINT: ClassVar[str] = "agents"

    @classmethod
    def create(cls, *args: Any, **kwargs: Any) -> Agent:
        raise ApiError(
            "Agents are created by CodeRepository branch harness_agent workflow reconciliation."
        )

    FILTERSET_FIELDS: ClassVar[dict[str, list[str]] | None] = {
        "uid": ["exact", "in"],
        "name": ["exact"],
        "search": ["exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "uid": "uid",
        "uid__in": "uid",
        "name": "str",
        "search": "str",
    }

    @classmethod
    def _sdk_owned_query_context(cls, operation: str) -> dict[str, str]:
        if operation in {f"{cls.__name__}.filter", f"{cls.__name__}.semantic_search"}:
            return super()._sdk_owned_query_context(operation)
        return {}

    @classmethod
    def search_logs(
        cls,
        *,
        organization_environment_uid: str | uuid.UUID,
        start_time: LogTime,
        end_time: LogTime,
        agent_uid: str | uuid.UUID | None = None,
        agent_session_uid: str | uuid.UUID | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        level: PublicLogLevel | None = None,
        event: str | None = None,
        request_id: str | None = None,
        outcome: LogSearchOutcome | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> EnvironmentLogSearchPage:
        return cls._search_environment_logs(
            organization_environment_uid=organization_environment_uid,
            start_time=start_time,
            end_time=end_time,
            agent_uid=agent_uid,
            agent_session_uid=agent_session_uid,
            cursor=cursor,
            limit=limit,
            level=level,
            event=event,
            request_id=request_id,
            outcome=outcome,
            timeout=timeout,
        )

    uid: str | None = Field(None, description="Public UID of the agent resource.")
    name: str = Field(
        ..., description="Human-readable display name for the agent inside the organization."
    )
    description: str = Field(
        ..., description="Human-facing description synchronized from the required Agent Card."
    )
    agent_card: dict[str, Any] = Field(
        ...,
        description="Required canonical Agent Card; its name and description define Agent identity.",
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

    last_session_at: datetime.datetime | None = Field(
        None,
        description="Timestamp of the most recent session recorded for this agent.",
    )
    runtime_release_uid: str | None = Field(
        None,
        description=(
            "Read-only public UID of the ResourceRelease currently bound to this Agent, "
            "or null when the Agent has no deployed runtime release."
        ),
    )
    code_repository_branch_uid: str | None = Field(
        None,
        description=(
            "Public UID of the canonical CodeRepositoryBranch attached through the "
            "typed CodeRepository Executor service, or null when the agent is not "
            "CodeRepositoryBranch-scoped."
        ),
    )
    repository_branch: str | None = Field(
        ...,
        description=(
            "Read-only repository branch projected from the canonical CodeRepositoryBranch, "
            "or null when the Agent is not CodeRepositoryBranch-scoped."
        ),
    )
    organization_environment_uid: str | None = Field(
        ...,
        description=(
            "Read-only public UID of the Organization Environment derived from the "
            "canonical CodeRepositoryBranch, or null when the Agent is not environment-scoped."
        ),
    )
    organization_environment_name: str | None = Field(
        ...,
        description=(
            "Read-only name of the Organization Environment derived from the canonical "
            "CodeRepositoryBranch, or null when the Agent is not environment-scoped."
        ),
    )
    runtime_update: AgentRuntimeUpdate = Field(
        ...,
        description="Read-only backend-owned runtime-currency state for this Agent.",
    )
    observability: ObservabilityLinks | None = Field(
        default=None,
        description="Backend-owned runtime observability and related-resource capabilities.",
    )

    @model_validator(mode="after")
    def _require_card_identity(self) -> Agent:
        if (
            not isinstance(self.agent_card.get("name"), str)
            or not self.agent_card["name"].strip()
            or not isinstance(self.agent_card.get("description"), str)
            or not self.agent_card["description"].strip()
        ):
            raise ValueError("Agent Card must have a name and description")
        if (
            self.name != self.agent_card["name"]
            or self.description != self.agent_card["description"]
        ):
            raise ValueError("Agent name and description must match the Agent Card")
        return self

    def get_logs(
        self,
        *,
        start_time: LogTime | None = None,
        end_time: LogTime | None = None,
        start: int | float | None = None,
        end: int | float | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        level: PublicLogLevel | None = None,
        severity: str | None = None,
        request_id: str | None = None,
        event: str | None = None,
        outcome: LogSearchOutcome | None = None,
        agent_session_uid: str | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> OwnerLogPage:
        return self._get_owner_logs(
            start_time=start_time,
            end_time=end_time,
            start=start,
            end=end,
            cursor=cursor,
            limit=limit,
            level=level,
            severity=severity,
            request_id=request_id,
            event=event,
            outcome=outcome,
            agent_session_uid=agent_session_uid,
            timeout=timeout,
        )

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
        - results stay scoped to the requested Organization Environment
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
            **cls._sdk_owned_query_context(f"{cls.__name__}.semantic_search"),
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

    def get_or_create_session(
        self,
        *,
        session_uid: str | None = None,
        handle_unique_id: str | None = None,
        name: str | None = None,
        parent_session_uid: str | None = None,
        llm_provider: str | None = None,
        llm_model: str | None = None,
        llm_thinking: str | None = None,
        timeout=None,
    ) -> AgentSession:
        """
        Get an existing session by UID, or get/create one by handle for this Agent.

        Hits:
            POST <detail_url>sessions/get-or-create-session/

        Request contract:
        - Send exactly one lookup key: `session_uid` or `handle_unique_id`.
        - `session_uid` resolves an existing AgentSession for this Agent.
        - `handle_unique_id` gets or creates a reusable session handle.
        - For ordinary User requests, the backend owns a root session with that User.
        - For runtime A2A requests, `parent_session_uid` proves the calling Agent;
          the backend inherits the parent session User into the child and handle.
        - The SDK never supplies a credential owner or forwards provider credentials.
        - Creation options are valid only with `handle_unique_id`.
        - Response is the canonical AgentSessionSerializer payload.
        """
        resolved_session_uid = str(session_uid or "").strip() if session_uid is not None else ""
        resolved_handle_unique_id = (
            str(handle_unique_id or "").strip() if handle_unique_id is not None else ""
        )
        if bool(resolved_session_uid) == bool(resolved_handle_unique_id):
            raise ValueError("Provide exactly one of session_uid or handle_unique_id")

        creation_options = {
            "name": name,
            "parent_session_uid": parent_session_uid,
            "llm_provider": llm_provider,
            "llm_model": llm_model,
            "llm_thinking": llm_thinking,
        }
        if resolved_session_uid and any(value is not None for value in creation_options.values()):
            raise ValueError("Creation options require handle_unique_id, not session_uid")

        if resolved_session_uid:
            body: dict[str, Any] = {"session_uid": resolved_session_uid}
        else:
            body = {"handle_unique_id": resolved_handle_unique_id}
            for key, value in creation_options.items():
                if value is not None:
                    body[key] = str(value)

        url = f"{self.get_detail_url()}sessions/get-or-create-session/"
        payload = {"json": serialize_to_json(body)}
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

        session_payload = response.json()
        if not isinstance(session_payload, dict):
            raise TypeError("get_or_create_session response must be an AgentSession object")
        return AgentSession(**session_payload)


class AgentSessionInsightsBase(BasePydanticModel):
    has_insights: bool = Field(
        True,
        description="Whether the session currently has an insights projection.",
    )
    agent_session_uid: str = Field(
        ...,
        description="Public UID of the AgentSession represented by this response.",
    )
    harness_version: str = Field(
        "",
        description="Version of the harness runtime that produced the session history.",
    )
    computed_at: datetime.datetime | None = Field(
        None,
        description="Timestamp when the insights projection was computed.",
    )
    reason: str | None = Field(
        None,
        description="Backend reason describing how the insights projection was produced.",
    )
    updated_at: datetime.datetime | None = Field(
        None,
        description="Timestamp of the newest state represented by the projection.",
    )


class PiAgentSessionInsights(AgentSessionInsightsBase):
    harness: Literal["pi"] = "pi"
    harness_protocol: Literal["pi-checkpoint-v1"] = "pi-checkpoint-v1"
    checkpoint_version: int | None = Field(
        None,
        description="Pi checkpoint version used to calculate these insights.",
    )
    bundle_hash: str = Field(
        "",
        description="Hash of the Pi checkpoint bundle used to calculate these insights.",
    )
    flushed_at: datetime.datetime | None = Field(
        None,
        description="Timestamp when the source Pi checkpoint was flushed.",
    )
    insights: dict[str, Any] = Field(
        default_factory=dict,
        description="Pi-native checkpoint insights payload.",
    )


class TauAgentSessionInsightsTokenTotals(BasePydanticModel):
    model_config = ConfigDict(populate_by_name=True)

    input: int = 0
    output: int = 0
    cache_read: int = Field(0, alias="cacheRead")
    cache_write: int = Field(0, alias="cacheWrite")
    total: int = 0


class TauAgentSessionInsightsModel(BasePydanticModel):
    model_config = ConfigDict(populate_by_name=True)

    provider: str | None = None
    model: str | None = None
    reasoning_effort: str | None = Field(None, alias="reasoningEffort")


class TauAgentSessionInsightsSession(BasePydanticModel):
    model_config = ConfigDict(populate_by_name=True)

    agent_session_id: str = Field(..., alias="agentSessionId")
    session_id: str | None = Field(None, alias="sessionId")
    thread_id: str | None = Field(None, alias="threadId")
    status: OpenValueSet[Literal["running", "completed", "error"]]
    started_at: datetime.datetime | None = Field(None, alias="startedAt")
    updated_at: datetime.datetime | None = Field(None, alias="updatedAt")
    last_error: str | None = Field(None, alias="lastError")


class TauAgentSessionInsightsUsage(BasePydanticModel):
    model_config = ConfigDict(populate_by_name=True)

    total_messages: int = Field(0, alias="totalMessages")
    user_messages: int = Field(0, alias="userMessages")
    assistant_messages: int = Field(0, alias="assistantMessages")
    assistant_turns: int = Field(0, alias="assistantTurns")
    tool_calls: int = Field(0, alias="toolCalls")
    tool_results: int = Field(0, alias="toolResults")
    estimated_cost_usd: float = Field(0, alias="estimatedCostUsd")
    tokens: TauAgentSessionInsightsTokenTotals


class TauAgentSessionInsightsContext(BasePydanticModel):
    model_config = ConfigDict(populate_by_name=True)

    source: str
    status: str
    tokens: int | None = None
    latest_compaction: datetime.datetime | None = Field(None, alias="latestCompaction")


class TauAgentSessionInsightsLastTurnModel(BasePydanticModel):
    provider: str | None = None
    model: str | None = None


class TauAgentSessionInsightsLastTurn(BasePydanticModel):
    model_config = ConfigDict(populate_by_name=True)

    completed_at: datetime.datetime | None = Field(None, alias="completedAt")
    finish_reason: str | None = Field(None, alias="finishReason")
    error_message: str | None = Field(None, alias="errorMessage")
    model: TauAgentSessionInsightsLastTurnModel
    tokens: TauAgentSessionInsightsTokenTotals


class TauAgentSessionInsightsDetails(BasePydanticModel):
    model_config = ConfigDict(populate_by_name=True)

    version: int
    model: TauAgentSessionInsightsModel
    session: TauAgentSessionInsightsSession
    usage: TauAgentSessionInsightsUsage
    context: TauAgentSessionInsightsContext
    last_turn: TauAgentSessionInsightsLastTurn | None = Field(None, alias="lastTurn")


class TauAgentSessionInsights(AgentSessionInsightsBase):
    harness: Literal["tau"] = "tau"
    harness_protocol: Literal["tau-session-v1"] = "tau-session-v1"
    entry_count: int = 0
    last_sequence: int | None = None
    active_branch_entry_count: int = 0
    entry_type_counts: dict[str, int] = Field(default_factory=dict)
    title: str | None = None
    insights: TauAgentSessionInsightsDetails


AgentSessionInsights = PiAgentSessionInsights | TauAgentSessionInsights


class AgentSession(
    EnvironmentLogSearchMixin,
    OwnerLogMixin,
    BaseObjectOrm,
    BasePydanticModel,
):
    ENDPOINT: ClassVar[str] = "agent-sessions"
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]] | None] = {
        "uid": ["exact", "in"],
        "agent_uid": ["exact", "in"],
        "agent_name": ["exact"],
        "created_by_user_uid": ["exact"],
        "is_archived": ["exact"],
        "status": ["exact"],
        "search": ["exact"],
        "q": ["exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "uid": "uid",
        "uid__in": "uid",
        "agent_uid": "uid",
        "agent_uid__in": "uid",
        "agent_name": "str",
        "created_by_user_uid": "uid",
        "is_archived": "bool",
        "status": "str",
        "search": "str",
        "q": "str",
    }

    @classmethod
    def search_logs(
        cls,
        *,
        organization_environment_uid: str | uuid.UUID,
        start_time: LogTime,
        end_time: LogTime,
        agent_session_uid: str | uuid.UUID | None = None,
        agent_uid: str | uuid.UUID | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        level: PublicLogLevel | None = None,
        event: str | None = None,
        request_id: str | None = None,
        outcome: LogSearchOutcome | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> EnvironmentLogSearchPage:
        return cls._search_environment_logs(
            organization_environment_uid=organization_environment_uid,
            start_time=start_time,
            end_time=end_time,
            agent_session_uid=agent_session_uid,
            agent_uid=agent_uid,
            cursor=cursor,
            limit=limit,
            level=level,
            event=event,
            request_id=request_id,
            outcome=outcome,
            timeout=timeout,
        )

    READ_QUERY_PARAMS: ClassVar[dict[str, str]] = {
        "ordering": "str",
        "limit": "str",
        "offset": "str",
    }

    @classmethod
    def _resolve_agent_session_uid(cls, agent_session: str | AgentSession) -> str:
        return cls._coerce_filter_uid(agent_session, field_name="agent_session")


















    @classmethod
    def resolve_runtime_access(
        cls,
        agent_session: str | AgentSession,
        *,
        timeout=None,
    ) -> AgentSessionRuntimeAccess:
        """
        Hits:
            POST <object_url>/<session_uid>/resolve-runtime-access/

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

        body: dict[str, Any] = {}
        payload: dict[str, Any] = {"json": serialize_to_json(body)}
        url = f"{cls.get_object_url()}/{session_uid}/resolve-runtime-access/"
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
        if not isinstance(data, dict):
            raise TypeError("Agent session runtime access response must be a JSON object")
        access = AgentSessionRuntimeAccess(**data)
        return access

    @classmethod
    def get_insights(
        cls,
        agent_session: str | AgentSession,
        *,
        timeout=None,
    ) -> AgentSessionInsights:
        session_uid = cls._resolve_agent_session_uid(agent_session)
        url = f"{cls.get_object_url().rstrip('/')}/{session_uid}/insights/"
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="GET",
            url=url,
            payload={},
            time_out=timeout,
        )
        if response.status_code != 200:
            raise_for_response(response)
        data = response.json()
        if not isinstance(data, dict):
            raise TypeError("Agent session insights response must be a JSON object")

        harness = data.get("harness")
        if harness == AgentHarnessKind.PI.value:
            return PiAgentSessionInsights(**data)
        if harness == AgentHarnessKind.TAU.value:
            return TauAgentSessionInsights(**data)
        raise TypeError(
            "Agent session insights response must include a supported "
            f"harness discriminator. Got: {harness!r}"
        )

    def insights(self, *, timeout=None) -> AgentSessionInsights:
        return type(self).get_insights(self, timeout=timeout)

    @classmethod
    def _set_archive_state(
        cls,
        agent_session: str | AgentSession,
        *,
        action: Literal["archive", "unarchive"],
        timeout=None,
    ) -> AgentSession:
        session_uid = cls._resolve_agent_session_uid(agent_session)
        url = f"{cls.get_object_url().rstrip('/')}/{session_uid}/{action}/"
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload={},
            time_out=timeout,
        )
        if response.status_code != 200:
            raise_for_response(response)
        data = response.json()
        if not isinstance(data, dict):
            raise TypeError(f"Agent session {action} response must be a JSON object")
        return cls(**data)

    @classmethod
    def archive_by_uid(cls, uid: str, *, timeout=None) -> AgentSession:
        return cls._set_archive_state(uid, action="archive", timeout=timeout)

    @classmethod
    def unarchive_by_uid(cls, uid: str, *, timeout=None) -> AgentSession:
        return cls._set_archive_state(uid, action="unarchive", timeout=timeout)

    def archive(self, *, timeout=None) -> AgentSession:
        return type(self)._set_archive_state(self, action="archive", timeout=timeout)

    def unarchive(self, *, timeout=None) -> AgentSession:
        return type(self)._set_archive_state(self, action="unarchive", timeout=timeout)

    uid: str | None = Field(None, description="Public UID of the agent session.")
    observability: ObservabilityLinks | None = Field(
        default=None,
        description="Backend-owned application-log capability for this exact session.",
    )
    runtime_capabilities: dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Read-only versioned runtime capabilities advertised by the session "
            "harness. Capability names are backend-extensible."
        ),
    )
    catalog_digest: str | None = Field(
        ...,
        pattern=r"^sha256:[0-9a-f]{64}$",
        description=(
            "Read-only digest of the model-provider catalog used to resolve the "
            "session, or null when no provider-control projection is available."
        ),
    )
    agent_uid: str | None = Field(
        None, description="Public UID of the agent definition used for this session."
    )
    created_by_user_uid: str | None = Field(
        None,
        description=(
            "Public UID of the session invocation and model-provider credential "
            "owner. For delegated sessions this is inherited from the immediate "
            "parent and may differ from the runtime responsible User."
        ),
    )
    parent_session_uid: str | None = Field(
        None, description="Public UID of the parent session, if any."
    )
    name: str = Field(
        "",
        description="Optional human-readable session name for UI and user-facing history.",
    )
    created_by_user: int | None = Field(
        None,
        exclude=True,
        description="Legacy internal user id for the session owner.",
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
    organization_environment_uid: str = Field(
        ...,
        description=(
            "Read-only public UID of the Organization Environment that owns the "
            "session through its Agent."
        ),
    )
    organization_environment_name: str = Field(
        ...,
        description=(
            "Read-only name of the Organization Environment that owns the session "
            "through its Agent."
        ),
    )
    harness: AgentHarnessKind | None = Field(
        None,
        description=(
            "Harness that owns session persistence. None means the connected backend "
            "predates the multi-harness contract."
        ),
    )
    harness_protocol: AgentHarnessProtocol | None = Field(
        None,
        description="Harness persistence protocol advertised by the backend.",
    )
    harness_version: str = Field(
        "",
        description="Version of the harness runtime that owns the session.",
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
    working: bool | None = Field(
        None, description="Whether the backend considers the session actively working."
    )
    started_at: datetime.datetime | None = Field(
        None,
        description="Timestamp when the session started.",
    )
    ended_at: datetime.datetime | None = Field(
        None,
        description="Timestamp when the session ended.",
    )
    is_archived: bool | None = Field(
        None,
        description="Whether the session is archived. None means the backend omitted archive state.",
    )
    archived_at: datetime.datetime | None = Field(
        None,
        description="Timestamp when the session was archived.",
    )
    llm_provider: str = Field(
        ...,
        description="Resolved LLM provider actually used for this session. Unlike Agent defaults, this is intended to be the authoritative runtime record.",
    )
    llm_model: str = Field(
        ...,
        description="Resolved LLM model actually used for this session. Unlike Agent defaults, this is intended to be the authoritative runtime record.",
    )
    llm_thinking: str = Field(
        "", description="Resolved thinking/reasoning setting used for this session."
    )
    active_provider: str | None = Field(
        None,
        description="Read-only active provider projection for the session.",
    )
    active_model: str | None = Field(
        None,
        description="Read-only active model projection for the session.",
    )
    active_thinking: str | None = Field(
        None,
        description="Read-only active thinking level projection for the session.",
    )
    engine_name: str = Field(
        "",
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
    bound_handle: dict[str, Any] | None = Field(
        None,
        description="Agent session handle currently bound to this session, if any.",
    )


__all__ = [
    "Agent",
    "AgentRuntimeImageDrift",
    "AgentRuntimeImageDriftCheck",
    "AgentRuntimeUpdate",
    "AgentRuntimeUpdateRemediation",
    "AgentHarnessKind",
    "AgentHarnessProtocol",
    "AgentSessionInsights",
    "AgentSessionInsightsBase",
    "PiAgentSessionInsights",
    "TauAgentSessionInsights",
    "TauAgentSessionInsightsContext",
    "TauAgentSessionInsightsDetails",
    "TauAgentSessionInsightsLastTurn",
    "TauAgentSessionInsightsLastTurnModel",
    "TauAgentSessionInsightsModel",
    "TauAgentSessionInsightsSession",
    "TauAgentSessionInsightsTokenTotals",
    "TauAgentSessionInsightsUsage",
    "AgentSemanticSearchResult",
    "AgentSessionRuntimeAccess",
    "AgentSession",
    "AgentSessionStatus",
]
