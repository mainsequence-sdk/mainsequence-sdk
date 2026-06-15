from __future__ import annotations

import datetime
from enum import Enum
from typing import Any, ClassVar, Literal

from pydantic import ConfigDict, Field

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
        "", description="Expected project commit hash, when the check is commit-based."
    )
    actual_commit_hash: str = Field(
        "", description="Actual runtime commit hash, when the check is commit-based."
    )


class AgentRuntimeImageDrift(BasePydanticModel):
    agent_kind: str = Field(..., description="Runtime family that produced the drift payload.")
    available: bool = Field(..., description="Whether drift information could be resolved.")
    has_drift: bool = Field(
        ..., description="Whether any included drift check is currently drifting."
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
    is_ready: bool = Field(
        False,
        description="Whether the resolved coding-agent runtime is currently routable.",
    )
    knative_service_runtime_uid: str | None = Field(
        None,
        description="Public UID of the linked Knative service runtime, when the backend has one.",
    )
    image_drift: AgentRuntimeImageDrift | None = Field(
        None,
        description="Runtime image drift payload for the resolved coding-agent runtime.",
    )

    @property
    def image_drift_dict(self) -> dict[str, Any] | None:
        if self.image_drift is None:
            return None
        return self.image_drift.model_dump()


class AgentSessionRuntimeReady(BasePydanticModel):
    model_config = ConfigDict(extra="allow")

    ready: bool = Field(..., description="Whether the target runtime became ready.")
    attempts: int = Field(..., description="Number of readiness attempts performed.")
    elapsed_seconds: float = Field(..., description="Elapsed wall-clock seconds spent polling.")
    status_code: int | None = Field(
        None,
        description="Last runtime status code observed by the backend readiness probe.",
    )
    detail: str = Field("", description="Backend readiness detail or timeout reason.")


class AgentSessionA2ANormalizedResponse(BasePydanticModel):
    model_config = ConfigDict(extra="allow")

    ok: bool = Field(..., description="Whether the normalized A2A response is usable.")
    kind: str | None = Field(None, description="Normalized A2A response kind.")
    state: str | None = Field(
        None, description="Normalized task state, when the response is task-like."
    )
    task_id: str | None = Field(None, description="Normalized A2A task identifier, when present.")
    context_id: str | None = Field(
        None, description="Normalized A2A context identifier, when present."
    )
    text: str = Field("", description="Best-effort normalized response text.")
    raw: dict[str, Any] = Field(
        default_factory=dict,
        description="Backend-provided raw normalization metadata.",
    )


class AgentSessionA2AChatResponse(BasePydanticModel):
    model_config = ConfigDict(extra="allow")

    ok: bool = Field(..., description="Whether the backend A2A transport completed successfully.")
    ready: AgentSessionRuntimeReady | None = Field(
        None,
        description="Runtime readiness result when wait_for_runtime was enabled.",
    )
    response: dict[str, Any] = Field(
        default_factory=dict,
        description="Raw A2A JSON-RPC response returned by the target runtime.",
    )
    normalized: AgentSessionA2ANormalizedResponse | None = Field(
        None,
        description="Backend-normalized A2A response summary.",
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
    name: str = Field(
        ..., description="Human-readable display name for the agent inside the organization."
    )
    agent_type: str = Field(
        "custom",
        description="Stable machine-readable runtime or workflow classifier for the agent. This is not the display name.",
    )
    agent_unique_id: str = Field(
        ...,
        description="Client-supplied organization-scoped stable identifier for the agent. Use this for deterministic existence checks and idempotent create flows.",
    )
    description: str = Field(
        "", description="Optional long-form description explaining what the agent is for."
    )
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

    last_session_at: datetime.datetime | None = Field(
        None,
        description="Timestamp of the most recent session recorded for this agent.",
    )
    has_agent_service: bool = Field(
        False,
        description="Whether the backend resolved this agent to a typed coding-agent service.",
    )
    agent_service_uid: str | None = Field(
        None,
        description="Public UID of the resolved typed coding-agent service, if one exists.",
    )
    agent_service_automatic_deployment: bool | None = Field(
        None,
        description="Service-level automatic deployment flag for the resolved typed coding-agent service.",
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
            raise ValueError("caller_agent_session_uid must be an AgentSession or a session uid")
        resolved_caller_session_uid = type(self)._coerce_filter_uid(
            resolved_caller_session_uid,
            field_name="caller_agent_session_uid",
        )

        resolved_handle_unique_id = str(handle_unique_id or "").strip()

        body = {
            "caller_agent_session_uid": resolved_caller_session_uid,
        }
        if resolved_handle_unique_id:
            body["handle_unique_id"] = resolved_handle_unique_id

        url = f"{self.get_detail_url()}allocate-a2a-target-session/"
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
        return response.json()

    def get_or_create_session_with_handle(
        self,
        *,
        handle_unique_id: str,
        name: str | None = None,
        llm_provider: str | None = None,
        llm_model: str | None = None,
        llm_thinking: str | None = None,
        session_metadata: dict[str, Any] | None = None,
        timeout=None,
    ) -> AgentSession:
        """
        Get or create this user's reusable session slot for this Agent.

        Hits:
            POST <detail_url>get_or_create_session_with_handle/

        Request contract:
        - `handle_unique_id` is required and is the stable session-slot key scoped
          by `(agent, authenticated user)`.
        - `name` is optional display text for a newly created session.
        - LLM fields are creation-time overrides only. If the handle already has a
          current session, the backend returns that session unchanged.
        - `session_metadata` is creation-time caller metadata.

        The response is expected to contain either a top-level `session` object or a
        direct AgentSession payload. The SDK returns the AgentSession object.
        """
        resolved_handle_unique_id = str(handle_unique_id or "").strip()
        if not resolved_handle_unique_id:
            raise ValueError("handle_unique_id is required")

        body: dict[str, Any] = {
            "handle_unique_id": resolved_handle_unique_id,
        }
        if name is not None:
            body["name"] = str(name)
        if llm_provider is not None:
            body["llm_provider"] = str(llm_provider)
        if llm_model is not None:
            body["llm_model"] = str(llm_model)
        if llm_thinking is not None:
            body["llm_thinking"] = str(llm_thinking)
        if session_metadata is not None:
            body["session_metadata"] = session_metadata

        url = f"{self.get_detail_url()}get_or_create_session_with_handle/"
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

        response_payload = response.json()
        session_payload = (
            response_payload.get("session")
            if isinstance(response_payload, dict)
            else response_payload
        )
        if not isinstance(session_payload, dict):
            raise TypeError(
                "get_or_create_session_with_handle response must include a session object"
            )
        return AgentSession(**session_payload)

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

    def send_a2a_request(
        self,
        *,
        caller_agent_session_uid: str | AgentSession,
        message: str | None = None,
        a2a_payload: dict[str, Any] | None = None,
        handle_unique_id: str | None = None,
        wait_for_runtime: bool = True,
        runtime_ready: dict[str, Any] | None = None,
        runtime_ready_timeout_seconds: float = 60,
        runtime_ready_poll_interval_seconds: float = 2,
        poll_task_until_stable: bool = True,
        timeout=None,
    ) -> dict[str, Any]:
        """
        Allocate or reuse a delegated target session and send an A2A request to it.

        This is the high-level runtime helper for agent-to-agent communication. It keeps
        runtime access tokens and readiness polling behind backend APIs.
        """
        allocation = self.allocate_a2a_target_session(
            caller_agent_session_uid=caller_agent_session_uid,
            handle_unique_id=handle_unique_id,
            timeout=timeout,
        )
        session_payload = allocation.get("session")
        allocated_session_uid = allocation.get("agent_session_uid")
        if not allocated_session_uid and isinstance(session_payload, dict):
            allocated_session_uid = session_payload.get("uid")
        if not allocated_session_uid:
            raise TypeError("A2A allocation response did not include agent_session_uid")

        chat = AgentSession.send_a2a_chat(
            str(allocated_session_uid),
            message=message,
            a2a_payload=a2a_payload,
            wait_for_runtime=wait_for_runtime,
            runtime_ready=runtime_ready,
            runtime_ready_timeout_seconds=runtime_ready_timeout_seconds,
            runtime_ready_poll_interval_seconds=runtime_ready_poll_interval_seconds,
            poll_task_until_stable=poll_task_until_stable,
            timeout=timeout,
        )
        chat_payload = chat.model_dump(mode="json")
        return {
            "handle_unique_id": allocation.get("handle_unique_id"),
            "agent_session_uid": str(allocated_session_uid),
            "allocation_state": allocation.get("allocation_state"),
            "allocation": allocation,
            "chat": chat_payload,
            "ready": chat_payload.get("ready"),
            "response": chat_payload.get("response"),
            "normalized": chat_payload.get("normalized"),
        }


class UserOrchestratorAgentService(BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/v1/user-orchestrator-agent-services"

    uid: str | None = Field(None, description="Public UID of the orchestrator agent service.")
    agent_uid: str | None = Field(None, description="Public UID of the resolved astro Agent.")
    user_uid: str | None = Field(None, description="Public UID of the owning user.")
    is_ready: bool = Field(False, description="Whether the service runtime is routable.")
    automatic_deployment: bool = Field(
        False,
        description="Whether this coding-agent service is eligible for automatic deployment flows.",
    )
    orchestrator_image_has_drift: bool = Field(
        False, description="Whether the orchestrator image is stale."
    )
    related_job: Any | None = Field(
        None, description="Backing job payload or UID when returned by the backend."
    )
    knative_service_runtime: Any | None = Field(
        None, description="Backing Knative service runtime payload."
    )
    subdomain: str = Field("", description="Public subdomain for the service.")


class UserProjectExecutorAgentService(BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/v1/project-executor-agent-services"

    uid: str | None = Field(None, description="Public UID of the project executor service.")
    agent_uid: str | None = Field(None, description="Public UID of the resolved executor Agent.")
    is_ready: bool = Field(False, description="Whether the executor runtime is currently ready.")
    automatic_deployment: bool = Field(
        False,
        description="Whether this coding-agent service is eligible for automatic deployment flows.",
    )
    image_drift: dict[str, Any] | None = Field(
        None, description="Executor image drift status payload."
    )
    project: Any | None = Field(
        None, description="Owning project payload or UID when returned by the backend."
    )
    related_job: Any | None = Field(
        None, description="Backing job payload or UID when returned by the backend."
    )
    knative_service_runtime: Any | None = Field(
        None, description="Backing Knative service runtime payload."
    )
    subdomain: str = Field("", description="Public subdomain for the service.")


class AgentSession(BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/v1/sessions"
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]] | None] = {
        "uid": ["exact", "in"],
        "agent_uid": ["exact", "in"],
        "status": ["exact"],
        "search": ["exact"],
        "q": ["exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "uid": "uid",
        "uid__in": "uid",
        "agent_uid": "uid",
        "agent_uid__in": "uid",
        "status": "str",
        "search": "str",
        "q": "str",
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

    @classmethod
    def wait_until_runtime_ready(
        cls,
        agent_session: str | AgentSession,
        *,
        timeout_seconds: float = 60,
        poll_interval_seconds: float = 2,
        timeout=None,
    ) -> AgentSessionRuntimeReady:
        """
        Wait for a session runtime to become routable through the backend readiness probe.

        Hits:
            POST <object_url>/<session_uid>/runtime_ready/
        """
        session_uid = cls._resolve_agent_session_uid(agent_session)
        timeout_seconds = float(timeout_seconds)
        poll_interval_seconds = float(poll_interval_seconds)
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than 0")
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be greater than 0")

        body = {
            "timeout_seconds": timeout_seconds,
            "poll_interval_seconds": poll_interval_seconds,
        }
        payload = {"json": serialize_to_json(body)}
        url = f"{cls.get_object_url()}/{session_uid}/runtime_ready/"
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload=payload,
            time_out=timeout,
        )
        if response.status_code not in (200, 504):
            raise_for_response(response, payload=payload)
        return AgentSessionRuntimeReady(**response.json())

    @classmethod
    def send_a2a_chat(
        cls,
        agent_session: str | AgentSession,
        *,
        message: str | None = None,
        a2a_payload: dict[str, Any] | None = None,
        wait_for_runtime: bool = True,
        runtime_ready: dict[str, Any] | None = None,
        runtime_ready_timeout_seconds: float = 60,
        runtime_ready_poll_interval_seconds: float = 2,
        poll_task_until_stable: bool = True,
        timeout=None,
    ) -> AgentSessionA2AChatResponse:
        """
        Send an A2A request to a session through the backend-managed runtime transport.

        Hits:
            POST <object_url>/<session_uid>/a2a_chat/
        """
        session_uid = cls._resolve_agent_session_uid(agent_session)
        normalized_message = None if message is None else str(message)
        if normalized_message is not None and not normalized_message.strip():
            raise ValueError("message must not be empty")
        if (normalized_message is None) == (a2a_payload is None):
            raise ValueError("Pass exactly one of message or a2a_payload")
        if a2a_payload is not None and not isinstance(a2a_payload, dict):
            raise TypeError("a2a_payload must be a JSON object")

        body: dict[str, Any] = {
            "wait_for_runtime": bool(wait_for_runtime),
            "poll_task_until_stable": bool(poll_task_until_stable),
        }
        if normalized_message is not None:
            body["message"] = normalized_message
        else:
            body["a2a_payload"] = a2a_payload

        if runtime_ready is None and wait_for_runtime:
            runtime_ready = {
                "timeout_seconds": float(runtime_ready_timeout_seconds),
                "poll_interval_seconds": float(runtime_ready_poll_interval_seconds),
            }
        if runtime_ready is not None:
            body["runtime_ready"] = runtime_ready

        payload = {"json": serialize_to_json(body)}
        url = f"{cls.get_object_url()}/{session_uid}/a2a_chat/"
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
        return AgentSessionA2AChatResponse(**response.json())

    uid: str | None = Field(None, description="Public UID of the agent session.")
    agent_uid: str | None = Field(
        None, description="Public UID of the agent definition used for this session."
    )
    created_by_user_uid: str | None = Field(
        None, description="Public UID of the actor who created the session."
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
    bound_handle: dict[str, Any] | None = Field(
        None,
        description="Agent session handle currently bound to this session, if any.",
    )


__all__ = [
    "Agent",
    "AgentRuntimeImageDrift",
    "AgentRuntimeImageDriftCheck",
    "AgentSemanticSearchResult",
    "AgentSessionA2AChatResponse",
    "AgentSessionA2ANormalizedResponse",
    "AgentSessionRuntimeReady",
    "AgentSessionRuntimeAccess",
    "UserOrchestratorAgentService",
    "UserProjectExecutorAgentService",
    "AgentSession",
    "AgentSessionStatus",
]
