from __future__ import annotations

import datetime
import json
import time
import uuid
from enum import Enum
from typing import Any, ClassVar

import requests
from pydantic import ConfigDict, Field

from .base import BaseObjectOrm, BasePydanticModel, ShareableObjectMixin
from .exceptions import ApiError, raise_for_response
from .utils import make_request, serialize_to_json

DEFAULT_AGENT_RUNTIME_READY_TIMEOUT_SECONDS = 300.0
DEFAULT_AGENT_RUNTIME_READY_POLL_INTERVAL_SECONDS = 10.0
DEFAULT_AGENT_RUNTIME_READY_REQUEST_TIMEOUT = (5.0, 10.0)
DEFAULT_AGENT_SESSION_LONG_REQUEST_TIMEOUT = (5.0, None)
DEFAULT_AGENT_RUNTIME_ACCESS_CACHE_TTL_SECONDS = 60.0
DEFAULT_AGENT_RUNTIME_ACCESS_CACHE_EXPIRY_SKEW_SECONDS = 30.0
STANDARD_A2A_MESSAGE_SEND_PATH = "/api/a2a/v1/message:send"
STANDARD_A2A_CONTENT_TYPE = "application/a2a+json"
STANDARD_A2A_OUTPUT_CONTRACT_METADATA_KEY = (
    "https://mainsequence.ai/a2a/extensions/output-contract/v1"
)


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
    agent_kind: str = Field("", description="Runtime family that produced the drift payload.")
    available: bool = Field(True, description="Whether drift information could be resolved.")
    has_drift: bool = Field(
        False, description="Whether any included drift check is currently drifting."
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


class AgentRuntimePaths(BasePydanticModel):
    model_config = ConfigDict(extra="allow")

    chat: str | None = Field(None, description="Runtime-relative deprecated A2A chat path.")
    session: str | None = Field(None, description="Runtime-relative session path.")
    cancel: str | None = Field(None, description="Runtime-relative turn cancellation path.")
    detach: str | None = Field(None, description="Runtime-relative detach path.")


class AgentSessionRuntimeAccess(BasePydanticModel):
    model_config = ConfigDict(extra="allow")

    coding_agent_service_id: str | None = Field(
        None,
        description="Identifier of the backing coding-agent service that owns the runtime.",
    )
    coding_agent_id: str | None = Field(
        None,
        description="Stable coding-agent routing identifier presented to the gateway.",
    )
    mode: str = Field(
        "token",
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
    runtime_paths: AgentRuntimePaths = Field(
        default_factory=AgentRuntimePaths,
        description="Runtime-relative paths returned by the control plane.",
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
        description="Last runtime status code observed by the SDK readiness poll.",
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
    raw: Any = Field(
        default_factory=dict,
        description="Backend-provided raw normalization metadata or raw transport content.",
    )
    events: list[dict[str, Any]] = Field(
        default_factory=list,
        description="Parsed A2A/SSE events when the backend returned a raw event stream.",
    )


class AgentSessionA2AChatResponse(BasePydanticModel):
    model_config = ConfigDict(extra="allow")

    ok: bool = Field(..., description="Whether the backend A2A transport completed successfully.")
    ready: AgentSessionRuntimeReady | None = Field(
        None,
        description="Runtime readiness result when wait_for_runtime was enabled.",
    )
    response: Any = Field(
        default_factory=dict,
        description="Raw A2A JSON-RPC response or raw transport content returned by the target runtime.",
    )
    normalized: AgentSessionA2ANormalizedResponse | None = Field(
        None,
        description="Backend-normalized A2A response summary.",
    )


def _parse_sse_events(raw_sse: str) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    current: dict[str, Any] = {}
    data_lines: list[str] = []

    def _flush() -> None:
        nonlocal current, data_lines
        if not current and not data_lines:
            return
        event = dict(current)
        if data_lines:
            data_text = "\n".join(data_lines)
            if data_text == "[DONE]":
                event["done"] = True
                event["data"] = "[DONE]"
            else:
                try:
                    event["data"] = json.loads(data_text)
                except Exception:
                    event["data"] = data_text
        events.append(event)
        current = {}
        data_lines = []

    for raw_line in raw_sse.splitlines():
        line = raw_line.rstrip("\r")
        if line == "":
            _flush()
            continue
        if line.startswith(":"):
            continue
        if ":" in line:
            field, value = line.split(":", 1)
            value = value[1:] if value.startswith(" ") else value
        else:
            field, value = line, ""
        if field == "data":
            data_lines.append(value)
        elif field in {"id", "event", "retry"}:
            current[field] = value
    _flush()
    return events


def _extract_text_from_sse_events(events: list[dict[str, Any]]) -> str:
    chunks: list[str] = []
    for event in events:
        data = event.get("data")
        if not isinstance(data, dict):
            continue
        text_delta = data.get("textDelta")
        if isinstance(text_delta, str):
            chunks.append(text_delta)
            continue
        text = data.get("text")
        if isinstance(text, str):
            chunks.append(text)
            continue
        result = data.get("result")
        if isinstance(result, dict):
            parts = result.get("parts")
            if isinstance(parts, list):
                for part in parts:
                    if isinstance(part, dict) and isinstance(part.get("text"), str):
                        chunks.append(part["text"])
    return "".join(chunks)


def _looks_like_sse_payload(value: object) -> bool:
    if not isinstance(value, str):
        return False
    return "\ndata:" in value or value.startswith("data:") or "\nevent:" in value


def _normalize_a2a_chat_response_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized_payload = dict(payload)
    response_value = normalized_payload.get("response")
    normalized = normalized_payload.get("normalized")
    if not isinstance(normalized, dict):
        normalized = {}

    raw_sse = response_value if _looks_like_sse_payload(response_value) else None
    if raw_sse is None:
        raw_value = normalized.get("raw")
        raw_sse = raw_value if _looks_like_sse_payload(raw_value) else None
    if raw_sse is None:
        text_value = normalized.get("text")
        raw_sse = text_value if _looks_like_sse_payload(text_value) else None
    if raw_sse is None:
        return normalized_payload

    events = _parse_sse_events(raw_sse)
    extracted_text = _extract_text_from_sse_events(events)
    normalized_payload["response"] = {
        "kind": "sse",
        "raw": raw_sse,
        "events": events,
    }
    normalized["events"] = events
    normalized["raw"] = {
        "kind": "sse",
        "raw": raw_sse,
        "events": events,
    }
    if extracted_text:
        normalized["text"] = extracted_text
    normalized_payload["normalized"] = normalized
    return normalized_payload


def _join_runtime_url(rpc_url: str, runtime_path: str) -> str:
    if not isinstance(rpc_url, str) or not rpc_url.strip():
        raise ApiError("Runtime access response is missing rpc_url.")
    if not isinstance(runtime_path, str) or not runtime_path.strip():
        raise ApiError("Runtime access response is missing a runtime path.")
    if runtime_path.startswith(("http://", "https://")):
        return runtime_path
    return f"{rpc_url.rstrip('/')}/{runtime_path.lstrip('/')}"


def _parse_json_text(value: str) -> Any | None:
    stripped = value.strip()
    if not stripped:
        return None
    try:
        return json.loads(stripped)
    except Exception:
        return None


def _normalize_runtime_chat_response(
    *,
    agent_session_uid: str,
    response: requests.Response,
) -> dict[str, Any]:
    content_type = (response.headers.get("Content-Type") or "").lower()
    if "application/json" in content_type:
        payload = response.json()
        text = ""
        parsed_json = payload
        if isinstance(payload, dict):
            normalized = payload.get("normalized")
            if isinstance(normalized, dict) and isinstance(normalized.get("text"), str):
                text = normalized["text"]
                parsed_json = _parse_json_text(text)
            elif isinstance(payload.get("text"), str):
                text = payload["text"]
                parsed_json = _parse_json_text(text)
        return {
            "ok": True,
            "agent_session_uid": agent_session_uid,
            "status_code": response.status_code,
            "events": [],
            "text": text,
            "json": parsed_json,
            "response": payload,
        }

    raw = response.text
    events = _parse_sse_events(raw)
    text = _extract_text_from_sse_events(events)
    parsed_json = _parse_json_text(text) if text else None
    return {
        "ok": True,
        "agent_session_uid": agent_session_uid,
        "status_code": response.status_code,
        "events": events,
        "text": text,
        "json": parsed_json,
        "response": {
            "kind": "sse",
            "raw": raw,
            "events": events,
            "text": text,
            "json": parsed_json,
        },
    }


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
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional backend metadata for the agent.",
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
    _RUNTIME_ACCESS_CACHE: ClassVar[dict[str, tuple[float | None, AgentSessionRuntimeAccess]]] = {}
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
    def _coerce_runtime_access(
        cls,
        runtime_access: AgentSessionRuntimeAccess | dict[str, Any],
    ) -> AgentSessionRuntimeAccess:
        if isinstance(runtime_access, AgentSessionRuntimeAccess):
            access = runtime_access
        elif isinstance(runtime_access, dict):
            access = AgentSessionRuntimeAccess(**runtime_access)
        else:
            raise TypeError("runtime_access must be an AgentSessionRuntimeAccess or dict")
        return access

    @staticmethod
    def _parse_runtime_access_expires_at(expires_at: str | None) -> float | None:
        if not expires_at:
            return None
        raw = str(expires_at).strip()
        if not raw:
            return None
        if raw.endswith("Z"):
            raw = f"{raw[:-1]}+00:00"
        try:
            parsed = datetime.datetime.fromisoformat(raw)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=datetime.UTC)
        return parsed.timestamp()

    @classmethod
    def _runtime_access_cache_expiry_epoch(
        cls,
        access: AgentSessionRuntimeAccess,
    ) -> float | None:
        expires_at_epoch = cls._parse_runtime_access_expires_at(access.expires_at)
        if expires_at_epoch is not None:
            return max(
                time.time(),
                expires_at_epoch - DEFAULT_AGENT_RUNTIME_ACCESS_CACHE_EXPIRY_SKEW_SECONDS,
            )
        return time.time() + DEFAULT_AGENT_RUNTIME_ACCESS_CACHE_TTL_SECONDS

    @classmethod
    def cache_runtime_access(
        cls,
        agent_session: str | AgentSession,
        runtime_access: AgentSessionRuntimeAccess | dict[str, Any],
    ) -> AgentSessionRuntimeAccess:
        session_uid = cls._resolve_agent_session_uid(agent_session)
        access = cls._coerce_runtime_access(runtime_access)
        cls._RUNTIME_ACCESS_CACHE[session_uid] = (
            cls._runtime_access_cache_expiry_epoch(access),
            access,
        )
        return access

    @classmethod
    def get_cached_runtime_access(
        cls,
        agent_session: str | AgentSession,
    ) -> AgentSessionRuntimeAccess | None:
        session_uid = cls._resolve_agent_session_uid(agent_session)
        cached = cls._RUNTIME_ACCESS_CACHE.get(session_uid)
        if cached is None:
            return None
        expires_at_epoch, access = cached
        if expires_at_epoch is not None and expires_at_epoch <= time.time():
            cls._RUNTIME_ACCESS_CACHE.pop(session_uid, None)
            return None
        return access

    @classmethod
    def clear_cached_runtime_access(
        cls,
        agent_session: str | AgentSession,
    ) -> None:
        session_uid = cls._resolve_agent_session_uid(agent_session)
        cls._RUNTIME_ACCESS_CACHE.pop(session_uid, None)

    @classmethod
    def _require_runtime_access(
        cls,
        agent_session: str | AgentSession,
        runtime_access: AgentSessionRuntimeAccess | dict[str, Any] | None,
    ) -> AgentSessionRuntimeAccess:
        if runtime_access is not None:
            return cls.cache_runtime_access(agent_session, runtime_access)
        cached = cls.get_cached_runtime_access(agent_session)
        if cached is None:
            raise ApiError(
                "Runtime access has not been resolved for this agent session. "
                "Call AgentSession.resolve_runtime_access(...) first and reuse the returned access."
            )
        return cached

    @classmethod
    def _resolve_runtime_access_for_message_send(
        cls,
        agent_session: str | AgentSession,
        *,
        timeout=None,
    ) -> AgentSessionRuntimeAccess:
        session_uid = cls._resolve_agent_session_uid(agent_session)
        cached = cls.get_cached_runtime_access(session_uid)
        if cached is not None:
            return cached
        return cls.resolve_runtime_access(
            session_uid,
            wait_for_runtime=False,
            cache=True,
            timeout=timeout,
        )

    @classmethod
    def _resolve_runtime_access_for_chat(
        cls,
        agent_session: str | AgentSession,
        runtime_access: AgentSessionRuntimeAccess | dict[str, Any] | None,
        *,
        wait_for_runtime: bool,
        timeout=None,
    ) -> AgentSessionRuntimeAccess:
        session_uid = cls._resolve_agent_session_uid(agent_session)
        if runtime_access is not None:
            access = cls._coerce_runtime_access(runtime_access)
            if not wait_for_runtime or access.is_ready:
                return cls.cache_runtime_access(session_uid, access)
            cls.clear_cached_runtime_access(session_uid)
            return cls.resolve_runtime_access(
                session_uid,
                wait_for_runtime=True,
                timeout=timeout,
            )

        cached = cls.get_cached_runtime_access(session_uid)
        if cached is not None:
            if not wait_for_runtime or cached.is_ready:
                return cached
            cls.clear_cached_runtime_access(session_uid)

        return cls.resolve_runtime_access(
            session_uid,
            wait_for_runtime=wait_for_runtime,
            timeout=timeout,
        )

    @staticmethod
    def _build_runtime_chat_body(
        *,
        message: str | None = None,
        a2a_payload: dict[str, Any] | None = None,
        poll_task_until_stable: bool | None = None,
        runtime_turn_timeout_seconds: float | None = None,
        omit_reasoning: bool | None = None,
        response_format: dict[str, Any] | None = None,
        json_repair: dict[str, Any] | None = None,
        runtime_request_fields: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        body = dict(runtime_request_fields or {})
        normalized_message = None if message is None else str(message)
        if normalized_message is not None and not normalized_message.strip():
            raise ValueError("message must not be empty")
        if normalized_message is not None and a2a_payload is not None:
            raise ValueError("Pass exactly one of message or a2a_payload")
        if a2a_payload is not None and not isinstance(a2a_payload, dict):
            raise TypeError("a2a_payload must be a JSON object")
        if runtime_turn_timeout_seconds is not None and runtime_turn_timeout_seconds <= 0:
            raise ValueError("runtime_turn_timeout_seconds must be greater than 0")
        if response_format is not None and not isinstance(response_format, dict):
            raise TypeError("response_format must be a JSON object")
        if json_repair is not None and not isinstance(json_repair, dict):
            raise TypeError("json_repair must be a JSON object")

        if normalized_message is not None:
            body["message"] = normalized_message
        elif a2a_payload is not None:
            body["a2a_payload"] = a2a_payload
        elif "message" not in body and "a2a_payload" not in body:
            raise ValueError("Pass exactly one of message or a2a_payload")

        if poll_task_until_stable is not None:
            body["poll_task_until_stable"] = bool(poll_task_until_stable)
        if runtime_turn_timeout_seconds is not None:
            body["runtime_turn_timeout_seconds"] = runtime_turn_timeout_seconds
        if omit_reasoning is not None:
            body["omit_reasoning"] = bool(omit_reasoning)
        if response_format is not None:
            body["response_format"] = response_format
        if json_repair is not None:
            body["json_repair"] = json_repair
        return body

    @classmethod
    def _runtime_url(cls, access: AgentSessionRuntimeAccess, path_name: str) -> str:
        runtime_paths = access.runtime_paths
        runtime_path = getattr(runtime_paths, path_name, None)
        if not runtime_path:
            raise ApiError(f"Runtime access response is missing runtime_paths.{path_name}.")
        return _join_runtime_url(access.rpc_url, runtime_path)

    @classmethod
    def _post_runtime(
        cls,
        access: AgentSessionRuntimeAccess,
        *,
        path_name: str,
        body: dict[str, Any],
        accept: str,
        timeout=None,
    ) -> requests.Response:
        url = cls._runtime_url(access, path_name)
        request_timeout = DEFAULT_AGENT_SESSION_LONG_REQUEST_TIMEOUT if timeout is None else timeout
        headers = {
            "Authorization": f"Bearer {access.token}",
            "Content-Type": "application/json",
            "Accept": accept,
        }
        return requests.post(
            url,
            headers=headers,
            json=serialize_to_json(body),
            timeout=request_timeout,
        )

    @classmethod
    def _get_runtime(
        cls,
        access: AgentSessionRuntimeAccess,
        *,
        path_name: str,
        accept: str = "application/json",
        timeout=None,
    ) -> requests.Response:
        url = cls._runtime_url(access, path_name)
        request_timeout = DEFAULT_AGENT_RUNTIME_READY_REQUEST_TIMEOUT if timeout is None else timeout
        headers = {
            "Authorization": f"Bearer {access.token}",
            "Accept": accept,
        }
        return requests.get(
            url,
            headers=headers,
            timeout=request_timeout,
        )

    @staticmethod
    def _runtime_json_payload(response: requests.Response, *, payload: dict[str, Any]) -> dict[str, Any]:
        if not (200 <= response.status_code < 300):
            raise_for_response(response, payload=payload)
        try:
            data = response.json()
        except Exception as exc:
            raise ApiError(
                "Runtime response must be a JSON object.",
                response=response,
                payload=payload,
            ) from exc
        if not isinstance(data, dict):
            raise TypeError("Runtime response must be a JSON object")
        return data

    @staticmethod
    def _runtime_status_is_ready(payload: dict[str, Any]) -> bool:
        runner = payload.get("runner")
        preflight = payload.get("preflight")
        return (
            payload.get("ok") is True
            and payload.get("state") == "ready"
            and isinstance(runner, dict)
            and runner.get("ready") is True
            and isinstance(preflight, dict)
            and preflight.get("ready") is True
        )

    @staticmethod
    def _runtime_status_failure_detail(payload: dict[str, Any]) -> str | None:
        last_error = payload.get("last_error")
        if last_error:
            return str(last_error)
        state = payload.get("state")
        if state in {"failed", "error", "detached"}:
            return f"Runtime session entered terminal state: {state}"
        return None

    @staticmethod
    def _runtime_status_response_payload(response: requests.Response) -> dict[str, Any]:
        if response.status_code in (503, 504):
            try:
                data = response.json()
                if isinstance(data, dict):
                    return data
            except Exception:
                pass
            return {
                "ok": False,
                "state": "starting",
                "detail": getattr(response, "text", "") or "runtime not ready",
            }
        if not (200 <= response.status_code < 300):
            raise_for_response(response, payload={})
        try:
            data = response.json()
        except Exception as exc:
            raise ApiError("Runtime status response must be a JSON object.") from exc
        if not isinstance(data, dict):
            raise TypeError("Runtime status response must be a JSON object")
        return data

    @classmethod
    def wait_for_runtime_access_ready(
        cls,
        runtime_access: AgentSessionRuntimeAccess | dict[str, Any],
        *,
        timeout_seconds: float = DEFAULT_AGENT_RUNTIME_READY_TIMEOUT_SECONDS,
        poll_interval_seconds: float = DEFAULT_AGENT_RUNTIME_READY_POLL_INTERVAL_SECONDS,
        timeout=None,
    ) -> AgentSessionRuntimeReady:
        access = cls._coerce_runtime_access(runtime_access)
        timeout_seconds = float(timeout_seconds)
        poll_interval_seconds = float(poll_interval_seconds)
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than 0")
        if poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be greater than 0")

        started_at = time.monotonic()
        deadline = started_at + timeout_seconds
        attempts = 0
        last_status_code: int | None = None
        last_detail = "runtime not ready"

        while True:
            attempts += 1
            try:
                response = cls._get_runtime(access, path_name="session", timeout=timeout)
                last_status_code = response.status_code
                payload = cls._runtime_status_response_payload(response)
            except requests.RequestException as exc:
                payload = {
                    "ok": False,
                    "state": "starting",
                    "detail": str(exc) or "runtime status request failed",
                }
            elapsed = time.monotonic() - started_at

            if cls._runtime_status_is_ready(payload):
                return AgentSessionRuntimeReady(
                    ready=True,
                    attempts=attempts,
                    elapsed_seconds=round(elapsed, 3),
                    status_code=last_status_code,
                    detail="Runtime session is ready.",
                    status=payload,
                )

            failure_detail = cls._runtime_status_failure_detail(payload)
            if failure_detail is not None:
                raise ApiError(failure_detail)

            detail = payload.get("detail") or payload.get("message")
            if isinstance(detail, str) and detail.strip():
                last_detail = detail

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return AgentSessionRuntimeReady(
                    ready=False,
                    attempts=attempts,
                    elapsed_seconds=round(time.monotonic() - started_at, 3),
                    status_code=last_status_code,
                    detail=last_detail,
                )
            time.sleep(min(poll_interval_seconds, remaining))

    @staticmethod
    def _build_standard_a2a_message_send_body(
        *,
        agent_session_uid: str,
        message: str,
        message_id: str | None = None,
        strict_dictionary: bool = False,
        json_repair_attempts: int = 3,
        return_immediately: bool = False,
    ) -> dict[str, Any]:
        normalized_message = str(message)
        if not normalized_message.strip():
            raise ValueError("message must not be empty")
        if json_repair_attempts < 1:
            raise ValueError("json_repair_attempts must be greater than 0")
        normalized_message_id = str(message_id).strip() if message_id is not None else ""
        if not normalized_message_id:
            normalized_message_id = f"msg-{uuid.uuid4()}"

        accepted_output_modes = ["application/json"] if strict_dictionary else ["text/plain"]
        body: dict[str, Any] = {
            "message": {
                "messageId": normalized_message_id,
                "role": "ROLE_USER",
                "contextId": agent_session_uid,
                "parts": [{"text": normalized_message}],
            },
            "configuration": {
                "acceptedOutputModes": accepted_output_modes,
                "returnImmediately": bool(return_immediately),
            },
        }
        if strict_dictionary:
            body["metadata"] = {
                STANDARD_A2A_OUTPUT_CONTRACT_METADATA_KEY: {
                    "response_format": {
                        "type": "dictionary",
                        "strict": True,
                    },
                    "jsonRepairAttempts": int(json_repair_attempts),
                }
            }
        return body

    @classmethod
    def _post_standard_a2a_message(
        cls,
        access: AgentSessionRuntimeAccess,
        *,
        body: dict[str, Any],
        timeout=None,
    ) -> requests.Response:
        url = _join_runtime_url(access.rpc_url, STANDARD_A2A_MESSAGE_SEND_PATH)
        request_timeout = DEFAULT_AGENT_SESSION_LONG_REQUEST_TIMEOUT if timeout is None else timeout
        headers = {
            "Authorization": f"Bearer {access.token}",
            "Content-Type": STANDARD_A2A_CONTENT_TYPE,
            "Accept": STANDARD_A2A_CONTENT_TYPE,
        }
        return requests.post(
            url,
            headers=headers,
            data=json.dumps(serialize_to_json(body)),
            timeout=request_timeout,
        )

    @staticmethod
    def extract_a2a_message_text(payload: dict[str, Any]) -> str:
        message = payload.get("message")
        if not isinstance(message, dict):
            return ""
        parts = message.get("parts")
        if not isinstance(parts, list):
            return ""
        chunks: list[str] = []
        for part in parts:
            if isinstance(part, dict) and isinstance(part.get("text"), str):
                chunks.append(part["text"])
        return "".join(chunks)

    @classmethod
    def send_a2a_message(
        cls,
        agent_session: str | AgentSession,
        *,
        message: str,
        message_id: str | None = None,
        strict_dictionary: bool = False,
        json_repair_attempts: int = 3,
        return_immediately: bool = False,
        timeout=None,
    ) -> dict[str, Any]:
        """
        Send one standard A2A message to the runtime for this agent session.
        """
        session_uid = cls._resolve_agent_session_uid(agent_session)
        body = cls._build_standard_a2a_message_send_body(
            agent_session_uid=session_uid,
            message=message,
            message_id=message_id,
            strict_dictionary=strict_dictionary,
            json_repair_attempts=json_repair_attempts,
            return_immediately=return_immediately,
        )
        access = cls._resolve_runtime_access_for_message_send(session_uid, timeout=timeout)
        response = cls._post_standard_a2a_message(
            access,
            body=body,
            timeout=timeout,
        )
        if response.status_code in (401, 403):
            cls.clear_cached_runtime_access(session_uid)
            access = cls.resolve_runtime_access(
                session_uid,
                wait_for_runtime=False,
                cache=True,
                timeout=timeout,
            )
            response = cls._post_standard_a2a_message(
                access,
                body=body,
                timeout=timeout,
            )
            if response.status_code in (401, 403):
                cls.clear_cached_runtime_access(session_uid)
        if not (200 <= response.status_code < 300):
            raise_for_response(response, payload=body)
        try:
            payload = response.json()
        except Exception as exc:
            raise ApiError(
                "Standard A2A response must be a JSON object.",
                response=response,
                payload=body,
            ) from exc
        if not isinstance(payload, dict):
            raise TypeError("Standard A2A response must be a JSON object")
        return payload

    @classmethod
    def a2a_chat(
        cls,
        agent_session: str | AgentSession,
        *,
        message: str | None = None,
        a2a_payload: dict[str, Any] | None = None,
        runtime_access: AgentSessionRuntimeAccess | dict[str, Any] | None = None,
        wait_for_runtime: bool = True,
        poll_task_until_stable: bool | None = None,
        runtime_turn_timeout_seconds: float | None = None,
        omit_reasoning: bool | None = None,
        response_format: dict[str, Any] | None = None,
        json_repair: dict[str, Any] | None = None,
        timeout=None,
        **runtime_request_fields: Any,
    ) -> dict[str, Any]:
        """
        Resolve runtime access when needed and send one A2A chat request directly
        to the runtime.
        """
        session_uid = cls._resolve_agent_session_uid(agent_session)
        body = cls._build_runtime_chat_body(
            message=message,
            a2a_payload=a2a_payload,
            poll_task_until_stable=poll_task_until_stable,
            runtime_turn_timeout_seconds=runtime_turn_timeout_seconds,
            omit_reasoning=omit_reasoning,
            response_format=response_format,
            json_repair=json_repair,
            runtime_request_fields=runtime_request_fields,
        )
        access = cls._resolve_runtime_access_for_chat(
            session_uid,
            runtime_access,
            wait_for_runtime=wait_for_runtime,
            timeout=timeout,
        )
        response = cls._post_runtime(
            access,
            path_name="chat",
            body=body,
            accept="text/event-stream",
            timeout=timeout,
        )
        if response.status_code == 401:
            cls.clear_cached_runtime_access(session_uid)
        if not (200 <= response.status_code < 300):
            raise_for_response(response, payload=body)
        return _normalize_runtime_chat_response(
            agent_session_uid=session_uid,
            response=response,
        )

    @classmethod
    def chat_runtime(
        cls,
        agent_session: str | AgentSession,
        *,
        message: str,
        runtime_access: AgentSessionRuntimeAccess | dict[str, Any] | None = None,
        wait_for_runtime: bool = True,
        runtime_turn_timeout_seconds: float | None = None,
        omit_reasoning: bool | None = True,
        response_format: dict[str, Any] | None = None,
        json_repair: dict[str, Any] | None = None,
        timeout=None,
        **runtime_request_fields: Any,
    ) -> dict[str, Any]:
        """
        Send one message through the resolved A2A runtime chat stream.
        """
        return cls.a2a_chat(
            agent_session,
            message=message,
            runtime_access=runtime_access,
            wait_for_runtime=wait_for_runtime,
            runtime_turn_timeout_seconds=runtime_turn_timeout_seconds,
            omit_reasoning=omit_reasoning,
            response_format=response_format,
            json_repair=json_repair,
            timeout=timeout,
            **runtime_request_fields,
        )

    @classmethod
    def cancel_runtime(
        cls,
        agent_session: str | AgentSession,
        *,
        reason: str = "client_requested",
        message: str | None = None,
        timeout=None,
    ) -> dict[str, Any]:
        """
        Cancel the active turn on the resolved A2A runtime.
        """
        session_uid = cls._resolve_agent_session_uid(agent_session)
        body = {"reason": reason}
        if message is not None:
            body["message"] = message
        access = cls._require_runtime_access(session_uid, None)
        response = cls._post_runtime(
            access,
            path_name="cancel",
            body=body,
            accept="application/json",
            timeout=timeout,
        )
        if response.status_code == 401:
            cls.clear_cached_runtime_access(session_uid)
        return cls._runtime_json_payload(response, payload=body)

    @classmethod
    def detach_runtime(
        cls,
        agent_session: str | AgentSession,
        *,
        reason: str = "client_done",
        timeout=None,
    ) -> dict[str, Any]:
        """
        Detach the resolved A2A runtime for one agent session.
        """
        session_uid = cls._resolve_agent_session_uid(agent_session)
        body = {"reason": reason}
        access = cls._require_runtime_access(session_uid, None)
        response = cls._post_runtime(
            access,
            path_name="detach",
            body=body,
            accept="application/json",
            timeout=timeout,
        )
        if response.status_code == 401:
            cls.clear_cached_runtime_access(session_uid)
        return cls._runtime_json_payload(response, payload=body)

    @classmethod
    def resolve_runtime_access(
        cls,
        agent_session: str | AgentSession,
        *,
        wait_for_runtime: bool = False,
        cache: bool = True,
        runtime_ready_timeout_seconds: float = DEFAULT_AGENT_RUNTIME_READY_TIMEOUT_SECONDS,
        poll_interval_seconds: float = DEFAULT_AGENT_RUNTIME_READY_POLL_INTERVAL_SECONDS,
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

        body: dict[str, Any] = {}
        payload: dict[str, Any] = {"json": serialize_to_json(body)}
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
        data = response.json()
        if not isinstance(data, dict):
            raise TypeError("Agent session runtime access response must be a JSON object")
        access = AgentSessionRuntimeAccess(**data)
        if wait_for_runtime:
            ready = cls.wait_for_runtime_access_ready(
                access,
                timeout_seconds=runtime_ready_timeout_seconds,
                poll_interval_seconds=poll_interval_seconds,
                timeout=timeout,
            )
            if not ready.ready:
                raise ApiError(ready.detail or "Runtime session did not become ready.")
            access.is_ready = True
        if cache:
            cls.cache_runtime_access(session_uid, access)
        return access

    @classmethod
    def wait_until_runtime_ready(
        cls,
        agent_session: str | AgentSession,
        *,
        runtime_access: AgentSessionRuntimeAccess | dict[str, Any] | None = None,
        timeout_seconds: float = DEFAULT_AGENT_RUNTIME_READY_TIMEOUT_SECONDS,
        poll_interval_seconds: float = DEFAULT_AGENT_RUNTIME_READY_POLL_INTERVAL_SECONDS,
        timeout=None,
    ) -> AgentSessionRuntimeReady:
        """
        Wait for a session runtime to become routable by polling the runtime.
        """
        session_uid = cls._resolve_agent_session_uid(agent_session)
        access = cls._require_runtime_access(session_uid, runtime_access)
        return cls.wait_for_runtime_access_ready(
            access,
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
            timeout=timeout,
        )

    @classmethod
    def send_a2a_chat(
        cls,
        agent_session: str | AgentSession,
        *,
        message: str | None = None,
        a2a_payload: dict[str, Any] | None = None,
        wait_for_runtime: bool = True,
        runtime_ready_timeout_seconds: float = DEFAULT_AGENT_RUNTIME_READY_TIMEOUT_SECONDS,
        poll_task_until_stable: bool | None = None,
        runtime_turn_timeout_seconds: float | None = None,
        omit_reasoning: bool | None = None,
        response_format: dict[str, Any] | None = None,
        json_repair: dict[str, Any] | None = None,
        timeout=None,
    ) -> AgentSessionA2AChatResponse:
        """
        Compatibility/debug helper for the backend A2A transport endpoint.

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
        if runtime_turn_timeout_seconds is not None and runtime_turn_timeout_seconds <= 0:
            raise ValueError("runtime_turn_timeout_seconds must be greater than 0")
        if response_format is not None and not isinstance(response_format, dict):
            raise TypeError("response_format must be a JSON object")
        if json_repair is not None and not isinstance(json_repair, dict):
            raise TypeError("json_repair must be a JSON object")

        body: dict[str, Any] = {
            "wait_for_runtime": bool(wait_for_runtime),
        }
        if poll_task_until_stable is not None:
            body["poll_task_until_stable"] = bool(poll_task_until_stable)
        if normalized_message is not None:
            body["message"] = normalized_message
        else:
            body["a2a_payload"] = a2a_payload

        if wait_for_runtime:
            body["runtime_ready_timeout_seconds"] = float(runtime_ready_timeout_seconds)
        if runtime_turn_timeout_seconds is not None:
            body["runtime_turn_timeout_seconds"] = float(runtime_turn_timeout_seconds)
        if omit_reasoning is not None:
            body["omit_reasoning"] = bool(omit_reasoning)
        if response_format is not None:
            body["response_format"] = response_format
        if json_repair is not None:
            body["json_repair"] = json_repair

        payload = {"json": serialize_to_json(body)}
        url = f"{cls.get_object_url()}/{session_uid}/a2a_chat/"
        request_timeout = DEFAULT_AGENT_SESSION_LONG_REQUEST_TIMEOUT if timeout is None else timeout
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload=payload,
            time_out=request_timeout,
        )
        if response.status_code != 200:
            raise_for_response(response, payload=payload)
        response_payload = response.json()
        if not isinstance(response_payload, dict):
            raise TypeError("a2a_chat response must be a JSON object")
        response_payload = _normalize_a2a_chat_response_payload(response_payload)
        return AgentSessionA2AChatResponse(**response_payload)

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
