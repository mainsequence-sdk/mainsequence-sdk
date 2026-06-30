from __future__ import annotations

import base64
import datetime
import json
import pathlib
import time
import uuid
from enum import Enum
from typing import Any, ClassVar

import requests
from pydantic import ConfigDict, Field

from .base import BaseObjectOrm, BasePydanticModel, ShareableObjectMixin
from .exceptions import ApiError, raise_for_response
from .utils import make_request, serialize_to_json

DEFAULT_AGENT_SESSION_LONG_REQUEST_TIMEOUT = (5.0, 900.0)
DEFAULT_AGENT_RUNTIME_ACCESS_CACHE_TTL_SECONDS = 60.0
DEFAULT_AGENT_RUNTIME_ACCESS_CACHE_EXPIRY_SKEW_SECONDS = 30.0
STANDARD_A2A_MESSAGE_SEND_PATH = "/api/a2a/v1/message:send"
STANDARD_A2A_CONTENT_TYPE = "application/a2a+json"
STANDARD_A2A_OUTPUT_CONTRACT_METADATA_KEY = (
    "https://mainsequence.ai/a2a/extensions/output-contract/v1"
)
MAX_INLINE_A2A_FILE_BYTES = 15 * 1024 * 1024


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

    pass


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


def _join_runtime_url(rpc_url: str, runtime_path: str) -> str:
    if not isinstance(rpc_url, str) or not rpc_url.strip():
        raise ApiError("Runtime access response is missing rpc_url.")
    if not isinstance(runtime_path, str) or not runtime_path.strip():
        raise ApiError("Runtime access response is missing a runtime path.")
    if runtime_path.startswith(("http://", "https://")):
        return runtime_path
    return f"{rpc_url.rstrip('/')}/{runtime_path.lstrip('/')}"


class Agent(ShareableObjectMixin, BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/v1/agents"
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]] | None] = {
        "uid": ["exact", "in"],
        "agent_type": ["exact"],
        "search": ["exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "uid": "uid",
        "uid__in": "uid",
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
            POST <detail_url>sessions/get_or_create_session/

        Request contract:
        - Send exactly one lookup key: `session_uid` or `handle_unique_id`.
        - `session_uid` resolves an existing AgentSession for this Agent.
        - `handle_unique_id` gets or creates a reusable session handle.
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

        url = f"{self.get_detail_url()}sessions/get_or_create_session/"
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


class CodingAgentService(BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "agents/v1/coding-agent-services"
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]] | None] = {
        "uid": ["exact", "in"],
        "agent_uid": ["exact"],
        "agent_type": ["exact"],
        "scope_kind": ["exact"],
        "user_uid": ["exact"],
        "project_uid": ["exact"],
        "automatic_deployment": ["exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "uid": "uid",
        "uid__in": "uid",
        "agent_uid": "uid",
        "agent_type": "str",
        "scope_kind": "str",
        "user_uid": "uid",
        "project_uid": "uid",
        "automatic_deployment": "bool",
    }

    uid: str | None = Field(None, description="Public UID of the coding-agent service.")
    agent_uid: str | None = Field(None, description="Public UID of the owning Agent.")
    agent_type: str | None = Field(
        None,
        description="Agent runtime type, such as astro-orchestrator or project-executor.",
    )
    scope: dict[str, Any] = Field(
        default_factory=dict,
        description="Typed scope projection, for example {kind: user, user_uid} or {kind: project, project_uid}.",
    )
    is_ready: bool = Field(False, description="Whether the service runtime is routable.")
    automatic_deployment: bool = Field(
        False,
        description="Whether this coding-agent service is eligible for automatic deployment flows.",
    )
    image_drift: dict[str, Any] | None = Field(
        None, description="Runtime image drift status payload."
    )
    related_job_uid: str | None = Field(
        None, description="Public UID of the backing job, if attached."
    )
    knative_service_runtime_uid: str | None = Field(
        None, description="Public UID of the backing Knative service runtime, if attached."
    )

    @classmethod
    def _post_collection_action(
        cls,
        action_name: str,
        body: dict[str, Any],
        *,
        timeout=None,
        expected_statuses: tuple[int, ...] = (200,),
    ) -> Any:
        payload = {"json": serialize_to_json(body)}
        url = f"{cls.get_object_url().rstrip('/')}/{action_name.strip('/')}/"
        response = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload=payload,
            time_out=timeout,
        )
        if response.status_code not in expected_statuses:
            raise_for_response(response, payload=payload)
        return response.json()

    @classmethod
    def resolve(
        cls,
        *,
        agent_type: str,
        user_uid: str | None = None,
        project_uid: str | None = None,
        timeout=None,
    ) -> CodingAgentService:
        body: dict[str, Any] = {"agent_type": str(agent_type)}
        if user_uid is not None:
            body["user_uid"] = cls._coerce_filter_uid(user_uid, field_name="user_uid")
        if project_uid is not None:
            body["project_uid"] = cls._coerce_filter_uid(project_uid, field_name="project_uid")
        return cls(**cls._post_collection_action("resolve", body, timeout=timeout))

    @classmethod
    def resolve_user_orchestrator(
        cls,
        *,
        user_uid: str,
        timeout=None,
    ) -> CodingAgentService:
        return cls.resolve(
            agent_type="astro-orchestrator",
            user_uid=user_uid,
            timeout=timeout,
        )

    @classmethod
    def resolve_project_executor(
        cls,
        *,
        project_uid: str,
        timeout=None,
    ) -> CodingAgentService:
        return cls.resolve(
            agent_type="project-executor",
            project_uid=project_uid,
            timeout=timeout,
        )

    @classmethod
    def deploy_current_project_version(
        cls,
        *,
        project_uid: str,
        timeout=None,
    ) -> dict[str, Any]:
        resolved_project_uid = cls._coerce_filter_uid(project_uid, field_name="project_uid")
        return cls._post_collection_action(
            f"project/{resolved_project_uid}/deploy-current-version",
            {},
            timeout=timeout,
            expected_statuses=(200, 201, 202),
        )

    @classmethod
    def deploy_project(
        cls,
        *,
        project_uid: str,
        llm_provider: str | None = None,
        llm_model: str | None = None,
        llm_thinking: str | None = None,
        automatic_deployment: bool | None = None,
        cpu_request: str | None = None,
        cpu_limit: str | None = None,
        memory_request: str | None = None,
        memory_limit: str | None = None,
        gpu_request: str | None = None,
        gpu_type: str | None = None,
        spot: bool | None = None,
        timeout=None,
        **extra: Any,
    ) -> dict[str, Any]:
        resolved_project_uid = cls._coerce_filter_uid(project_uid, field_name="project_uid")
        body: dict[str, Any] = {
            key: value
            for key, value in {
                "llm_provider": llm_provider,
                "llm_model": llm_model,
                "llm_thinking": llm_thinking,
                "automatic_deployment": automatic_deployment,
                "cpu_request": cpu_request,
                "cpu_limit": cpu_limit,
                "memory_request": memory_request,
                "memory_limit": memory_limit,
                "gpu_request": gpu_request,
                "gpu_type": gpu_type,
                "spot": spot,
                **extra,
            }.items()
            if value is not None
        }
        return cls._post_collection_action(
            f"project/{resolved_project_uid}/deploy",
            body,
            timeout=timeout,
            expected_statuses=(200, 201, 202),
        )

    def reconcile_runtime(
        self,
        *,
        reason: str | None = None,
        timeout=None,
    ) -> dict[str, Any]:
        body = {} if reason is None else {"reason": str(reason)}
        payload = {"json": serialize_to_json(body)}
        response = make_request(
            s=type(self).build_session(),
            loaders=type(self).LOADERS,
            r_type="POST",
            url=self.get_action_url("reconcile-runtime"),
            payload=payload,
            time_out=timeout,
        )
        if response.status_code not in (200, 202):
            raise_for_response(response, payload=payload)
        return response.json()


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
            cache=True,
            timeout=timeout,
        )

    @staticmethod
    def _build_standard_a2a_message_send_body(
        *,
        agent_session_uid: str,
        message: str,
        files: list[Any] | None = None,
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
        parts: list[dict[str, Any]] = [{"text": normalized_message}]
        for file_spec in files or []:
            parts.append(AgentSession._build_standard_a2a_raw_file_part(file_spec))

        body: dict[str, Any] = {
            "message": {
                "messageId": normalized_message_id,
                "role": "ROLE_USER",
                "contextId": agent_session_uid,
                "parts": parts,
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

    @staticmethod
    def _build_standard_a2a_raw_file_part(file_spec: Any) -> dict[str, Any]:
        if isinstance(file_spec, (str, pathlib.Path)):
            path = pathlib.Path(file_spec).expanduser()
            media_type = "application/pdf" if path.suffix.lower() == ".pdf" else ""
            filename = path.name
        elif isinstance(file_spec, dict):
            raw_path = file_spec.get("path")
            if raw_path is None:
                raise ValueError("A2A file attachment requires a path")
            path = pathlib.Path(str(raw_path)).expanduser()
            media_type = str(
                file_spec.get("media_type") or file_spec.get("mediaType") or ""
            ).strip()
            if not media_type and path.suffix.lower() == ".pdf":
                media_type = "application/pdf"
            filename = str(file_spec.get("filename") or path.name).strip()
        else:
            raise TypeError("A2A file attachment must be a path or dict")

        if not media_type:
            raise ValueError(f"A2A file attachment '{path}' requires media_type")
        if not filename:
            raise ValueError(f"A2A file attachment '{path}' requires filename")

        file_bytes = path.read_bytes()
        if len(file_bytes) > MAX_INLINE_A2A_FILE_BYTES:
            size_mib = len(file_bytes) / (1024 * 1024)
            limit_mib = MAX_INLINE_A2A_FILE_BYTES / (1024 * 1024)
            raise ValueError(
                f"A2A attachment '{filename}' is {size_mib:.1f} MiB; "
                f"inline attachments are limited to {limit_mib:.0f} MiB."
            )

        return {
            "raw": base64.b64encode(file_bytes).decode("ascii"),
            "filename": filename,
            "mediaType": media_type,
        }

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
        files: list[Any] | None = None,
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
            files=files,
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
    def resolve_runtime_access(
        cls,
        agent_session: str | AgentSession,
        *,
        cache: bool = True,
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
        if cache:
            cls.cache_runtime_access(session_uid, access)
        return access

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
    "AgentSemanticSearchResult",
    "AgentSessionRuntimeAccess",
    "CodingAgentService",
    "AgentSession",
    "AgentSessionStatus",
]
