from __future__ import annotations

import json
from collections.abc import Iterator
from enum import Enum
from typing import Any, ClassVar, Literal

import requests
from pydantic import ConfigDict, Field, model_validator

# replace these imports with your real package paths
from .base import BaseObjectOrm, BasePydanticModel
from .utils import make_request


class ResourceReleaseKind(str, Enum):
    STREAMLIT_DASHBOARD = "streamlit_dashboard"
    AGENT = "agent"


class AgentClientError(Exception):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        response_text: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text
        self.payload = payload or {}


class AgentPromptRequest(BasePydanticModel):
    prompt: str = Field(..., min_length=1)
    task_id: str | None = None
    context_id: str | None = None


class AgentQueryResponse(BasePydanticModel):
    ok: bool = True
    kind: str | None = None
    state: str | None = None
    task_id: str | None = None
    context_id: str | None = None
    text: str | None = None
    raw: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(from_attributes=True)


class AgentStreamEvent(BasePydanticModel):
    event: str = "message"
    id: str | None = None
    retry: int | None = None
    data: Any = None
    raw: str | None = None

    model_config = ConfigDict(from_attributes=True)


class ResourceRelease(BasePydanticModel, BaseObjectOrm):
    """
    Pydantic/API representation of the Django ResourceRelease.
    """


    id: int | None = None
    release_kind: ResourceReleaseKind
    resource: int
    readme_resource:int | None  =None
    related_job: int
    subdomain: str = Field(..., min_length=1, max_length=63)


    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    def get_detail_url(self) -> str:
        base = type(self).get_object_url().rstrip("/")
        return f"{base}/{self.id}/"

    def get_action_url(self, action_name: str) -> str:
        return f"{self.get_detail_url().rstrip('/')}/{action_name.strip('/')}/"

    @property
    def is_agent(self) -> bool:
        return self.release_kind == ResourceReleaseKind.AGENT

    def as_agent(self) -> Agent:
        return Agent.model_validate(self.model_dump())


class Agent(ResourceRelease):
    AGENT_PROMPT_ACTION: ClassVar[str] = "agent-prompt"
    release_kind: Literal[ResourceReleaseKind.AGENT] = ResourceReleaseKind.AGENT

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    @classmethod
    def class_name(cls):
        return ResourceRelease.__name__

    @model_validator(mode="after")
    def validate_agent_release(self) -> Agent:
        if self.release_kind != ResourceReleaseKind.AGENT:
            raise ValueError("This ResourceRelease is not an agent.")
        return self

    @classmethod
    def filter(cls, timeout=None, **kwargs):
        kwargs["release_kind"] = ResourceReleaseKind.AGENT.value
        return super().filter(timeout=timeout, **kwargs)

    @classmethod
    def iter_filter(cls, timeout=None, **kwargs):
        kwargs["release_kind"] = ResourceReleaseKind.AGENT.value
        return super().iter_filter(timeout=timeout, **kwargs)

    @classmethod
    def get(cls, pk=None, timeout=None, **filters):
        filters["release_kind"] = ResourceReleaseKind.AGENT.value
        return super().get(pk=pk, timeout=timeout, **filters)

    @classmethod
    def create(cls, timeout=None, files=None, *args, **kwargs):
        kwargs["release_kind"] = ResourceReleaseKind.AGENT.value
        return super().create(timeout=timeout, files=files, *args, **kwargs)

    def query(
            self,
            prompt: str,
            *,
            task_id: str | None = None,
            context_id: str | None = None,
            timeout: float | tuple[float, float] | None = None,
    ) -> AgentQueryResponse:
        payload = AgentPromptRequest(
            prompt=prompt,
            task_id=task_id,
            context_id=context_id,
        ).model_dump(exclude_none=True)

        url = self.get_action_url(self.AGENT_PROMPT_ACTION)
        session = type(self).build_session()

        response = make_request(
            s=session,
            loaders=type(self).LOADERS,
            r_type="POST",
            url=url,
            payload={"json": payload},
            time_out=timeout,
        )

        if response.status_code not in (200, 202):
            raise AgentClientError(
                "Failed to query agent.",
                status_code=response.status_code,
                response_text=response.text,
            )

        try:
            data = response.json()
        except ValueError as exc:
            raise AgentClientError(
                "Agent query did not return valid JSON.",
                status_code=response.status_code,
                response_text=response.text,
            ) from exc

        return AgentQueryResponse.model_validate(data)

    def query_stream(
            self,
            prompt: str,
            *,
            task_id: str | None = None,
            context_id: str | None = None,
            timeout: float | tuple[float, float] | None = None,
    ) -> Iterator[AgentStreamEvent]:
        payload = AgentPromptRequest(
            prompt=prompt,
            task_id=task_id,
            context_id=context_id,
        ).model_dump(exclude_none=True)

        url = self.get_action_url(self.AGENT_PROMPT_ACTION)
        session = type(self).build_session()

        def _generator() -> Iterator[AgentStreamEvent]:
            response: requests.Response | None = None
            try:
                response = session.post(
                    url,
                    json=payload,
                    headers={"Accept": "text/event-stream"},
                    timeout=timeout,
                    stream=True,
                )

                if response.status_code not in (200, 202):
                    raise AgentClientError(
                        "Failed to stream agent response.",
                        status_code=response.status_code,
                        response_text=response.text,
                    )

                content_type = response.headers.get("content-type", "").lower()

                # Graceful fallback if backend still returns plain JSON
                if "text/event-stream" not in content_type:
                    try:
                        data = response.json()
                    except ValueError as exc:
                        raise AgentClientError(
                            "Expected SSE or JSON from agent endpoint.",
                            status_code=response.status_code,
                            response_text=response.text,
                        ) from exc

                    yield AgentStreamEvent(
                        event="result",
                        data=data,
                        raw=json.dumps(data),
                    )
                    return

                yield from self._iter_sse_events(response)

            finally:
                if response is not None:
                    response.close()
                session.close()

        return _generator()

    @staticmethod
    def _iter_sse_events(response: requests.Response) -> Iterator[AgentStreamEvent]:
        """
        Proper SSE parser:
        - supports multi-line data
        - ignores comments
        - emits an event on blank line boundary
        """
        event_name = "message"
        event_id: str | None = None
        retry: int | None = None
        data_lines: list[str] = []

        for raw_line in response.iter_lines(decode_unicode=True):
            if raw_line is None:
                continue

            line = raw_line.rstrip("\r")

            # blank line => dispatch event
            if line == "":
                if data_lines or event_id is not None or retry is not None or event_name != "message":
                    raw_data = "\n".join(data_lines)
                    yield AgentStreamEvent(
                        event=event_name or "message",
                        id=event_id,
                        retry=retry,
                        data=Agent._decode_sse_data(raw_data),
                        raw=raw_data,
                    )

                event_name = "message"
                event_id = None
                retry = None
                data_lines = []
                continue

            # comment / keepalive
            if line.startswith(":"):
                continue

            field, _, value = line.partition(":")
            if value.startswith(" "):
                value = value[1:]

            if field == "event":
                event_name = value or "message"
            elif field == "id":
                event_id = value or None
            elif field == "retry":
                try:
                    retry = int(value)
                except ValueError:
                    retry = None
            elif field == "data":
                data_lines.append(value)

        # flush last event if stream closed without trailing blank line
        if data_lines or event_id is not None or retry is not None or event_name != "message":
            raw_data = "\n".join(data_lines)
            yield AgentStreamEvent(
                event=event_name or "message",
                id=event_id,
                retry=retry,
                data=Agent._decode_sse_data(raw_data),
                raw=raw_data,
            )

    @staticmethod
    def _decode_sse_data(raw_data: str) -> Any:
        raw_data = raw_data.strip()
        if not raw_data:
            return None

        try:
            return json.loads(raw_data)
        except json.JSONDecodeError:
            return raw_data