"""
base.py

Minimal, production-oriented client-side base for Agent Tools.

Requirements from Jose:
- Single public class: BaseAgentTool
- No registries / decorators / factories
- No HtmlApp
- No Jupyter/notebook helpers
- Keep _send_agent_tool_to_backend(payload) for AgentTool upsert
- Provide introspection for:
    - input_schema
    - output_schema (defined envelope with status/response/error)
    - config_schema
- Provide canonical runtime entrypoint:
    - run_and_response(): calls user run(), wraps into stable response envelope, then send_to_backend()

This file intentionally avoids any backend design advice.
"""

from __future__ import annotations

import os
import traceback
from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel

import mainsequence.client as msc
from mainsequence import logger

_REF_TEMPLATE = "#/$defs/{model}"




class BaseAgentTool(ABC):
    """
    Minimal base class for client-side Agent Tools.

    Tool authors implement:
        def run(self, *args, **kwargs): ...

    Runners should call:
        tool.run_and_response(*args, **kwargs)

    The response is ALWAYS a dict:
        {
          "status": "OK" | "ERROR",
          "response": <any JSON-serializable>,
          "error": { ... } | None
        }

    This class also provides introspection helpers to build:
        - input_schema
        - output_schema (stable response envelope)
        - config_schema
    to match your backend AgentTool model fields.
    """

    # ---- metadata overrides ----
    tool_description: str

    # ---- config model (Pydantic) ----
    configuration_class: type[BaseModel] #necessary on runtime

    # ---- Optional strict IO models ----

    output_model: type[BaseModel] | None = None



    def __init__(self, configuration: Any | None = None):
        """
        Accepts:
          - configuration as instance of configuration_class
          - configuration as dict (will be parsed into configuration_class)
          - configuration=None (will try configuration_class() if available)

        If you don't use configuration, you can ignore it.
        """
        cfg_cls =self.__class__.configuration_class

        if cfg_cls and isinstance(cfg_cls, type) and issubclass(cfg_cls, BaseModel):
            if configuration is None:
                # attempt default construction (will raise if required fields exist)
                self.configuration = cfg_cls()
            elif isinstance(configuration, cfg_cls):
                self.configuration = configuration
            elif isinstance(configuration, dict):
                self.configuration = cfg_cls(**configuration)
            else:
                raise TypeError(
                    f"configuration must be {cfg_cls.__name__}, dict, or None; got {type(configuration)}"
                )
        else:
            # No declared configuration class. Allow None or any BaseModel.
            if configuration is not None and not isinstance(configuration, BaseModel):
                raise TypeError(
                    "configuration must be a pydantic BaseModel instance or None "
                    "(or set configuration_class on the tool)."
                )
            self.configuration = configuration

    # -----------------------
    # Tool author implements
    # -----------------------
    @abstractmethod
    def run(self, *args, **kwargs) -> Any:
        raise NotImplementedError

    # -----------------------
    # Canonical runtime entrypoint
    # -----------------------
    def run_and_response(self, *args, **kwargs) -> dict[str, Any]:
        """
        Execute user run() and ALWAYS return a stable response envelope.

        This is the method your runner should call.
        """
        try:
            raw = self.run(*args, **kwargs)
            envelope = self._normalize_user_return(raw)
        except Exception as e:
            envelope = {
                "status": "ERROR",
                "response": None,
                "error": {
                    "message": str(e),
                    "error_type": e.__class__.__name__,
                    "details": None,
                    "traceback": traceback.format_exc(),
                },
            }

        # Best-effort persistence hook (no hard assumptions)
        try:
            self.send_to_backend(envelope)
        except Exception as e:
            logger.warning(
                "[%s] send_to_backend failed: %s", self.__class__.__name__, str(e), exc_info=True
            )

        return envelope

    def _normalize_user_return(self, raw: Any) -> dict[str, Any]:
        """
        Normalize various user return types into the stable response envelope.

        Supported:
          - dict with 'status' and 'response'
          - pydantic BaseModel -> becomes response (OK)
          - any other value -> becomes response (OK)
        """
        if isinstance(raw, dict) and "status" in raw and "response" in raw:
            status = raw.get("status", "OK")
            if status not in ("OK", "ERROR"):
                status = "OK"
            response = self._to_jsonable(raw.get("response"))
            err = raw.get("error")
            err = self._to_jsonable(err) if err is not None else None
            return {"status": status, "response": response, "error": err}

        # If they returned a pydantic model, dump it
        if isinstance(raw, BaseModel):
            return {"status": "OK", "response": raw.model_dump(mode="json"), "error": None}

        return {"status": "OK", "response": self._to_jsonable(raw), "error": None}

    @staticmethod
    def _to_jsonable(value: Any) -> Any:
        """
        Convert common Python/Pydantic objects into JSON-serializable structures.
        """
        if value is None:
            return None
        if isinstance(value, BaseModel):
            return value.model_dump(mode="json")
        if isinstance(value, dict):
            return {str(k): BaseAgentTool._to_jsonable(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [BaseAgentTool._to_jsonable(v) for v in value]
        if isinstance(value, set):
            return [BaseAgentTool._to_jsonable(v) for v in sorted(value, key=lambda x: str(x))]
        # Common "jsonable" objects
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception as e:
                logger.exception(f"value {value} is not jsonable:")
                raise e
        return value

    # -----------------------
    # Persistence hook (override if you want)
    # -----------------------
    def send_to_backend(self, envelope: dict[str, Any]) -> None:
        """
        Best-effort hook to persist the tool response.

        Default behavior:
          - If SDK helper exists: mainsequence.client.models_tdag.add_tool_response_to_jobrun -> use it
          - Else: store envelope as JSON artifact and attach via add_output()

        You can override this if you have a preferred mechanism.
        """

        # If your SDK provides a direct helper, prefer it.

        job_run_id=os.getenv("JOB_RUN_ID", None)
        if job_run_id is None:
            raise Exception("JOB_RUN_ID environment variable not set")

        msc.AgentTool.add_tool_response_to_jobrun(job_run_id=job_run_id, envelope=envelope)  # type: ignore


    # -----------------------
    # Tool metadata + schema introspection
    # -----------------------
    @classmethod
    def agent_tool_payload(cls) -> dict[str, Any]:
        """
        Build a dict that matches your backend AgentTool model fields:
          slug, name, description, entrypoint,
          input_schema, output_schema, config_schema,
          attributes
        """
        return {
            "description": cls.get_tool_description(),
            "output_schema": cls.get_output_schema(),
            "config_schema": cls.get_config_schema(),
        }

    @classmethod
    def register_to_backend(cls) -> None:
        """
        Explicit registration/upsert of this tool metadata.
        (No registries, no automatic side effects.)
        """
        payload=cls.agent_tool_payload()
        msc.AgentTool.update_metadata(**payload)  # type: ignore



    @classmethod
    def get_tool_name(cls) -> str:
        if isinstance(getattr(cls, "tool_name", None), str) and cls.tool_name:
            return cls.tool_name
        return cls.__name__

    @classmethod
    def get_tool_description(cls) -> str:

        return cls.tool_description





    @classmethod
    def get_config_schema(cls) -> dict[str, Any] | None:
        cfg_cls = getattr(cls, "configuration_class", None)
        if cfg_cls and isinstance(cfg_cls, type) and issubclass(cfg_cls, BaseModel):
            return cfg_cls.model_json_schema(ref_template=_REF_TEMPLATE)
        return None


    @classmethod
    def get_output_schema(cls) -> dict[str, Any]:
        """
        Output schema is always a stable response envelope:
          {"status": "OK"|"ERROR", "response": ..., "error": ...}

        If output_model is set, response is typed accordingly.
        """
        defs: dict[str, Any] = {}

        # Build "response" schema
        out_model = getattr(cls, "output_model", None)
        if out_model and isinstance(out_model, type) and issubclass(out_model, BaseModel):
            response_schema = out_model.model_json_schema(ref_template=_REF_TEMPLATE)
            if "$defs" in response_schema:
                defs.update(response_schema.pop("$defs"))
        else:
            response_schema = {}

        # error schema (always the same shape)
        error_schema: dict[str, Any] = {
            "title": "ToolError",
            "type": "object",
            "properties": {
                "message": {"type": "string"},
                "error_type": {"type": ["string", "null"]},
                "details": {"type": ["object", "null"]},
                "traceback": {"type": ["string", "null"]},
            },
            "required": ["message"],
            "additionalProperties": True,
        }

        envelope: dict[str, Any] = {
            "title": f"{cls.__name__}Output",
            "type": "object",
            "properties": {
                "status": {"type": "string", "enum": ["OK", "ERROR"]},
                "response": {"anyOf": [response_schema, {"type": "null"}]},
                "error": {"anyOf": [error_schema, {"type": "null"}]},
            },
            "required": ["status", "response", "error"],
            "additionalProperties": False,
        }

        if defs:
            envelope["$defs"] = defs

        return envelope

