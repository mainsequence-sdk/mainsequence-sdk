from __future__ import annotations

import json
import os
import traceback
from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel

import mainsequence.client as msc
from mainsequence import logger

_REF_TEMPLATE = "#/$defs/{model}"


class BaseJobApi(ABC):
    """
    Base class for JobApi implementations.

    No custom envelope.
    - run() returns the successful business output only
    - failures are reported as Problem Details objects
    - input_schema/output_schema are generated from Pydantic models
    """

    job_api_description: str
    input_model: type[BaseModel] | None = None
    output_model: type[BaseModel] | None = None

    def __init__(self, input_data: Any | None = None):
        input_cls = self.__class__.input_model

        if input_cls and isinstance(input_cls, type) and issubclass(input_cls, BaseModel):
            if input_data is None:
                self.input = input_cls()
            elif isinstance(input_data, input_cls):
                self.input = input_data
            elif isinstance(input_data, dict):
                self.input = input_cls(**input_data)
            else:
                raise TypeError(
                    f"input_data must be {input_cls.__name__}, dict, or None; got {type(input_data)}"
                )
        else:
            self.input = input_data

    @abstractmethod
    def run(self, input_data: Any | None = None) -> Any:
        raise NotImplementedError

    def run_and_report(self) -> Any:
        try:
            raw_output = self.run(self.input)
            output = self._coerce_output(raw_output)
            self.report_result(status="OK", output=output, error=None)
            return output
        except Exception as exc:
            problem = self.problem_from_exception(exc)
            self.report_result(status="ERROR", output=None, error=problem)
            raise

    def report_result(self, status: str, output: Any, error: dict[str, Any] | None) -> None:
        job_run_id = os.getenv("JOB_RUN_ID")
        if not job_run_id:
            raise RuntimeError("JOB_RUN_ID environment variable not set")

        msc.JobApi.report_job_run_result(
            job_run_id=job_run_id,
            status=status,
            output=output,
            error=error,
        )

    @classmethod
    def register_to_backend(cls, job_run_id: str | None = None) -> None:
        resolved_job_run_id = job_run_id or os.getenv("JOB_RUN_ID")
        if not resolved_job_run_id:
            raise RuntimeError("JOB_RUN_ID environment variable not set")

        msc.JobApi.update_metadata(
            job_run_id=resolved_job_run_id,
            description=cls.get_job_api_description(),
            input_schema=cls.get_input_schema(),
            output_schema=cls.get_output_schema(),
        )



    @classmethod
    def get_job_api_description(cls) -> str:
        return getattr(cls, "job_api_description", "") or ""



    @classmethod
    def get_input_schema(cls) -> dict[str, Any] | None:
        input_cls = getattr(cls, "input_model", None)
        if input_cls and isinstance(input_cls, type) and issubclass(input_cls, BaseModel):
            return input_cls.model_json_schema(ref_template=_REF_TEMPLATE)
        return None

    @classmethod
    def get_output_schema(cls) -> dict[str, Any] | None:
        output_cls = getattr(cls, "output_model", None)
        if output_cls and isinstance(output_cls, type) and issubclass(output_cls, BaseModel):
            return output_cls.model_json_schema(ref_template=_REF_TEMPLATE)
        return None

    def _coerce_output(self, value: Any) -> Any:
        output_cls = getattr(self, "output_model", None)

        if output_cls and isinstance(output_cls, type) and issubclass(output_cls, BaseModel):
            if value is None:
                return None
            if isinstance(value, output_cls):
                return value.model_dump(mode="json")
            if isinstance(value, BaseModel):
                return value.model_dump(mode="json")
            if isinstance(value, dict):
                return output_cls(**value).model_dump(mode="json")
            return output_cls.model_validate(value).model_dump(mode="json")

        return self._to_jsonable(value)

    @staticmethod
    def _to_jsonable(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, BaseModel):
            return value.model_dump(mode="json")
        if isinstance(value, dict):
            return {str(k): BaseJobApi._to_jsonable(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [BaseJobApi._to_jsonable(v) for v in value]
        if isinstance(value, set):
            return [BaseJobApi._to_jsonable(v) for v in sorted(value, key=lambda x: str(x))]
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception as exc:
                logger.exception("value %s is not jsonable", value)
                raise exc
        return value

    @staticmethod
    def problem_from_exception(exc: Exception) -> dict[str, Any]:
        return {
            "type": "about:blank",
            "title": exc.__class__.__name__,
            "status": 500,
            "detail": str(exc),
            "traceback": traceback.format_exc(),
        }

    @classmethod
    def from_runtime_environment(cls) -> BaseJobApi:
        raw_input = os.getenv("JOB_API_INPUT", "").strip()
        if not raw_input:
            return cls(input_data={})

        try:
            parsed = json.loads(raw_input)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"JOB_API_INPUT is not valid JSON: {exc}") from exc

        if not isinstance(parsed, dict):
            raise RuntimeError("JOB_API_INPUT must be a JSON object")

        return cls(input_data=parsed)