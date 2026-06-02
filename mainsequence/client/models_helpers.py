from __future__ import annotations

import datetime
import json
import pathlib
from collections.abc import Collection
from decimal import Decimal
from enum import Enum
from pathlib import PurePosixPath
from typing import Any, ClassVar, Literal, Union
from uuid import UUID

import yaml
from pydantic import BaseModel, Field, PositiveInt

from .base import BaseObjectOrm, BasePydanticModel, ShareableObjectMixin
from .compute_validation import (
    decimal_to_storage,
    normalize_string,
    validate_and_normalize_compute_fields,
)
from .exceptions import raise_for_response
from .metatables.core import _require_local_pod_project, _resolve_local_pod_project
from .models_foundry import (
    Project,
    ProjectImage,
)
from .utils import make_request


def get_model_class(model_class: str):
    local_model = globals().get(model_class)
    if local_model is not None:
        return local_model

    raise KeyError(f"Unknown mainsequence SDK model class {model_class!r}.")


class CrontabSchedule(BaseModel):
    type: Literal["crontab"] = Field(
        default="crontab",
        description="Schedule type for cron-style execution.",
        examples=["crontab"],
    )
    start_time: datetime.datetime | None = Field(
        default=None,
        description="Optional ISO datetime when the schedule becomes active.",
        examples=["2026-03-14T09:00:00Z"],
    )
    expression: str = Field(
        ...,
        min_length=1,
        description="Five-field crontab expression: minute hour day_of_month month_of_year day_of_week.",
        examples=["0 * * * *", "0 0 * * 1-5"],
    )


class IntervalSchedule(BaseModel):
    type: Literal["interval"] = Field(
        default="interval",
        description="Schedule type for fixed-interval execution.",
        examples=["interval"],
    )
    start_time: datetime.datetime | None = Field(
        default=None,
        description="Optional ISO datetime when the schedule becomes active.",
        examples=["2026-03-14T09:00:00Z"],
    )
    every: PositiveInt = Field(
        ...,
        description="Run every N units of the selected period.",
        examples=[1, 5, 15],
    )
    period: Literal["seconds", "minutes", "hours", "days"] = Field(
        ...,
        description="Unit used by the interval schedule.",
        examples=["hours", "days"],
    )


Schedule = Union[CrontabSchedule, IntervalSchedule]


class PeriodicTask(BasePydanticModel):
    name: str = Field(
        ...,
        description="Display name for the periodic task.",
        examples=["Nightly build"],
    )
    task: str = Field(
        ...,
        description="Backend task identifier executed by the scheduler.",
        examples=["tdag.pod_manager.tasks.run_job_in_celery"],
    )
    schedule: Schedule | None = Field(
        default=None,
        description="Nested schedule definition for the periodic task.",
        examples=[{"type": "crontab", "expression": "0 2 * * *"}],
    )


class Job(BaseObjectOrm, BasePydanticModel):
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "uid": ["in", "exact"],
        "project__uid": ["in", "exact"],
        "name": ["in", "exact", "contains"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "uid": "uid",
        "uid__in": "uid",
        "project__uid": "uid",
        "project__uid__in": "uid",
        "name": "str",
        "name__in": "str",
        "name__contains": "str",
    }

    CPU_MIN: ClassVar[Decimal] = Decimal("0.25")
    CPU_MAX: ClassVar[Decimal] = Decimal("30")
    MEMORY_MIN: ClassVar[Decimal] = Decimal("0.5")
    MEMORY_MAX: ClassVar[Decimal] = Decimal("110")
    MEMORY_PER_CPU_MIN: ClassVar[Decimal] = Decimal("1")
    MEMORY_PER_CPU_MAX: ClassVar[Decimal] = Decimal("6.5")
    GPU_MIN: ClassVar[int] = 1
    GPU_MAX: ClassVar[int] = 8
    DEFAULT_ALLOWED_EXECUTION_EXTENSIONS: ClassVar[frozenset[str]] = frozenset(
        {".py", ".ipynb", ".yaml"}
    )

    uid: str | None = Field(
        default=None,
        description="Public UID of the job.",
        examples=["7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da"],
    )

    name: str = Field(
        ...,
        min_length=1,
        description="Human-readable job name.",
        examples=["Daily feature build"],
    )

    project_uid: str | None = Field(
        default=None,
        description="Public UID of the owning project.",
        examples=["5a28020a-0f1b-47ee-aab8-334286234bea"],
    )

    project_repo_hash: str | None = Field(
        default=None,
        description=(
            "Git commit hash used by the job. This may be auto-filled from the selected related image."
        ),
        examples=["4f3c2b1a9d8e7f6c5b4a39281716151413121110"],
    )

    execution_path: str | None = Field(
        default=None,
        description=(
            "Repository-relative file path from the content root. Allowed extensions are .py, .ipynb, and .yaml."
        ),
        examples=["scripts/test.py", "jobs/train_model.py", "notebooks/eda.ipynb"],
    )

    app_name: str | None = Field(
        default=None,
        description="Application name to run instead of a file-based execution path.",
        examples=["data-monitor"],
    )

    task_schedule: PeriodicTask | None = Field(
        default=None,
        description="Nested periodic task configuration returned by the API.",
        examples=[
            {
                "name": "Nightly build",
                "task": "tdag.pod_manager.tasks.run_job_in_celery",
                "schedule": {
                    "type": "crontab",
                    "expression": "0 2 * * *",
                },
            }
        ],
    )

    cpu_request: str | None = Field(
        default=None,
        description="Requested CPU in vCPU units, stored as a normalized string.",
        examples=["0.25", "1", "4"],
    )

    cpu_limit: str | None = Field(
        default=None,
        description="CPU limit in vCPU units, stored as a normalized string.",
        examples=["0.25", "1", "4"],
    )

    memory_request: str | None = Field(
        default=None,
        description="Requested memory in GiB, stored as a normalized string.",
        examples=["0.5", "2", "16"],
    )

    memory_limit: str | None = Field(
        default=None,
        description="Memory limit in GiB, stored as a normalized string.",
        examples=["0.5", "2", "16"],
    )

    gpu_request: str | None = Field(
        default=None,
        description="Requested GPU count, stored as a string.",
        examples=["1", "2"],
    )

    gpu_type: str | None = Field(
        default=None,
        description="GPU accelerator type.",
        examples=["nvidia-tesla-t4", "nvidia-l4"],
    )

    spot: bool = Field(
        default=False,
        description="Whether the job should prefer spot or preemptible capacity.",
        examples=[False, True],
    )

    max_runtime_seconds: int | None = Field(
        default=None,
        gt=0,
        description="Maximum allowed runtime in seconds before the job is aborted.",
        examples=[3600, 14400],
    )

    related_image_uid: str | None = Field(
        default=None,
        description="Public UID of the execution image.",
        examples=["f3cb8477-df47-49cb-a151-80b746fb1243"],
    )

    @staticmethod
    def _coerce_uid(obj: Any, *, field_name: str) -> str | None:
        if obj is None:
            return None
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, str):
            normalized = obj.strip()
            if normalized:
                return normalized
        if hasattr(obj, "uid") and obj.uid not in (None, ""):
            return str(obj.uid).strip()
        if isinstance(obj, dict) and obj.get("uid") not in (None, ""):
            return str(obj["uid"]).strip()
        raise TypeError(
            f"{field_name} must be a uid string, an object with .uid, a dict with 'uid', or None. "
            f"Got: {type(obj)!r}"
        )

    @staticmethod
    def _normalize_str(value: Any) -> str | None:
        return normalize_string(value)

    @staticmethod
    def _decimal_to_storage(value: Decimal | None) -> str | None:
        return decimal_to_storage(value)

    @classmethod
    def _resolve_project_uid(
        cls,
        project_uid: str | Project | dict[str, Any] | None = None,
    ) -> str:
        ref = project_uid
        if ref is None:
            resolution = _resolve_local_pod_project()
            if resolution.project is not None:
                ref = resolution.project

        resolved = cls._coerce_uid(ref, field_name="project_uid")
        if resolved is None:
            raise ValueError(
                "project_uid is required. Pass project_uid or configure a local pod project."
            )
        return resolved

    @classmethod
    def _normalize_allowed_execution_extensions(
        cls,
        allowed_execution_extensions: Collection[str] | str | None,
    ) -> set[str]:
        if allowed_execution_extensions is None:
            return set(cls.DEFAULT_ALLOWED_EXECUTION_EXTENSIONS)

        raw_values = (
            [allowed_execution_extensions]
            if isinstance(allowed_execution_extensions, str)
            else list(allowed_execution_extensions)
        )

        normalized: set[str] = set()
        for raw in raw_values:
            ext = str(raw).strip().lower()
            if not ext:
                raise ValueError("allowed_execution_extensions cannot contain empty values.")
            if not ext.startswith("."):
                ext = f".{ext}"
            if ext not in cls.DEFAULT_ALLOWED_EXECUTION_EXTENSIONS:
                raise ValueError(
                    f"Unsupported extension {ext!r}. "
                    f"Allowed extensions: {', '.join(sorted(cls.DEFAULT_ALLOWED_EXECUTION_EXTENSIONS))}."
                )
            normalized.add(ext)

        if not normalized:
            raise ValueError("allowed_execution_extensions cannot be empty.")

        return normalized

    @classmethod
    def _build_target_payload(
        cls,
        *,
        execution_path: str | None = None,
        app_name: str | None = None,
        allowed_execution_extensions: Collection[str] | str | None = None,
    ) -> dict[str, Any]:
        execution_path = cls._normalize_str(execution_path)
        app_name = cls._normalize_str(app_name)

        if bool(execution_path) == bool(app_name):
            raise ValueError("Pass exactly one of execution_path or app_name.")

        payload: dict[str, Any] = {}

        if execution_path is not None:
            execution_path = execution_path.replace("\\", "/")
            path_obj = PurePosixPath(execution_path)
            allowed_extensions = cls._normalize_allowed_execution_extensions(
                allowed_execution_extensions
            )

            if execution_path.endswith("/"):
                raise ValueError("execution_path must point to a file, not a directory.")
            if path_obj.is_absolute():
                raise ValueError("execution_path must be repository-relative.")
            if ".." in path_obj.parts:
                raise ValueError("execution_path cannot contain '..' path traversal.")

            suffix = path_obj.suffix.lower()
            if suffix not in allowed_extensions:
                raise ValueError(
                    f"Invalid file type. Allowed extensions: {', '.join(sorted(allowed_extensions))}."
                )

            payload["execution_path"] = execution_path

        if app_name is not None:
            payload["app_name"] = app_name

        return payload

    @classmethod
    def _validate_and_normalize_compute_fields(
        cls,
        *,
        cpu_request: Any,
        memory_request: Any,
        gpu_request: Any,
        gpu_type: Any,
        require_cpu_and_memory: bool = True,
        output_format: Literal["decimal", "k8s"] = "decimal",
    ) -> dict[str, str | None]:
        return validate_and_normalize_compute_fields(
            cpu_request=cpu_request,
            memory_request=memory_request,
            gpu_request=gpu_request,
            gpu_type=gpu_type,
            require_cpu_and_memory=require_cpu_and_memory,
            output_format=output_format,
        )

    @classmethod
    def _normalize_task_schedule_payload(
        cls,
        *,
        task_schedule: Any = None,
        task_schedule_id: int | None = None,
    ) -> dict[str, Any] | None:
        if task_schedule is not None and task_schedule_id is not None:
            raise ValueError("Pass only one of task_schedule or task_schedule_id.")

        if task_schedule_id is not None:
            raise ValueError(
                "task_schedule_id is not supported by the current backend. Pass task_schedule instead."
            )

        if task_schedule in (None, "", {}):
            return None

        if isinstance(task_schedule, str):
            raw_value = task_schedule.strip()
            if not raw_value:
                return None
            try:
                task_schedule = json.loads(raw_value)
            except json.JSONDecodeError as exc:
                raise ValueError("task_schedule must be a valid JSON object.") from exc

        if hasattr(task_schedule, "model_dump"):
            payload = task_schedule.model_dump(mode="json", exclude_none=True)
        elif isinstance(task_schedule, dict):
            payload = dict(task_schedule)
        else:
            raise TypeError(
                "task_schedule must be a dict, JSON object string, CrontabSchedule, IntervalSchedule, or PeriodicTask."
            )

        if not isinstance(payload, dict):
            raise ValueError("task_schedule must serialize to an object.")

        schedule_payload = payload.get("schedule")
        if schedule_payload is None:
            schedule_payload = payload
            payload = {"schedule": dict(schedule_payload)}
        elif not isinstance(schedule_payload, dict):
            raise ValueError("task_schedule.schedule must be an object.")
        else:
            payload["schedule"] = dict(schedule_payload)

        schedule_type = str(payload["schedule"].get("type") or "").strip().lower()
        if not schedule_type:
            raise ValueError("task_schedule.schedule.type is required.")

        if schedule_type == "interval":
            every = payload["schedule"].get("every")
            try:
                every_int = int(every)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    "task_schedule.schedule.every must be a positive integer."
                ) from exc
            if every_int <= 0:
                raise ValueError("task_schedule.schedule.every must be a positive integer.")

            period = str(payload["schedule"].get("period") or "").strip().lower()
            allowed_periods = {"seconds", "minutes", "hours", "days"}
            if period not in allowed_periods:
                raise ValueError(
                    f"task_schedule.schedule.period must be one of: {', '.join(sorted(allowed_periods))}."
                )

            payload["schedule"]["type"] = "interval"
            payload["schedule"]["every"] = every_int
            payload["schedule"]["period"] = period
        elif schedule_type == "crontab":
            expression = str(payload["schedule"].get("expression") or "").strip()
            if not expression:
                raise ValueError(
                    "task_schedule.schedule.expression is required for crontab schedules."
                )
            if len(expression.split()) != 5:
                raise ValueError(
                    "task_schedule.schedule.expression must have 5 crontab fields: "
                    "minute hour day_of_month month_of_year day_of_week."
                )

            payload["schedule"]["type"] = "crontab"
            payload["schedule"]["expression"] = expression
        else:
            raise ValueError("task_schedule.schedule.type must be either 'interval' or 'crontab'.")

        return payload

    @classmethod
    def _build_create_payload(
        cls,
        *,
        name: str,
        project_uid: str | Project | dict[str, Any] | None = None,
        project_repo_hash: str | None = None,
        execution_path: str | None = None,
        app_name: str | None = None,
        task_schedule: PeriodicTask | Schedule | dict[str, Any] | str | None = None,
        task_schedule_id: int | None = None,
        cpu_request: str | int | float | Decimal | None = None,
        memory_request: str | int | float | Decimal | None = None,
        gpu_request: str | int | None = None,
        gpu_type: str | None = None,
        spot: bool | None = None,
        max_runtime_seconds: int | None = None,
        related_image_uid: str | ProjectImage | dict[str, Any] | None = None,
        allowed_execution_extensions: Collection[str] | str | None = None,
    ) -> dict[str, Any]:
        normalized_name = cls._normalize_str(name)
        if not normalized_name:
            raise ValueError("name is required.")

        payload: dict[str, Any] = {
            "name": normalized_name,
            "project_uid": cls._resolve_project_uid(project_uid=project_uid),
        }

        normalized_project_repo_hash = cls._normalize_str(project_repo_hash)
        if normalized_project_repo_hash is not None:
            payload["project_repo_hash"] = normalized_project_repo_hash

        payload.update(
            cls._build_target_payload(
                execution_path=execution_path,
                app_name=app_name,
                allowed_execution_extensions=allowed_execution_extensions,
            )
        )

        normalized_compute = cls._validate_and_normalize_compute_fields(
            cpu_request=cpu_request,
            memory_request=memory_request,
            gpu_request=gpu_request,
            gpu_type=gpu_type,
            require_cpu_and_memory=True,
        )

        payload["cpu_request"] = normalized_compute["cpu_request"]
        payload["memory_request"] = normalized_compute["memory_request"]

        if normalized_compute["gpu_request"] is not None:
            payload["gpu_request"] = normalized_compute["gpu_request"]
        if normalized_compute["gpu_type"] is not None:
            payload["gpu_type"] = normalized_compute["gpu_type"]

        normalized_task_schedule = cls._normalize_task_schedule_payload(
            task_schedule=task_schedule,
            task_schedule_id=task_schedule_id,
        )
        if normalized_task_schedule is not None:
            payload["task_schedule"] = normalized_task_schedule

        if spot is not None:
            payload["spot"] = bool(spot)

        if max_runtime_seconds is not None:
            max_runtime_seconds = int(max_runtime_seconds)
            if max_runtime_seconds <= 0:
                raise ValueError("max_runtime_seconds must be a positive integer.")
            payload["max_runtime_seconds"] = max_runtime_seconds

        image_uid = cls._coerce_uid(related_image_uid, field_name="related_image_uid")
        if image_uid is None:
            raise ValueError("related_image_uid is required.")
        payload["related_image_uid"] = image_uid

        return payload

    @classmethod
    def create(
        cls,
        *,
        name: str,
        project_uid: str | Project | dict[str, Any] | None = None,
        project_repo_hash: str | None = None,
        execution_path: str | None = None,
        app_name: str | None = None,
        task_schedule: PeriodicTask | Schedule | dict[str, Any] | str | None = None,
        task_schedule_id: int | None = None,
        cpu_request: str | int | float | Decimal | None = None,
        memory_request: str | int | float | Decimal | None = None,
        gpu_request: str | int | None = None,
        gpu_type: str | None = None,
        spot: bool | None = None,
        max_runtime_seconds: int | None = None,
        related_image_uid: str | ProjectImage | dict[str, Any] | None = None,
        allowed_execution_extensions: Collection[str] | str | None = None,
        timeout: int | None = None,
    ) -> Job:
        payload = cls._build_create_payload(
            name=name,
            project_uid=project_uid,
            project_repo_hash=project_repo_hash,
            execution_path=execution_path,
            app_name=app_name,
            task_schedule=task_schedule,
            task_schedule_id=task_schedule_id,
            cpu_request=cpu_request,
            memory_request=memory_request,
            gpu_request=gpu_request,
            gpu_type=gpu_type,
            spot=spot,
            max_runtime_seconds=max_runtime_seconds,
            related_image_uid=related_image_uid,
            allowed_execution_extensions=allowed_execution_extensions,
        )

        request_payload = {"json": cls.serialize_for_json(payload)}

        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=f"{cls.get_object_url()}/",
            payload=request_payload,
            time_out=timeout,
        )

        if r.status_code not in (200, 201, 202):
            raise_for_response(r, payload=request_payload)

        return cls(**r.json())

    @classmethod
    def bulk_get_or_create(
        cls,
        *,
        yaml_file: str | pathlib.Path,
        project_uid: str | Project | dict[str, Any],
        strict: bool = False,
        timeout: int | None = None,
    ) -> list[Job] | dict[str, Any]:
        """
        Validate a batch YAML file and synchronize its jobs with the backend.

        Request body:
          - project_uid
          - jobs
          - strict

        Strict mode notes:
          - jobs that exist remotely but are not present in the YAML may be deleted
          - jobs linked to dashboards or resource releases are protected and may be returned
            in the response under `not_deleted`
        """
        resolved_project_uid = cls._coerce_uid(project_uid, field_name="project_uid")
        if resolved_project_uid is None:
            raise ValueError("project_uid is required.")

        yaml_path = pathlib.Path(yaml_file).expanduser()
        if not yaml_path.is_file():
            raise FileNotFoundError(f"Jobs file not found: {yaml_path}")

        with yaml_path.open("r", encoding="utf-8") as f:
            raw_config = yaml.safe_load(f) or {}

        if not isinstance(raw_config, dict) or "jobs" not in raw_config:
            raise ValueError("Job batch file must define a top-level 'jobs' key.")

        jobs_config = raw_config["jobs"]
        if not isinstance(jobs_config, list):
            raise ValueError("The 'jobs' key must contain a list.")

        normalized_jobs: list[dict[str, Any]] = []
        for index, raw_job in enumerate(jobs_config):
            if not isinstance(raw_job, dict):
                raise ValueError(f"jobs[{index}] must be a mapping.")

            job_data = dict(raw_job)

            if {"project", "project_id", "project_uid"} & set(job_data):
                raise ValueError(
                    f"jobs[{index}] must not define project/project_id/project_uid. "
                    "Pass the target project_uid to Job.bulk_get_or_create()."
                )
            if "timeout" in job_data:
                raise ValueError(f"jobs[{index}] must not define timeout.")
            if {"related_image", "related_image_id"} & set(job_data):
                raise ValueError(
                    f"jobs[{index}] must use related_image_uid, not related_image or related_image_id."
                )

            try:
                normalized_job = cls._build_create_payload(
                    project_uid=resolved_project_uid,
                    **job_data,
                )
                normalized_job.pop("project_uid", None)
                normalized_jobs.append(normalized_job)
            except TypeError as exc:
                raise ValueError(f"jobs[{index}] has unsupported or missing fields: {exc}") from exc
            except Exception as exc:
                raise ValueError(f"jobs[{index}] is invalid: {exc}") from exc

        request_payload = {
            "json": cls.serialize_for_json(
                {
                    "project_uid": resolved_project_uid,
                    "jobs": normalized_jobs,
                    "strict": bool(strict),
                }
            )
        }
        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=f"{cls.get_object_url()}/sync_jobs/",
            payload=request_payload,
            time_out=timeout,
        )
        if r.status_code not in (200, 201, 202):
            raise_for_response(r, payload=request_payload)

        response_data = r.json()
        if not isinstance(response_data, list):
            return response_data

        jobs_out: list[Job] = []
        for item in response_data:
            if not isinstance(item, dict):
                return response_data
            try:
                jobs_out.append(cls(**item))
            except Exception:
                return response_data

        return jobs_out

    @classmethod
    def create_from_configuration(cls, job_configuration):
        url = cls.get_object_url() + "/create_from_configuration/"
        s = cls.build_session()
        payload = dict(job_configuration)
        payload["project_uid"] = _require_local_pod_project(
            f"{cls.__name__}.create_from_configuration"
        ).uid

        r = make_request(
            s=s,
            loaders=cls.LOADERS,
            r_type="POST",
            url=url,
            payload={"json": cls.serialize_for_json(payload)},
        )
        if r.status_code not in [200, 201, 202]:
            raise_for_response(r)

        return r.json()

    def run_job(
        self,
        *,
        timeout: int | None = None,
        command_args: list[str] | None = None,
    ) -> dict[str, Any]:
        job_uid = self._public_detail_reference()
        if command_args is not None and not all(isinstance(arg, str) for arg in command_args):
            raise TypeError("command_args must be a list of strings.")

        url = f"{self.get_object_url()}/{job_uid}/run_job/"
        s = self.build_session()

        payload: dict[str, Any] = {}
        if command_args is not None:
            payload["json"] = {"command_args": list(command_args)}

        r = make_request(
            s=s,
            loaders=self.LOADERS,
            r_type="POST",
            url=url,
            payload=payload,
            time_out=timeout,
        )

        if r.status_code not in (200, 201, 202):
            raise_for_response(r)

        return r.json()


class JobRun(BaseObjectOrm, BasePydanticModel):
    PUBLIC_LOOKUP_FIELD: ClassVar[str] = "uid"
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "job__uid": ["in", "exact"],
        "uid": ["in", "exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "job__uid": "str",
        "uid": "str",
    }

    uid: str | None = Field(
        default=None,
        description="Public UID of the job run. This is the identifier used by JobRun detail endpoints.",
        examples=["4c1d77c8-8a42-42b8-a9c1-06be9a336e5d"],
    )
    name: str = Field(
        ...,
        min_length=1,
        description="The name of the job run.",
        examples=["daily-training-run"],
    )
    unique_identifier: str = Field(
        ...,
        min_length=1,
        description="Runtime workload identifier for this specific job run. This is not the public API identifier.",
        examples=["jobrun_2026_03_14_abc123"],
    )

    job_uid: str | None = Field(
        default=None,
        description="Public UID of the associated job.",
        examples=["ab6a5d50-8a3e-4f0d-a9bb-7e84180bd50e"],
    )
    job_name: str | None = Field(
        default=None,
        description="Read-only helper field containing the associated job name.",
        examples=["daily-training-job"],
    )

    execution_start: datetime.datetime | None = Field(
        default=None,
        description="The timestamp when execution started.",
        examples=["2026-03-14T09:12:00Z"],
    )
    execution_end: datetime.datetime | None = Field(
        default=None,
        description="The timestamp when execution finished.",
        examples=["2026-03-14T09:47:32Z"],
    )

    response_status: str | None = Field(
        default=None,
        description="The response status returned by the backend or execution system.",
        examples=["success"],
    )
    response_error: str | None = Field(
        default=None,
        description="Error text captured by the backend or execution system, when available.",
        examples=["Container exited with code 1"],
    )
    status: str | None = Field(
        default=None,
        description="The current lifecycle status of the job run.",
        examples=["completed"],
    )

    cpu_usage: float | None = Field(
        default=None,
        description="Observed CPU usage for the job run.",
        examples=[1.37],
    )
    memory_usage: float | None = Field(
        default=None,
        description="Observed memory usage for the job run.",
        examples=[2.84],
    )

    cpu_request: str | None = Field(
        default=None,
        description="The CPU request applied to this job run.",
        examples=["1"],
    )
    cpu_limit: str | None = Field(
        default=None,
        description="The CPU limit applied to this job run.",
        examples=["2"],
    )
    memory_request: str | None = Field(
        default=None,
        description="The memory request applied to this job run.",
        examples=["4Gi"],
    )
    memory_limit: str | None = Field(
        default=None,
        description="The memory limit applied to this job run.",
        examples=["8Gi"],
    )
    gpu_request: str | None = Field(
        default=None,
        description="Number of GPUs requested for this job run.",
        examples=["1"],
    )
    gpu_type: str | None = Field(
        default=None,
        description="GPU type requested for this job run.",
        examples=["nvidia-l4"],
    )

    triggered_by: str | None = Field(
        default=None,
        description="A string describing what or who triggered this run.",
        examples=["user"],
    )
    triggered_by_id: int | None = Field(
        default=None,
        description="The ID of the object or user that triggered this run.",
        examples=[7],
    )

    commit_hash: str | None = Field(
        default=None,
        description="The commit hash associated with the code version used for this run.",
        examples=["a1b2c3d4e5f6g7h8i9j0"],
    )

    command_args: list[str] = Field(
        default_factory=list,
        description="Per-run command arguments requested for this job run.",
        examples=[["sync", "--from", "2026-04-01"]],
    )

    def get_logs(self, *, timeout: int | None = None) -> dict[str, Any]:
        job_run_uid = self._public_detail_reference()
        url = f"{self.get_object_url()}/{job_run_uid}/get_logs/"
        s = self.build_session()

        r = make_request(
            s=s,
            loaders=self.LOADERS,
            r_type="GET",
            url=url,
            payload={},
            time_out=timeout,
        )

        if r.status_code != 200:
            raise_for_response(r)

        return r.json()

    def job_run_status(
        self,
        *,
        status: str | None = None,
        git_hash: str | None = None,
        timeout: int | None = None,
    ) -> dict[str, Any]:
        """
        Update the backend job-run status detail action for this run.

        This hits:
            POST /pods/job-run/{uid}/status/
        """
        job_run_uid = self._public_detail_reference()
        url = f"{self.get_object_url()}/{job_run_uid}/status/"
        s = self.build_session()
        payload: dict[str, Any] = {}
        if status is not None:
            payload["status"] = status
        if git_hash is not None:
            payload["git_hash"] = git_hash

        r = make_request(
            s=s,
            loaders=self.LOADERS,
            r_type="POST",
            url=url,
            payload=payload,
            time_out=timeout,
        )

        if r.status_code != 200:
            raise_for_response(r)

        return r.json()


class ProjectResource(BaseObjectOrm, BasePydanticModel):
    SEARCH_FIELDS: ClassVar[list[str]] = [
        "project__uid",
        "uid",
        "repo_commit_sha",
        "resource_type",
    ]
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "project__uid": ["exact"],
        "uid": ["in", "exact"],
        "repo_commit_sha": ["exact"],
        "resource_type": ["exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "project__uid": "uid",
        "uid": "uid",
    }

    uid: str | None = Field(
        None,
        title="Project Resource UID",
        description="Public UID of the project resource.",
        examples=["857bec7b-dd77-4272-aecd-13fc2138eacc"],
    )
    project_uid: str | None = Field(
        None,
        title="Project UID",
        description="Public UID of the project this resource belongs to.",
        examples=["5a28020a-0f1b-47ee-aab8-334286234bea"],
    )
    name: str | None = Field(
        None,
        title="Resource Name",
        description="Display name of the resource discovered in the project's repository.",
        examples=["analytics_dashboard.py"],
    )
    resource_type: Literal["dashboard", "fastapi", "markdown"] | None = Field(
        None,
        title="Resource Type",
        description="Type of the project resource. Allowed values are `dashboard`,  `fastapi`, and `markdown`.",
        examples=["dashboard", "fastapi", "markdown"],
    )
    code: str | None = Field(
        None,
        title="Code",
        description="Raw file contents of the resource, when available.",
        examples=["print('hello world')"],
    )
    path: str | None = Field(
        None,
        title="Path",
        description="Repository path where the resource was discovered.",
        examples=["src/dashboards/analytics_dashboard.py"],
    )
    filesize: int | None = Field(
        None,
        title="File Size",
        description="Size of the resource file in bytes.",
        examples=[2048],
    )
    last_modified: datetime.datetime | None = Field(
        None,
        title="Last Modified",
        description="Timestamp of the last known modification to the resource.",
        examples=["2026-03-15T10:30:00Z"],
    )
    created_at: datetime.datetime | None = Field(
        None,
        title="Created At",
        description="Timestamp when the project resource record was created.",
        examples=["2026-03-14T09:00:00Z"],
    )
    updated_at: datetime.datetime | None = Field(
        None,
        title="Updated At",
        description="Timestamp when the project resource record was last updated.",
        examples=["2026-03-15T11:45:00Z"],
    )
    repo_commit_sha: str | None = Field(
        None,
        title="Repository Commit SHA",
        description="Repository commit SHA associated with this discovered resource, if available.",
        examples=["a1b2c3d4e5f678901234567890abcdef12345678"],
    )

    def _create_release(
        self,
        release_kind: ResourceReleaseKind,
        timeout=None,
        files=None,
        *args,
        **kwargs,
    ) -> ResourceRelease:
        resource_uid = self._public_detail_reference()
        kwargs["resource_uid"] = resource_uid
        kwargs["release_kind"] = release_kind.value
        return ResourceRelease.create(timeout=timeout, files=files, *args, **kwargs)

    def create_dashboard(self, timeout=None, files=None, *args, **kwargs) -> ResourceRelease:
        return self._create_release(
            ResourceReleaseKind.STREAMLIT_DASHBOARD,
            timeout,
            files,
            *args,
            **kwargs,
        )

    def create_fastapi(self, timeout=None, files=None, *args, **kwargs) -> ResourceRelease:
        return self._create_release(
            ResourceReleaseKind.FAST_API,
            timeout,
            files,
            *args,
            **kwargs,
        )


class ResourceReleaseKind(str, Enum):
    STREAMLIT_DASHBOARD = "streamlit_dashboard"
    FAST_API = "fastapi"


class ResourceRelease(ShareableObjectMixin, BaseObjectOrm, BasePydanticModel):
    uid: str | None = Field(
        None,
        title="Resource Release UID",
        description="Public UID of the resource release.",
        examples=["0ce33c15-e3b1-4677-a66e-70460b89198f"],
    )
    subdomain: str = Field(
        title="Subdomain",
        description="DNS-safe label used as the subdomain for this release.",
        examples=["analytics-123"],
    )
    resource_uid: str | None = Field(
        None,
        title="Resource UID",
        description="Public UID of the primary project resource for this release.",
        examples=["857bec7b-dd77-4272-aecd-13fc2138eacc"],
    )
    readme_resource_uid: str | None = Field(
        None,
        title="README Resource UID",
        description="Public UID of the optional README/supporting project resource.",
        examples=["b50b17b4-9a47-4b0e-b75a-b65fbdf81b0d"],
    )
    related_job_uid: str | None = Field(
        None,
        title="Related Job UID",
        description="Public UID of the job associated with this resource release.",
        examples=["7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da"],
    )
    release_kind: ResourceReleaseKind | None = Field(
        None,
        title="Release Kind",
        description="Type of resource release.",
        examples=["streamlit_dashboard"],
    )
    cpu_request: str | None = Field(
        None,
        title="CPU Request",
        description="Requested CPU for the release. Accepts decimal vCPU values or Kubernetes quantities such as 500m.",
        examples=["500m", "1"],
    )
    memory_request: str | None = Field(
        None,
        title="Memory Request",
        description="Requested memory for the release. Accepts decimal GiB values or Kubernetes quantities such as 1Gi.",
        examples=["1Gi", "2Gi"],
    )
    gpu_request: str | None = Field(
        None,
        title="GPU Request",
        description="Requested GPU count, stored as a string.",
        examples=["1", None],
    )
    gpu_type: str | None = Field(
        None,
        title="GPU Type",
        description="GPU accelerator type.",
        examples=["nvidia-tesla-t4", None],
    )
    spot: bool = Field(
        default=False,
        title="Spot",
        description="Whether the release should prefer spot or preemptible capacity.",
        examples=[False, True],
    )

    @classmethod
    def create(
        cls,
        *,
        resource_uid: str | ProjectResource | dict[str, Any],
        release_kind: ResourceReleaseKind | str,
        related_image_uid: str | ProjectImage | dict[str, Any],
        cpu_request: str | int | float | Decimal | None = None,
        memory_request: str | int | float | Decimal | None = None,
        gpu_request: str | int | None = None,
        gpu_type: str | None = None,
        spot: bool | None = None,
        timeout: int | None = None,
        files=None,
    ) -> ResourceRelease:
        resolved_resource_uid = Job._coerce_uid(resource_uid, field_name="resource_uid")
        if resolved_resource_uid is None:
            raise ValueError("resource_uid is required.")

        resolved_image_uid = Job._coerce_uid(related_image_uid, field_name="related_image_uid")
        if resolved_image_uid is None:
            raise ValueError("related_image_uid is required.")

        if isinstance(release_kind, ResourceReleaseKind):
            normalized_release_kind = release_kind.value
        else:
            normalized_release_kind = Job._normalize_str(release_kind)
            allowed_release_kinds = {kind.value for kind in ResourceReleaseKind}
            if normalized_release_kind not in allowed_release_kinds:
                raise ValueError(
                    "release_kind must be one of: " + ", ".join(sorted(allowed_release_kinds)) + "."
                )

        normalized_compute = Job._validate_and_normalize_compute_fields(
            cpu_request=cpu_request,
            memory_request=memory_request,
            gpu_request=gpu_request,
            gpu_type=gpu_type,
            require_cpu_and_memory=True,
            output_format="k8s",
        )

        payload: dict[str, Any] = {
            "resource_uid": resolved_resource_uid,
            "related_image_uid": resolved_image_uid,
            "release_kind": normalized_release_kind,
            "cpu_request": normalized_compute["cpu_request"],
            "memory_request": normalized_compute["memory_request"],
        }

        if normalized_compute["gpu_request"] is not None:
            payload["gpu_request"] = normalized_compute["gpu_request"]
        if normalized_compute["gpu_type"] is not None:
            payload["gpu_type"] = normalized_compute["gpu_type"]
        if spot is not None:
            payload["spot"] = bool(spot)

        request_payload = {"json": cls.serialize_for_json(payload)}
        if files:
            request_payload["files"] = files

        r = make_request(
            s=cls.build_session(),
            loaders=cls.LOADERS,
            r_type="POST",
            url=f"{cls.get_object_url()}/",
            payload=request_payload,
            time_out=timeout,
        )

        if r.status_code not in (200, 201, 202):
            raise_for_response(r, payload=request_payload)

        return cls(**r.json())
