from __future__ import annotations

import datetime
import json
import pathlib
from collections.abc import Collection
from decimal import Decimal
from pathlib import PurePosixPath
from typing import Any, ClassVar, Literal, Union

import yaml
from pydantic import BaseModel, Field, PositiveInt

from ..compute_validation import (
    decimal_to_storage,
    normalize_string,
    validate_and_normalize_compute_fields,
)
from .base import ShareableObjectMixin
from .exceptions import raise_for_response
from .models_tdag import POD_PROJECT, Project, ProjectImage
from .models_vam import *
from .utils import make_request


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
    CPU_MIN: ClassVar[Decimal] = Decimal("0.25")
    CPU_MAX: ClassVar[Decimal] = Decimal("30")
    MEMORY_MIN: ClassVar[Decimal] = Decimal("0.5")
    MEMORY_MAX: ClassVar[Decimal] = Decimal("110")
    MEMORY_PER_CPU_MIN: ClassVar[Decimal] = Decimal("1")
    MEMORY_PER_CPU_MAX: ClassVar[Decimal] = Decimal("6.5")
    GPU_MIN: ClassVar[int] = 1
    GPU_MAX: ClassVar[int] = 8
    DEFAULT_ALLOWED_EXECUTION_EXTENSIONS: ClassVar[frozenset[str]] = frozenset({".py", ".ipynb", ".yaml"})

    id: int | None = Field(
        default=None,
        description="Unique job identifier.",
        examples=[123],
    )

    name: str = Field(
        ...,
        min_length=1,
        description="Human-readable job name.",
        examples=["Daily feature build"],
    )

    project: int | Project | None = Field(
        default=None,
        description="Owning project, either as a project id or a Project object.",
        examples=[42],
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
        examples=["portfolio-monitor"],
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

    related_image: int | ProjectImage | None = Field(
        default=None,
        description="Execution image, either as an image id or a ProjectImage object.",
        examples=[77],
    )

    @staticmethod
    def _coerce_id(obj: Any, *, field_name: str) -> int | None:
        if obj is None:
            return None
        if isinstance(obj, int):
            return obj
        if hasattr(obj, "id") and obj.id is not None:
            return int(obj.id)
        if isinstance(obj, dict) and obj.get("id") is not None:
            return int(obj["id"])
        raise TypeError(
            f"{field_name} must be an int id, an object with .id, or None. Got: {type(obj)!r}"
        )

    @staticmethod
    def _normalize_str(value: Any) -> str | None:
        return normalize_string(value)

    @staticmethod
    def _decimal_to_storage(value: Decimal | None) -> str | None:
        return decimal_to_storage(value)

    @classmethod
    def _resolve_project_id(
        cls,
        *,
        project: int | Project | None = None,
        project_id: int | Project | None = None,
    ) -> int:
        ref = project_id if project_id is not None else project
        if ref is None and POD_PROJECT is not None:
            ref = POD_PROJECT

        resolved = cls._coerce_id(ref, field_name="project")
        if resolved is None:
            raise ValueError("project is required. Pass project/project_id or set POD_PROJECT.")
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
            raise ValueError("task_schedule_id is not supported by the current backend. Pass task_schedule instead.")

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
                raise ValueError("task_schedule.schedule.every must be a positive integer.") from exc
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
                raise ValueError("task_schedule.schedule.expression is required for crontab schedules.")
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
        project: int | Project | None = None,
        project_id: int | Project | None = None,
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
        related_image: int | ProjectImage | None = None,
        related_image_id: int | ProjectImage | None = None,
        allowed_execution_extensions: Collection[str] | str | None = None,
    ) -> dict[str, Any]:
        normalized_name = cls._normalize_str(name)
        if not normalized_name:
            raise ValueError("name is required.")

        payload: dict[str, Any] = {
            "name": normalized_name,
            "project": cls._resolve_project_id(project=project, project_id=project_id),
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

        image_ref = related_image_id if related_image_id is not None else related_image
        image_id = cls._coerce_id(image_ref, field_name="related_image")
        if image_id is None:
            raise ValueError("related_image_id is required.")
        payload["related_image"] = image_id

        return payload

    @classmethod
    def create(
        cls,
        *,
        name: str,
        project: int | Project | None = None,
        project_id: int | Project | None = None,
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
        related_image: int | ProjectImage | None = None,
        related_image_id: int | ProjectImage | None = None,
        allowed_execution_extensions: Collection[str] | str | None = None,
        timeout: int | None = None,
    ) -> Job:
        payload = cls._build_create_payload(
            name=name,
            project=project,
            project_id=project_id,
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
            related_image=related_image,
            related_image_id=related_image_id,
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
        project_id: int | Project,
        strict: bool = False,
        timeout: int | None = None,
    ) -> list[Job] | dict[str, Any]:
        """
        Validate a batch YAML file and synchronize its jobs with the backend.

        Request body:
          - project
          - jobs
          - strict

        Strict mode notes:
          - jobs that exist remotely but are not present in the YAML may be deleted
          - jobs linked to dashboards or resource releases are protected and may be returned
            in the response under `not_deleted`
        """
        resolved_project_id = cls._coerce_id(project_id, field_name="project_id")
        if resolved_project_id is None:
            raise ValueError("project_id is required.")

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

            if "project" in job_data or "project_id" in job_data:
                raise ValueError(
                    f"jobs[{index}] must not define project or project_id. "
                    "Pass the target project_id to Job.bulk_get_or_create()."
                )
            if "timeout" in job_data:
                raise ValueError(f"jobs[{index}] must not define timeout.")
            if "related_image" in job_data and "related_image_id" in job_data:
                raise ValueError(
                    f"jobs[{index}] must not define both related_image and related_image_id."
                )
            if "related_image_id" in job_data:
                job_data["related_image"] = job_data.pop("related_image_id")

            try:
                normalized_job = cls._build_create_payload(
                    project_id=resolved_project_id,
                    **job_data,
                )
                normalized_job.pop("project", None)
                normalized_jobs.append(normalized_job)
            except TypeError as exc:
                raise ValueError(
                    f"jobs[{index}] has unsupported or missing fields: {exc}"
                ) from exc
            except Exception as exc:
                raise ValueError(f"jobs[{index}] is invalid: {exc}") from exc

        request_payload = {
            "json": cls.serialize_for_json(
                {
                    "project": resolved_project_id,
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
        payload["project_id"] = POD_PROJECT.id

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

    def run_job(self, *, timeout: int | None = None) -> dict[str, Any]:
        if self.id is None:
            raise ValueError("Job must have an id before it can be run.")

        url = f"{self.get_object_url()}/{self.id}/run_job/"
        s = self.build_session()

        r = make_request(
            s=s,
            loaders=self.LOADERS,
            r_type="POST",
            url=url,
            payload={},
            time_out=timeout,
        )

        if r.status_code not in (200, 201, 202):
            raise_for_response(r)

        return r.json()

class JobRun(BaseObjectOrm, BasePydanticModel):
    id: int | None = Field(
        default=None,
        description="The unique ID of the job run.",
        examples=[123],
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
        description="A unique identifier for this specific job run.",
        examples=["jobrun_2026_03_14_abc123"],
    )

    job: int | Job | None = Field(
        default=None,
        description="The associated job ID or Job object.",
        examples=[42],
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

    cpu_limit: str | None = Field(
        default=None,
        description="The CPU limit applied to this job run.",
        examples=["2"],
    )
    memory_limit: str | None = Field(
        default=None,
        description="The memory limit applied to this job run.",
        examples=["8Gi"],
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

    @staticmethod
    def _coerce_id(obj: Any, *, field_name: str) -> int | None:
        if obj is None:
            return None
        if isinstance(obj, int):
            return obj
        if hasattr(obj, "id") and obj.id is not None:
            return int(obj.id)
        if isinstance(obj, dict) and obj.get("id") is not None:
            return int(obj["id"])
        raise TypeError(
            f"{field_name} must be an int id, an object with .id, or None. Got: {type(obj)!r}"
        )

    def get_logs(self, *, timeout: int | None = None) -> dict[str, Any]:
        if self.id is None:
            raise ValueError("JobRun must have an id before logs can be fetched.")

        url = f"{self.get_object_url()}/{self.id}/get_logs/"
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



class ProjectResource(BaseObjectOrm, BasePydanticModel):
    SEARCH_FIELDS: ClassVar[list[str]] = [
        "project__id",
        "id",
        "repo_commit_sha",
        "resource_type",
    ]
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "project__id": ["exact"],
        "id": ["in", "exact"],
        "repo_commit_sha": ["exact"],
        "resource_type": ["exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "project__id": "id",
        "id": "id",
    }

    # present on ProjectResourceSerializer
    id: int | None = Field(
        None,
        title="Project Resource ID",
        description="Unique identifier of the project resource.",
        examples=[101],
    )
    project: int | Project | None = Field(
        None,
        title="Project",
        description="Project this resource belongs to. Can be either the project ID or the expanded Project object.",
        examples=[12],
    )
    name: str | None = Field(
        None,
        title="Resource Name",
        description="Display name of the resource discovered in the project's repository.",
        examples=["analytics_dashboard.py"],
    )
    resource_type: Literal["dashboard", "agent", "markdown"] | None = Field(
        None,
        title="Resource Type",
        description="Type of the project resource. Allowed values are `dashboard`, `agent`, and `markdown`.",
        examples=["dashboard", "markdown"],
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
        if self.id is None:
            raise ValueError("ProjectResource must have an id before creating a release.")

        kwargs["resource"] = self.id
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

    def create_agent(self, timeout=None, files=None, *args, **kwargs) -> ResourceRelease:
        return self._create_release(
            ResourceReleaseKind.AGENT,
            timeout,
            files,
            *args,
            **kwargs,
        )


class ResourceReleaseKind(str, Enum):
    STREAMLIT_DASHBOARD = "streamlit_dashboard"
    AGENT = "agent"


class ResourceRelease(ShareableObjectMixin, BaseObjectOrm, BasePydanticModel):
    # present on ResourceReleaseSerializer
    id: int | None = Field(
        None,
        title="Resource Release ID",
        description="Unique identifier of the resource release.",
        examples=[123],
    )
    subdomain: str  = Field(
        title="Subdomain",
        description="DNS-safe label used as the subdomain for this release.",
        examples=["analytics-123"],
    )
    resource: int | ProjectResource | None = Field(
        None,
        title="Resource",
        description="Primary project resource for this release. Can be either the resource ID or the expanded ProjectResource object.",
        examples=[42],
    )
    readme_resource: int | ProjectResource | None = Field(
        None,
        title="README Resource",
        description="Optional project resource containing README or supporting documentation for this release. Can be either the resource ID or the expanded ProjectResource object.",
        examples=[84],
    )
    related_job: int | Job  = Field(
        title="Related Job",
        description="Job associated with this resource release. Can be either the job ID or the expanded Job object.",
        examples=[7],
    )
    release_kind: ResourceReleaseKind | None = Field(
        None,
        title="Release Kind",
        description="Type of resource release.",
        examples=["streamlit_dashboard"],
    )
    related_image: int | ProjectImage | None = Field(
        None,
        title="Related Image",
        description="Execution image, either as an image id or the expanded ProjectImage object.",
        examples=[94],
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
        resource: int | ProjectResource,
        release_kind: ResourceReleaseKind | str,
        readme_resource: int | ProjectResource | None = None,
        readme_resource_id: int | ProjectResource | None = None,
        related_image: int | ProjectImage | None = None,
        related_image_id: int | ProjectImage | None = None,
        cpu_request: str | int | float | Decimal | None = None,
        memory_request: str | int | float | Decimal | None = None,
        gpu_request: str | int | None = None,
        gpu_type: str | None = None,
        spot: bool | None = None,
        timeout: int | None = None,
        files=None,
    ) -> ResourceRelease:
        resource_id = Job._coerce_id(resource, field_name="resource")
        if resource_id is None:
            raise ValueError("resource is required.")

        if isinstance(release_kind, ResourceReleaseKind):
            normalized_release_kind = release_kind.value
        else:
            normalized_release_kind = Job._normalize_str(release_kind)
            allowed_release_kinds = {kind.value for kind in ResourceReleaseKind}
            if normalized_release_kind not in allowed_release_kinds:
                raise ValueError(
                    "release_kind must be one of: "
                    + ", ".join(sorted(allowed_release_kinds))
                    + "."
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
            "resource": resource_id,
            "release_kind": normalized_release_kind,
            "cpu_request": normalized_compute["cpu_request"],
            "memory_request": normalized_compute["memory_request"],
        }

        readme_ref = readme_resource_id if readme_resource_id is not None else readme_resource
        readme_id = Job._coerce_id(readme_ref, field_name="readme_resource")
        if readme_id is not None:
            payload["readme_resource"] = readme_id

        image_ref = related_image_id if related_image_id is not None else related_image
        image_id = Job._coerce_id(image_ref, field_name="related_image")
        if image_id is not None:
            payload["related_image"] = image_id

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
