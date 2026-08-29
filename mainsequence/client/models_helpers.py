from __future__ import annotations

import datetime
import json
from collections.abc import Collection
from decimal import Decimal
from enum import Enum
from pathlib import PurePosixPath
from typing import Any, ClassVar, Literal
from uuid import UUID

from pydantic import BaseModel, Field, PositiveInt

from mainsequence.code_repository_context import (
    resolve_code_repository_branch_uid,
    resolve_organization_environment_uid,
)

from .base import (
    BaseObjectOrm,
    BasePydanticModel,
    CurrentCodeRepositoryBranchCollectionMixin,
    ShareableObjectMixin,
)
from .compute_validation import (
    decimal_to_storage,
    normalize_string,
    validate_and_normalize_compute_fields,
)
from .exceptions import raise_for_response
from .models_foundry import (
    CodeRepositoryBranch,
    CodeRepositoryImage,
)
from .observability import (
    ObservabilityLinks,
    OwnerLogMixin,
    OwnerResourceUsageMixin,
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


Schedule = CrontabSchedule | IntervalSchedule


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


class AutomaticRedeploymentPolicy(BaseModel):
    tag_regex: str | None = Field(
        ...,
        title="Tag Regex",
        description=(
            "Regular expression matched against immutable repository tags. "
            "Null enables promotion for every qualifying exact commit."
        ),
        examples=[None, r"^v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)$"],
    )
    policy_revision: PositiveInt | None = Field(
        default=None,
        title="Policy Revision",
        description=("Backend-owned immutable revision. Omit it from create and update requests."),
        examples=[1, 3],
    )


class Job(CurrentCodeRepositoryBranchCollectionMixin, BaseObjectOrm, BasePydanticModel):
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "uid": ["in", "exact"],
        "code_repository_branch_uid": ["in", "exact"],
        "name": ["in", "exact", "contains"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "uid": "uid",
        "uid__in": "uid",
        "code_repository_branch_uid": "uid",
        "code_repository_branch_uid__in": "uid",
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

    code_repository_branch_uid: str | None = Field(
        default=None,
        description="Public UID of the owning CodeRepositoryBranch.",
        examples=["5a28020a-0f1b-47ee-aab8-334286234bea"],
    )

    organization_environment_uid: str | None = Field(
        default=None,
        description=(
            "Read-only Organization Environment UID derived by the backend "
            "from the owning CodeRepositoryBranch."
        ),
        examples=["58218213-5e4e-43de-a5bd-6757f4e1c8f6"],
    )

    code_repository_commit_hash: str | None = Field(
        default=None,
        description=(
            "Exact full Git commit represented by the selected CodeRepositoryImage. "
            "The backend derives this field; null is reserved for platform-maintenance Jobs."
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

    related_image_uid: str = Field(
        ...,
        description="Public UID of the exact persisted execution image.",
        examples=["f3cb8477-df47-49cb-a151-80b746fb1243"],
    )
    image_status: str = Field(
        ...,
        description="Canonical backend readiness state for the exact Job image.",
        examples=["ready", "building", "error"],
    )
    automatic_deployment: bool = Field(
        default=False,
        description=(
            "Whether future qualifying immutable repository events may promote "
            "this Job to another exact image. It never selects an initial image."
        ),
    )
    automatic_redeployment_policy: AutomaticRedeploymentPolicy | None = Field(
        default=None,
        description=("Standalone Job promotion policy. Target-owned backing Jobs return null."),
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
    def _resolve_code_repository_branch_uid(
        cls,
        code_repository_branch_uid: str | CodeRepositoryBranch | dict[str, Any] | None = None,
    ) -> str:
        return resolve_code_repository_branch_uid(
            "Job.create",
            supplied_uid=code_repository_branch_uid,
        )

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
        code_repository_branch_uid: str | CodeRepositoryBranch | dict[str, Any] | None = None,
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
        related_image_uid: str | CodeRepositoryImage | dict[str, Any] | None = None,
        automatic_deployment: bool = False,
        automatic_redeployment_policy: AutomaticRedeploymentPolicy | dict[str, Any] | None = None,
        allowed_execution_extensions: Collection[str] | str | None = None,
    ) -> dict[str, Any]:
        normalized_name = cls._normalize_str(name)
        if not normalized_name:
            raise ValueError("name is required.")

        payload: dict[str, Any] = {
            "name": normalized_name,
            "code_repository_branch_uid": cls._resolve_code_repository_branch_uid(
                code_repository_branch_uid=code_repository_branch_uid
            ),
        }

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

        payload["automatic_deployment"] = bool(automatic_deployment)
        image_uid = cls._coerce_uid(related_image_uid, field_name="related_image_uid")
        if automatic_deployment:
            if image_uid is not None:
                raise ValueError(
                    "related_image_uid must be omitted when automatic_deployment is enabled; "
                    "the backend derives the initial exact image."
                )
        else:
            if image_uid is None:
                raise ValueError(
                    "related_image_uid is required when automatic_deployment is disabled."
                )
            payload["related_image_uid"] = image_uid
        if automatic_redeployment_policy is not None:
            policy_payload = (
                automatic_redeployment_policy.model_dump(exclude_none=False)
                if isinstance(automatic_redeployment_policy, BaseModel)
                else dict(automatic_redeployment_policy)
            )
            policy_payload.pop("policy_revision", None)
            if "tag_regex" not in policy_payload:
                raise ValueError(
                    "automatic_redeployment_policy.tag_regex is required; use None for every qualifying exact event."
                )
            payload["automatic_redeployment_policy"] = policy_payload

        return payload

    @classmethod
    def create(
        cls,
        *,
        name: str,
        code_repository_branch_uid: str | CodeRepositoryBranch | dict[str, Any] | None = None,
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
        related_image_uid: str | CodeRepositoryImage | dict[str, Any] | None = None,
        automatic_deployment: bool = False,
        automatic_redeployment_policy: AutomaticRedeploymentPolicy | dict[str, Any] | None = None,
        allowed_execution_extensions: Collection[str] | str | None = None,
        timeout: int | None = None,
    ) -> Job:
        payload = cls._build_create_payload(
            name=name,
            code_repository_branch_uid=code_repository_branch_uid,
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
            automatic_deployment=automatic_deployment,
            automatic_redeployment_policy=automatic_redeployment_policy,
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

    def run_job(
        self,
        *,
        timeout: int | None = None,
        command_args: list[str] | None = None,
    ) -> dict[str, Any]:
        job_uid = self._public_detail_reference()
        if command_args is not None and not all(isinstance(arg, str) for arg in command_args):
            raise TypeError("command_args must be a list of strings.")

        url = f"{self.get_object_url()}/{job_uid}/run-job/"
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


class JobRun(
    OwnerLogMixin,
    OwnerResourceUsageMixin,
    BaseObjectOrm,
    BasePydanticModel,
):
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
    code_repository_uid: str | None = Field(
        ...,
        description="Read-only public UID of the associated CodeRepository, or null for an unscoped run.",
        examples=["1d0530c0-65d1-4db0-856b-dc29d8260a09"],
    )
    code_repository_name: str | None = Field(
        ...,
        description="Read-only name of the associated CodeRepository, or null for an unscoped run.",
        examples=["market-data-service"],
    )
    code_repository_branch_uid: str | None = Field(
        ...,
        description="Read-only public UID of the associated CodeRepositoryBranch, or null for an unscoped run.",
        examples=["5a28020a-0f1b-47ee-aab8-334286234bea"],
    )
    code_repository_branch_name: str | None = Field(
        ...,
        description="Read-only repository branch name associated with the run, or null for an unscoped run.",
        examples=["main"],
    )
    organization_environment_uid: str = Field(
        ...,
        description=(
            "Read-only public UID of the Organization Environment resolved from the "
            "JobRun's owning CodeRepositoryBranch."
        ),
        examples=["58218213-5e4e-43de-a5bd-6757f4e1c8f6"],
    )
    observability: ObservabilityLinks | None = Field(
        default=None,
        description="Backend-owned application log and resource-usage capabilities.",
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
    runtime_image_uid: str = Field(
        ...,
        description="Public UID of the immutable image snapshot executed by this run.",
    )
    runtime_image_digest: str = Field(
        ...,
        description="Immutable digest of the image snapshot executed by this run.",
        examples=["sha256:" + "a" * 64],
    )

    command_args: list[str] = Field(
        default_factory=list,
        description="Per-run command arguments requested for this job run.",
        examples=[["sync", "--from", "2026-04-01"]],
    )

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
            POST /job-runs/{uid}/status/
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


class CodeRepositoryResource(CurrentCodeRepositoryBranchCollectionMixin, BaseObjectOrm, BasePydanticModel):
    SEARCH_FIELDS: ClassVar[list[str]] = [
        "code_repository_branch_uid",
        "uid",
        "repo_commit_sha",
        "resource_type",
    ]
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "code_repository_branch_uid": ["exact"],
        "uid": ["in", "exact"],
        "repo_commit_sha": ["exact"],
        "resource_type": ["exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "code_repository_branch_uid": "uid",
        "uid": "uid",
    }
    CODE_REPOSITORY_BRANCH_FILTER_FIELD: ClassVar[str] = "code_repository_branch_uid"

    uid: str | None = Field(
        None,
        title="CodeRepository Resource UID",
        description="Public UID of the CodeRepository resource.",
        examples=["857bec7b-dd77-4272-aecd-13fc2138eacc"],
    )
    code_repository_branch_uid: str | None = Field(
        None,
        title="CodeRepositoryBranch UID",
        description="Public UID of the CodeRepositoryBranch this resource belongs to.",
        examples=["5a28020a-0f1b-47ee-aab8-334286234bea"],
    )
    name: str | None = Field(
        None,
        title="Resource Name",
        description="Display name of the resource discovered in the CodeRepository checkout.",
        examples=["analytics_dashboard.py"],
    )
    resource_type: (
        Literal[
            "configuration",
            "notebook",
            "script",
            "dashboard",
            "agent",
            "fastapi",
            "code_repository_agent_card",
            "markdown",
        ]
        | None
    ) = Field(
        None,
        title="Resource Type",
        description=(
            "Canonical backend discriminator for the discovered CodeRepository resource. "
            "Allowed values are `configuration`, `notebook`, `script`, `dashboard`, "
            "`agent`, `fastapi`, `code_repository_agent_card`, and `markdown`."
        ),
        examples=[
            "configuration",
            "notebook",
            "script",
            "dashboard",
            "agent",
            "fastapi",
            "code_repository_agent_card",
            "markdown",
        ],
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
        description="Timestamp when the CodeRepository resource record was created.",
        examples=["2026-03-14T09:00:00Z"],
    )
    updated_at: datetime.datetime | None = Field(
        None,
        title="Updated At",
        description="Timestamp when the CodeRepository resource record was last updated.",
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
        return ResourceRelease.create(*args, timeout=timeout, files=files, **kwargs)

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
    FAST_API = "fastapi"
    STATIC_SITE = "static_site"


class ResourceRelease(
    CurrentCodeRepositoryBranchCollectionMixin,
    OwnerLogMixin,
    OwnerResourceUsageMixin,
    ShareableObjectMixin,
    BaseObjectOrm,
    BasePydanticModel,
):
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "uid": ["exact", "in"],
        "code_repository_branch_uid": ["exact"],
        "resource__uid": ["exact", "in"],
        "related_job__uid": ["exact", "in"],
        "release_kind": ["exact", "in"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "uid": "uid",
        "code_repository_branch_uid": "uid",
        "resource__uid": "uid",
        "related_job__uid": "uid",
        "release_kind": "str",
    }

    uid: str | None = Field(
        None,
        title="Resource Release UID",
        description="Public UID of the resource release.",
        examples=["0ce33c15-e3b1-4677-a66e-70460b89198f"],
    )
    code_repository_branch_uid: str | None = Field(
        None,
        title="CodeRepositoryBranch UID",
        description="Public UID of the CodeRepositoryBranch that owns this release.",
        examples=["9d81d63f-b8c9-404d-9f1a-5f2ad29dbf16"],
    )
    name: str | None = Field(
        None,
        title="Name",
        description="Display name of the release. Present on collection rows.",
        examples=["Competition Analysis"],
    )
    resource_uid: str | None = Field(
        None,
        title="Resource UID",
        description="Public UID of the primary CodeRepository resource for this release.",
        examples=["857bec7b-dd77-4272-aecd-13fc2138eacc"],
    )
    readme_resource_uid: str | None = Field(
        None,
        title="README Resource UID",
        description="Public UID of the optional README/supporting CodeRepository resource.",
        examples=["b50b17b4-9a47-4b0e-b75a-b65fbdf81b0d"],
    )
    related_job_uid: str | None = Field(
        None,
        title="Related Job UID",
        description="Public UID of the job associated with this resource release.",
        examples=["7d0ab07c-d1c0-4b7f-9c69-3c1a41c0a4da"],
    )
    observability: ObservabilityLinks | None = Field(
        default=None,
        description="Backend-owned runtime observability and related-history capabilities.",
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
    automatic_deployment: bool = Field(
        default=False,
        title="Automatic Deployment",
        description=(
            "Whether repository synchronization should rotate this release to the "
            "current CodeRepository commit."
        ),
        examples=[False, True],
    )
    automatic_redeployment_policy: AutomaticRedeploymentPolicy | None = Field(
        default=None,
        title="Automatic Redeployment Policy",
        description=(
            "Backend-owned tag matching policy and its current immutable revision. "
            "The revision is returned by the API and is not part of create requests."
        ),
    )
    revision_retention_count: PositiveInt = Field(
        default=3,
        title="Revision Retention Count",
        description=(
            "Number of immutable release revisions retained by the platform. "
            "This positive release setting can be supplied on create or patch."
        ),
        examples=[3],
    )
    active_revision: str | None = Field(
        default=None,
        title="Active Revision UID",
        description=(
            "Public UID of the ready immutable revision currently serving behind "
            "the stable release URL."
        ),
        examples=["19128ab6-d72f-460c-8525-d758fa92676a", None],
    )
    desired_revision: str | None = Field(
        default=None,
        title="Desired Revision UID",
        description=(
            "Public UID of the accepted immutable revision currently being "
            "materialized. It may differ from the active revision during deployment."
        ),
        examples=["2a9370a7-c07f-439c-bcd9-629e3e916699", None],
    )
    cors_allowed_origins: list[str] | None = Field(
        default=None,
        title="CORS Allowed Origins",
        description=(
            "Browser origins allowed to call a FastAPI release. The backend omits "
            "this field for other release kinds."
        ),
        examples=[["https://app.example.com", "https://*.site-dev.main-sequence.app"]],
    )

    @classmethod
    def create(
        cls,
        *,
        resource_uid: str | CodeRepositoryResource | dict[str, Any],
        release_kind: ResourceReleaseKind | str,
        related_image_uid: str | CodeRepositoryImage | dict[str, Any],
        cpu_request: str | int | float | Decimal | None = None,
        memory_request: str | int | float | Decimal | None = None,
        gpu_request: str | int | None = None,
        gpu_type: str | None = None,
        spot: bool | None = None,
        automatic_deployment: bool | None = None,
        revision_retention_count: int | None = None,
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
        if automatic_deployment is not None:
            payload["automatic_deployment"] = bool(automatic_deployment)
        if revision_retention_count is not None:
            payload["revision_retention_count"] = cls._normalize_revision_retention_count(
                revision_retention_count
            )

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

    @staticmethod
    def _normalize_revision_retention_count(value: int) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError("revision_retention_count must be a positive integer.")
        return value

    @classmethod
    def patch_by_uid(cls, uid: str, *args, _into=None, **kwargs):
        if "revision_retention_count" in kwargs:
            kwargs["revision_retention_count"] = cls._normalize_revision_retention_count(
                kwargs["revision_retention_count"]
            )
        return super().patch_by_uid(uid, *args, _into=_into, **kwargs)

    def deploy_current_version(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> DeploymentRun:
        data = self._request_detail_action(
            r_type="POST",
            action_name="deploy-current-version",
            payload={},
            timeout=timeout,
            expected_statuses=(200, 202),
        )
        return DeploymentRun(**data)


class DeploymentRunTarget(BaseModel):
    uid: str | None = None
    name: str = ""
    kind: str = ""


class DeploymentRunLogReference(BaseModel):
    state: str
    url: str
    retention_expires_at: datetime.datetime | None = None


class DeploymentRunError(BaseModel):
    code: str = ""
    detail: str = ""


class DeploymentRunStep(BaseModel):
    uid: str
    sequence: PositiveInt
    key: str
    name: str = ""
    kind: Literal[
        "source",
        "image_build",
        "validation",
        "runtime_deploy",
        "runtime_readiness",
        "publish",
        "cleanup",
        "orchestration",
    ]
    required: bool = True
    state: Literal[
        "pending",
        "running",
        "succeeded",
        "failed",
        "cancelled",
        "skipped",
        "blocked",
        "superseded",
    ]
    outcome: str = ""
    artifact_context: dict[str, Any] = Field(default_factory=dict)
    started_at: datetime.datetime | None = None
    finished_at: datetime.datetime | None = None
    error: DeploymentRunError | None = None


class DeploymentRunPipeline(BaseModel):
    key: str
    version: PositiveInt
    current_step_key: str | None = None
    steps: list[DeploymentRunStep] = Field(default_factory=list)


class DeploymentRunLogEntry(BaseModel):
    sequence: int
    timestamp: datetime.datetime | None = None
    step_uid: str | None = None
    source: str
    stream: Literal["stdout", "stderr"]
    level: Literal["debug", "info", "warning", "error"]
    text: str = ""


class DeploymentRunLogSource(BaseModel):
    source: str
    state: Literal["pending", "available", "partial", "expired", "unavailable"]


class DeploymentRunLogPage(BaseModel):
    run_uid: str
    entries: list[DeploymentRunLogEntry] = Field(default_factory=list)
    sources: list[DeploymentRunLogSource] = Field(default_factory=list)
    next_cursor: str | None = None
    complete: bool
    retention_expires_at: datetime.datetime | None = None


class DeploymentRun(CurrentCodeRepositoryBranchCollectionMixin, BaseObjectOrm, BasePydanticModel):
    ENDPOINT: ClassVar[str] = "deployment-runs"
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "code_repository_branch_uid": ["exact", "in"],
        "target_type": ["exact", "in"],
        "target_uid": ["exact", "in"],
        "target_kind": ["exact", "in"],
        "operation": ["exact", "in"],
        "state": ["exact", "in"],
        "source": ["exact", "in"],
        "commit_sha": ["exact", "in"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "code_repository_branch_uid": "uid",
        "target_type": "str",
        "target_uid": "uid",
        "target_kind": "str",
        "operation": "str",
        "state": "str",
        "source": "str",
        "commit_sha": "str",
    }
    READ_QUERY_PARAMS: ClassVar[dict[str, str]] = {
        "created_after": "str",
        "created_before": "str",
        "ordering": "str",
        "search": "str",
    }

    uid: str
    target_type: str
    target: DeploymentRunTarget
    code_repository_branch_uid: str | None = None
    operation: str
    source: str
    commit_sha: str = ""
    configuration_revision: int | None = None
    state: str
    outcome: str = ""
    pipeline: DeploymentRunPipeline
    created_at: datetime.datetime
    started_at: datetime.datetime | None = None
    finished_at: datetime.datetime | None = None
    revision_context: dict[str, Any] = Field(default_factory=dict)
    trigger_context: dict[str, Any] = Field(default_factory=dict)
    artifact_context: dict[str, Any] = Field(default_factory=dict)
    cleanup_context: dict[str, Any] = Field(default_factory=dict)
    result: dict[str, Any] = Field(default_factory=dict)
    builder_image: str = ""
    builder_runtime: str = ""
    logs: DeploymentRunLogReference
    error: DeploymentRunError | None = None

    def get_logs(
        self,
        *,
        cursor: str | None = None,
        limit: int | None = None,
        step_uid: str | UUID | None = None,
        source: str | None = None,
        level: str | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> DeploymentRunLogPage:
        params = {
            key: value
            for key, value in {
                "cursor": cursor,
                "limit": limit,
                "step_uid": str(step_uid) if step_uid is not None else None,
                "source": source,
                "level": level,
                "organization_environment_uid": resolve_organization_environment_uid(
                    "DeploymentRun.get_logs"
                ),
            }.items()
            if value is not None
        }
        response = make_request(
            s=self.build_session(),
            loaders=self.LOADERS,
            r_type="GET",
            url=f"{self.get_object_url()}/{self._public_detail_reference()}/logs/",
            payload={"params": params},
            time_out=timeout,
        )
        raise_for_response(response, payload={"params": params})
        return DeploymentRunLogPage(**response.json())
