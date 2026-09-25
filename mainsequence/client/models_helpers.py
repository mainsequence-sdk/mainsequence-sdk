from __future__ import annotations

import datetime
import time
from decimal import Decimal
from typing import Any, ClassVar, Literal
from uuid import UUID

from pydantic import BaseModel, Field, PositiveInt

from .base import (
    BaseObjectOrm,
    BasePydanticModel,
    CurrentCodeRepositoryBranchCollectionMixin,
    ShareableObjectMixin,
)
from .exceptions import raise_for_response
from .observability import (
    EnvironmentLogSearchMixin,
    EnvironmentLogSearchPage,
    LogSearchOutcome,
    LogTime,
    ObservabilityLinks,
    OwnerLogMixin,
    OwnerResourceUsageMixin,
    PublicLogLevel,
)
from .utils import make_request
from .value_sets import OpenStrEnum, OpenValueSet


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
    COLLECTION_CREATE_SUPPORTED: ClassVar[bool] = False
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

    description: str = Field(
        default="",
        description=(
            "Optional human-readable purpose or context for the Job. "
            "It does not affect execution behavior."
        ),
        examples=["Refresh the daily prices dataset."],
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
            "Repository-relative file path from the content root. Allowed extensions are .py and .yaml."
        ),
        examples=["scripts/test.py", "jobs/train_model.py"],
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

    scheduled_command_args: list[str] = Field(
        default_factory=list,
        description=(
            "Opaque argv entries copied into each future scheduler-created JobRun. "
            "Manual runs use only the command_args supplied to Job.run_job()."
        ),
        examples=[["--start-date", "2026-09-08T16:43:00Z", "--family", "jobs"]],
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
        ...,
        description=(
            "Public UID of the exact persisted execution image. Null while the "
            "backend has not bound one to the job."
        ),
        examples=["f3cb8477-df47-49cb-a151-80b746fb1243", None],
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
    def _normalize_command_args(value: Any, *, field_name: str) -> list[str]:
        if not isinstance(value, list) or not all(isinstance(arg, str) for arg in value):
            raise TypeError(f"{field_name} must be a list of strings.")
        return list(value)


    @classmethod
    def patch_by_uid(cls, uid: str, *args, _into=None, **kwargs):
        if "scheduled_command_args" in kwargs:
            kwargs["scheduled_command_args"] = cls._normalize_command_args(
                kwargs["scheduled_command_args"],
                field_name="scheduled_command_args",
            )
        return super().patch_by_uid(uid, *args, _into=_into, **kwargs)

    def run_job(
        self,
        *,
        timeout: int | None = None,
        command_args: list[str] | None = None,
    ) -> dict[str, Any]:
        """Start a manual run with optional opaque argv entries.

        ``command_args`` applies only to this run. It does not configure
        arguments for scheduler-created runs.
        """
        job_uid = self._public_detail_reference()
        if command_args is not None:
            command_args = self._normalize_command_args(
                command_args,
                field_name="command_args",
            )

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
    EnvironmentLogSearchMixin,
    OwnerLogMixin,
    OwnerResourceUsageMixin,
    BaseObjectOrm,
    BasePydanticModel,
):
    _OBSERVABILITY_ENVIRONMENT_OPTIONAL_FIELDS: ClassVar[frozenset[str]] = frozenset(
        {"application_logs_url"}
    )
    PUBLIC_LOOKUP_FIELD: ClassVar[str] = "uid"
    FILTERSET_FIELDS: ClassVar[dict[str, list[str]]] = {
        "job__uid": ["in", "exact"],
        "uid": ["in", "exact"],
    }
    FILTER_VALUE_NORMALIZERS: ClassVar[dict[str, str]] = {
        "job__uid": "str",
        "uid": "str",
    }

    @classmethod
    def search_logs(
        cls,
        *,
        organization_environment_uid: str | UUID,
        start_time: LogTime,
        end_time: LogTime,
        job_run_uid: str | UUID | None = None,
        job_uid: str | UUID | None = None,
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
            job_run_uid=job_run_uid,
            job_uid=job_uid,
            cursor=cursor,
            limit=limit,
            level=level,
            event=event,
            request_id=request_id,
            outcome=outcome,
            timeout=timeout,
        )

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
    runtime_image_uid: str | None = Field(
        ...,
        description=(
            "Public UID of the immutable image snapshot executed by this run. "
            "Null for a run the backend has not resolved an image for."
        ),
        examples=["6cfdb152-923e-45b9-a150-c4541c68b0d1", None],
    )
    runtime_image_digest: str | None = Field(
        ...,
        description=(
            "Immutable digest of the image snapshot executed by this run. Null "
            "for a run the backend has not resolved an image for."
        ),
        examples=["sha256:" + "a" * 64, None],
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
        examples=["main.py"],
    )
    resource_type: (
        Literal[
            "configuration",
            "notebook",
            "script",
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
            "Allowed values are `configuration`, `notebook`, `script`, `agent`, "
            "`fastapi`, `code_repository_agent_card`, and `markdown`."
        ),
        examples=[
            "configuration",
            "notebook",
            "script",
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
        examples=["api/pricing/main.py"],
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


class ResourceReleaseKind(OpenStrEnum):
    """The backend's canonical release-kind vocabulary.

    Open (ADR 0033): a kind a later backend adds is read as sent, rather than
    failing the release that carries it and the listing that release is in.
    """

    AGENT = "agent"
    FAST_API = "fastapi"
    HARNESS_AGENT = "harness_agent"
    STATIC_SITE = "static_site"


class ResourceReleaseRuntimeRouting(BasePydanticModel):
    state: OpenValueSet[Literal["routable", "unavailable"]]
    active_revision_uid: str | None = None


class ResourceReleaseRuntimeNotice(BasePydanticModel):
    code: str
    severity: OpenValueSet[Literal["info", "warning", "error"]]
    title: str
    message: str


class ResourceReleaseRuntimeOperation(BasePydanticModel):
    uid: str
    status: OpenValueSet[Literal["queued", "running", "succeeded", "failed", "superseded"]]
    created_at: datetime.datetime
    started_at: datetime.datetime | None = None
    finished_at: datetime.datetime | None = None
    support_reference: str


class ResourceReleaseRuntimeAdmission(BasePydanticModel):
    state: OpenValueSet[Literal["ready", "waking", "unavailable"]]
    can_request: bool
    notice: ResourceReleaseRuntimeNotice | None = None
    operation: ResourceReleaseRuntimeOperation | None = None
    retry_after_ms: int | None = Field(default=None, ge=0)


class ResourceReleaseRuntimeReplicas(BasePydanticModel):
    desired: int | None = Field(default=None, ge=0)
    actual: int | None = Field(default=None, ge=0)


class ResourceReleaseRuntimeWake(BasePydanticModel):
    operation_uid: str
    state: OpenValueSet[
        Literal[
            "requested", "in_progress", "serving", "failed", "expired", "superseded"
        ]
    ]
    requested_at: datetime.datetime
    deadline_at: datetime.datetime


class ResourceReleaseRuntimePresence(BasePydanticModel):
    phase: OpenValueSet[
        Literal[
            "not_deployed",
            "idle",
            "observing",
            "provisioning",
            "pulling_image",
            "starting",
            "serving",
            "redeploying",
            "failed",
        ]
    ]
    replicas: ResourceReleaseRuntimeReplicas
    detail: str
    observed_at: datetime.datetime | None = None
    wake: ResourceReleaseRuntimeWake | None = None


class ResourceReleaseRuntimeAccess(BasePydanticModel):
    resource_release_uid: str
    # The canonical vocabulary ResourceRelease.release_kind reads (#119): this
    # payload names a release kind, so it names it the same way.
    release_kind: ResourceReleaseKind
    routing: ResourceReleaseRuntimeRouting
    runtime_access: ResourceReleaseRuntimeAdmission
    runtime_presence: ResourceReleaseRuntimePresence
    access: dict[str, Any] | None = None


class ResourceRelease(
    CurrentCodeRepositoryBranchCollectionMixin,
    EnvironmentLogSearchMixin,
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

    @classmethod
    def search_logs(
        cls,
        *,
        organization_environment_uid: str | UUID,
        start_time: LogTime,
        end_time: LogTime,
        resource_release_uid: str | UUID | None = None,
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
            resource_release_uid=resource_release_uid,
            cursor=cursor,
            limit=limit,
            level=level,
            event=event,
            request_id=request_id,
            outcome=outcome,
            timeout=timeout,
        )

    def resolve_runtime_access(
        self,
        *,
        static_site_release_uid: str | UUID | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> ResourceReleaseRuntimeAccess:
        """Ask Django for the deployed runtime's current admission decision."""

        if self.uid is None:
            raise ValueError("ResourceRelease.uid is required to resolve runtime access")
        body = {}
        if static_site_release_uid is not None:
            body["static_site_release_uid"] = str(static_site_release_uid)
        payload = {"json": self.serialize_for_json(body)}
        response = make_request(
            s=self.build_session(),
            loaders=self.LOADERS,
            r_type="POST",
            url=f"{self.get_detail_url()}resolve-runtime-access/",
            payload=payload,
            time_out=timeout,
        )
        if response.status_code != 200:
            raise_for_response(response, payload=payload)
        data = response.json()
        if not isinstance(data, dict):
            raise TypeError("ResourceRelease runtime access response must be a JSON object")
        return ResourceReleaseRuntimeAccess.model_validate(data)

    def wait_for_runtime_access(
        self,
        *,
        static_site_release_uid: str | UUID | None = None,
        wait_timeout_seconds: float = 600.0,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> ResourceReleaseRuntimeAccess:
        """Poll Django with backend-directed bounded backoff until admission settles."""

        if wait_timeout_seconds <= 0:
            raise ValueError("wait_timeout_seconds must be greater than 0")
        deadline = time.monotonic() + wait_timeout_seconds
        while True:
            access = self.resolve_runtime_access(
                static_site_release_uid=static_site_release_uid,
                timeout=timeout,
            )
            if access.runtime_access.can_request:
                return access
            if access.runtime_access.state != "waking":
                detail = (
                    access.runtime_access.notice.message
                    if access.runtime_access.notice is not None
                    else access.runtime_presence.detail
                )
                raise RuntimeError(detail)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(
                    f"Timed out waiting for ResourceRelease {self.uid} runtime access"
                )
            retry_after_ms = access.runtime_access.retry_after_ms
            if retry_after_ms is None:
                raise RuntimeError(
                    "Transient runtime access response is missing retry_after_ms."
                )
            time.sleep(min(remaining, max(0.1, retry_after_ms / 1000.0)))

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
    release_kind: ResourceReleaseKind = Field(
        ...,
        title="Release Kind",
        description="Type of resource release.",
        examples=["fastapi"],
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
    COLLECTION_CREATE_SUPPORTED: ClassVar[bool] = False

    revision_retention_count: PositiveInt = Field(
        default=3,
        title="Revision Retention Count",
        description=(
            "Number of immutable release revisions retained by the platform. "
            "This positive release setting can be supplied in a workflow or patch."
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
    kind: OpenValueSet[
        Literal[
            "source",
            "image_build",
            "validation",
            "runtime_deploy",
            "runtime_readiness",
            "publish",
            "cleanup",
            "orchestration",
        ]
    ]
    required: bool = True
    state: OpenValueSet[
        Literal[
            "pending",
            "running",
            "succeeded",
            "failed",
            "cancelled",
            "skipped",
            "blocked",
            "superseded",
        ]
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


class DeploymentRunBillingComponents(BasePydanticModel):
    image_build: Decimal | None = Field(max_digits=18, decimal_places=6)
    image_registry_storage: Decimal | None = Field(max_digits=18, decimal_places=6)
    image_registry_service: Decimal | None = Field(max_digits=18, decimal_places=6)


DeploymentRunPricingState = OpenValueSet[
    Literal[
        "priced",
        "partial",
        "pending",
        "unavailable",
        "failed",
    ]
]


class DeploymentRunBilling(BasePydanticModel):
    scope: Literal["image_lifecycle"]
    total_cost: Decimal | None = Field(max_digits=18, decimal_places=6)
    currency: str = Field(min_length=1)
    pricing_state: DeploymentRunPricingState
    components: DeploymentRunBillingComponents
    priced_rows: int = Field(ge=0)
    unpriced_rows: int = Field(ge=0)
    reused_image_count: int = Field(ge=0)


class DeploymentRunRuntimeBilling(BasePydanticModel):
    scope: Literal["knative_runtime"]
    total_cost: Decimal | None = Field(max_digits=18, decimal_places=6)
    base_cost: Decimal | None = Field(max_digits=18, decimal_places=6)
    currency: str = Field(min_length=1)
    pricing_state: DeploymentRunPricingState
    priced_rows: int = Field(ge=0)
    unpriced_rows: int = Field(ge=0)
    is_complete: bool


class DeploymentRunCostSummary(BasePydanticModel):
    total_cost: Decimal | None = Field(max_digits=18, decimal_places=6)
    currency: str = Field(min_length=1)
    is_complete: bool


class DeploymentRunLogEntry(BaseModel):
    sequence: int
    timestamp: datetime.datetime | None = None
    step_uid: str | None = None
    source: str
    stream: OpenValueSet[Literal["stdout", "stderr"]]
    level: OpenValueSet[Literal["debug", "info", "warning", "error"]]
    text: str = ""


class DeploymentRunLogSource(BaseModel):
    source: str
    state: OpenValueSet[Literal["pending", "available", "partial", "expired", "unavailable"]]


class DeploymentRunLogPage(BaseModel):
    run_uid: str
    start_time: datetime.datetime | None = None
    end_time: datetime.datetime | None = None
    entries: list[DeploymentRunLogEntry] = Field(default_factory=list)
    sources: list[DeploymentRunLogSource] = Field(default_factory=list)
    next_cursor: str | None = None
    complete: bool
    retention_expires_at: datetime.datetime | None = None


class DeploymentRun(
    CurrentCodeRepositoryBranchCollectionMixin,
    EnvironmentLogSearchMixin,
    BaseObjectOrm,
    BasePydanticModel,
):
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

    @classmethod
    def search_logs(
        cls,
        *,
        organization_environment_uid: str | UUID,
        start_time: LogTime,
        end_time: LogTime,
        deployment_run_uid: str | UUID | None = None,
        target_type: Literal[
            "job",
            "code_repository_executor",
            "user_orchestrator",
            "resource_release",
            "static_site",
            "code_repository_image",
        ]
        | None = None,
        target_uid: str | UUID | None = None,
        step_uid: str | UUID | None = None,
        source: Literal[
            "orchestrator",
            "code_repository_image_build",
            "static_site_build",
        ]
        | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        level: PublicLogLevel | None = None,
        event: str | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> EnvironmentLogSearchPage:
        return cls._search_environment_logs(
            organization_environment_uid=organization_environment_uid,
            start_time=start_time,
            end_time=end_time,
            deployment_run_uid=deployment_run_uid,
            target_type=target_type,
            target_uid=target_uid,
            step_uid=step_uid,
            source=source,
            cursor=cursor,
            limit=limit,
            level=level,
            event=event,
            timeout=timeout,
        )

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
    billing: DeploymentRunBilling
    runtime_billing: DeploymentRunRuntimeBilling
    cost_summary: DeploymentRunCostSummary

    def get_logs(
        self,
        *,
        organization_environment_uid: str | UUID | None = None,
        start_time: LogTime | None = None,
        end_time: LogTime | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        step_uid: str | UUID | None = None,
        source: Literal[
            "orchestrator",
            "code_repository_image_build",
            "static_site_build",
        ]
        | None = None,
        level: PublicLogLevel | None = None,
        event: str | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> DeploymentRunLogPage:
        params = {
            key: value
            for key, value in {
                "cursor": cursor,
                "limit": limit,
                "start_time": (
                    start_time.isoformat()
                    if isinstance(start_time, datetime.datetime)
                    else start_time
                ),
                "end_time": (
                    end_time.isoformat() if isinstance(end_time, datetime.datetime) else end_time
                ),
                "step_uid": str(step_uid) if step_uid is not None else None,
                "source": source,
                "level": level,
                "event": event,
                "organization_environment_uid": (
                    str(organization_environment_uid)
                    if organization_environment_uid is not None
                    else None
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
