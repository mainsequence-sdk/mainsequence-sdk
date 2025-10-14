import datetime
from typing import Any, Literal, Union

from pydantic import BaseModel, Field, PositiveInt

from .models_tdag import POD_PROJECT
from .models_vam import *
from .utils import MARKETS_CONSTANTS


def get_right_account_class(account: Account):
    from mainsequence.client import models_vam as model_module

    execution_venue_symbol = account.execution_venue.symbol
    AccountClass = getattr(
        model_module, MARKETS_CONSTANTS.ACCOUNT_VENUE_FACTORY[execution_venue_symbol]
    )
    account, _ = AccountClass.get(id=account.id)
    return account


class Slide(BasePydanticModel):
    id: int | None = None

    number: PositiveInt = Field(
        ...,
        description="1-based position of the slide within its presentation",
        example=3,
    )
    body: str | None = Field(
        default=None,
        description="Raw slide content in markdown/HTML/etc.",
    )
    created_at: datetime.datetime = Field(
        default_factory=datetime.datetime.utcnow,
        description="Timestamp when the slide row was created",
        example="2025-06-02T12:34:56Z",
    )
    updated_at: datetime.datetime = Field(
        default_factory=datetime.datetime.utcnow,
        description="Timestamp automatically updated on save",
        example="2025-06-02T12:34:56Z",
    )


class Presentation(BaseObjectOrm, BasePydanticModel):
    id: int | None = None
    title: str = Field(..., max_length=255)
    description: str = Field("", description="Free-form description of the deck")
    slides: list[Slide]

    # These come from the DB and are read-only in normal create/update requests
    created_at: datetime.datetime | None = None
    updated_at: datetime.datetime | None = None


class FileResource(BaseModel):
    """Base model for a resource that is a file."""

    path: str = Field(..., min_length=1, description="The filesystem path to the resource.")


class ScriptResource(FileResource):
    pass


class NotebookResource(FileResource):
    pass


class AppResource(BaseModel):
    """An app to be used by a job."""

    name: str = Field(..., min_length=1, description="The name of the app.")
    configuration: dict[str, Any] = Field(
        default_factory=dict, description="Key-value configuration for the app configuration."
    )


Resource = Union[
    dict[Literal["script"], ScriptResource],
    dict[Literal["notebook"], NotebookResource],
    dict[Literal["app"], AppResource],
]


class CrontabSchedule(BaseModel):
    """A schedule defined by a standard crontab expression."""

    type: Literal["crontab"]
    start_time: datetime.datetime | None = None
    expression: str = Field(
        ..., min_length=1, description="A valid cron string, e.g., '0 5 * * 1-5'."
    )


class IntervalSchedule(BaseModel):
    """A schedule that repeats at a fixed interval."""

    type: Literal["interval"]
    start_time: datetime.datetime | None = None
    every: PositiveInt = Field(..., description="The frequency of the interval (must be > 0).")
    period: Literal["seconds", "minutes", "hours", "days"]


Schedule = Union[CrontabSchedule, IntervalSchedule]


class Job(BaseObjectOrm, BasePydanticModel):
    """A single, named job with its resource and schedule."""

    name: str = Field(..., min_length=1, description="A human-readable name for the job.")
    resource: Resource
    schedule: Schedule | None = Field(default=None, description="The job's execution schedule.")

    @classmethod
    def create_from_configuration(cls, job_configuration):
        url = cls.get_object_url() + "/create_from_configuration/"
        s = cls.build_session()
        job_configuration["project_id"] = POD_PROJECT.id
        r = make_request(
            s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload={"json": job_configuration}
        )
        if r.status_code not in [200, 201]:
            raise Exception(r.text)
        return r.json()


class ProjectConfiguration(BaseModel):
    """The root model for the entire project configuration."""

    name: str = Field(..., min_length=1, description="The name of the project.")
    jobs: list[Job]
