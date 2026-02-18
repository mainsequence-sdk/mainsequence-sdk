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




class FileResource(BaseModel):
    """Base model for a resource that is a file."""

    path: str = Field(..., min_length=1, description="The filesystem path to the resource.")


class ScriptResource(FileResource):
    pass



class AppResource(BaseModel):
    """An app to be used by a job."""

    name: str = Field(..., min_length=1, description="The name of the app.")
    configuration: dict[str, Any] = Field(
        default_factory=dict, description="Key-value configuration for the app configuration."
    )


Resource = Union[
    dict[Literal["script"], ScriptResource],
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
            raise_for_response(r)

        return r.json()


class ProjectConfiguration(BaseModel):
    """The root model for the entire project configuration."""

    name: str = Field(..., min_length=1, description="The name of the project.")
    jobs: list[Job]
