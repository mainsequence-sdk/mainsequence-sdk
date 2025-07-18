from .models_vam import *
from .base import MARKETS_CONSTANTS
from .models_tdag import DynamicTableMetaData, LocalTimeSerie
from .models_tdag import LocalTimeSerie, POD_PROJECT
import datetime

from pydantic import BaseModel, Field, PositiveInt


def get_right_account_class(account: Account):
    from mainsequence.client import models_vam as model_module
    execution_venue_symbol = account.execution_venue.symbol
    AccountClass = getattr(model_module, MARKETS_CONSTANTS.ACCOUNT_VENUE_FACTORY[execution_venue_symbol])
    account, _ = AccountClass.get(id=account.id)
    return account


class MarketsTimeSeriesDetails(BaseObjectOrm, BasePydanticModel):
    id: Optional[int] = None
    unique_identifier: str
    source_table: Union[DynamicTableMetaData, int]
    description: Optional[str] = Field(None, description="Descriptions of the data source")
    data_frequency_id: Optional[DataFrequency] = None
    assets_in_data_source: Optional[List[int]]
    extra_properties: Optional[Dict]

    def __str__(self):
        return self.class_name() + f" {self.unique_identifier}"

    @classmethod
    def get(cls,*args,**kwargs):
        return super().get(*args,**kwargs)

    @classmethod
    def filter(cls,*args,**kwargs):
        return super().filter(*args,**kwargs)

    def append_asset_list_source(self, asset_list: List[Asset]):
        if asset_list:
            asset_id_list = [a.id for a in asset_list]
            self.append_assets(asset_id_list=asset_id_list)
            print("Added assets to bars")

    def append_assets(self, asset_id_list:list, timeout=None):
        url = f"{self.get_object_url()}/{self.id}/append_assets/"

        payload = {"json": {"asset_id_list": asset_id_list}}
        r = make_request(s=self.build_session(), loaders=self.LOADERS, r_type="PATCH", url=url, payload=payload,
                         time_out=timeout)
        if r.status_code in [200] == False:
            raise Exception(f" {r.text()}")
        return self.__class__(**r.json())

    @classmethod
    def register_in_backend(
        cls,
        unique_identifier,
        time_serie,
        data_frequency_id,
        asset_list:List[Asset],
        description=""
    ):

        # if run for the first time save this as reference in VAM
        bar_source = MarketsTimeSeriesDetails.update_or_create(
            unique_identifier=unique_identifier,
            related_local_time_serie__id=time_serie.local_time_serie.id,
            data_frequency_id=data_frequency_id,
            description=description,
        )

        if bar_source is None:
            raise ValueError("No historical bars source found")

        bar_source.append_asset_list_source(asset_list=asset_list)


class HistoricalBarsSource(MarketsTimeSeriesDetails):
    execution_venues: list
    data_mode: Literal['live', 'backtest'] = Field(
        description="Indicates whether the source is for live data or backtesting."
    )
    adjusted:bool

    @classmethod
    def register_in_backend(
            cls,
            unique_identifier:str,
            time_serie,
            execution_venues_symbol,
            data_mode,
            description: str = "",
            create_bars: bool = True
    ):
        bar_source = None
        try:
            bar_source = cls.get(
                data_frequency_id=time_serie.frequency_id,
                execution_venues__symbol__in=[execution_venues_symbol],
                data_mode=data_mode
            )

            bar_source = bar_source.patch(related_local_time_serie__id=time_serie.local_time_serie.id)

        except Exception as e:
            print(f"Exception when getting historical bar source {e}")

            # if run for the first time save this as reference in VAM
            bar_source = cls.update_or_create(
                unique_identifier=f"{execution_venues_symbol}_{time_serie.frequency_id}",
                related_local_time_serie__id=time_serie.local_time_serie.id,
                description=description,
                execution_venues_symbol__in=[execution_venues_symbol],
                data_frequency_id=time_serie.frequency_id,
                data_mode=data_mode
            )

        if bar_source is None:
            raise ValueError("No historical bars source found")

        bar_source.append_asset_list_source(time_serie)

class Slide(BasePydanticModel):
    id:Optional[int]=None

    number: PositiveInt = Field(
        ...,
        description="1-based position of the slide within its presentation",
        example=3,
    )
    body: Optional[str] = Field(
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
    id:Optional[int]=None
    title: str = Field(..., max_length=255)
    description: str = Field("", description="Free-form description of the deck")
    slides:List[Slide]

    # These come from the DB and are read-only in normal create/update requests
    created_at: Optional[datetime.datetime] = None
    updated_at: Optional[datetime.datetime] = None


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
    configuration: Dict[str, Any] = Field(
        default_factory=dict, description="Key-value configuration for the app configuration."
    )

Resource = Union[
    Dict[Literal["script"], ScriptResource],
    Dict[Literal["notebook"], NotebookResource],
    Dict[Literal["app"], AppResource],
]

class CrontabSchedule(BaseModel):
    """A schedule defined by a standard crontab expression."""
    type: Literal["crontab"]
    start_time: Optional[datetime.datetime] = None
    expression: str = Field(..., min_length=1, description="A valid cron string, e.g., '0 5 * * 1-5'.")

class IntervalSchedule(BaseModel):
    """A schedule that repeats at a fixed interval."""
    type: Literal["interval"]
    start_time: Optional[datetime.datetime] = None
    every: PositiveInt = Field(..., description="The frequency of the interval (must be > 0).")
    period: Literal["seconds", "minutes", "hours", "days"]

Schedule = Union[CrontabSchedule, IntervalSchedule]

class Job(BaseObjectOrm, BasePydanticModel):
    """A single, named job with its resource and schedule."""
    name: str = Field(..., min_length=1, description="A human-readable name for the job.")
    resource: Resource
    schedule: Optional[Schedule] = Field(default=None, description="The job's execution schedule.")

    @classmethod
    def create_from_configuration(cls, job_configuration):
        url = cls.get_object_url() + f"/create_from_configuration/"
        s = cls.build_session()
        job_configuration["project_id"] = POD_PROJECT.id
        r = make_request(s=s, loaders=cls.LOADERS, r_type="POST", url=url, payload={"json": job_configuration})
        if r.status_code not in [200, 201]:
            raise Exception(r.text)
        return r.json()

class ProjectConfiguration(BaseModel):
    """The root model for the entire project configuration."""
    name: str = Field(..., min_length=1, description="The name of the project.")
    jobs: List[Job]
