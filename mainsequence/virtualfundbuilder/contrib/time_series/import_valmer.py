from mainsequence.client.models_tdag import Artifact
from datetime import timedelta
from typing import Union

import pandas as pd

from mainsequence.tdag.time_series import TimeSerie
from mainsequence.client import  Asset,DataUpdates
from mainsequence.client.models_helpers import MarketsTimeSeriesDetails,DataFrequency
from mainsequence.client.utils import DoesNotExist


from mainsequence.virtualfundbuilder.utils import TIMEDELTA
import numpy as np
import pandas.api.types as ptypes


class ImportValmer(TimeSerie):
    @TimeSerie._post_init_routines()
    def __init__(
            self,
            artifact_name: str,
            bucket_name: str,
            *args, **kwargs
    ):
        self.artifact_name = artifact_name
        self.bucket_name = bucket_name
        self.artifact_data = None
        super().__init__(*args, **kwargs)

    def maximum_forward_fill(self):
        return timedelta(days=1) - TIMEDELTA

    def get_explanation(self):
        explanation = (
            "### Data From Valmer\n\n"
        )
        return explanation

    def _get_artifact_data(self):
        if self.artifact_data is None:
            source_artifact = Artifact.get(bucket__name=self.bucket_name, name=self.artifact_name)
            self.artifact_data = pd.read_csv(source_artifact.content, encoding='latin1')

        return self.artifact_data

    def _get_asset_list(self) -> Union[None, list]:
        source_data = self._get_artifact_data()

        asset_columns = ["ticker", "name", "isin", "security_type", "security_type_2", "security_market_sector", "exchange_code"]
        #buld register
        source_data['ticker'] = source_data["Instrumento"]
        source_data['name'] = source_data["Instrumento"]
        source_data['isin'] = source_data['Isin'].apply(lambda x: None if pd.isna(x) else x)
        source_data['security_type'] = None
        source_data['security_type_2'] = None
        source_data['security_market_sector'] = None
        source_data['exchange_code'] = None
        bulk_data = source_data[asset_columns].to_dict('records')

        assets = []
        batch_size = 500
        for i in range(0, len(bulk_data), batch_size):
            self.logger.info(f"Batch register assets {i} to {i + batch_size} / {len(bulk_data)}")
            batch = bulk_data[i:i + batch_size]
            asset_ids_batch = Asset.batch_get_or_register_custom_assets(asset_list=batch)
            self.logger.info(f"Query assets {i} to {i + batch_size} / {len(bulk_data)}")
            bulk_assets = Asset.filter(id__in=asset_ids_batch, timeout=60*5)
            assets += bulk_assets

        ticker_map =  {
            a.ticker: a.unique_identifier for a in assets
        }

        source_data["unique_identifier"] = source_data["ticker"].map(ticker_map)
        self.source_data = source_data.drop(columns=asset_columns)
        return assets
    
    def _get_column_metadata(self):
        from mainsequence.client.models_tdag import ColumnMetaData
        columns_metadata = [ColumnMetaData(column_name="instrumento",
                                          dtype="str",
                                          label="Instrumento",
                                          description=(
                                              "Unique identifier provided by Valmer; it’s a composition of the "
                                              "columns `tv_emisora_serie`, and is also used as a ticker for custom "
                                              "assets in Valmer."
                                          )
                                          ),
                            ColumnMetaData(column_name="moneda",
                                           dtype="str",
                                           label="Moneda",
                                           description=(
                                               "Correspondes to Valmer code for curries be aware this may not match Figi Currency assets"
                                           )
                                           ),
                            
                            ]
        return columns_metadata
    
    def update(self, update_statistics: "DataUpdates"):
        source_data = self.source_data

        assert source_data is not None, "Source data is not available"

        source_data.rename(columns={"Fecha": "time_index"}, inplace=True)
        source_data['time_index'] = pd.to_datetime(source_data['time_index'], utc=True)

        # make columns lower case
        for col in source_data.columns:
            source_data.rename(columns={col: col.lower()}, inplace=True)

        source_data.set_index(["time_index", "unique_identifier"], inplace=True)

        source_data = update_statistics.filter_df_by_latest_value(source_data)

        self._set_column_metadata()
        return source_data

    def  _run_post_update_routines(self, error_on_last_update,update_statistics:DataUpdates):
        MARKET_TIME_SERIES_UNIQUE_IDENTIFIER = "vector_de_precios_valmer"
        try:
            markets_time_series_details = MarketsTimeSeriesDetails.get(
                unique_identifier=MARKET_TIME_SERIES_UNIQUE_IDENTIFIER,
            )
            if markets_time_series_details.related_local_time_serie.id != self.local_time_serie.id:
                markets_time_series_details = markets_time_series_details.patch(related_local_time_serie__id=self.local_time_serie.id)
        except DoesNotExist:
            markets_time_series_details = MarketsTimeSeriesDetails.update_or_create(
                unique_identifier=MARKET_TIME_SERIES_UNIQUE_IDENTIFIER,
                related_local_time_serie__id=self.local_time_serie.id,
                data_frequency_id=DataFrequency.one_d,
                description="This time series contains the valuation prices from the price provider VALMER",
            )

        new_assets = []
        for asset in update_statistics.asset_list:
            if asset.id not in markets_time_series_details.assets_in_data_source:
                new_assets.append(asset)

        markets_time_series_details.append_asset_list_source(asset_list=new_assets)


if __name__ == "__main__":
    ts = ImportValmer(
        bucket_name="Vector de precios",
        artifact_name="Vector_20250430.csv",
        local_kwargs_to_ignore=["bucket_name", "artifact_name"]
    )

    ts.run(
        debug_mode=True,
        force_update=True,
    )