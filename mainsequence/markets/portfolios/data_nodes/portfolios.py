from __future__ import annotations

import copy
import os
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd
import pytz

import mainsequence.client as msc
import mainsequence.tdag.data_nodes.build_operations as build_operations
from mainsequence.client import Asset, AssetCategory
from mainsequence.client.models_tdag import UpdateStatistics
from mainsequence.tdag.data_nodes import APIDataNode, DataNode, RecordDefinition

from .base import (
    PortfolioCanonicalDataNode,
    PortfolioCanonicalDataNodeConfiguration,
    _class_import_path,
    _empty_flat_frame,
    _is_canonical_frame,
    _record_definitions_from_dtype_map,
    _require_columns,
    _reset_frame_index,
)
from .constants import (
    PORTFOLIO_CANONICAL_TIME_INDEX_NAME,
    PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER,
    PORTFOLIOS_COLUMN_DESCRIPTIONS,
    PORTFOLIOS_COLUMN_DTYPES_MAP,
    PORTFOLIOS_COLUMN_LABELS,
    PORTFOLIOS_INDEX_NAMES,
    SCHEMA_BOOTSTRAP_PORTFOLIO_IDENTIFIER,
)
from .portfolio_identity import get_or_create_portfolio_index_asset
from .portfolio_weights import PortfolioWeights


def translate_to_pandas_freq(custom_freq: str) -> str:
    freq_mapping = {
        "d": "D",
        "m": "min",
        "mo": "M",
    }

    import re

    match = re.match(r"(\d+)([a-z]+)", custom_freq)
    if not match:
        raise ValueError(f"Invalid frequency format: {custom_freq}")

    number, unit = match.groups()
    if unit not in freq_mapping:
        raise ValueError(f"Unsupported frequency unit: {unit}")
    return f"{number}{freq_mapping[unit]}"


class PortfoliosDataNode(PortfolioCanonicalDataNode):
    """Canonical portfolio values DataNode and portfolio workflow orchestrator."""

    OFFSET_START = datetime(2018, 1, 1, tzinfo=pytz.utc)

    def __init__(
        self,
        config: PortfolioCanonicalDataNodeConfiguration | None = None,
        *args,
        portfolio_configuration: Any | None = None,
        namespace: str | None = None,
        **kwargs,
    ):
        self.portfolio_configuration = portfolio_configuration
        if portfolio_configuration is not None:
            self._initialize_from_portfolio_configuration(portfolio_configuration)
        super().__init__(config, *args, namespace=namespace, **kwargs)

    def _initialize_configuration(self, init_kwargs: dict) -> None:
        """Hash every workflow instance as the canonical Portfolios table."""
        for runtime_key in (
            "portfolio_configuration",
            "portfolio_resolver",
            "portfolio_index_asset",
            "portfolio_description",
            "metadata_updater",
        ):
            init_kwargs.pop(runtime_key, None)
        init_kwargs["time_series_class_import_path"] = _class_import_path(PortfoliosDataNode)
        config = build_operations.create_config(
            kwargs=init_kwargs,
            ts_class_name=PortfoliosDataNode.__name__,
        )
        for field_name, value in config.__dict__.items():
            setattr(self, field_name, value)

    def set_portfolio_configuration(
        self,
        portfolio_configuration: Any,
        *,
        portfolio_resolver: Any | None = None,
        portfolio_description: str | None = None,
        metadata_updater: Any | None = None,
    ) -> PortfoliosDataNode:
        self.portfolio_configuration = portfolio_configuration
        self._portfolio_configuration = portfolio_configuration
        self._portfolio_resolver = portfolio_resolver
        self._portfolio_description = portfolio_description
        self._portfolio_metadata_updater = metadata_updater
        self._initialize_from_portfolio_configuration(portfolio_configuration)
        return self

    def _initialize_from_portfolio_configuration(self, portfolio_configuration: Any) -> None:
        from ..contrib.prices.data_nodes import get_interpolated_prices_timeseries
        from .signal_weights import SignalWeights

        self.portfolio_configuration = portfolio_configuration
        self._portfolio_configuration = portfolio_configuration
        portfolio_build_configuration = portfolio_configuration.portfolio_build_configuration
        self.portfolio_build_configuration = portfolio_build_configuration
        self.execution_configuration = portfolio_build_configuration.execution_configuration
        self.backtesting_weights_config = (
            portfolio_build_configuration.backtesting_weights_configuration
        )
        self.portfolio_markets_config = portfolio_configuration.portfolio_markets_configuration
        self.commission_fee = self.execution_configuration.commission_fee
        self.portfolio_prices_frequency = portfolio_build_configuration.portfolio_prices_frequency
        self.assets_configuration = portfolio_build_configuration.assets_configuration
        self.portfolio_frequency = (
            self.assets_configuration.prices_configuration.upsample_frequency_id
        )

        self.signal_weights = self.backtesting_weights_config.signal_weights_instance
        if not isinstance(self.signal_weights, SignalWeights):
            raise TypeError(
                "PortfoliosDataNode requires signal_weights_instance to inherit "
                "from SignalWeights."
            )

        self.rebalancer = self.backtesting_weights_config.rebalance_strategy_instance
        self.rebalancer_explanation = ""

        asset_list = None
        if not self.assets_configuration.assets_category_unique_id:
            asset_list = self.signal_weights.get_asset_list()
            portfolio_asset_uid = self.signal_weights.get_asset_uid_to_override_portfolio_price()
            if portfolio_asset_uid is not None:
                asset = msc.Asset.get_or_none(unique_identifier=portfolio_asset_uid)
                if asset is None:
                    raise Exception(
                        f"{portfolio_asset_uid} not found. be sure that is on the price transaltion table"
                    )
                asset_list = asset_list + [asset]
                asset_list = list({a.id: a for a in asset_list}.values())

        self.bars_ts = get_interpolated_prices_timeseries(
            copy.deepcopy(self.assets_configuration),
            asset_list=asset_list,
        )

    def set_portfolio_values_frame(
        self,
        portfolio_values_frame: pd.DataFrame,
        *,
        portfolio_index_asset_unique_identifier: str | None = None,
        portfolio_configuration: Any | None = None,
        portfolio_index_asset: Any | None = None,
        portfolio_resolver: Any | None = None,
        portfolio_description: str | None = None,
        metadata_updater: Any | None = None,
    ) -> PortfoliosDataNode:
        """Attach runtime value inputs without changing table identity."""
        self._portfolio_values_frame = portfolio_values_frame
        self._portfolio_index_asset_unique_identifier = portfolio_index_asset_unique_identifier
        self._portfolio_configuration = portfolio_configuration
        self._portfolio_index_asset = portfolio_index_asset
        self._portfolio_resolver = portfolio_resolver
        self._portfolio_description = portfolio_description
        self._portfolio_metadata_updater = metadata_updater
        return self

    def dependencies(self) -> dict[str, DataNode | APIDataNode]:
        if getattr(self, "portfolio_configuration", None) is None:
            return {}
        return {"signal_weights": self.signal_weights, "bars_ts": self.bars_ts}

    def run(
        self,
        debug_mode: bool = True,
        *,
        update_tree: bool = True,
        force_update: bool = False,
        update_only_tree: bool = False,
        remote_scheduler: object | None = None,
        override_update_stats: UpdateStatistics | None = None,
    ):
        if getattr(self, "portfolio_configuration", None) is None:
            return super().run(
                debug_mode=debug_mode,
                update_tree=update_tree,
                force_update=force_update,
                update_only_tree=update_only_tree,
                remote_scheduler=remote_scheduler,
                override_update_stats=override_update_stats,
            )

        _portfolio, index_asset = self._resolve_portfolio_identity()
        portfolio_uid = str(index_asset.unique_identifier)
        self._resolved_portfolio_index_asset_unique_identifier = portfolio_uid
        portfolio_weights_node = self._canonical_portfolio_weights_node()
        portfolio_weights_node.ensure_storage_ready()

        portfolio_values_result = super().run(
            debug_mode=debug_mode,
            update_tree=update_tree,
            force_update=force_update,
            update_only_tree=update_only_tree,
            remote_scheduler=remote_scheduler,
            override_update_stats=override_update_stats,
        )
        if update_only_tree:
            return portfolio_values_result

        weights = getattr(self, "_last_canonical_weights_frame", pd.DataFrame())
        portfolio_weights_result = None
        if weights is not None and not weights.empty:
            portfolio_weights_node.set_weights_frame(
                weights,
                portfolio_index_asset_unique_identifier=portfolio_uid,
                portfolio_configuration=self.portfolio_configuration,
                portfolio_index_asset=index_asset,
                portfolio_description=self._resolve_portfolio_description(),
            )
            portfolio_weights_result = portfolio_weights_node.run(
                debug_mode=debug_mode,
                update_tree=False,
                force_update=force_update,
                remote_scheduler=remote_scheduler,
            )

        return {
            "portfolio_weights": portfolio_weights_result,
            "portfolio_values": portfolio_values_result,
        }

    def update(self) -> pd.DataFrame:
        raw_frame = self._calculate_portfolio_values()
        config = self._canonical_config()
        frame = (
            self.validate_frame(raw_frame, config=config)
            if _is_canonical_frame(raw_frame, config=config)
            else self.validate_frame(
                normalize_portfolio_values_frame(
                    raw_frame,
                    portfolio_index_asset_unique_identifier=(
                        self._resolve_portfolio_index_asset_unique_identifier()
                    ),
                    config=config,
                ),
                config=config,
            )
        )
        self._upsert_portfolio_metadata_if_available(frame)
        return frame

    def _calculate_portfolio_values(self) -> pd.DataFrame:
        if getattr(self, "portfolio_configuration", None) is not None:
            return self._calculate_portfolio_workflow_values()

        portfolio_values_frame = getattr(self, "_portfolio_values_frame", None)
        if portfolio_values_frame is None:
            return self.get_canonical_frame()
        return portfolio_values_frame

    def _calculate_portfolio_workflow_values(self) -> pd.DataFrame:
        self.logger.debug("Starting update of portfolio weights.")
        self._last_canonical_weights_frame = pd.DataFrame()
        self._last_canonical_portfolio_values_frame = pd.DataFrame()
        start_date, end_date = self._calculate_start_end_dates()
        self.logger.debug(f"Update from {start_date} to {end_date}")

        if start_date is None:
            self.logger.info("Start date is None, no update is done")
            return pd.DataFrame()

        new_index, index_freq = self._generate_new_index(
            start_date,
            end_date,
            self.rebalancer.calendar,
        )
        if len(new_index) == 0:
            self.logger.info("No new portfolio weights to update")
            return pd.DataFrame()

        signal_weights = self.signal_weights.interpolate_index(new_index).dropna()
        if len(signal_weights) == 0:
            self.logger.info("No signal weights found, no update is done")
            return pd.DataFrame()

        new_index = new_index[
            new_index <= signal_weights.index.max() + self.signal_weights.maximum_forward_fill()
        ]

        expected_columns = ["unique_identifier"]
        assert (
            signal_weights.columns.names == expected_columns
        ), f"signal_weights must have columns named {expected_columns}"

        raw_prices, interpolated_prices = self._interpolate_bars_index(
            new_index=new_index,
            bars_ts=self.bars_ts,
            index_freq=index_freq,
            unique_identifiers=list(signal_weights.columns.get_level_values("unique_identifier")),
        )

        latest_value = self._latest_portfolio_time_index_value()
        if latest_value is not None:
            interpolated_prices = interpolated_prices[
                interpolated_prices.index.get_level_values("time_index") > latest_value
            ]
            signal_weights = signal_weights[signal_weights.index > latest_value]

        if interpolated_prices.empty:
            raise ValueError(
                "Interpolated Prices are empty. Check if asset prices exist for time window"
            )

        weights = self.rebalancer.apply_rebalance_logic(
            signal_weights=signal_weights,
            start_date=start_date,
            prices_df=interpolated_prices,
            end_date=end_date,
            last_rebalance_weights=self._get_last_weights(),
            price_type=self.assets_configuration.price_type,
        )

        weights = self._postprocess_weights(weights)
        if len(weights) == 0:
            self.logger.info("No portfolio weights to update")
            return pd.DataFrame()

        portfolio_returns = self._calculate_portfolio_returns(weights, raw_prices)
        portfolio = self._apply_cumulative_portfolio_values(portfolio_returns)
        if len(portfolio) > 0 and latest_value is not None:
            portfolio = portfolio[portfolio.index > latest_value]

        portfolio = self._resample_portfolio_with_calendar(portfolio)
        asset_uid_to_override_portfolio_price = (
            self.signal_weights.get_asset_uid_to_override_portfolio_price()
        )
        if asset_uid_to_override_portfolio_price is not None:
            new_portfolio_price = self.bars_ts.get_ranged_data_per_asset(
                range_descriptor={
                    asset_uid_to_override_portfolio_price: {
                        "start_date": portfolio.index.min(),
                        "start_date_operand": ">=",
                    }
                }
            )
            if new_portfolio_price.empty:
                self.logger.error("No Prices on portfolio target asset")
                return pd.DataFrame()

            new_portfolio_price = new_portfolio_price.reset_index("unique_identifier", drop=True)
            union_index = new_portfolio_price.index.union(portfolio.index.unique()).unique()
            new_portfolio_price = new_portfolio_price.reindex(union_index).ffill().bfill()
            new_portfolio_price = new_portfolio_price.reindex(portfolio.index)
            portfolio["calculated_close"] = portfolio["close"]
            portfolio["close"] = new_portfolio_price["close"]
            portfolio["return"] = portfolio["close"].pct_change().fillna(0.0)

        self.logger.info(f"{len(portfolio)} new portfolio values have been calculated.")
        self._last_canonical_weights_frame = weights
        self._last_canonical_portfolio_values_frame = portfolio
        return portfolio

    def _canonical_namespace(self) -> str | None:
        namespace = getattr(self, "hash_namespace", "") or ""
        return namespace or None

    def _canonical_portfolio_weights_node(self) -> PortfolioWeights:
        node = getattr(self, "_portfolio_weights_node", None)
        if node is None:
            node = PortfolioWeights(namespace=self._canonical_namespace())
            self._portfolio_weights_node = node
        return node

    def _resolve_portfolio_identity(self) -> tuple[Any, Any]:
        portfolio = getattr(self, "target_portfolio", None)
        index_asset = getattr(self, "index_asset", None)
        if portfolio is not None and getattr(index_asset, "unique_identifier", None):
            return portfolio, index_asset

        portfolio, index_asset = get_or_create_portfolio_index_asset(
            self.portfolio_configuration,
            portfolio_resolver=getattr(self, "_portfolio_resolver", None),
        )
        self.target_portfolio = portfolio
        self.index_asset = index_asset
        return portfolio, index_asset

    def _portfolio_index_asset_unique_identifier(self) -> str:
        _portfolio, index_asset = self._resolve_portfolio_identity()
        unique_identifier = getattr(index_asset, "unique_identifier", None)
        if not unique_identifier:
            raise ValueError("PortfolioIndexAsset must expose unique_identifier.")
        return str(unique_identifier)

    def _resolve_portfolio_description(self) -> str | None:
        explicit_description = self.__dict__.get("_portfolio_description")
        if explicit_description is not None:
            return str(explicit_description)
        front_end_details = getattr(self.portfolio_markets_config, "front_end_details", None)
        if front_end_details is None:
            return None
        description = getattr(front_end_details, "description", None)
        return None if description is None else str(description)

    def _latest_portfolio_time_index_value(self):
        update_statistics = getattr(self, "update_statistics", None)
        if update_statistics is None:
            return None

        portfolio_uid = getattr(self, "_resolved_portfolio_index_asset_unique_identifier", None)
        if portfolio_uid:
            progress = getattr(update_statistics, "index_progress", None) or {}
            progress_value = progress.get(portfolio_uid)
            if isinstance(progress_value, dict):
                return progress_value.get("max") or progress_value.get("time_index")
            if progress_value is not None:
                return progress_value

        return getattr(update_statistics, "max_time_index_value", None)

    def _portfolio_dimension_range_map(
        self,
        *,
        start_date,
        start_date_operand: str = ">=",
    ) -> list[dict]:
        return [
            {
                "coordinate": {
                    PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER: (
                        self._portfolio_index_asset_unique_identifier()
                    )
                },
                "start_date": start_date,
                "start_date_operand": start_date_operand,
            }
        ]

    def _calculate_start_end_dates(self):
        update_statics_from_dependencies = self.bars_ts.update_statistics
        progress_values = update_statics_from_dependencies.get_index_progress_leaf_values()
        earliest_last_value = min(progress_values) if progress_values else None

        if earliest_last_value is None:
            self.logger.warning(
                f"update_statics_from_dependencies {update_statics_from_dependencies}"
            )
            raise Exception("Prices are empty")

        if self.assets_configuration.prices_configuration.forward_fill_to_now:
            end_date = datetime.now(pytz.utc)
        else:
            end_date = earliest_last_value + self.bars_ts.maximum_forward_fill

        start_date = self._latest_portfolio_time_index_value() or self.OFFSET_START
        max_td_env = os.getenv("MAX_TD_FROM_LATEST_VALUE", None)
        if max_td_env is not None:
            new_end_date = start_date + pd.Timedelta(max_td_env)
            end_date = new_end_date if new_end_date < end_date else end_date

        return start_date, end_date

    def _generate_new_index(self, start_date, end_date, rebalancer_calendar):
        upsample_freq = self.assets_configuration.prices_configuration.upsample_frequency_id

        if "d" in upsample_freq:
            assert upsample_freq == "1d", "Only '1d' frequency is implemented."
            upsample_freq = translate_to_pandas_freq(upsample_freq)
            freq = upsample_freq.replace("days", "d")
            schedule = rebalancer_calendar.schedule(start_date=start_date, end_date=end_date)
            new_index = schedule.set_index("market_close").index
            new_index.name = None
            new_index = new_index[new_index <= end_date]
        else:
            upsample_freq = translate_to_pandas_freq(upsample_freq)
            self.logger.warning("Matching new index with calendar")
            freq = upsample_freq
            new_index = pd.date_range(start=start_date, end=end_date, freq=freq)
        return new_index, freq

    def _postprocess_weights(self, weights):
        latest_value = self._latest_portfolio_time_index_value()
        if latest_value is not None:
            weights = weights[weights.index > latest_value]
        if weights.empty:
            return pd.DataFrame()

        weights = weights.stack()
        required_columns = ["weights_before", "weights_current", "price_current", "price_before"]
        for col in required_columns:
            assert col in weights.columns, f"Column '{col}' is missing in weights"

        weights = weights.dropna(subset=["weights_current"])
        if latest_value is not None:
            weights = weights[weights.index.get_level_values("time_index") > latest_value]

        if latest_value is not None:
            last_weights = self._get_last_weights()
            if last_weights is not None and not last_weights.empty:
                weights = pd.concat([last_weights, weights], axis=0).fillna(0)

        return weights

    def _calculate_portfolio_returns(
        self,
        weights: pd.DataFrame,
        prices: pd.DataFrame,
    ) -> pd.DataFrame:
        weights = weights.reset_index().pivot(
            index="time_index",
            columns=["unique_identifier"],
            values=["price_current", "weights_before", "weights_current"],
        )

        price_current = weights.price_current
        weights_before = weights.weights_before.fillna(0)
        weights_current = weights.weights_current.fillna(0)
        prices = prices[self.assets_configuration.price_type.value].unstack()
        first_price_date = (
            prices.stack().dropna().index.union(price_current.stack().dropna().index)[0][0]
        )

        prices = price_current.combine_first(prices).sort_index().ffill()
        prices = prices.reindex(weights.index)
        returns = (prices / prices.shift(1) - 1).fillna(0.0)
        returns.replace([np.inf, -np.inf], 0, inplace=True)
        weights_before = weights_before.reindex(returns.index, method="ffill").dropna()
        weights_current = weights_current.reindex(returns.index, method="ffill").dropna()
        weighted_returns = (weights_before * returns).dropna()
        weights_diff = (weights_current - weights_before).fillna(0)
        fees = (weights_diff.abs() * self.commission_fee).sum(axis=1)
        portfolio_returns = pd.DataFrame({"return": weighted_returns.sum(axis=1) - fees})
        return portfolio_returns[portfolio_returns.index >= first_price_date]

    def _apply_cumulative_portfolio_values(self, portfolio: pd.DataFrame) -> pd.DataFrame:
        last_portfolio = 1
        latest_value = self._latest_portfolio_time_index_value()
        if latest_value is not None:
            last_obs = self.get_df_between_dates(
                dimension_range_map=self._portfolio_dimension_range_map(
                    start_date=latest_value,
                    start_date_operand=">=",
                )
            )
            if last_obs is not None and not last_obs.empty:
                last_obs = last_obs.sort_index()
                latest_time_index = last_obs.index.get_level_values("time_index").max()
                last_obs = last_obs[
                    last_obs.index.get_level_values("time_index") == latest_time_index
                ]
                last_portfolio = last_obs["close"].iloc[0]
                portfolio = portfolio[portfolio.index > latest_time_index]

        portfolio["close"] = last_portfolio * np.cumprod(portfolio["return"] + 1)
        return portfolio

    def _get_last_weights(self):
        latest_value = self._latest_portfolio_time_index_value()
        if latest_value is None:
            return None

        portfolio_weights_node = self._canonical_portfolio_weights_node()
        last_obs = portfolio_weights_node.get_df_between_dates(
            dimension_range_map=self._portfolio_dimension_range_map(
                start_date=latest_value,
                start_date_operand=">=",
            )
        )
        if last_obs is None or last_obs.empty:
            return None

        last_obs = last_obs.sort_index()
        latest_time_index = last_obs.index.get_level_values("time_index").max()
        last_weights = last_obs[
            last_obs.index.get_level_values("time_index") == latest_time_index
        ].copy()
        if PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER in last_weights.index.names:
            last_weights = last_weights.droplevel(PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER)
        return last_weights.rename(columns={"weight": "weights_current"})

    def _interpolate_bars_index(
        self,
        new_index: pd.DatetimeIndex,
        unique_identifiers: list,
        index_freq: str,
        bars_ts: DataNode | APIDataNode,
    ):
        prices_config = self.assets_configuration.prices_configuration
        fetch_end_date = new_index.max()
        raw_prices = bars_ts.get_df_between_dates(
            start_date=new_index.min() - pd.Timedelta(index_freq),
            end_date=fetch_end_date,
            great_or_equal=True,
            less_or_equal=True,
            dimension_filters={"unique_identifier": unique_identifiers},
        )

        if len(raw_prices) == 0:
            self.logger.info(
                f"No prices data in index interpolation for node {bars_ts.storage_hash}"
            )
            return pd.DataFrame(), pd.DataFrame()

        raw_prices.sort_values("time_index", inplace=True)
        final_index_for_interpolation = new_index
        if prices_config.forward_fill_to_now:
            fill_end_date = datetime.now(pytz.utc)
            last_ts_in_df = raw_prices.index.get_level_values("time_index").max()
            self.logger.info(f"Forward-filling prices from {last_ts_in_df} to {fill_end_date}")
            pandas_freq = translate_to_pandas_freq(self.portfolio_prices_frequency)
            final_index_for_interpolation = pd.date_range(
                start=new_index.min(),
                end=fill_end_date,
                freq=pandas_freq,
            )

        interpolated_prices = raw_prices.unstack(["unique_identifier"])
        interpolated_prices = interpolated_prices.reindex(
            final_index_for_interpolation,
            method="ffill",
        )
        interpolated_prices.index.names = ["time_index"]
        interpolated_prices = interpolated_prices.stack(["unique_identifier"])
        return raw_prices, interpolated_prices

    def _resample_portfolio_with_calendar(self, portfolio: pd.DataFrame) -> pd.DataFrame:
        if len(portfolio) == 0:
            return portfolio
        portfolio.index = pd.to_datetime(portfolio.index)
        portfolio["close_time"] = portfolio.index.strftime("%Y-%m-%d %H:%M:%S")
        return (
            portfolio.resample(pd.to_timedelta(self.portfolio_frequency_to_pandas())).last().ffill()
        )

    def portfolio_frequency_to_pandas(self):
        return translate_to_pandas_freq(self.portfolio_prices_frequency)

    def get_asset_list(self):
        if self.assets_configuration.assets_category_unique_id:
            asset_category = AssetCategory.get(
                unique_identifier=self.assets_configuration.assets_category_unique_id
            )
            return Asset.filter(id__in=asset_category.assets)
        return self.signal_weights.get_asset_list()

    def get_portfolio_about_text(self):
        portfolio_about = """Portfolio created with Main Sequence Portfolios engine with the following signal and
rebalance details:"""
        import json

        return json.dumps(portfolio_about)

    def build_prefix(self):
        reba_strat = self.rebalance_strategy_name
        signa_name = self.signal_weights_name
        return f"{reba_strat}_{signa_name}"

    def _resolve_portfolio_index_asset_unique_identifier(self) -> str:
        explicit_identifier = getattr(
            self,
            "_portfolio_index_asset_unique_identifier",
            None,
        )
        if explicit_identifier:
            return str(explicit_identifier)

        portfolio_index_asset = getattr(self, "_portfolio_index_asset", None)
        portfolio_index_asset_identifier = getattr(
            portfolio_index_asset,
            "unique_identifier",
            None,
        )
        if portfolio_index_asset_identifier:
            return str(portfolio_index_asset_identifier)

        portfolio_configuration = getattr(self, "_portfolio_configuration", None)
        if portfolio_configuration is not None:
            _portfolio, resolved_index_asset = get_or_create_portfolio_index_asset(
                portfolio_configuration,
                portfolio_resolver=getattr(self, "_portfolio_resolver", None),
            )
            resolved_identifier = getattr(resolved_index_asset, "unique_identifier", None)
            if resolved_identifier:
                return str(resolved_identifier)

        raise ValueError(
            "PortfoliosDataNode requires a portfolio_index_asset_unique_identifier, "
            "a PortfolioIndexAsset, or a portfolio_configuration that can resolve "
            "one before canonical rows can be written."
        )

    def _upsert_portfolio_metadata_if_available(self, frame: pd.DataFrame) -> None:
        portfolio_configuration = getattr(self, "_portfolio_configuration", None)
        portfolio_description = self.__dict__.get("_portfolio_description")
        if portfolio_configuration is None and portfolio_description is None:
            return

        flat = frame.reset_index()
        if flat.empty or PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER not in flat.columns:
            return
        unique_identifier = flat[PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER].iloc[0]
        if unique_identifier in (None, "", SCHEMA_BOOTSTRAP_PORTFOLIO_IDENTIFIER):
            return

        from ..simple_tables.portfolio_metadata import (
            _extract_portfolio_description,
            upsert_portfolio_metadata,
        )

        if (
            portfolio_description is None
            and _extract_portfolio_description(portfolio_configuration) is None
        ):
            return

        upsert_portfolio_metadata(
            unique_identifier=str(unique_identifier),
            description=portfolio_description,
            portfolio_configuration=portfolio_configuration,
            updater=getattr(self, "_portfolio_metadata_updater", None),
        )

    @staticmethod
    def normalize_values_frame(
        portfolio_values_frame: pd.DataFrame,
        *,
        portfolio_index_asset_unique_identifier: str,
    ) -> pd.DataFrame:
        return normalize_portfolio_values_frame(
            portfolio_values_frame,
            portfolio_index_asset_unique_identifier=(portfolio_index_asset_unique_identifier),
        )

    @classmethod
    def _default_identifier(cls) -> str:
        return "mainsequence.markets.portfolios"

    @classmethod
    def _default_description(cls) -> str:
        return (
            "Canonical Portfolios portfolio value series indexed by time_index and "
            "portfolio_index_asset_unique_identifier."
        )

    @classmethod
    def _required_index_names(cls) -> list[str]:
        return list(PORTFOLIOS_INDEX_NAMES)

    @classmethod
    def _required_records(cls) -> list[RecordDefinition]:
        return _record_definitions_from_dtype_map(
            PORTFOLIOS_COLUMN_DTYPES_MAP,
            labels=PORTFOLIOS_COLUMN_LABELS,
            descriptions=PORTFOLIOS_COLUMN_DESCRIPTIONS,
        )

    @classmethod
    def _schema_bootstrap_index_values(cls) -> dict[str, Any]:
        return {
            PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER: (SCHEMA_BOOTSTRAP_PORTFOLIO_IDENTIFIER),
        }


def normalize_portfolio_values_frame(
    portfolio_values_frame: pd.DataFrame,
    *,
    portfolio_index_asset_unique_identifier: str,
    config: PortfolioCanonicalDataNodeConfiguration | None = None,
) -> pd.DataFrame:
    """Normalize Portfolios portfolio values into canonical PortfoliosDataNode rows."""
    config = PortfoliosDataNode._validate_config(config or PortfoliosDataNode.default_config())
    flat = _reset_frame_index(portfolio_values_frame)
    if flat.empty:
        flat = _empty_flat_frame(config=config)

    if PORTFOLIO_CANONICAL_TIME_INDEX_NAME not in flat.columns and "index" in flat.columns:
        flat = flat.rename(columns={"index": PORTFOLIO_CANONICAL_TIME_INDEX_NAME})
    flat[PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER] = str(portfolio_index_asset_unique_identifier)
    if "calculated_close" not in flat.columns and "close" in flat.columns:
        flat["calculated_close"] = flat["close"]
    if "close_time" not in flat.columns and PORTFOLIO_CANONICAL_TIME_INDEX_NAME in flat.columns:
        flat["close_time"] = flat[PORTFOLIO_CANONICAL_TIME_INDEX_NAME]

    _require_columns(
        flat,
        required_columns=list(config.column_dtypes_map),
        frame_name="PortfoliosDataNode",
    )
    return PortfoliosDataNode.validate_frame(
        flat[list(config.column_dtypes_map)],
        config=config,
    )
