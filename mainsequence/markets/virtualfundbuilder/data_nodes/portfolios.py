from __future__ import annotations

from typing import Any

import pandas as pd

from mainsequence.tdag.data_nodes import RecordDefinition

from .base import (
    VFBCanonicalDataNode,
    VFBCanonicalDataNodeConfiguration,
    _empty_flat_frame,
    _record_definitions_from_dtype_map,
    _require_columns,
    _reset_frame_index,
)
from .constants import (
    PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER,
    PORTFOLIOS_COLUMN_DESCRIPTIONS,
    PORTFOLIOS_COLUMN_DTYPES_MAP,
    PORTFOLIOS_COLUMN_LABELS,
    PORTFOLIOS_INDEX_NAMES,
    SCHEMA_BOOTSTRAP_PORTFOLIO_IDENTIFIER,
    VFB_CANONICAL_TIME_INDEX_NAME,
)
from .portfolio_identity import get_or_create_portfolio_index_asset


class PortfoliosDataNode(VFBCanonicalDataNode):
    """Canonical DataNode for VFB portfolio value series."""

    def set_portfolio_values_frame(
        self,
        portfolio_values_frame: pd.DataFrame,
        *,
        portfolio_index_asset_unique_identifier: str | None = None,
        portfolio_configuration: Any | None = None,
        portfolio_index_asset: Any | None = None,
        portfolio_resolver: Any | None = None,
    ) -> PortfoliosDataNode:
        """Attach runtime value inputs without changing table identity."""
        self._portfolio_values_frame = portfolio_values_frame
        self._portfolio_index_asset_unique_identifier = portfolio_index_asset_unique_identifier
        self._portfolio_configuration = portfolio_configuration
        self._portfolio_index_asset = portfolio_index_asset
        self._portfolio_resolver = portfolio_resolver
        return self

    def update(self) -> pd.DataFrame:
        return self.validate_frame(
            self._calculate_portfolio_values(),
            config=self._canonical_config(),
        )

    def _calculate_portfolio_values(self) -> pd.DataFrame:
        portfolio_values_frame = getattr(self, "_portfolio_values_frame", None)
        if portfolio_values_frame is None:
            return self.get_canonical_frame()

        return normalize_portfolio_values_frame(
            portfolio_values_frame,
            portfolio_index_asset_unique_identifier=(
                self._resolve_portfolio_index_asset_unique_identifier()
            ),
            config=self._canonical_config(),
        )

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
            "Canonical VFB portfolio value series indexed by time_index and "
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

    @classmethod
    def _source_table_initializer_name(cls) -> str | None:
        return "initialize_portfolios_source_table"


def normalize_portfolio_values_frame(
    portfolio_values_frame: pd.DataFrame,
    *,
    portfolio_index_asset_unique_identifier: str,
    config: VFBCanonicalDataNodeConfiguration | None = None,
) -> pd.DataFrame:
    """Normalize VFB portfolio values into canonical PortfoliosDataNode rows."""
    config = PortfoliosDataNode._validate_config(config or PortfoliosDataNode.default_config())
    flat = _reset_frame_index(portfolio_values_frame)
    if flat.empty:
        flat = _empty_flat_frame(config=config)

    if VFB_CANONICAL_TIME_INDEX_NAME not in flat.columns and "index" in flat.columns:
        flat = flat.rename(columns={"index": VFB_CANONICAL_TIME_INDEX_NAME})
    flat[PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER] = str(portfolio_index_asset_unique_identifier)
    if "calculated_close" not in flat.columns and "close" in flat.columns:
        flat["calculated_close"] = flat["close"]
    if "close_time" not in flat.columns and VFB_CANONICAL_TIME_INDEX_NAME in flat.columns:
        flat["close_time"] = flat[VFB_CANONICAL_TIME_INDEX_NAME]

    _require_columns(
        flat,
        required_columns=list(config.column_dtypes_map),
        frame_name="PortfoliosDataNode",
    )
    return PortfoliosDataNode.validate_frame(
        flat[list(config.column_dtypes_map)],
        config=config,
    )
