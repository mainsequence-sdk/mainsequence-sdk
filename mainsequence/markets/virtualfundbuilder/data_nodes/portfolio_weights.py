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
    ASSET_UNIQUE_IDENTIFIER,
    PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER,
    PORTFOLIO_WEIGHT_SOURCE_COLUMN_ALIASES,
    PORTFOLIO_WEIGHTS_COLUMN_DESCRIPTIONS,
    PORTFOLIO_WEIGHTS_COLUMN_DTYPES_MAP,
    PORTFOLIO_WEIGHTS_COLUMN_LABELS,
    PORTFOLIO_WEIGHTS_INDEX_NAMES,
    SCHEMA_BOOTSTRAP_ASSET_IDENTIFIER,
    SCHEMA_BOOTSTRAP_PORTFOLIO_IDENTIFIER,
)
from .portfolio_identity import (
    canonical_portfolio_configuration,
    compute_portfolio_configuration_hash,
    get_or_create_portfolio_index_asset,
)


class PortfolioWeights(VFBCanonicalDataNode):
    """Canonical DataNode for executed VFB portfolio weights."""

    def set_weights_frame(
        self,
        weights_frame: pd.DataFrame,
        *,
        portfolio_index_asset_unique_identifier: str | None = None,
        portfolio_configuration: Any | None = None,
        portfolio_index_asset: Any | None = None,
        portfolio_resolver: Any | None = None,
        portfolio_description: str | None = None,
        metadata_updater: Any | None = None,
    ) -> PortfolioWeights:
        """Attach runtime calculation inputs without changing table identity."""
        self._weights_frame = weights_frame
        self._portfolio_index_asset_unique_identifier = portfolio_index_asset_unique_identifier
        self._portfolio_configuration = portfolio_configuration
        self._portfolio_index_asset = portfolio_index_asset
        self._portfolio_resolver = portfolio_resolver
        self._portfolio_description = portfolio_description
        self._portfolio_metadata_updater = metadata_updater
        return self

    def update(self) -> pd.DataFrame:
        frame = self.validate_frame(
            self._calculate_weights(),
            config=self._canonical_config(),
        )
        self._upsert_portfolio_metadata_if_available(frame)
        return frame

    def _calculate_weights(self) -> pd.DataFrame:
        weights_frame = getattr(self, "_weights_frame", None)
        if weights_frame is None:
            return self.get_canonical_frame()

        return normalize_portfolio_weights_frame(
            weights_frame,
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
            "PortfolioWeights requires a portfolio_index_asset_unique_identifier, "
            "a PortfolioIndexAsset, or a portfolio_configuration that can resolve "
            "one before canonical rows can be written."
        )

    def _upsert_portfolio_metadata_if_available(self, frame: pd.DataFrame) -> None:
        portfolio_configuration = getattr(self, "_portfolio_configuration", None)
        portfolio_description = getattr(self, "_portfolio_description", None)
        if portfolio_configuration is None and portfolio_description is None:
            return

        flat = frame.reset_index()
        if flat.empty or PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER not in flat.columns:
            return
        unique_identifier = flat[PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER].iloc[0]
        if unique_identifier in (None, "", SCHEMA_BOOTSTRAP_PORTFOLIO_IDENTIFIER):
            return

        from .portfolio_metadata import (
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
    def canonical_portfolio_configuration(
        portfolio_configuration: Any,
    ) -> dict[str, Any]:
        return canonical_portfolio_configuration(portfolio_configuration)

    @staticmethod
    def compute_portfolio_configuration_hash(
        portfolio_configuration: Any,
    ) -> str:
        return compute_portfolio_configuration_hash(portfolio_configuration)

    @staticmethod
    def normalize_weights_frame(
        weights_frame: pd.DataFrame,
        *,
        portfolio_index_asset_unique_identifier: str,
    ) -> pd.DataFrame:
        return normalize_portfolio_weights_frame(
            weights_frame,
            portfolio_index_asset_unique_identifier=(portfolio_index_asset_unique_identifier),
        )

    @classmethod
    def _default_identifier(cls) -> str:
        return "mainsequence.markets.portfolio_weights"

    @classmethod
    def _default_description(cls) -> str:
        return (
            "Canonical executed VFB portfolio weights indexed by time_index, "
            "portfolio_index_asset_unique_identifier, and asset unique_identifier."
        )

    @classmethod
    def _required_index_names(cls) -> list[str]:
        return list(PORTFOLIO_WEIGHTS_INDEX_NAMES)

    @classmethod
    def _required_records(cls) -> list[RecordDefinition]:
        return _record_definitions_from_dtype_map(
            PORTFOLIO_WEIGHTS_COLUMN_DTYPES_MAP,
            labels=PORTFOLIO_WEIGHTS_COLUMN_LABELS,
            descriptions=PORTFOLIO_WEIGHTS_COLUMN_DESCRIPTIONS,
        )

    @classmethod
    def _schema_bootstrap_index_values(cls) -> dict[str, Any]:
        return {
            PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER: (SCHEMA_BOOTSTRAP_PORTFOLIO_IDENTIFIER),
            ASSET_UNIQUE_IDENTIFIER: SCHEMA_BOOTSTRAP_ASSET_IDENTIFIER,
        }


def normalize_portfolio_weights_frame(
    weights_frame: pd.DataFrame,
    *,
    portfolio_index_asset_unique_identifier: str,
    config: VFBCanonicalDataNodeConfiguration | None = None,
) -> pd.DataFrame:
    """Normalize postprocessed VFB weights into canonical PortfolioWeights rows."""
    config = PortfolioWeights._validate_config(config or PortfolioWeights.default_config())
    flat = _reset_frame_index(weights_frame)
    if flat.empty:
        flat = _empty_flat_frame(config=config)

    flat = flat.rename(columns=PORTFOLIO_WEIGHT_SOURCE_COLUMN_ALIASES)
    flat[PORTFOLIO_INDEX_ASSET_UNIQUE_IDENTIFIER] = str(portfolio_index_asset_unique_identifier)

    _require_columns(
        flat,
        required_columns=list(config.column_dtypes_map),
        frame_name="PortfolioWeights",
    )
    return PortfolioWeights.validate_frame(
        flat[list(config.column_dtypes_map)],
        config=config,
    )
