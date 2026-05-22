from __future__ import annotations

from dataclasses import asdict
from typing import Any

import numpy as np
import pandas as pd
from pydantic import BaseModel

import mainsequence.tdag.data_nodes.build_operations as build_operations
from mainsequence.tdag.data_nodes import DataNodeMetaData, RecordDefinition

from .base import (
    SignalWeightsConfiguration,
    VFBCanonicalDataNode,
    VFBCanonicalDataNodeConfiguration,
    _class_import_path,
    _drop_excluded_keys,
    _empty_flat_frame,
    _is_canonical_frame,
    _normalize_pivoted_signal_weights,
    _record_definitions_from_dtype_map,
    _require_columns,
    _reset_frame_index,
)
from .constants import (
    ASSET_UNIQUE_IDENTIFIER,
    SCHEMA_BOOTSTRAP_ASSET_IDENTIFIER,
    SCHEMA_BOOTSTRAP_SIGNAL_UID,
    SIGNAL_UID,
    SIGNAL_UID_EXCLUDED_CONFIGURATION_KEYS,
    SIGNAL_WEIGHTS_COLUMN_DESCRIPTIONS,
    SIGNAL_WEIGHTS_COLUMN_DTYPES_MAP,
    SIGNAL_WEIGHTS_COLUMN_LABELS,
    SIGNAL_WEIGHTS_INDEX_NAMES,
)


class SignalWeights(VFBCanonicalDataNode):
    """Canonical DataNode for VFB signal weights."""

    def __init__(
        self,
        config: SignalWeightsConfiguration | None = None,
        *args,
        namespace: str | None = None,
        **kwargs,
    ):
        resolved_config = self._validate_config(config or self.default_config())
        self.signal_configuration = resolved_config.signal_configuration
        self.use_canonical_signal_weights = True
        self.canonical_signal_weights_node = self
        super().__init__(resolved_config, *args, namespace=namespace, **kwargs)

    def _initialize_configuration(self, init_kwargs: dict) -> None:
        """Hash every signal subclass as the canonical SignalWeights table."""
        init_kwargs["time_series_class_import_path"] = _class_import_path(SignalWeights)
        config = build_operations.create_config(
            kwargs=init_kwargs,
            ts_class_name=SignalWeights.__name__,
        )
        for field_name, value in asdict(config).items():
            setattr(self, field_name, value)

    def set_signal_weights_frame(
        self,
        signal_weights_frame: pd.DataFrame,
        *,
        signal: Any | None = None,
        signal_configuration: Any | None = None,
        signal_description: str | None = None,
        metadata_updater: Any | None = None,
    ) -> SignalWeights:
        """Attach runtime signal inputs without changing table identity."""
        self._signal_weights_frame = signal_weights_frame
        if signal is not None and signal_configuration is not None:
            raise ValueError("Pass either signal or signal_configuration, not both.")
        resolved_signal_configuration = (
            signal_configuration if signal_configuration is not None else signal
        )
        if resolved_signal_configuration is not None:
            self.signal_configuration = resolved_signal_configuration
        self._signal_description = signal_description
        self._signal_metadata_updater = metadata_updater
        return self

    def update(self) -> pd.DataFrame:
        config = self._canonical_config()
        raw_frame = self._calculate_signal_weights()
        frame = (
            self.validate_frame(raw_frame, config=config)
            if _is_canonical_frame(raw_frame, config=config)
            else self.validate_frame(
                normalize_signal_weights_frame(
                    raw_frame,
                    signal_uid=self.signal_uid,
                    config=config,
                ),
                config=config,
            )
        )
        self._upsert_signal_metadata_if_available()
        return frame

    def _calculate_signal_weights(self) -> pd.DataFrame:
        signal_weights_frame = getattr(self, "_signal_weights_frame", None)
        if signal_weights_frame is not None:
            return signal_weights_frame

        if self.signal_configuration is None:
            return self.get_canonical_frame()

        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _calculate_signal_weights()."
        )

    def _signal_uid_payload(self) -> dict[str, Any]:
        if self.signal_configuration is None:
            raise ValueError(
                f"{self.__class__.__name__} requires signal_configuration to compute signal_uid."
            )
        return {
            "time_series_class_import_path": _class_import_path(self.__class__),
            "config": self.signal_configuration,
        }

    def _resolve_signal_uid(self) -> str:
        return compute_signal_uid(self._signal_uid_payload())

    @property
    def signal_uid(self) -> str:
        return self._resolve_signal_uid()

    def _upsert_signal_metadata_if_available(self) -> None:
        if self.signal_configuration is None:
            return
        from . import upsert_signal_metadata as _upsert_signal_metadata

        _upsert_signal_metadata(
            signal=self,
            signal_uid=self.signal_uid,
            signal_description=getattr(self, "_signal_description", None),
            updater=getattr(self, "_signal_metadata_updater", None),
        )

    def get_explanation(self):
        return f"{self.__class__.__name__}: canonical VFB signal weights."

    def maximum_forward_fill(self):
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement maximum_forward_fill()."
        )

    def get_asset_list(self) -> None | list:
        return None

    def get_asset_uid_to_override_portfolio_price(self):
        return None

    def interpolate_index(
        self,
        new_index: pd.DatetimeIndex,
        *,
        signal_weights_node=None,
        use_canonical_signal_weights: bool | None = None,
    ):
        """
        Get interpolated signal weights for a time index.

        Signal weights are valid only for a bounded duration, enforced by
        maximum_forward_fill().
        """
        weights_source = self._resolve_signal_weights_source(
            signal_weights_node=signal_weights_node,
            use_canonical_signal_weights=use_canonical_signal_weights,
        )
        weights = self._get_signal_weights_between_dates(
            weights_source=weights_source,
            start_date=new_index.min(),
            end_date=new_index.max(),
        )

        if len(weights) == 0:
            update_statistics = getattr(weights_source, "update_statistics", None)
            if update_statistics is None:
                update_statistics = self.update_statistics

            if not update_statistics.index_progress:
                raise Exception("Signal has not been updated")

            last_observation = self._get_signal_weights_between_dates(
                weights_source=weights_source,
                dimension_range_map=update_statistics.get_dimension_range_map_great_or_equal(),
            )
            if last_observation is None or last_observation.empty:
                return pd.DataFrame()
            last_date = last_observation.index.get_level_values("time_index")[0]

            if last_date < new_index.min():
                self.logger.warning(
                    f"No weights data at start of the portfolio at {new_index.min()}"
                    f" will use last available weights {last_date}"
                )
                weights = self._get_signal_weights_between_dates(
                    weights_source=weights_source,
                    start_date=last_date,
                    end_date=new_index.max(),
                )

        if len(weights) == 0:
            self.logger.warning("No weights data in index interpolation")
            return pd.DataFrame()

        weights = self._normalize_signal_weights_time_index(weights)
        weights_pivot = (
            weights.reset_index()
            .pivot(index="time_index", columns=["unique_identifier"], values="signal_weight")
            .fillna(0)
        )
        weights_pivot["last_weights"] = weights_pivot.index.get_level_values(level="time_index")

        combined_index = weights_pivot.index.union(new_index)
        combined_index.name = "time_index"
        weights_reindex = weights_pivot.reindex(combined_index)

        weights_reindex["last_weights"] = weights_reindex["last_weights"].ffill()
        weights_reindex["diff_to_last_weights"] = (
            weights_reindex.index.get_level_values(level="time_index")
            - weights_reindex["last_weights"]
        )

        invalid_forward_fills = (
            weights_reindex["diff_to_last_weights"] >= self.maximum_forward_fill()
        )
        weights_reindex.drop(columns=["last_weights", "diff_to_last_weights"], inplace=True)

        weights_reindex = weights_reindex.ffill()
        weights_reindex[invalid_forward_fills] = np.nan

        if weights_reindex.isna().values.any():
            self.logger.info("Could not fully interpolate for signal weights")

        weights_reindex = weights_reindex.loc[new_index]
        weights_reindex.index.name = "time_index"
        return weights_reindex

    def _resolve_signal_weights_source(
        self,
        *,
        signal_weights_node=None,
        use_canonical_signal_weights: bool | None = None,
    ):
        if use_canonical_signal_weights is None:
            use_canonical_signal_weights = (
                signal_weights_node is not None
                or getattr(self, "use_canonical_signal_weights", False)
                or getattr(self, "canonical_signal_weights_node", None) is not None
            )

        if not use_canonical_signal_weights:
            return self

        if signal_weights_node is not None:
            return signal_weights_node

        canonical_node = getattr(self, "canonical_signal_weights_node", None)
        if canonical_node is not None:
            return canonical_node

        hash_namespace = getattr(self, "hash_namespace", "") or None
        return SignalWeights(namespace=hash_namespace)

    def _get_signal_weights_between_dates(
        self,
        *,
        weights_source,
        start_date=None,
        end_date=None,
        dimension_range_map=None,
    ) -> pd.DataFrame:
        kwargs = {
            "start_date": start_date,
            "end_date": end_date,
            "dimension_range_map": dimension_range_map,
        }
        kwargs = {key: value for key, value in kwargs.items() if value is not None}

        if weights_source is self and not getattr(self, "use_canonical_signal_weights", False):
            return self.get_df_between_dates(**kwargs)

        return weights_source.get_df_between_dates(
            **kwargs,
            dimension_filters={SIGNAL_UID: [self.signal_uid]},
        )

    def _normalize_signal_weights_time_index(self, weights: pd.DataFrame) -> pd.DataFrame:
        if weights is None or weights.empty:
            return weights

        normalized = weights.copy()
        if isinstance(normalized.index, pd.MultiIndex) and "time_index" in normalized.index.names:
            arrays = []
            for level_number, level_name in enumerate(normalized.index.names):
                level_values = normalized.index.get_level_values(level_number)
                if level_name == "time_index":
                    level_values = pd.to_datetime(level_values, utc=True)
                arrays.append(level_values)
            normalized.index = pd.MultiIndex.from_arrays(
                arrays,
                names=normalized.index.names,
            )
            return normalized

        if normalized.index.name == "time_index":
            normalized.index = pd.to_datetime(normalized.index, utc=True)
            return normalized

        if "time_index" in normalized.columns:
            normalized["time_index"] = pd.to_datetime(
                normalized["time_index"],
                utc=True,
            )
        return normalized

    @classmethod
    def default_config(
        cls,
        *,
        identifier: str | None = None,
        description: str | None = None,
        extra_records: list[RecordDefinition] | None = None,
        signal_configuration: Any | None = None,
    ) -> SignalWeightsConfiguration:
        return cls._validate_config(
            SignalWeightsConfiguration(
                index_names=cls._required_index_names(),
                records=cls._records_with_extra(extra_records=extra_records),
                signal_configuration=signal_configuration,
                node_metadata=DataNodeMetaData(
                    identifier=identifier or cls._default_identifier(),
                    description=description or cls._default_description(),
                ),
            )
        )

    @classmethod
    def from_signal_configuration(
        cls,
        signal_configuration: Any,
        *,
        namespace: str | None = None,
        **kwargs,
    ) -> SignalWeights:
        return cls(
            config=cls.default_config(signal_configuration=signal_configuration),
            namespace=namespace,
            **kwargs,
        )

    @classmethod
    def _validate_config(
        cls,
        config: VFBCanonicalDataNodeConfiguration,
    ) -> SignalWeightsConfiguration:
        if not isinstance(config, SignalWeightsConfiguration):
            if isinstance(config, VFBCanonicalDataNodeConfiguration):
                config = SignalWeightsConfiguration(
                    index_names=config.index_names,
                    records=config.records,
                    node_metadata=config.node_metadata,
                )
            else:
                raise TypeError(f"{cls.__name__} requires a SignalWeightsConfiguration.")
        return super()._validate_config(config)

    @classmethod
    def _default_identifier(cls) -> str:
        return "mainsequence.markets.signal_weights"

    @classmethod
    def _default_description(cls) -> str:
        return (
            "Canonical VFB signal weights indexed by time_index, signal_uid, "
            "and asset unique_identifier."
        )

    @classmethod
    def _required_index_names(cls) -> list[str]:
        return list(SIGNAL_WEIGHTS_INDEX_NAMES)

    @classmethod
    def _required_records(cls) -> list[RecordDefinition]:
        return _record_definitions_from_dtype_map(
            SIGNAL_WEIGHTS_COLUMN_DTYPES_MAP,
            labels=SIGNAL_WEIGHTS_COLUMN_LABELS,
            descriptions=SIGNAL_WEIGHTS_COLUMN_DESCRIPTIONS,
        )

    @classmethod
    def _schema_bootstrap_index_values(cls) -> dict[str, Any]:
        return {
            SIGNAL_UID: SCHEMA_BOOTSTRAP_SIGNAL_UID,
            ASSET_UNIQUE_IDENTIFIER: SCHEMA_BOOTSTRAP_ASSET_IDENTIFIER,
        }

    @staticmethod
    def canonical_signal_configuration(signal: Any) -> dict[str, Any]:
        return canonical_signal_configuration(signal)

    @staticmethod
    def compute_signal_uid(signal: Any) -> str:
        return compute_signal_uid(signal)


def canonical_signal_configuration(signal: Any) -> dict[str, Any]:
    """Return the canonical hash payload for a VFB signal producer.

    The payload keeps signal class/config identity and removes storage/update
    identity so the same signal config keeps the same `signal_uid` across
    namespaces and backend table instances.
    """
    payload = _signal_configuration_payload(signal)
    serialized_payload = build_operations.Serializer().serialize_init_kwargs(payload)
    return _drop_excluded_keys(
        dict(serialized_payload),
        excluded_keys=SIGNAL_UID_EXCLUDED_CONFIGURATION_KEYS,
    )


def compute_signal_uid(signal: Any) -> str:
    """Compute the deterministic VFB `signal_uid` for a signal producer."""
    payload = canonical_signal_configuration(signal)
    _update_hash, storage_hash = build_operations.hash_signature(payload)
    return storage_hash


def normalize_signal_weights_frame(
    signal_weights_frame: pd.DataFrame,
    *,
    signal_uid: str,
    config: VFBCanonicalDataNodeConfiguration | None = None,
) -> pd.DataFrame:
    """Normalize signal output into canonical SignalWeights rows."""
    config = SignalWeights._validate_config(config or SignalWeights.default_config())
    flat = _reset_frame_index(signal_weights_frame)
    if flat.empty:
        flat = _empty_flat_frame(config=config)

    if "signal_weight" not in flat.columns:
        flat = _normalize_pivoted_signal_weights(flat)
    flat[SIGNAL_UID] = str(signal_uid)

    _require_columns(
        flat,
        required_columns=list(config.column_dtypes_map),
        frame_name="SignalWeights",
    )
    return SignalWeights.validate_frame(
        flat[list(config.column_dtypes_map)],
        config=config,
    )


def _signal_configuration_payload(signal: Any) -> dict[str, Any]:
    if isinstance(signal, SignalWeights):
        return signal._signal_uid_payload()

    build_configuration = getattr(signal, "build_configuration", None)
    if callable(build_configuration):
        build_configuration = build_configuration()
    if isinstance(build_configuration, dict):
        return dict(build_configuration)

    if isinstance(signal, BaseModel):
        return {
            "config": signal,
            "time_series_class_import_path": _class_import_path(signal.__class__),
        }

    signal_config = getattr(signal, "config", None)
    if signal_config is not None:
        return {
            "config": signal_config,
            "time_series_class_import_path": _class_import_path(signal.__class__),
        }

    if isinstance(signal, dict):
        return dict(signal)

    raise TypeError(
        "signal must be a DataNode-like object with build_configuration/config, "
        "a Pydantic configuration model, or a dict payload."
    )


@build_operations.serialize_argument.register(SignalWeights)
def _serialize_signal_weights_argument(value: SignalWeights, pickle_ts: bool = False) -> dict:
    return canonical_signal_configuration(value)
