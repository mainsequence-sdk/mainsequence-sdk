# src/instruments/base_instrument.py
import datetime
import inspect
import json
from collections.abc import Mapping
from typing import Any, ClassVar

from pydantic import BaseModel, Field, PrivateAttr

from .json_codec import JSONMixin


class InstrumentModel(BaseModel, JSONMixin):
    """
    Common base for all Pydantic instrument models.
    Adds a shared optional 'main_sequence_uid' field and shared config.
    """

    main_sequence_asset_id: int | None = Field(
        default=None, description="Optional UID linking this instrument to a main sequence record."
    )

    # Keep your existing behavior (QuantLib types, etc.)
    model_config = {"arbitrary_types_allowed": True}

    _valuation_date: datetime.datetime | None = PrivateAttr(default=None)

    _DEFAULT_REGISTRY: ClassVar[dict[str, type["InstrumentModel"]]] = {}

    def __init_subclass__(cls, **kwargs):
        """Auto-register concrete subclasses for rebuild()."""
        super().__init_subclass__(**kwargs)
        if cls is InstrumentModel:
            return  # don't register the base itself
        # Skip abstract classes (like Bond once we mark it ABC)
        if inspect.isabstract(cls):
            return
        name = cls.__name__
        InstrumentModel._DEFAULT_REGISTRY[name] = cls
    # public read access (still not serialized)
    @property
    def valuation_date(self) -> datetime.datetime | None:
        return self._valuation_date

    # explicit setter method (per your request)
    def set_valuation_date(self, value: datetime.datetime | None) -> None:
        self._valuation_date = value

    def serialize_for_backend(self):
        serialized = {}
        data = self.model_dump_json()
        data = json.loads(data)
        serialized["instrument_type"] = type(self).__name__
        serialized["instrument"] = data

        return json.dumps(serialized)

    @classmethod
    def rebuild(
        cls,
        data: str | dict[str, Any],
        registry: Mapping[str, type["InstrumentModel"]] | None = None,
    ) -> "InstrumentModel":
        """
        Rebuild a single instrument from its wire format.

        Accepts either:
          - a dict: {"instrument_type": "FixedRateBond", "instrument": {...}}
          - a JSON string of the same shape

        Optional `registry` maps instrument_type -> InstrumentModel subclass.
        Falls back to InstrumentModel._DEFAULT_REGISTRY.
        """
        import mainsequence.instruments as msi
        # Parse JSON if needed
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError as err:
                # Keep the original cause so stacktraces show *why* parsing failed
                raise ValueError("Invalid JSON for instrument.") from err

        if not isinstance(data, dict):
            raise ValueError("Instrument payload must be dict or JSON string.")

        t = data.get("instrument_type")
        payload = data.get("instrument", {})
        if not t or not isinstance(payload, dict):
            raise ValueError("Expected {'instrument_type': <str>, 'instrument': <dict>}.")

        # Merge registries (explicit registry overrides defaults)
        effective_registry: dict[str, type[InstrumentModel]] = dict(cls._DEFAULT_REGISTRY)
        if registry:
            effective_registry.update(registry)

        target_cls = effective_registry.get(t)
        if target_cls is None:
            target_cls = getattr(msi, t,None)
        if target_cls is None:
            raise ValueError(f"Unknown instrument type: {t}")
        if not hasattr(target_cls, "from_json"):
            raise TypeError(f"Instrument type {t} is not JSON-rebuildable (missing from_json).")

        return target_cls.from_json(payload)
