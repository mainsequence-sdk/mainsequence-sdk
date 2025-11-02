from __future__ import annotations

import datetime
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field, field_validator

from .base_instrument import (
    InstrumentModel as Instrument,  # runtime_checkable Protocol: requires .price() -> float
)

Instrument._DEFAULT_REGISTRY.update(
    {
        "EuropeanOption": globals().get("EuropeanOption"),
        "VanillaFXOption": globals().get("VanillaFXOption"),
        "KnockOutFXOption": globals().get("KnockOutFXOption"),
        "FixedRateBond": globals().get("FixedRateBond"),
        "FloatingRateBond": globals().get("FloatingRateBond"),
        "InterestRateSwap": globals().get("InterestRateSwap"),
    }
)
# Optionally: prune any Nones if some classes aren't imported yet
Instrument._DEFAULT_REGISTRY = {
    k: v for k, v in Instrument._DEFAULT_REGISTRY.items() if v is not None
}


@dataclass(frozen=False)
class PositionLine:
    """
    A single position: an instrument and the number of units held.
    Units may be negative for short positions.
    """

    instrument: Instrument
    units: float
    extra_market_info: dict = None

    def unit_price(self) -> float:
        return float(self.instrument.price())

    def market_value(self) -> float:
        return self.units * self.unit_price()


class Position(BaseModel):
    """
    A collection of instrument positions with convenient aggregations.

    - Each line is an (instrument, units) pair.
    - `price()` returns the sum of units * instrument.price().
    - `get_cashflows(aggregate=...)` merges cashflows from instruments that expose `get_cashflows()`.
      * Expects each instrument's `get_cashflows()` to return a dict[str, list[dict]], like the swap.
      * Amounts are scaled by `units`. Unknown structures are passed through best-effort.
    - `get_greeks()` sums greeks from instruments that expose `get_greeks()`.
    """

    lines: list[PositionLine] = Field(default_factory=list)
    position_date: datetime.datetime | None = None
    model_config = {"arbitrary_types_allowed": True}

    @classmethod
    def from_json_dict(
        cls, data: dict[str, Any], registry: Mapping[str, type] | None = None
    ) -> Position:
        # default registry with your known instruments

        lines: list[PositionLine] = []
        for item in data.get("lines", []):
            inst = Instrument.rebuild(item, registry=registry)
            units = item["units"]
            extra_market_info = item.get("extra_market_info")
            lines.append(
                PositionLine(instrument=inst, units=units, extra_market_info=extra_market_info)
            )
        return cls(lines=lines)

        # ---------------- JSON ENCODING ----------------

    def _instrument_payload(self, inst: Any) -> dict[str, Any]:
        """
        Robustly obtain a JSON-serializable dict from an instrument.
        Tries, in order: to_json_dict(), to_json() (parse), model_dump(mode="json").
        """
        # 1) Preferred: your JSONMixin path
        to_jd = getattr(inst, "to_json_dict", None)
        if callable(to_jd):
            payload = to_jd()
            if isinstance(payload, dict):
                return payload

        # 2) Accept a JSON string and parse it
        to_js = getattr(inst, "to_json", None)
        if callable(to_js):
            s = to_js()
            if isinstance(s, (bytes, bytearray)):
                s = s.decode("utf-8")
            if isinstance(s, str):
                try:
                    obj = json.loads(s)
                    if isinstance(obj, dict):
                        return obj
                except Exception:
                    pass  # fall through

        # 3) Pydantic models without JSONMixin
        md = getattr(inst, "model_dump", None)
        if callable(md):
            return md(mode="json")

        raise TypeError(
            f"Instrument {type(inst).__name__} is not JSON-serializable. "
            f"Provide to_json_dict()/to_json() or a Pydantic model."
        )

    def to_json_dict(self) -> dict[str, Any]:
        """
        Serialize the position as:
        {
          "lines": [
            { "instrument_type": "...", "instrument": { ... }, "units": <float> },
            ...
          ]
        }
        """
        out_lines: list[dict] = []
        for line in self.lines:
            inst = line.instrument
            out_lines.append(
                {
                    "instrument_type": type(inst).__name__,
                    "instrument": self._instrument_payload(inst),
                    "units": float(line.units),
                    "extra_market_info": line.extra_market_info,
                }
            )
        return {"lines": out_lines}

    # ---- validation ---------------------------------------------------------
    @field_validator("lines")
    @classmethod
    def _validate_lines(cls, v: list[PositionLine]) -> list[PositionLine]:
        for i, line in enumerate(v):
            inst = line.instrument
            # Accept anything implementing the Instrument Protocol (price() -> float)
            if not hasattr(inst, "price") or not callable(inst.price):
                raise TypeError(
                    f"lines[{i}].instrument must implement price() -> float; got {type(inst).__name__}"
                )
        return v

    # ---- mutation helpers ---------------------------------------------------
    def add(self, instrument: Instrument, units: float = 1.0) -> None:
        """Append a new position line."""
        self.lines.append(PositionLine(instrument=instrument, units=units))

    def extend(self, items: Iterable[tuple[Instrument, float]]) -> None:
        """Append many (instrument, units) items."""
        for inst, qty in items:
            self.add(inst, qty)

    # ---- pricing ------------------------------------------------------------
    def price(self) -> float:
        """Total market value: Σ units * instrument.price()."""
        return float(sum(line.market_value() for line in self.lines))

    def price_breakdown(self) -> list[dict[str, Any]]:
        """
        Line-by-line price decomposition.
        Returns: [{instrument, units, unit_price, market_value}, ...]
        """
        out: list[dict[str, Any]] = []
        for line in self.lines:
            out.append(
                {
                    "instrument": type(line.instrument).__name__,
                    "units": line.units,
                    "unit_price": line.unit_price(),
                    "market_value": line.market_value(),
                }
            )
        return out

    # ---- cashflows ----------------------------------------------------------
    def get_cashflows(self, aggregate: bool = False) -> dict[str, list[dict[str, Any]]]:
        """
        Merge cashflows from all instruments that implement `get_cashflows()`.

        Returns a dict keyed by leg/label (e.g., "fixed", "floating") with lists of cashflow dicts.
        Each cashflow's 'amount' is scaled by position units. Original fields are preserved;
        metadata 'instrument' and 'units' are added for traceability.

        If aggregate=True, amounts are summed by payment date within each leg.
        """
        combined: dict[str, list[dict[str, Any]]] = defaultdict(list)

        for idx, line in enumerate(self.lines):
            inst = line.instrument
            if not hasattr(inst, "get_cashflows"):
                continue  # silently skip instruments without cashflows
            flows = inst.get_cashflows()  # type: ignore[attr-defined]
            if not isinstance(flows, dict):
                continue

            for leg, items in flows.items():
                if not isinstance(items, (list, tuple)):
                    continue
                for cf in items:
                    if not isinstance(cf, dict):
                        continue
                    scaled = dict(cf)  # shallow copy
                    # scale common amount field if present
                    if "amount" in scaled and isinstance(scaled["amount"], (int, float)):
                        scaled["amount"] = float(scaled["amount"]) * line.units
                    # annotate
                    scaled.setdefault("instrument", type(inst).__name__)
                    scaled.setdefault("units", line.units)
                    scaled.setdefault("position_index", idx)
                    combined[leg].append(scaled)

        if not aggregate:
            return dict(combined)

        # Aggregate amounts by payment date (fallback to 'date' or 'fixing_date' if needed)
        aggregated: dict[str, list[dict[str, Any]]] = {}
        for leg, items in combined.items():
            buckets: dict[datetime.date, float] = defaultdict(float)
            exemplars: dict[datetime.date, dict[str, Any]] = {}

            for cf in items:
                # identify a date field
                dt = cf.get("payment_date") or cf.get("date") or cf.get("fixing_date")
                if isinstance(dt, datetime.date):
                    amount = float(cf.get("amount", 0.0))
                    buckets[dt] += amount
                    # keep exemplar fields for output ordering/context
                    if dt not in exemplars:
                        exemplars[dt] = {
                            k: v
                            for k, v in cf.items()
                            if k not in {"amount", "units", "position_index"}
                        }
                # if no usable date, just pass through (unaggregated)
                else:
                    buckets_key = None  # sentinel
                    # Collect undated flows under today's key to avoid loss
                    buckets[datetime.date.today()] += float(cf.get("amount", 0.0))

            # build sorted list
            leg_rows: list[dict[str, Any]] = []
            for dt, amt in sorted(buckets.items(), key=lambda kv: kv[0]):
                row = {"payment_date": dt, "amount": amt}
                # attach exemplar metadata if any
                ex = exemplars.get(dt)
                if ex:
                    row.update({k: v for k, v in ex.items() if k in ("leg", "rate", "spread")})
                leg_rows.append(row)
            aggregated[leg] = leg_rows

        return aggregated

    # ---- greeks (optional) --------------------------------------------------
    def get_greeks(self) -> dict[str, float]:
        """
        Aggregate greeks from instruments that implement `get_greeks()`.

        For each instrument i with dictionary Gi and units ui, returns Σ ui * Gi[key].
        Keys not common across all instruments are included on a best-effort basis.
        """
        totals: dict[str, float] = defaultdict(float)
        for line in self.lines:
            inst = line.instrument
            getg = getattr(inst, "get_greeks", None)
            if callable(getg):
                g = getg()
                if isinstance(g, dict):
                    for k, v in g.items():
                        if isinstance(v, (int, float)):
                            totals[k] += line.units * float(v)
        return dict(totals)

    # ---- convenience constructors -------------------------------------------
    @classmethod
    def from_single(cls, instrument: Instrument, units: float = 1.0) -> Position:
        return cls(lines=[PositionLine(instrument=instrument, units=units)])

    # Mao interface

    def units_by_id(self) -> dict[str, float]:
        """Map instrument id -> units."""
        return {line.instrument.content_hash(): float(line.units) for line in self.lines}

    def npvs_by_id(self, *, apply_units: bool = True) -> dict[str, float]:
        """
        Return PVs per instrument id. If apply_units=True, PVs are already scaled by line.units.
        """
        out: dict[str, float] = {}
        for line in self.lines:
            ins = line.instrument
            ins_id = ins.content_hash()
            pv = float(ins.price())
            if apply_units:
                pv *= float(line.units)
            out[ins_id] = pv
        return out

    def cashflows_by_id(
        self, cutoff: datetime.date | None = None, *, apply_units: bool = True
    ) -> pd.DataFrame:
        """
        Aggregate cashflows across all lines.

        Returns a DataFrame with columns: ['ins_id', 'payment_date', 'amount'].
        If apply_units=True, amounts are multiplied by line.units.
        """
        rows = []
        for line in self.lines:
            ins = line.instrument
            ins_id = ins.content_hash()

            s = ins.get_net_cashflows()  # Expect Series indexed by payment_date
            if s is None:
                continue
            if not isinstance(s, pd.Series):
                # Be conservative: try converting if possible; otherwise skip
                try:
                    s = pd.Series(s)
                except Exception:
                    continue

            df = s.to_frame("amount").reset_index()
            # Normalize index/column name for payment date
            if "payment_date" not in df.columns:
                # typical reset_index name is 'index' or the original index name
                idx_col = "payment_date" if s.index.name == "payment_date" else "index"
                df = df.rename(columns={idx_col: "payment_date"})

            if cutoff is not None:
                df = df[df["payment_date"] <= cutoff]

            if apply_units:
                df["amount"] = df["amount"].astype(float) * float(line.units)
            else:
                df["amount"] = df["amount"].astype(float)

            df["ins_id"] = ins_id
            rows.append(df[["ins_id", "payment_date", "amount"]])

        if not rows:
            return pd.DataFrame(columns=["ins_id", "payment_date", "amount"])

        return pd.concat(rows, ignore_index=True)

    def agg_net_cashflows(self) -> pd.DataFrame:
        """
        Aggregate 'net' cashflows from all instruments.
        Preferred: instrument.get_net_cashflows() -> pd.Series indexed by payment_date.
        Fallback:  instrument.get_cashflows() -> dict[leg] -> list[dict] with 'amount' and a date field.
        Returns DataFrame with ['payment_date','amount'] summed across instruments & units.
        """
        rows = []
        for line in self.lines:
            inst = line.instrument
            units = float(line.units)

            # Preferred API (already used in your other app)
            s = getattr(inst, "get_net_cashflows", None)
            if callable(s):
                ser = s()
                if isinstance(ser, pd.Series):
                    df = ser.to_frame("amount").reset_index()  # index is payment_date
                    # Normalize column name
                    if "index" in df.columns and "payment_date" not in df.columns:
                        df = df.rename(columns={"index": "payment_date"})
                    df["amount"] = df["amount"].astype(float) * units
                    rows.append(df[["payment_date", "amount"]])
                    continue  # next line

            # Fallback: flatten get_cashflows()
            g = getattr(inst, "get_cashflows", None)
            if callable(g):
                flows = g()
                flat = []
                for leg, items in (flows or {}).items():
                    for cf in items or []:
                        pay = (
                            cf.get("payment_date")
                            or cf.get("date")
                            or cf.get("pay_date")
                            or cf.get("fixing_date")
                        )
                        amt = cf.get("amount")
                        if pay is None or amt is None:
                            continue
                        flat.append(
                            {
                                "payment_date": pd.to_datetime(pay).date(),
                                "amount": float(amt) * units,
                            }
                        )
                if flat:
                    rows.append(pd.DataFrame(flat))

        if not rows:
            return pd.DataFrame(columns=["payment_date", "amount"])

        df_all = pd.concat(rows, ignore_index=True)
        df_all["payment_date"] = pd.to_datetime(df_all["payment_date"]).dt.date
        df_all = df_all.groupby("payment_date", as_index=False)["amount"].sum()
        return df_all

    def position_total_npv(self) -> float:
        """Σ units * instrument.price()."""
        tot = 0.0
        for line in self.lines:
            tot += float(line.units) * float(line.instrument.price())
        return float(tot)

    def position_carry_to_cutoff(
        self, valuation_date: datetime.date, cutoff: datetime.date
    ) -> float:
        """
        Carry = Σ net cashflow amounts with valuation_date < payment_date ≤ cutoff.
        Positive = inflow to the bank; negative = outflow.
        """
        cf = self.agg_net_cashflows()
        if cf.empty:
            return 0.0
        mask = (cf["payment_date"] > valuation_date) & (cf["payment_date"] <= cutoff)
        return float(cf.loc[mask, "amount"].sum())


def npv_table(
    npv_base: dict[str, float],
    npv_bumped: dict[str, float] | None = None,
    units: dict[str, float] | None = None,
    *,
    include_total: bool = True,
) -> pd.DataFrame:
    """
    Build a raw (unformatted) NPV table for programmatic use.

    Columns: instrument, units, base, bumped, delta  (bumped/delta are NaN if npv_bumped is None)
    """
    ids = sorted(npv_base.keys())
    rows = []
    for ins_id in ids:
        base = float(npv_base.get(ins_id, np.nan))
        bumped = float(npv_bumped.get(ins_id, np.nan)) if npv_bumped is not None else np.nan
        delta = (
            bumped - base
            if npv_bumped is not None and np.isfinite(base) and np.isfinite(bumped)
            else np.nan
        )
        u = float(units.get(ins_id, np.nan)) if units else np.nan
        rows.append(
            {"instrument": ins_id, "units": u, "base": base, "bumped": bumped, "delta": delta}
        )

    df = pd.DataFrame(rows)

    if include_total and not df.empty:
        tot = {
            "instrument": "TOTAL",
            "units": np.nan,
            "base": df["base"].sum(skipna=True),
            "bumped": df["bumped"].sum(skipna=True) if npv_bumped is not None else np.nan,
            "delta": df["delta"].sum(skipna=True) if npv_bumped is not None else np.nan,
        }
        df = pd.concat([df, pd.DataFrame([tot])], ignore_index=True)

    return df


def portfolio_stats(
    position, bumped_position, valuation_date: datetime.date, cutoff: datetime.date
):
    """
    Returns a dict with base/bumped NPV and Carry to cutoff, plus deltas.
    """
    npv_base = position.position_total_npv()
    npv_bump = bumped_position.position_total_npv()
    carry_base = position.position_carry_to_cutoff(valuation_date, cutoff)
    carry_bump = bumped_position.position_carry_to_cutoff(valuation_date, cutoff)

    return {
        "npv_base": npv_base,
        "npv_bumped": npv_bump,
        "npv_delta": npv_bump - npv_base,
        "carry_base": carry_base,
        "carry_bumped": carry_bump,
        "carry_delta": carry_bump - carry_base,
    }
