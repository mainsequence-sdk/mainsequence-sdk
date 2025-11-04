# mainsequence/instruments/instruments/bond.py
import datetime
import json
import hashlib
import threading
from typing import Any
import math
import QuantLib as ql
from pydantic import Field, PrivateAttr

from mainsequence.instruments.pricing_models.bond_pricer import (
    create_floating_rate_bond_with_curve,
)
from mainsequence.instruments.pricing_models.indices import get_index
from mainsequence.instruments.utils import to_py_date, to_ql_date

from .base_instrument import InstrumentModel
from .ql_fields import (
    QuantLibBDC as QBDC,
)
from .ql_fields import (
    QuantLibCalendar as QCalendar,
)
from .ql_fields import (
    QuantLibDayCounter as QDayCounter,
)
from .ql_fields import (
    QuantLibPeriod as QPeriod,
)
from .ql_fields import (
    QuantLibSchedule as QSchedule,
)
from collections import OrderedDict

# ---- GLOBAL BOND CACHE ----
# [bond_key][price_key] -> float (NPV)
BOND_CACHE_PER_INSTRUMENT_LIMIT = 256
_BOND_CACHE_LOCK = threading.RLock()
_BOND_PRICE_CACHE: dict[str, "OrderedDict[str, float]"] = {}
_BOND_ZSPREAD_CACHE: dict[str, "OrderedDict[str, float]"] = {}
_BOND_DURATION_CACHE: dict[str, "OrderedDict[str, float]"] = {}  # <- NEW



def clear_global_bond_cache() -> None:
    with _BOND_CACHE_LOCK:
        _BOND_PRICE_CACHE.clear()
def global_bond_cache_stats() -> dict[str, int]:
    with _BOND_CACHE_LOCK:
        return {
            "instruments": len(_BOND_PRICE_CACHE),
            "entries": sum(len(v) for v in _BOND_PRICE_CACHE.values()),
            "per_instrument_limit": BOND_CACHE_PER_INSTRUMENT_LIMIT,
        }


# ---- GLOBAL VERSION TICKS (auto-bumped by observers) ----
# Incremented whenever the corresponding Observable notifies:
#   - YieldTermStructure (via its Handle)
#   - Index (for fixings updates)
_YTS_VERSION: dict[int, int] = {}    # yts_id -> version
_INDEX_VERSION: dict[int, int] = {}  # index_id -> version



class Bond(InstrumentModel):
    """
    Shared pricing lifecycle for vanilla bonds.

    Subclasses must implement:
      - _get_default_discount_curve(): Optional[ql.YieldTermStructureHandle]
      - _create_bond(discount_curve: ql.YieldTermStructureHandle) -> ql.Bond
        (return a ql.FixedRateBond or ql.FloatingRateBond, etc. *without* assuming any global state)
    """

    face_value: float = Field(...)
    issue_date: datetime.date = Field(...)
    maturity_date: datetime.date = Field(...)

    day_count: QDayCounter = Field(...)
    calendar: QCalendar = Field(default_factory=ql.TARGET)
    business_day_convention: QBDC = Field(default=ql.Following)
    settlement_days: int = Field(default=2)
    schedule: QSchedule | None = Field(None)

    benchmark_rate_index_name: str | None = Field(
        ...,
        description="A default index benchmark rate, helpful when doing"
        "analysis and we want to  map the bond to a bencharmk for example to"
        "the SOFR Curve or to de US Treasury curve etc",
    )

    model_config = {"arbitrary_types_allowed": True}

    _bond: ql.Bond | None = PrivateAttr(default=None)
    _with_yield: float | None = PrivateAttr(default=None)
    _engine: ql.PricingEngine | None = PrivateAttr(default=None)
    _last_discount_curve_handle: ql.YieldTermStructureHandle | None = PrivateAttr(default=None)
    _curve_observer: ql.Observer | None = PrivateAttr(default=None)

    def get_bond(self):
        return self._bond
    # ---- valuation lifecycle ----
    def _on_valuation_date_set(self) -> None:
        self._bond = None
        self._engine = None
        self._last_discount_curve_handle = None
        self._with_yield = None
        # unhook curve observer if we had one
        if self._curve_observer and self._last_discount_curve_handle is not None:
            try:
                self._curve_observer.unregisterWith(self._last_discount_curve_handle)
            except Exception:
                pass
        self._curve_observer = None


    # ---- internal helpers (new) ----
    def _normalize_with_yield(self, y: float | None) -> float | None:
        """Make float arguments hashable and robust to tiny FP noise."""
        return None if y is None else round(float(y), 12)

    def _curve_key_for_observer(self, handle: "ql.YieldTermStructureHandle") -> int | None:
        """Return the integer identity we use as the YTS key for versioning."""
        try:
            yts = handle.currentLink()
        except Exception:
            yts = None
        return id(yts) if yts is not None else id(handle)

    def _ensure_curve_observer(self, handle: "ql.YieldTermStructureHandle") -> None:
        """Register an observer on the discount handle so in-place curve updates bump the version tick."""

        def _bump():
            try:
                key = self._curve_key_for_observer(handle)
                if key is not None:
                    _YTS_VERSION[key] = _YTS_VERSION.get(key, 0) + 1
            except Exception:
                pass

        # If we already had an observer, unhook from the previous handle and rebuild the observer
        if self._curve_observer is not None and self._last_discount_curve_handle is not None:
            try:
                self._curve_observer.unregisterWith(self._last_discount_curve_handle)
            except Exception:
                pass
            self._curve_observer = None

        # Create a fresh observer bound to this handle and register
        if self._curve_observer is None:
            self._curve_observer = ql.Observer(_bump)
        self._curve_observer.registerWith(handle)

    # ---- index helpers shared by all bonds (DRY) ----
    def _get_index_by_name(
            self,
            index_name: str,
            *,
            forwarding_curve: ql.YieldTermStructureHandle | None = None,
            hydrate_fixings: bool = True,
    ) -> ql.IborIndex:
        """
        Build a QuantLib index by name for the bond's valuation_date, optionally
        with a custom forwarding curve. This centralizes the get_index() usage.
        """
        if self.valuation_date is None:
            raise ValueError("Set valuation_date before requesting an index: set_valuation_date(dt).")
        if not index_name:
            raise ValueError("Index name is empty.")
        return get_index(
            index_name,
            target_date=self.valuation_date,
            forwarding_curve=forwarding_curve,
            hydrate_fixings=hydrate_fixings,
        )

    def get_benchmark_index_curve(self) -> ql.YieldTermStructureHandle:
        """
        Return the forwarding term structure for the bond's benchmark index
        (given by benchmark_rate_index_name). Mirrors FloatingRateBond.get_index_curve().
        """
        if not self.benchmark_rate_index_name:
            raise ValueError("benchmark_rate_index_name is not set for this instrument.")
        idx = self._get_index_by_name(self.benchmark_rate_index_name, hydrate_fixings=True)
        return idx.forwardingTermStructure()

    def get_benchmark_index(self) -> ql.IborIndex:
        if not self.benchmark_rate_index_name:
            raise ValueError("benchmark_rate_index_name is not set.")
        return self._get_index_by_name(self.benchmark_rate_index_name, hydrate_fixings=True)

    def _val_ordinal(self) -> int:
        """Use day granularity for pricing context."""
        if self.valuation_date is None:
            return -1
        vd = self.valuation_date
        if isinstance(vd, datetime.datetime):
            return vd.date().toordinal()
        return vd.toordinal()

    def _instrument_cache_key(self) -> str:
        """
        Stable identifier for this instrument (structure) at a valuation date,
        built from serialize_for_backend() plus valuation_date.
        """
        payload = json.loads(self.serialize_for_backend())
        payload["valuation_date"] = self.valuation_date.isoformat() if self.valuation_date else None
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return hashlib.sha1(canonical.encode()).hexdigest()

    def _curve_cache_id_from_handle(self, handle: "ql.YieldTermStructureHandle") -> str:
        """
        Deterministic id for a curve handle suitable for cross-instance cache hits:
          - identity of the underlying YieldTermStructure object
          - its reference date
        """
        try:
            yts = handle.currentLink()
        except Exception:
            yts = None
        yts_id = id(yts) if yts is not None else id(handle)
        try:
            ref_serial = int(handle.referenceDate().serialNumber())
        except Exception as e:
            raise e
        return f"yts:{yts_id}|ref:{ref_serial}"

    def _context_key_for_handle(self, handle: "ql.YieldTermStructureHandle") -> str:
        """Build the cache context key for an explicit curve handle (yts id + ref + version ticks + val-date)."""
        self._ensure_curve_observer(handle)
        base = self._curve_cache_id_from_handle(handle)
        yts_key = self._curve_key_for_observer(handle)
        yts_ver = _YTS_VERSION.get(yts_key, 0)
        fixv = self._fixings_version()
        return f"{base}|v:{yts_ver}|fixv:{fixv}|val:{self._val_ordinal()}"

    def _normalize_currency(self, x: float) -> float:
        """Stabilize currency inputs for cache keys."""
        return round(float(x), 8)

    def _price_context_key(self, with_yield: float | None) -> str:
        """
        Key that captures pricing context beyond the instrument structure.
        - default curve: defined by the curve handle identity + ref date + valuation date
        """
        val_ord = self._val_ordinal()
        if with_yield is not None:
            wy = self._normalize_with_yield(with_yield)
            # include dc/comp/freq implicitly defined in _resolve_discount_curve for flat curves
            return f"flat|y:{wy}|comp:Compounded|freq:Annual|val:{val_ord}"
        handle = self._get_default_discount_curve()
        if handle is None:
            raise ValueError(
                "No discount curve available. Either pass with_yield=... to price(), "
                "or the instrument must supply a default discount curve."
            )
        # ensure we observe curve updates so version ticks bump when quotes/relinks change
        self._ensure_curve_observer(handle)
        # curve identity & ref date
        base = self._curve_cache_id_from_handle(handle)
        # curve version tick
        yts_key = self._curve_key_for_observer(handle)
        yts_ver = _YTS_VERSION.get(yts_key, 0)
        # fixings version tick (default 0 for non-floaters; overridden in FloatingRateBond)
        fixv = self._fixings_version()
        return f"{base}|v:{yts_ver}|fixv:{fixv}|val:{val_ord}"


    def pricing_engine_id(self) -> str:
        """Human-readable id of the current engine/curve setup."""
        try:
            return self._price_context_key(self._with_yield)
        except Exception:
            return "unpriced"

    def _fixings_version(self) -> int:
        """Default: instruments without index fixings contribute no extra versioning."""
        return 0
    # ---- hooks for subclasses ----
    def _get_default_discount_curve(self) -> ql.YieldTermStructureHandle | None:
        """
        Subclasses return a curve if they have one (e.g., floating uses its index curve),
        or None if they require with_yield or an explicitly supplied handle.
        """
        return None

    def _create_bond(self, discount_curve: ql.YieldTermStructureHandle | None) -> ql.Bond:
        """Subclasses must create and return a QuantLib bond (Fixed or Floating).
        discount_curve may be None: subclasses must not assume it is present for cashflow-only usage.
        """
        raise NotImplementedError

    def _ensure_instrument(self) -> None:
        if self.valuation_date is None:
            raise ValueError(
                "Set valuation_date before building instrument: set_valuation_date(dt)."
            )

        ql_calc_date = to_ql_date(self.valuation_date)
        ql.Settings.instance().evaluationDate = ql_calc_date
        ql.Settings.instance().includeReferenceDateEvents = False
        ql.Settings.instance().enforceTodaysHistoricFixings = False

        # Build only if not already built
        if self._bond is None:
            self._bond = self._create_bond(None)  # << NO discount curve required here

    # ---- internal helpers ----
    def _resolve_discount_curve(self, with_yield: float | None) -> ql.YieldTermStructureHandle:
        """
        Priority:
          1) If with_yield provided -> build a flat curve off that yield.
          2) Otherwise, use subclass-provided default curve.
        """
        ql_calc_date = to_ql_date(self.valuation_date)

        if with_yield is not None:
            # Compounded Annual for YTM-style flat curves; day_count from instrument
            flat = ql.FlatForward(
                ql_calc_date, with_yield, self.day_count, ql.Compounded, ql.Annual
            )
            return ql.YieldTermStructureHandle(flat)

        default = self._get_default_discount_curve()
        if default is None:
            raise ValueError(
                "No discount curve available. Either pass with_yield=... to price(), "
                "or the instrument must supply a default discount curve."
            )
        return default

    def _setup_pricer(self, with_yield: float | None = None) -> None:
        if self.valuation_date is None:
            raise ValueError("Set valuation_date before pricing: set_valuation_date(dt).")

        ql_calc_date = to_ql_date(self.valuation_date)
        ql.Settings.instance().evaluationDate = ql_calc_date
        ql.Settings.instance().includeReferenceDateEvents = False
        ql.Settings.instance().enforceTodaysHistoricFixings = False

        # Build or rebuild only when needed
        if self._bond is None or self._with_yield != with_yield:
            discount_curve = self._resolve_discount_curve(with_yield)
            bond = self._create_bond(discount_curve)
            # Ensure engine is attached (safe even if subclass already set one)
            engine=ql.DiscountingBondEngine(discount_curve)
            bond.setPricingEngine(engine)
            self._bond = bond
            self._engine = engine
            self._with_yield = with_yield
            self._ensure_curve_observer(discount_curve)
            self._last_discount_curve_handle = discount_curve

    # ---- public API shared by all vanilla bonds ----
    def price(self, with_yield: float | None = None) -> float:
        if self.valuation_date is None:
            raise ValueError("Set valuation_date before pricing: set_valuation_date(dt).")

        inst_key = self._instrument_cache_key()
        price_key = self._price_context_key(with_yield)

        # Global cache hit (no engine/bond build)
        with _BOND_CACHE_LOCK:
            bucket = _BOND_PRICE_CACHE.get(inst_key)
            if bucket is not None and price_key in bucket:
                val = bucket[price_key]
                bucket.move_to_end(price_key)  # LRU promote
                return val

        # Miss -> build and compute
        self._setup_pricer(with_yield=with_yield)
        npv = float(self._bond.NPV())

        with _BOND_CACHE_LOCK:
            bucket = _BOND_PRICE_CACHE.setdefault(inst_key, OrderedDict())
            bucket[price_key] = npv
            bucket.move_to_end(price_key)
            while len(bucket) > BOND_CACHE_PER_INSTRUMENT_LIMIT:
                bucket.popitem(last=False)
        return npv

    # ---- public API: z-spread (currency dirty -> constant spread, cached) ----
    def z_spread(
            self,
            target_dirty_ccy: float,
            *,
            discount_curve: ql.YieldTermStructureHandle | ql.YieldTermStructure | None = None,
            use_quantlib: bool = True,
            tol: float = 1e-12,
            max_iter: int = 200,
    ) -> float:
        """
        Compute the constant z-spread (decimal, e.g., 0.005 = 50bp) so that the bond's
        DIRTY price in currency equals `target_dirty_ccy`, using the specified discount curve
        or (by default) the instrument's index/benchmark/default curve.
        Cached per instrument + curve context + target price, like price().
        """
        # Ensure we at least have the instrument built (no engine required)
        self._ensure_instrument()

        # Resolve the base curve handle (prefer explicit arg; else index; else benchmark; else default)
        if discount_curve is not None:
            h = (
                discount_curve
                if isinstance(discount_curve, ql.YieldTermStructureHandle)
                else ql.YieldTermStructureHandle(discount_curve)
            )
        else:
            if hasattr(self, "get_index_curve"):
                try:
                    h = getattr(self, "get_index_curve")()
                except Exception:
                    h = None
            else:
                h = None
            if h is None:
                if hasattr(self, "get_index_curve"):
                    h = self.get_index_curve()
                else:
                    h = self.get_benchmark_index_curve()
            if h is None:
                raise ValueError(
                    "No discount curve available for z-spread. "
                    "Pass `discount_curve=...`, implement get_index_curve(), "
                    "or set benchmark_rate_index_name."
                )

        # Build cache keys
        inst_key = self._instrument_cache_key()
        ctx_key = self._context_key_for_handle(h)
        tgt = self._normalize_currency(target_dirty_ccy)
        z_key = f"z|{ctx_key}|dirty:{tgt}|method:{'ql' if use_quantlib else 'cont'}"

        # Cache hit
        with _BOND_CACHE_LOCK:
            bucket = _BOND_ZSPREAD_CACHE.get(inst_key)
            if bucket is not None and z_key in bucket:
                val = bucket[z_key]
                bucket.move_to_end(z_key)
                return val

        # Compute
        if use_quantlib:
            try:
                z = self._z_spread_quantlib(target_dirty_ccy, h, tol=tol, max_iter=max_iter)
            except Exception:
                # robust fallback
                z = self._z_spread_continuous(target_dirty_ccy, h, tol=tol, max_iter=max_iter)
        else:
            z = self._z_spread_continuous(target_dirty_ccy, h, tol=tol, max_iter=max_iter)

        # Store
        with _BOND_CACHE_LOCK:
            bucket = _BOND_ZSPREAD_CACHE.setdefault(inst_key, OrderedDict())
            bucket[z_key] = z
            bucket.move_to_end(z_key)
            while len(bucket) > BOND_CACHE_PER_INSTRUMENT_LIMIT:
                bucket.popitem(last=False)
        return z

    # ---- internal: continuous-comp z-spread (robust, currency-based) ----
    def _z_spread_continuous(
            self,
            target_dirty_ccy: float,
            h: ql.YieldTermStructureHandle,
            *,
            tol: float,
            max_iter: int,
    ) -> float:
        dc = h.dayCounter()
        ref = h.referenceDate()
        settle = self._bond.settlementDate()

        # Future cashflows only, honoring includeReferenceDateEvents
        flows = []
        for cf in self._bond.cashflows():
            if cf.hasOccurred(settle):
                continue
            flows.append((cf.date(), float(cf.amount())))
        if not flows:
            raise ValueError("No future cashflows; z-spread is undefined.")

        base_df_settle = h.discount(settle)
        t_settle = dc.yearFraction(ref, settle)

        def dirty_with_z(s: float) -> float:
            z_settle = math.exp(-s * t_settle)
            pv = 0.0
            for d, amt in flows:
                t = dc.yearFraction(ref, d)
                df = h.discount(d) * math.exp(-s * t)
                pv += amt * (df / (base_df_settle * z_settle))
            return pv

        def f(s: float) -> float:
            return dirty_with_z(s) - float(target_dirty_ccy)

        # Bracket ±500bp, then expand if needed
        a, b = -0.05, 0.05
        fa, fb = f(a), f(b)
        expand = 0
        while fa * fb > 0.0 and expand < 24:
            a *= 2.0
            b *= 2.0
            fa, fb = f(a), f(b)
            expand += 1
        if fa * fb > 0.0:
            raise RuntimeError("z-spread: could not bracket the root. Check target price and curve.")

        guess = 0.5 * (a + b)
        step = 0.5 * (b - a)
        try:
            return float(ql.Brent().solve(lambda x: f(x), float(tol), float(guess), float(step)))
        except Exception:
            # Deterministic bisection fallback on the bracket
            lo, hi = a, b
            flo, fhi = fa, fb
            if flo > 0 and fhi < 0:
                lo, hi, flo, fhi = hi, lo, fhi, flo
            for _ in range(max_iter):
                mid = 0.5 * (lo + hi)
                fmid = f(mid)
                if abs(fmid) < tol or 0.5 * (hi - lo) < tol:
                    return float(mid)
                if fmid < 0.0:
                    lo, flo = mid, fmid
                else:
                    hi, fhi = mid, fhi
            return float(0.5 * (lo + hi))

    # ---- internal: QuantLib-based z-spread (expects clean price per 100) ----
    def _z_spread_quantlib(
            self,
            target_dirty_ccy: float,
            h: ql.YieldTermStructureHandle,
            *,
            tol: float,
            max_iter: int,
    ) -> float:
        # 1) currency dirty -> clean per 100
        scale = 100.0 / float(self.face_value)
        dirty_per100 = float(target_dirty_ccy) * scale
        accrued_per100 = float(self._bond.accruedAmount()) * scale
        clean_per100 = dirty_per100 - accrued_per100

        # 2) unwrap handle to the underlying YieldTermStructure (shared_ptr)
        yts = h.currentLink() if isinstance(h, ql.YieldTermStructureHandle) else h

        # 3) day counter & settlement
        dc = yts.dayCounter() if hasattr(yts, "dayCounter") else h.dayCounter()
        settle = self._bond.settlementDate()

        # 4) build BondPrice if available (QL >= 1.30), else pass float
        price_arg = ql.BondPrice(clean_per100, ql.BondPrice.Clean)

        # 5) correct order: (bond, price, curve, dayCounter, comp, freq, settle, tol, maxIter, guess)
        guess = 0.0
        return float(
            ql.BondFunctions.zSpread(
                self._bond,
                price_arg,
                yts,  # <-- CURVE HERE (3rd arg)
                dc,  # <-- DayCounter
                ql.Continuous,  # compounding convention for z-spread
                ql.NoFrequency,
                settle,
                float(tol),
                int(max_iter),
                float(guess),
            )
        )

    def analytics(self, with_yield: float | None = None) -> dict:
        self._setup_pricer(with_yield=with_yield)
        _ = self._bond.NPV()
        return {
            "clean_price": self._bond.cleanPrice(),
            "dirty_price": self._bond.dirtyPrice(),
            "accrued_amount": self._bond.accruedAmount(),
        }

    def get_cashflows(self) -> dict[str, list[dict[str, Any]]]:
        """
        Generic cashflow extractor.
        For fixed bonds, you'll see "fixed" + "redemption".
        For floaters, you'll see "floating" + "redemption".
        """
        self._setup_pricer()
        ql.Settings.instance().evaluationDate = to_ql_date(self.valuation_date)

        out: dict[str, list[dict[str, Any]]] = {"fixed": [], "floating": [], "redemption": []}

        for cf in self._bond.cashflows():
            if cf.hasOccurred():
                continue

            f_cpn = ql.as_floating_rate_coupon(cf)
            if f_cpn is not None:
                out["floating"].append(
                    {
                        "payment_date": to_py_date(f_cpn.date()),
                        "fixing_date": to_py_date(f_cpn.fixingDate()),
                        "rate": float(f_cpn.rate()),
                        "spread": float(f_cpn.spread()),
                        "amount": float(f_cpn.amount()),
                    }
                )
                continue

            x_cpn = ql.as_fixed_rate_coupon(cf)
            if x_cpn is not None:
                out["fixed"].append(
                    {
                        "payment_date": to_py_date(x_cpn.date()),
                        "rate": float(x_cpn.rate()),
                        "amount": float(x_cpn.amount()),
                    }
                )
                continue

            # Redemption/principal
            out["redemption"].append(
                {
                    "payment_date": to_py_date(cf.date()),
                    "amount": float(cf.amount()),
                }
            )

        # Trim empty legs to stay tidy
        return {k: v for k, v in out.items() if len(v) > 0}

    def get_cashflows_df(self):
        """Convenience dataframe with coupon + redemption aligned."""
        self._ensure_instrument()  # << build-only; no curve/yield needed

        import pandas as pd

        cfs = self.get_cashflows()
        legs = [k for k in ("fixed", "floating") if k in cfs]
        if not legs and "redemption" not in cfs:
            return pd.DataFrame()

        # build coupon df
        df_cpn = None
        for leg in legs:
            df_leg = (
                pd.DataFrame(cfs[leg])
                if len(cfs[leg])
                else pd.DataFrame(columns=["payment_date", "amount"])
            )
            if not df_leg.empty:
                df_leg = df_leg[["payment_date", "amount"]].set_index("payment_date")
            if df_cpn is None:
                df_cpn = df_leg
            else:
                # if both fixed and floating exist (exotics), sum them
                df_cpn = df_cpn.add(df_leg, fill_value=0.0)

        df_red = pd.DataFrame(cfs.get("redemption", []))
        if not df_red.empty:
            df_red = df_red.set_index("payment_date")[["amount"]]

        if df_cpn is None and df_red is None:
            return pd.DataFrame()

        if df_cpn is None:
            df_out = df_red.rename(columns={"amount": "net_cashflow"})
        elif df_red is None or df_red.empty:
            df_out = df_cpn.rename(columns={"amount": "net_cashflow"})
        else:
            idx = df_cpn.index.union(df_red.index)
            df_cpn = df_cpn.reindex(idx).fillna(0.0)
            df_red = df_red.reindex(idx).fillna(0.0)
            df_out = (df_cpn["amount"] + df_red["amount"]).to_frame("net_cashflow")

        return df_out

    def get_net_cashflows(self):
        """Shorthand Series of combined coupon + redemption."""
        df = self.get_cashflows_df()
        return df["net_cashflow"] if "net_cashflow" in df.columns else df.squeeze()

    def get_yield(self, override_clean_price: float | None = None) -> float:
        """
        Yield-to-maturity based on current clean price (or override), compounded annually.
        """
        self._setup_pricer()
        ql.Settings.instance().evaluationDate = to_ql_date(self.valuation_date)

        clean_price = (
            override_clean_price if override_clean_price is not None else self._bond.cleanPrice()
        )
        freq: ql.Frequency = self.coupon_frequency.frequency()
        settlement: ql.Date = self._bond.settlementDate()

        ytm = self._bond.bondYield(clean_price, self.day_count, ql.Compounded, freq, settlement)
        return float(ytm)

    def get_ql_bond(
        self, *, build_if_needed: bool = True, with_yield: float | None = None
    ) -> ql.Bond:
        """
        Safely access the underlying QuantLib bond.
        If you don't pass a yield and there is no default curve, we build without an engine.
        """
        if self.valuation_date is None:
            raise ValueError(
                "Set valuation_date before accessing the QuantLib bond (set_valuation_date(dt))."
            )

        if build_if_needed:
            # If caller gave a yield OR we have a default curve, do full pricing setup.
            if with_yield is not None or self._get_default_discount_curve() is not None:
                self._setup_pricer(
                    with_yield=with_yield if with_yield is not None else self._with_yield
                )
            else:
                # No curve, no yield -> build instrument only (good for fixed cashflows)
                self._ensure_instrument()

        if self._bond is None:
            raise RuntimeError(
                "Underlying QuantLib bond is not available. "
                "Call price()/analytics() first or use get_ql_bond(build_if_needed=True, "
                "with_yield=...) to build it."
            )
        return self._bond

    def duration(
        self,
        with_yield: float | None = None,
        *,
        duration_type = ql.Duration.Modified,
    ) -> float:
        """
        Return bond duration (default: Modified) and cache it using the same
        hashed context design as price()/z_spread().

        Notes
        -----
        - Zero-coupon bonds: for Modified duration we follow the provided guide and
          return time-to-maturity in year fractions (0 if matured).
        - Coupon-bearing bonds: we compute YTM from the current clean price and then
          call QuantLib's BondFunctions.duration(..).
        - Cache key includes the instrument hash and the pricing context (flat yield
          or default curve + version ticks + valuation date), plus the duration type.
        """

        if self.valuation_date is None:
            raise ValueError("Set valuation_date before computing duration: set_valuation_date(dt).")

        # ---------- build cache keys ----------
        inst_key = self._instrument_cache_key()

        # Build a context key. For zero-coupon we may not need a curve; avoid raising if none.
        try:
            ctx_key = self._price_context_key(with_yield)
        except Exception:
            # e.g. zero-coupon duration with no curve/yield doesn't need a curve context;
            # still include valuation date to remain stable across time.
            ctx_key = f"val:{self._val_ordinal()}"

        # Tag the duration type to avoid collisions if caller requests different types.
        if duration_type == ql.Duration.Modified:
            dtype_str = "Modified"
        elif duration_type == ql.Duration.Macaulay:
            dtype_str = "Macaulay"
        elif duration_type == ql.Duration.Simple:
            dtype_str = "Simple"
        elif duration_type == ql.Duration.Effective:
            dtype_str = "Effective"
        else:
            dtype_str = f"Type{int(duration_type)}"

        dur_key = f"dur|{ctx_key}|dtype:{dtype_str}"

        # ---------- cache hit ----------
        with _BOND_CACHE_LOCK:
            bucket = _BOND_DURATION_CACHE.get(inst_key)
            if bucket is not None and dur_key in bucket:
                val = bucket[dur_key]
                bucket.move_to_end(dur_key)  # LRU promote
                return val

        # ---------- compute ----------
        # Special-case ZeroCouponBond per your guide
        if isinstance(self, ZeroCouponBond) and duration_type == ql.Duration.Modified:
            # Build only the instrument (no engine needed)
            self._ensure_instrument()

            vd = self.valuation_date
            mty = self.maturity_date
            if mty <= vd:
                dur_val = 0.0
            else:
                dcc: ql.DayCounter = self.day_count
                dur_val = max(0.0, dcc.yearFraction(to_ql_date(vd), to_ql_date(mty)))

        else:
            # Coupon-bearing bonds (and any non-default type): use QL functions
            # Ensure pricer so cleanPrice/settlementDate/etc. are available
            self._setup_pricer(with_yield=with_yield)
            qb: ql.Bond = self._bond  # type: ignore[assignment]
            dcc: ql.DayCounter = self.day_count

            # Frequency: use instrument's coupon frequency if available; else NoFrequency
            try:
                freq: ql.Frequency = self.coupon_frequency.frequency()  # Fixed & Floating have this
            except Exception:
                freq = ql.NoFrequency

            comp = ql.Compounded
            bp = ql.BondPrice(float(qb.cleanPrice()), ql.BondPrice.Clean)
            settle = qb.settlementDate()
            ytm = qb.bondYield(bp, dcc, comp, freq, settle)

            try:
                dur_val = float(
                    ql.BondFunctions.duration(qb, ytm, dcc, comp, freq, duration_type)
                )
            except Exception as e:
                # Stay robust (consistent with your guide)
                print(e)
                dur_val = 0.0

        # ---------- store in cache ----------
        with _BOND_CACHE_LOCK:
            bucket = _BOND_DURATION_CACHE.setdefault(inst_key, OrderedDict())
            bucket[dur_key] = float(dur_val)
            bucket.move_to_end(dur_key)
            while len(bucket) > BOND_CACHE_PER_INSTRUMENT_LIMIT:
                bucket.popitem(last=False)

        return float(dur_val)

    def carry_roll_down(
            self,
            horizon: ql.Period | int | datetime.timedelta | datetime.date,
            *,
            clean: bool = False,
    ) -> dict[str, float]:
        """
        Compute carry + roll-down over a horizon using the already-built engine & curve.
        - Uses self.analytics(...) for today's clean/dirty/accrued (no rebuild if engine is set).
        - No relinks, no engine setup here. Raises if the bond wasn't priced first.

        Returns per-100 (except *_ccy which are currency):
          p0_dirty_per100, p0_clean_per100,
          p1_dirty_per100_unchanged_curve, p1_dirty_per100_const_yield,
          cr_dirty, carry_const_dirty, roll_down_dirty,
          coupons_between_ccy, cr_plus_coupons_dirty,
          and (if clean=True) clean-price counterparts and accrued at horizon.
        """
        # ---- Preconditions: must already be priced and linked to a curve/yield ----
        if self.valuation_date is None:
            raise ValueError("Set valuation_date before carry_roll_down().")
        if self._bond is None or self._engine is None or self._last_discount_curve_handle is None:
            raise RuntimeError("Price the bond first (price() or analytics()) before carry_roll_down().")

        qb: ql.Bond = self._bond
        h = self._last_discount_curve_handle
        scale = 100.0 / float(self.face_value)

        # --- Today's prices via your analytics() (uses existing engine; no rebuild if with_yield unchanged) ---
        an = self.analytics(with_yield=self._with_yield)
        p0_clean = float(an["clean_price"])
        p0_dirty = float(an["dirty_price"])
        a0_ccy = float(an["accrued_amount"])

        # Settlement today (uses current QL Settings already set when you priced)
        s0: ql.Date = qb.settlementDate()

        # Current YTM from today's clean (for constant-yield carry)
        dcc: ql.DayCounter = self.day_count
        try:
            freq: ql.Frequency = self.coupon_frequency.frequency()
        except Exception:
            freq = ql.NoFrequency
        comp = ql.Compounded
        ytm0 = qb.bondYield(ql.BondPrice(p0_clean, ql.BondPrice.Clean), dcc, comp, freq, s0)

        # ---- Horizon valuation date and settlement (no global Settings change) ----
        asof_qld = to_ql_date(self.valuation_date)
        if isinstance(horizon, ql.Period):
            vd1 = self.calendar.advance(asof_qld, horizon, self.business_day_convention)
        elif isinstance(horizon, int):
            vd1 = self.calendar.advance(asof_qld, ql.Period(int(horizon), ql.Days), self.business_day_convention)
        elif isinstance(horizon, datetime.timedelta):
            vd1 = to_ql_date(self.valuation_date + horizon)
        elif isinstance(horizon, datetime.date):
            vd1 = to_ql_date(horizon)
        else:
            raise ValueError("Unsupported horizon type. Use ql.Period | int(days) | timedelta | date.")

        s1 = self.calendar.advance(vd1, ql.Period(self.settlement_days, ql.Days), ql.Following)
        if s1 <= s0:
            raise ValueError("Horizon/settlement must be after today's settlement date.")

        # ---- Unchanged-curve forward dirty at horizon: sum DF(t0,T)*CF / DF(t0,S1) ----
        def _df(d: ql.Date) -> float:
            return float(h.discount(d))

        pv_after_s1 = 0.0
        coupons_between_ccy = 0.0
        for cf in qb.cashflows():
            d = cf.date()
            amt = float(cf.amount())
            if d > s1:
                pv_after_s1 += amt * _df(d)
            elif d > s0:  # cashflows received in (s0, s1]
                coupons_between_ccy += amt

        df_s1 = _df(s1)
        p1_dirty_curve = 0.0 if df_s1 == 0.0 else (pv_after_s1 / df_s1) * scale

        # ---- Constant-yield dirty at horizon (no engine; BondFunctions) ----
        try:
            p1_clean_const = float(ql.BondFunctions.cleanPrice(qb, ytm0, dcc, comp, freq, s1))
        except Exception:
            p1_clean_const = 0.0
        try:
            a1_ccy = float(ql.BondFunctions.accruedAmount(qb, s1))
        except Exception:
            a1_ccy = 0.0
        p1_dirty_const = p1_clean_const + a1_ccy * scale

        # ---- Returns (per 100) ----
        cr_dirty = p1_dirty_curve - p0_dirty  # carry + roll (ex-coupon)
        carry_const_dirty = p1_dirty_const - p0_dirty  # constant-yield carry (ex-coupon)
        roll_down_dirty = p1_dirty_curve - p1_dirty_const  # roll-down

        out: dict[str, float] = {
            "p0_dirty_per100": p0_dirty,
            "p0_clean_per100": p0_clean,
            "p1_dirty_per100_unchanged_curve": p1_dirty_curve,
            "p1_dirty_per100_const_yield": p1_dirty_const,
            "cr_dirty": cr_dirty,
            "carry_const_dirty": carry_const_dirty,
            "roll_down_dirty": roll_down_dirty,
            "coupons_between_ccy": coupons_between_ccy,
            "cr_plus_coupons_dirty": cr_dirty + coupons_between_ccy * scale,
        }

        if clean:
            p1_clean_curve = p1_dirty_curve - a1_ccy * scale
            out.update({
                "accrued0_ccy": a0_ccy,
                "accrued1_ccy": a1_ccy,
                "accrued1_per100": a1_ccy * scale,
                "p1_clean_per100_unchanged_curve": p1_clean_curve,
                "p1_clean_per100_const_yield": p1_clean_const,
                "cr_clean": p1_clean_curve - p0_clean,
                "carry_const_clean": p1_clean_const - p0_clean,
            })

        return out


class FixedRateBond(Bond):
    """Plain-vanilla fixed-rate bond following the shared Bond lifecycle."""
    coupon_frequency: QPeriod = Field(...)
    coupon_rate: float = Field(...)

    model_config = {"arbitrary_types_allowed": True}

    # Optional market curve if you want to discount off a curve instead of a flat yield
    _discount_curve: ql.YieldTermStructureHandle | None = PrivateAttr(default=None)

    def reset_curve(self, curve: ql.YieldTermStructureHandle) -> None:
        self._discount_curve = curve

    def _get_default_discount_curve(self) -> ql.YieldTermStructureHandle | None:
        return self._discount_curve

    def _build_schedule(self) -> ql.Schedule:
        if self.schedule is not None:
            return self.schedule
        return ql.Schedule(
            to_ql_date(self.issue_date),
            to_ql_date(self.maturity_date),
            self.coupon_frequency,
            self.calendar,
            self.business_day_convention,
            self.business_day_convention,
            ql.DateGeneration.Forward,
            False,
        )

    def _create_bond(self, discount_curve: ql.YieldTermStructureHandle | None) -> ql.Bond:
        ql.Settings.instance().evaluationDate = to_ql_date(self.valuation_date)
        sched = self._build_schedule()

        dates = list(sched.dates())
        asof = ql.Settings.instance().evaluationDate
        has_periods_left = len(dates) >= 2 and any(
            dates[i + 1] > asof for i in range(len(dates) - 1)
        )
        if not has_periods_left:
            maturity = dates[-1] if dates else to_ql_date(self.maturity_date)
            return ql.ZeroCouponBond(
                self.settlement_days,
                self.calendar,
                self.face_value,
                maturity,
                self.business_day_convention,
                100.0,
                to_ql_date(self.issue_date),
            )

        return ql.FixedRateBond(
            self.settlement_days, self.face_value, sched, [self.coupon_rate], self.day_count
        )


class ZeroCouponBond(Bond):
    redemption_pct: float = Field(default=100.0, description="Maturity payoff as % of face (100 = par).")
    model_config = {"arbitrary_types_allowed": True}
    _discount_curve: ql.YieldTermStructureHandle | None = PrivateAttr(default=None)

    def reset_curve(self, curve: ql.YieldTermStructureHandle) -> None:
        self._discount_curve = curve

    def _get_default_discount_curve(self) -> ql.YieldTermStructureHandle | None:
        return self._discount_curve

    def _build_schedule(self) -> ql.Schedule:
        # Optional: if your base class uses schedule for settlement logic
        return ql.Schedule(
            to_ql_date(self.issue_date),
            to_ql_date(self.maturity_date),
            ql.Period(ql.Once),
            self.calendar,
            self.business_day_convention,
            self.business_day_convention,
            ql.DateGeneration.Forward,
            False,
        )

    def _create_bond(self, discount_curve: ql.YieldTermStructureHandle | None) -> ql.Bond:
        ql.Settings.instance().evaluationDate = to_ql_date(self.valuation_date)
        return ql.ZeroCouponBond(
            self.settlement_days,
            self.calendar,
            self.face_value,
            to_ql_date(self.maturity_date),
            self.business_day_convention,
            self.redemption_pct,
            to_ql_date(self.issue_date),
        )

class FloatingRateBond(Bond):
    """Floating-rate bond with specified floating rate index (backward compatible)."""
    coupon_frequency: QPeriod = Field(...)
    floating_rate_index_name: str = Field(...)
    spread: float = Field(default=0.0)
    # All other fields (issue_date, maturity_date, coupon_frequency, day_count, calendar, etc.)
    # are inherited from Bond

    model_config = {"arbitrary_types_allowed": True}

    _bond: ql.FloatingRateBond | None = PrivateAttr(default=None)
    _index: ql.IborIndex | None = PrivateAttr(default=None)
    _index_observer: ql.Observer | None = PrivateAttr(default=None)

    # ---------- lifecycle ----------
    def _ensure_index(self) -> None:
        if self._index is not None:
            return
        if self.valuation_date is None:
            raise ValueError("Set valuation_date before pricing: set_valuation_date(dt).")
        self._index = self._get_index_by_name(self.floating_rate_index_name, hydrate_fixings=True)
        # Observe index so added fixings bump version tick
        self._register_index_observer()

    def _register_index_observer(self) -> None:
        if self._index is None:
            return

        def _bump():
            try:
                key = id(self._index) if self._index is not None else None
                if key is not None:
                    _INDEX_VERSION[key] = _INDEX_VERSION.get(key, 0) + 1
            except Exception:
                pass

        # If an observer existed, unhook and rebuild so the callback is tied to the current index
        if self._index_observer is not None:
            try:
                self._index_observer.unregisterWith(self._index)
            except Exception:
                pass
            self._index_observer = None

        self._index_observer = ql.Observer(_bump)
        self._index_observer.registerWith(self._index)

    def _on_valuation_date_set(self) -> None:
        super()._on_valuation_date_set()
        # unhook index observer if any
        if getattr(self, "_index_observer", None) and getattr(self, "_index", None) is not None:
            try:
                self._index_observer.unregisterWith(self._index)
            except Exception:
                pass
        self._index_observer = None
        self._index = None

    def reset_curve(self, curve: ql.YieldTermStructureHandle) -> None:
        """Optional: re-link a custom curve to this index and rebuild."""
        if self.valuation_date is None:
            raise ValueError("Set valuation_date before reset_curve().")

        self._index = self._get_index_by_name(
                        self.floating_rate_index_name,
                        forwarding_curve = curve,
                    hydrate_fixings = True,
                )

        private = ql.RelinkableYieldTermStructureHandle()
        link = curve.currentLink() if hasattr(curve, "currentLink") else curve
        private.linkTo(link)
        self._index = self._index.clone(private)
        self._register_index_observer()
        # Force rebuild on next price()
        self._bond = None
        self._with_yield = None

    def _fixings_version(self) -> int:
        """Include index fixings tick in the cache key for floaters."""
        return _INDEX_VERSION.get(id(self._index), 0) if self._index is not None else 0
    # ---- Bond hooks ----
    def _get_default_discount_curve(self) -> ql.YieldTermStructureHandle | None:
        self._ensure_index()
        # Forecasting and (by default) discounting off the index curve for compatibility
        return self._index.forwardingTermStructure()

    def _create_bond(self, discount_curve: ql.YieldTermStructureHandle | None) -> ql.Bond:
        self._ensure_index()
        ql_calc_date = to_ql_date(self.valuation_date)
        forecasting = self._index.forwardingTermStructure()

        return create_floating_rate_bond_with_curve(
            calculation_date=ql_calc_date,
            face=self.face_value,
            issue_date=to_ql_date(self.issue_date),
            maturity_date=to_ql_date(self.maturity_date),
            floating_rate_index=self._index,
            spread=self.spread,
            coupon_frequency=self.coupon_frequency,
            day_count=self.day_count,
            calendar=self.calendar,
            business_day_convention=self.business_day_convention,
            settlement_days=self.settlement_days,
            curve=forecasting,
            discount_curve=discount_curve,  # may be None (OK)
            seed_past_fixings_from_curve=True,
            schedule=self.schedule,
        )

    # ---------- public API (kept for backward compatibility) ----------
    def get_index_curve(self):
        self._ensure_index()
        return self._index.forwardingTermStructure()

    # price(with_yield) and analytics(with_yield) are inherited from Bond and remain compatible

    def get_cashflows(self) -> dict[str, list[dict[str, Any]]]:
        """
        Keep the original floater-specific structure (floating + redemption).
        """
        self._setup_pricer()
        ql.Settings.instance().evaluationDate = to_ql_date(self.valuation_date)

        out: dict[str, list[dict[str, Any]]] = {"floating": [], "redemption": []}

        for cf in self._bond.cashflows():
            if cf.hasOccurred():
                continue

            cpn = ql.as_floating_rate_coupon(cf)
            if cpn is not None:
                out["floating"].append(
                    {
                        "payment_date": to_py_date(cpn.date()),
                        "fixing_date": to_py_date(cpn.fixingDate()),
                        "rate": float(cpn.rate()),
                        "spread": float(cpn.spread()),
                        "amount": float(cpn.amount()),
                    }
                )
            else:
                out["redemption"].append(
                    {
                        "payment_date": to_py_date(cf.date()),
                        "amount": float(cf.amount()),
                    }
                )

        return out
