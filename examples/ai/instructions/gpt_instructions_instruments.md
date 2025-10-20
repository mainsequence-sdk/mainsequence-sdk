# `mainsequence.instruments` — Instructions & Reference

> **Import pattern**
>
> ```python
> import mainsequence.instruments as msi
> # Optional: import specific classes so they are registered for rebuild()
> from mainsequence.instruments.bond import FixedRateBond, FloatingRateBond
> from mainsequence.instruments.interest_rate_swap import InterestRateSwap
> from mainsequence.instruments.vanilla_fx_option import VanillaFXOption
> from mainsequence.instruments.knockout_fx_option import KnockOutFXOption
> from mainsequence.instruments.european_option import EuropeanOption
> ```
>
> Classes auto‑register at import time. Importing the modules above ensures
> `msi.Instrument.rebuild(...)` can locate the types.

---

## 0) What this submodule provides

- **Instrument models (Pydantic)** wrapping QuantLib with a clean `.price()` API and JSON helpers.
- **Positions API** to aggregate instruments, PVs, cashflows, and greeks.
- **Index & curve factory** keyed by **client‑side UIDs** (no hardcoded IDs in code).
- **Data interface** that can read from your platform tables or from a local/mock backend.

Runtime requirements: Python 3.10+, QuantLib 1.29+.

---

## 1) Available instruments (exhaustive)

All models inherit from `msi.Instrument` (Pydantic + JSON mixin). Dates are Python
`date`/`datetime`. QuantLib types are handled by custom serializers.

### 1.1 Bonds

#### `FixedRateBond`  *(in `mainsequence.instruments.bond`)*
**Fields**
- `face_value: float`
- `issue_date: date`
- `maturity_date: date`
- `coupon_rate: float`
- `coupon_frequency: ql.Period`  (e.g., `"6M"`)
- `day_count: ql.DayCounter`  (e.g., `"Thirty360_USA"`, `"Actual365Fixed"`)
- `calendar: ql.Calendar`  (serialized as `{"name": "<Calendar::name()>"}`)
- `business_day_convention: ql.BusinessDayConvention`  (e.g., `"Following"`)
- `settlement_days: int = 2`
- `schedule: ql.Schedule | None`
- `benchmark_rate_index_name: str | None`  *(optional tag for mapping/analytics)*

**API**
- `.set_valuation_date(dt)`
- `.price(with_yield: float | None = None) -> float`
  - If `with_yield` is given, builds a flat curve at that YTM.
  - Else, uses any curve provided via `.reset_curve(handle)`.
- `.analytics(with_yield: float | None = None) -> dict` → `clean_price`, `dirty_price`, `accrued_amount`
- `.get_cashflows() -> dict` → `{"fixed":[...], "redemption":[...]}` (future only)
- `.get_yield(override_clean_price: float | None = None) -> float`
- `.get_ql_bond(build_if_needed=True, with_yield: float | None = None) -> ql.Bond`

**Notes**
- If the provided `schedule` has no remaining periods as of valuation date, the bond auto‑degrades to
  a `ZeroCouponBond` (redemption‑only) for robust pricing.

---

#### `FloatingRateBond`  *(in `mainsequence.instruments.bond`)*
**Fields (extends Bond)**
- `floating_rate_index_name: str`  **(index UID; see §3 & §4)**
- `spread: float = 0.0`
- (inherits `issue_date`, `maturity_date`, `coupon_frequency`, `day_count`, `calendar`, etc.)

**API**
- `.set_valuation_date(dt)`
- `.price(with_yield: float | None = None) -> float`
- `.reset_curve(curve_handle)` — relinks a custom forwarding/discount curve for the index.
- `.get_cashflows() -> dict` → `{"floating":[...], "redemption":[...]}` with fixing dates, rate, spread, amounts.
- `.get_index_curve()` — returns the linked forwarding term structure.

**Notes**
- By default, both **forecasting and discounting** use the index curve returned by the index factory.
- Past fixings are hydrated automatically (see §3.3).

---

### 1.2 Interest‑rate swap

#### `InterestRateSwap`  *(in `mainsequence.instruments.interest_rate_swap`)*
**Fields**
- Core: `notional: float`, `start_date: date`, `maturity_date: date`, `fixed_rate: float`
- Fixed leg: `fixed_leg_tenor: Period`, `fixed_leg_convention: BDC`, `fixed_leg_daycount: DayCounter`
- Float leg: `float_leg_tenor: Period`, `float_leg_spread: float`, `float_leg_index_name: str` **(UID)**
- Alternative: `tenor: ql.Period | None` → if set, maturity = spot‑start + tenor (T+1 on index calendar)

**API**
- `.set_valuation_date(dt)`
- `.price() -> float`
- `.get_cashflows() -> dict` → `{"fixed":[...], "floating":[...]}` (future only)
- `.get_net_cashflows() -> pd.Series` (payment‑date index)
- `.reset_curve(curve)` — rebuilds on a different forwarding/discount curve while keeping conventions.

**Factory**
- `InterestRateSwap.from_tiie(notional, start_date, fixed_rate, float_leg_spread=0.0, tenor=None, maturity_date=None)`  
  Builds a **TIIE(28D)** IRS with standard conventions (both legs 28D, ACT/360, ModifiedFollowing).

**Notes**
- Fixings ≤ valuation date are **backfilled** from storage; same‑day coupons are **seeded** from the curve
  when necessary so pricing is stable at T. See §3.3.

---

### 1.3 Options

#### `EuropeanOption` (equity, Black–Scholes–Merton)
**Fields**: `underlying: str`, `strike: float`, `maturity: date`, `option_type: "call"|"put"`  
**API**: `.price()`, `.get_greeks()`  
**Data**: pulls spot/vol/rate/dividend from the **equities_daily** data interface (mock or platform).

#### `VanillaFXOption` (Garman–Kohlhagen)
**Fields**: `currency_pair: str` (e.g., `"EURUSD"`), `strike`, `maturity`, `option_type`, `notional`  
**API**: `.price()` (NPV × notional), `.get_greeks()`, `.get_market_info()`

#### `KnockOutFXOption` (barrier; analytic → MC fallback)
**Fields**: `currency_pair`, `strike`, `barrier`, `barrier_type: "up_and_out"|"down_and_out"`, `maturity`, `option_type`, `notional`, `rebate=0.0`  
**API**: `.price()` (NPV × notional), `.get_greeks()`, `.get_barrier_info()`  
**Notes**: Validates barrier vs spot; uses analytic engines when available, else MC engine.

---

## 2) JSON serialization & rebuild

Every instrument inherits a JSON mixin:

- `obj.to_json_dict()` → Python dict (QuantLib fields serialized to compact tokens)
- `obj.to_json()` → canonical JSON string (sorted keys; stable for hashing)
- `obj.content_hash()` / `msi.Instrument.hash_payload(payload)` → stable content hash
- `msi.Instrument.rebuild(data)` — rebuilds from `{"instrument_type": "...", "instrument": {...}}` or a JSON string.

### 2.1 Field encoders (what JSON looks like)
- **Period** → `"28D"`, `"3M"`, `"6M"`, `"2Y"` …
- **DayCounter** → `"Actual360"`, `"Actual365Fixed"`, `"Thirty360_USA"` …
- **BusinessDayConvention** → `"Following"`, `"ModifiedFollowing"`, `"Unadjusted"` …
- **Calendar** → `{"name": "<Calendar::name()>"}` using the QuantLib **display name** (e.g., `"TARGET"`, `"Mexican stock exchange"`). A factory resolves it back.
- **Schedule** → explicit `{"dates":[...], ...}` (with true calendar name embedded).
- **IborIndex (when serialized ad‑hoc)** → `{"family":"USDLibor","tenor":"3M"}`; TIIE indices are handled by the central factory (see §3).

> **Tip:** Ensure you import the modules that define the instrument classes before calling `Instrument.rebuild(...)`
> so the runtime registry already contains the types.

### 2.2 Position serialization
`Position.to_json_dict()` returns:
```json
{
  "lines": [
    {"instrument_type":"FixedRateBond","instrument":{...},"units":5.0,"extra_market_info":null},
    {"instrument_type":"InterestRateSwap","instrument":{...},"units":-2.0}
  ]
}
```
…and `Position.from_json_dict(...)` rebuilds each instrument via the same registry.

---

## 3) Indices & curves — registration and resolution

This package **never hardcodes** production IDs. Instead, it asks the client to provide **Constants** (names → values) once, and then uses those values everywhere.

### 3.1 How an index is built
- Call `get_index(index_identifier, target_date, forwarding_curve=None, hydrate_fixings=True)`.
- We look up `INDEX_CONFIGS[index_identifier]` to get:
  - `curve_uid` (which discount curve to use),
  - `calendar`, `day_counter`, `period`, `settlement_days`, `bdc`, `end_of_month`.
- We construct a `ql.IborIndex` **whose QuantLib name is exactly your `index_identifier`** (the stable UID).
- If `forwarding_curve` is not provided, we call `build_zero_curve(target_date, index_identifier)` (see below).

### 3.2 How the zero curve is built
- Using `curve_uid` from the index config, we fetch curve **nodes** for the valuation date:
  `[{ "days_to_maturity": <int>, "zero": <decimal> }, ...]`.
- We convert (day, zero) to discount factors assuming **simple** accrual on the configured day counter (e.g., ACT/360 for VALMER TIIE zeros):
  `df = 1 / (1 + zero * T)` with `T = DC.yearFraction(asof, asof + days)`.
- We build a `ql.DiscountCurve` (extrapolation enabled) and return a `ql.YieldTermStructureHandle`.

### 3.3 How historical fixings are hydrated
- `add_historical_fixings(target_date, ibor_index)` loads fixings by **UID** (taken from `index.familyName()` / the name we set).
- We filter to valid fixing dates on the index calendar and to strictly `< target_date`, then `index.addFixings(...)` in bulk.
- For coupons with fixing dates on/before valuation that still lack a stored fixing, we **seed** a forward from the same curve.

### 3.4 What you must register (Constants → your UIDs)
Provide values for these **symbolic names** in your Constants store (once):

**Reference rate identifiers (index UIDs)**
- `REFERENCE_RATE__TIIE_28`
- `REFERENCE_RATE__TIIE_91`
- `REFERENCE_RATE__TIIE_182`
- `REFERENCE_RATE__TIIE_OVERNIGHT`
- `REFERENCE_RATE__CETE_28`
- `REFERENCE_RATE__CETE_91`
- `REFERENCE_RATE__CETE_182`
- `REFERENCE_RATE__USD_SOFR`

**Curve identifiers (curve UIDs used by the indices above)**
- `ZERO_CURVE__VALMER_TIIE_28`
- `ZERO_CURVE__BANXICO_M_BONOS_OTR`
- `ZERO_CURVE__UST_CMT_ZERO_CURVE_UID`  *(example mapping for SOFR/UST)*

Keep these **values stable** over time. Instruments and positions rebuilt later will still resolve to the same index/curve.

---

## 4) Production storage — tables you must set **client‑side**

When `MSI_DATA_BACKEND=mainsequence` (default), curves and fixings are read from the tables you select in the platform’s **Instruments → Config** page.

You **must** set both:
- **Discount curves storage node** → table that stores your discount curves (serialized day→zero map per curve UID and date).
- **Reference rates fixings storage node** → table with index fixings by reference rate UID.

If these are missing, explicit runtime errors are raised (with a link to the configuration page).

**Optional environment switches**
- `MSI_DATA_BACKEND=mock` to use the bundled mock readers (no platform calls).
- For the mock **TIIE zero** curve CSV, set:  
  `export TIIE_ZERO_CSV=/absolute/path/to/MEXDERSWAP_IRSTIIEPR.csv`

**Defaults file (for reference)**  
`instruments.default.toml` shows friendly default names like:  
`DISCOUNT_CURVES_TABLE="discount_curves"`, `REFERENCE_RATES_FIXING_TABLE="fixing_rates_1d"` — but the **actual** production reads are governed by the table IDs you set in the configuration screen above.

---

## 5) Positions API

### 5.1 Core types
- `PositionLine(instrument: Instrument, units: float, extra_market_info: dict | None = None)`
- `Position(lines: list[PositionLine], position_date: datetime | None = None)`

### 5.2 Aggregations
- `.price()` → Σ units × instrument.price()
- `.price_breakdown()` → list of `{instrument, units, unit_price, market_value}`
- `.get_cashflows(aggregate=False)` → merges each instrument’s `.get_cashflows()`; scales amounts by units.
- `.get_greeks()` → sums keys across instruments that expose `.get_greeks()`.
- `.agg_net_cashflows()` → DataFrame of combined (coupon + redemption) by date.
- `.position_total_npv()` and `.position_carry_to_cutoff(valuation_date, cutoff)`.
- Helpers: `npv_table(npv_base, npv_bumped, units)`, `portfolio_stats(position, bumped_position, valuation_date, cutoff)`.

### 5.3 JSON I/O
- `Position.to_json_dict()` / `Position.from_json_dict(...)` for round‑trip persistence.

---

## 6) Quick examples

### 6.1 TIIE‑28D IRS (2Y) priced off your configured curve
```python
import datetime as dt, QuantLib as ql
import mainsequence.instruments as msi
from mainsequence.instruments.interest_rate_swap import InterestRateSwap

val = dt.date.today()
ql.Settings.instance().evaluationDate = ql.Date(val.day, val.month, val.year)

swap = InterestRateSwap.from_tiie(
    notional=100_000_000,
    start_date=val,
    fixed_rate=0.095,
    float_leg_spread=0.0000,
    tenor=ql.Period("2Y"),
)
swap.set_valuation_date(val)
print("PV:", swap.price())
print("Cashflows:", swap.get_cashflows())
```

### 6.2 Serialize → rebuild → price
```python
from mainsequence.instruments.bond import FixedRateBond
import json, datetime as dt, QuantLib as ql

val = dt.date.today()
b = FixedRateBond(
    face_value=1_000_000,
    issue_date=val,
    maturity_date=val.replace(year=val.year+5),
    coupon_rate=0.05,
    coupon_frequency=ql.Period("6M"),
    day_count=ql.Thirty360(ql.Thirty360.USA),
    calendar=ql.TARGET(),
)
b.set_valuation_date(val)
payload = {"instrument_type": type(b).__name__, "instrument": b.to_json_dict()}

rebuilt = msi.Instrument.rebuild(payload)  # class auto‑discovered if module imported
rebuilt.set_valuation_date(val)
print(rebuilt.price(with_yield=0.051))
```

### 6.3 Position aggregation
```python
from mainsequence.instruments import Position, PositionLine
from mainsequence.instruments.vanilla_fx_option import VanillaFXOption
import datetime as dt

val = dt.date.today()
opt = VanillaFXOption(currency_pair="EURUSD", strike=1.10,
                      maturity=val.replace(year=val.year+1),
                      option_type="call", notional=1_000_000)
opt.set_valuation_date(val)

pos = Position(lines=[PositionLine(instrument=opt, units=-3.0)])
print(pos.price())
print(pos.price_breakdown())
print(pos.get_cashflows(aggregate=True))
```

---

## 7) Utilities & settings

- `to_ql_date(py_date: date) -> ql.Date`  
- `to_py_date(qld: ql.Date) -> datetime.datetime` (UTC)

Environment:
- `MSI_DATA_BACKEND` (default: `mainsequence`; set to `mock` for local demos).
- `TIIE_ZERO_CSV` for the mock TIIE zeros file (when using mock backend).

---

## 8) Troubleshooting & tips

- **“Unknown instrument type” on rebuild** → Import the module that defines the class so the registry contains it.
- **Fixings not found** → Ensure the *Reference rates fixings storage node* is set in Instruments → Config and the index UID matches your Constant.
- **Wrong calendar applied** → The calendar serializer uses QuantLib’s `Calendar::name()`. Ensure your wheel exposes the required calendar classes (falls back to TARGET when missing).
- **Pricing at T** → Same‑day coupons on floaters/swaps may need seeded forwards; this module already does it for you when fixings are missing on/before valuation.

---

*Happy pricing with `mainsequence.instruments`!*
