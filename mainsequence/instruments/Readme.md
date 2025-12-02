<p align="center">
  <img src="logo.png" alt="QuantLibDev logo" width="160" />
</p>

# Main Sequence Instrument Pricer — Pricing building blocks & examples (Python / QuantLib)

This repository contains a small but complete stack for derivatives and fixed‑income analytics in **Python** on top of **QuantLib**:

* **Data layer**: mock market data (`APIDataNode`) with realistic shapes (deposits, swaps, zero curves, FX).
* **Pricing models**: curve builders, Black–Scholes(-Merton), FX GK, barrier MC/analytic, bond & swap pricers.
* **Instruments** (Pydantic wrappers): fixed/float bonds, vanilla/KO FX options, vanilla interest‑rate swaps.
* **Utilities**: date conversions, plotting helpers, cash‑flow inspection.

It is designed to be **extensible** (drop‑in new instruments/models) and **inspectable** (print/plot every intermediate object: schedules, DFs, forwards, cash‑flows).

> **Note on data**: All market data here are mocked for repeatable demos. Wire your own sources by extending `APIDataNode`.

---

## 1) Architecture & flow

```
src/
├─ data_interface/
│  └─ data_interface.py      # APIDataNode: mock quotes & fixings  or extract from main sequence platform(equity, FX, swaps, TIIE zeros)
├─ pricing_models/
│  ├─ swap_pricer.py         # curve builders (USD IRS bootstrap, TIIE ZeroCurve), indices, helpers
│  ├─ bond_pricer.py         # discount curve from bonds, fixed-rate bond construction & plotting
│  ├─ black_scholes.py       # BSM process (equities)
│  ├─ fx_option_pricer.py    # Garman–Kohlhagen process (FX)
│  └─ knockout_fx_pricer.py  # Barrier (analytic/MC) engine setup
├─ instruments/
│  ├─ fixed_rate_bond.py     # FixedRateBond (Pydantic wrapper)
│  ├─ interest_rate_swap.py  # Vanilla IRS + TIIE swap wrapper
│  ├─ european_option.py     # Equity European option (BSM)
│  ├─ vanilla_fx_option.py   # FX European option (GK)
│  └─ knockout_fx_option.py  # FX knock‑out barrier option
└─ utils.py                  # date conversions, tiny helpers
```

**Typical flow:**

1. **Fetch market data** via `APIDataNode.get_historical_data(...)` or `get_historical_fixings(...)`.
2. **Build a curve / process** (e.g., `build_yield_curve`, `build_tiie_zero_curve_from_valmer`, `create_bsm_model`, `create_fx_garman_kohlhagen_model`).
3. **Construct instrument** (bond/swap/option). Specify calendars, day counts, schedules.
4. **Attach pricing engine** (discounting engine, analytic, MC, etc.).
5. **Inspect**: NPV, clean/dirty price, Greeks, legs/cash‑flows, zero rates/DFs.

### Curves provided

* **USD IRS bootstrap** (Deposits + Swaps) → `PiecewiseLogCubicDiscount` (`pricing_models/swap_pricer.py::build_yield_curve`).
  Uses `DepositRateHelper` for 3M/6M and `SwapRateHelper` for 1Y…10Y.
* **MXN TIIE "new methodology"** → VALMER zeros to `ZeroCurve` (`build_tiie_zero_curve_from_valmer`).
* **Bond discount curve** from ZCB + coupon bonds (`pricing_models/bond_pricer.py::build_discount_curve_from_bonds`).

### Indices & fixings

* Load past fixings to avoid missing‑fixing errors: `add_historical_fixings(calc_date, index)`.
* TIIE calendars are handled (falls back to TARGET if MXN calendar class is unavailable in your wheel).

---

## 2) How to extend

### A. New market data source

1. Add a new branch in `APIDataNode.get_historical_data(...)` (e.g., ` "ois_quotes": ...`).
2. Return a minimal, typed structure that your pricer expects (lists of nodes/quotes).
3. For fixings, implement `get_historical_fixings(index_name, start, end)` with the right calendar filter.

### B. New curve builder

* Create a function under `pricing_models/` that:

  * Reads your quotes from `APIDataNode`.
  * Builds the right **rate helpers** (e.g., `DepositRateHelper`, `OISRateHelper`, `FuturesRateHelper`, `SwapRateHelper`).
  * Returns a `ql.YieldTermStructureHandle` (enable extrapolation if needed).

**Tip:** Use a temporary flat handle for helper indices (as in `build_yield_curve`).

### C. New instrument

* Add a Pydantic model under `instruments/` wrapping a QuantLib instrument:

  * Inputs: notional/strike/dates/tenors/day‑counts, etc.
  * In `_setup_pricer()`: set `evaluationDate`, build the model/process & index, create the instrument, attach engine.
  * Provide `price()` and, if useful, `.analytics()`/`.get_greeks()`.

### D. New pricing model / engine

* Drop helpers into `pricing_models/`. Example patterns:

  * **Equity/FX**: return a `BlackScholesMertonProcess` (spot, dividend/foreign curve, risk‑free, vol).
  * **Barriers/Exotics**: pick an analytic engine if supported; else MC engine with step/sample controls.

### E. Testing & debugging

* Always print **evaluation date**, calendar, day count.
* Log **schedule** boundaries and business‑day adjustments.
* For floaters, confirm **fixing dates** and that **historic fixings** are loaded.
* Inspect curve by sampling zeros/DFs and plotting.

---

## 3) Examples

> All snippets are **complete and runnable**; they use only what’s already in `src/`. Replace mocked data with your feeds by extending `APIDataNode`.

### 3.1 Price a fixed bond and a TIIE(28D)+spread floater

```python
import datetime as dt
import QuantLib as ql
from src.pricing_models.swap_pricer import build_tiie_zero_curve_from_valmer, make_tiie_28d_index, add_historical_fixings

calc_date = ql.Date.todaysDate()
ql.Settings.instance().evaluationDate = calc_date

# Build MXN TIIE curve (VALMER zeros → ZeroCurve)
curve = build_tiie_zero_curve_from_valmer(calc_date)

# Fixed 2Y, 28D coupons, ACT/360
cal = ql.Mexico() if hasattr(ql, "Mexico") else ql.TARGET()
start = calc_date
end   = cal.advance(start, ql.Period("2Y"))
sched = ql.Schedule(start, end, ql.Period("28D"), cal, ql.ModifiedFollowing, ql.ModifiedFollowing, ql.DateGeneration.Forward, False)
fixed = ql.FixedRateBond(1, 100_000_000, sched, [0.095], ql.Actual360())
fixed.setPricingEngine(ql.DiscountingBondEngine(curve))

# Floater TIIE-28D + 50bp, start at spot, load fixings
idx = make_tiie_28d_index(curve)
add_historical_fixings(calc_date, idx)
spot = idx.valueDate(calc_date)
fs   = ql.Schedule(spot, end, ql.Period("28D"), idx.fixingCalendar(), ql.ModifiedFollowing, ql.ModifiedFollowing, ql.DateGeneration.Forward, False)
floater = ql.FloatingRateBond(1, 100_000_000, fs, idx, ql.Actual360(), ql.ModifiedFollowing, 1, [1.0], [0.0050], [], [], False, 100.0, spot)
floater.setPricingEngine(ql.DiscountingBondEngine(curve))

print("Fixed NPV:", fixed.NPV())
print("Float NPV:", floater.NPV())
```

### 3.2 Bump **one quoted node** and **re-bootstrap** the USD IRS curve, then reprice

```python
import QuantLib as ql
from src.data_interface import APIDataNode

calc_date = ql.Date.todaysDate()
ql.Settings.instance().evaluationDate = calc_date

# 1) Load the quoted curve (deposits + swaps)
base = APIDataNode.get_historical_data("interest_rate_swaps", {"USD_rates": {}})["curve_nodes"]

# 2) Apply a node bump: +100bp at 5Y
bumped = []
for n in base:
    n = dict(n)
    if n["type"].lower() == "swap" and n["tenor"].upper() == "5Y":
        n["rate"] = float(n["rate"]) + 100/10_000.0
    bumped.append(n)

# 3) Build bootstrapped curves (helpers: deposits + swaps)
cal = ql.TARGET(); dc = ql.Actual365Fixed()

 def build(nodes):
    tmp = ql.YieldTermStructureHandle(ql.FlatForward(calc_date, 0.02, dc))
    ibor = ql.USDLibor(ql.Period("3M"), tmp)
    rh = []
    for nd in nodes:
        q = ql.QuoteHandle(ql.SimpleQuote(float(nd["rate"])))
        if nd["type"].lower()=="deposit":
            rh.append(ql.DepositRateHelper(q, ql.Period(nd["tenor"]), 2, cal, ql.ModifiedFollowing, False, dc))
        else:
            rh.append(ql.SwapRateHelper(q, ql.Period(nd["tenor"]), cal, ql.Annual, ql.Unadjusted, ql.Thirty360(ql.Thirty360.USA), ibor))
    curve = ql.PiecewiseLogCubicDiscount(calc_date, rh, dc); curve.enableExtrapolation(); return ql.YieldTermStructureHandle(curve)

usd0 = build(base)
usd1 = build(bumped)

# 4) Rebuild instruments on the bumped curve
from src.pricing_models.swap_pricer import add_historical_fixings

# Fixed 7Y
sched = ql.Schedule(calc_date, cal.advance(calc_date, ql.Period("7Y")), ql.Period("6M"), cal, ql.ModifiedFollowing, ql.ModifiedFollowing, ql.DateGeneration.Forward, False)
fix0 = ql.FixedRateBond(2, 100_000_000, sched, [0.045], ql.Thirty360(ql.Thirty360.USA)); fix0.setPricingEngine(ql.DiscountingBondEngine(usd0))
fix1 = ql.FixedRateBond(2, 100_000_000, sched, [0.045], ql.Thirty360(ql.Thirty360.USA)); fix1.setPricingEngine(ql.DiscountingBondEngine(usd1))

# Floater 7Y (USDLibor 3M)
idx0 = ql.USDLibor(ql.Period("3M"), usd0); add_historical_fixings(calc_date, idx0)
idx1 = ql.USDLibor(ql.Period("3M"), usd1); add_historical_fixings(calc_date, idx1)
fs   = ql.Schedule(calc_date, cal.advance(calc_date, ql.Period("7Y")), ql.Period("3M"), cal, ql.ModifiedFollowing, ql.ModifiedFollowing, ql.DateGeneration.Forward, False)
flt0 = ql.FloatingRateBond(2, 100_000_000, fs, idx0, ql.Actual360(), ql.ModifiedFollowing, 1, [1.0], [0.0], [], [], False, 100.0, calc_date); flt0.setPricingEngine(ql.DiscountingBondEngine(usd0))
flt1 = ql.FloatingRateBond(2, 100_000_000, fs, idx1, ql.Actual360(), ql.ModifiedFollowing, 1, [1.0], [0.0], [], [], False, 100.0, calc_date); flt1.setPricingEngine(ql.DiscountingBondEngine(usd1))

print("Fixed ΔPV (5Y +100bp):", fix1.NPV() - fix0.NPV())
print("Float ΔPV (5Y +100bp):", flt1.NPV() - flt0.NPV())
```

### 3.3 Plot zero curves (built from swaps) — built‑in helper

```python
import datetime as dt
import QuantLib as ql
from src.pricing_models.swap_pricer import plot_swap_zero_curve

calc_date = dt.date.today()
years, zeros = plot_swap_zero_curve(calc_date, max_years=12, step_months=6, show=True)
```

### 3.4 FX option (Garman–Kohlhagen)

```python
import datetime as dt
from src.instruments.vanilla_fx_option import VanillaFXOption

opt = VanillaFXOption(
    currency_pair = "EURUSD",
    strike        = 1.10,
    maturity      = dt.date.today().replace(year=dt.date.today().year + 1),
    option_type   = "call",
    notional      = 1_000_000,
)
print("FX option NPV:", opt.price())
print("Greeks:", opt.get_greeks())
```

### 3.5 FX knock‑out barrier (analytic/MC fallback)

```python
import datetime as dt
from src.instruments.knockout_fx_option import KnockOutFXOption

bar = KnockOutFXOption(
    currency_pair = "EURUSD",
    strike        = 1.09,
    barrier       = 1.18,
    maturity      = dt.date.today().replace(year=dt.date.today().year + 1),
    option_type   = "call",
    barrier_type  = "up_and_out",
    notional      = 1_000_000,
    rebate        = 0.0,
)
print("Barrier NPV:", bar.price())
print("Greeks:", bar.get_greeks())
print("Barrier info:", bar.get_barrier_info())
```

---

## 4) Setup & notes

* **Python** ≥ 3.10, **QuantLib** ≥ 1.29 recommended.
* Install deps (typical): `pip install QuantLib numpy pandas matplotlib plotly pydantic`.
* For the **TIIE zero curve**, either place `data/MEXDERSWAP_IRSTIIEPR.csv` as in this repo, or set:

```bash
export TIIE_ZERO_CSV=/absolute/path/to/MEXDERSWAP_IRSTIIEPR.csv
```

* Calendars: if `ql.Mexico()` / `ql.MXNCurrency()` are missing in your wheel, code falls back to TARGET/USD *labels only*; math is unaffected but holiday treatment differs.

* Reproducibility: mock fixings are generated on the appropriate business days; for strict auditing, replace mocks with your historical time series.

---

## 5) Contributing

* Keep new code **small, inspectable, and testable**. Favor functions that return concrete QuantLib handles/objects.
* Document **assumptions** explicitly (evaluation date, calendars, DC conventions, compounding).
* When versions differ across wheels, guard with `hasattr(...)` / try‑except and comment the alternative path.

Happy pricing!
