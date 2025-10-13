# src/instruments/json_codec.py
from __future__ import annotations

import hashlib
import inspect  # <-- ADD
import json
from typing import Any

import QuantLib as ql

from mainsequence.instruments.pricing_models.indices import get_index as _index_by_name

# ----------------------------- ql.Period -------------------------------------

_UNITS_TO_SHORT = {
    ql.Days: "D",
    ql.Weeks: "W",
    ql.Months: "M",
    ql.Years: "Y",
}


def period_to_json(p: str | ql.Period | None) -> str | None:
    """
    Encode a QuantLib Period as a compact string like '28D', '3M', '6M', '2Y'.
    Accepts strings and passes them through (idempotent).
    """
    if p is None:
        return None
    if isinstance(p, ql.Period):
        return f"{p.length()}{_UNITS_TO_SHORT[p.units()]}"
    return str(p)


def period_from_json(v: str | ql.Period | None) -> ql.Period | None:
    """Decode strings like '28D', '3M' into ql.Period; pass ql.Period through."""
    if v is None or isinstance(v, ql.Period):
        return v
    return ql.Period(str(v))


# ----------------------------- ql.DayCounter ---------------------------------

# Prefer explicit enumerations you actually use in this codebase.
_DAYCOUNT_FACTORIES = {
    "Actual360": lambda: ql.Actual360(),
    "Actual365Fixed": lambda: ql.Actual365Fixed(),
    # Default to USA when we can't introspect Thirty360 convention via SWIG.
    "Thirty360": lambda: ql.Thirty360(ql.Thirty360.USA),
    "Thirty360_USA": lambda: ql.Thirty360(ql.Thirty360.USA),
    "Thirty360_BondBasis": lambda: ql.Thirty360(ql.Thirty360.BondBasis),
    "Thirty360_European": lambda: ql.Thirty360(ql.Thirty360.European),
    "Thirty360_ISMA": lambda: ql.Thirty360(ql.Thirty360.ISMA),
    "Thirty360_ISDA": lambda: ql.Thirty360(ql.Thirty360.ISDA),
}


def daycount_to_json(dc: ql.DayCounter) -> str:
    """Encode common DayCounters to a stable string token."""
    if isinstance(dc, ql.Actual360):
        return "Actual360"
    if isinstance(dc, ql.Actual365Fixed):
        return "Actual365Fixed"
    if isinstance(dc, ql.Thirty360):
        # SWIG doesn't expose convention reliably; default to USA in JSON.
        return "Thirty360_USA"
    # Fallback to class name (caller should ensure a known value)
    return dc.__class__.__name__


def daycount_from_json(v: str | ql.DayCounter) -> ql.DayCounter:
    """Decode from a string token back to a DayCounter instance."""
    if isinstance(v, ql.DayCounter):
        return v
    key = str(v)
    factory = _DAYCOUNT_FACTORIES.get(key)
    if not factory and key == "Thirty360":
        factory = _DAYCOUNT_FACTORIES["Thirty360"]
    if not factory:
        raise ValueError(f"Unsupported day_count '{key}'")
    return factory()


# ----------------------------- ql.Calendar -----------------------------------
# We standardize on: {"name": "<QuantLib class name>", "market": <int optional>}
# Example: {"name": "Mexico"}, {"name": "UnitedStates", "market": 1}


def _build_calendar_display_factory() -> dict[str, callable]:
    """
    Build a mapping: display name (Calendar::name()) -> zero-arg callable that
    constructs a concrete ql.Calendar. Handles classes with Market enums too.
    """
    factory: dict[str, callable] = {}

    def _try_register(ctor):
        try:
            inst = ctor()
            disp = inst.name()
            factory.setdefault(disp, ctor)
            return True
        except Exception:
            return False

    for _, cls in inspect.getmembers(ql, predicate=inspect.isclass):
        try:
            if not issubclass(cls, ql.Calendar) or cls is ql.Calendar:
                continue
        except TypeError:
            continue

        # Case A: no-arg constructor (TARGET, Mexico, Turkey, ...)
        _try_register(lambda c=cls: c())

        # Case B: int-valued attributes (legacy Market enums on the class)
        for attr_name, attr_val in inspect.getmembers(cls):
            if attr_name.startswith("_"):
                continue
            if isinstance(attr_val, int):
                _try_register(lambda c=cls, e=attr_val: c(e))

        # Case C: nested Market enum classes (common style)
        for attr_name, attr_val in inspect.getmembers(cls):
            if not inspect.isclass(attr_val):
                continue
            if "market" in attr_name.lower():
                for mname, mval in inspect.getmembers(attr_val):
                    if mname.startswith("_"):
                        continue
                    if isinstance(mval, int):
                        _try_register(lambda c=cls, e=mval: c(e))

    return factory


# Build once + case-insensitive mirror
_CAL_DISP_FACTORY: dict[str, callable] = _build_calendar_display_factory()
_CAL_DISP_FACTORY_CI: dict[str, callable] = {k.casefold(): v for k, v in _CAL_DISP_FACTORY.items()}


def _calendar_from_display_name(display: str) -> ql.Calendar:
    ctor = _CAL_DISP_FACTORY.get(display) or _CAL_DISP_FACTORY_CI.get(display.casefold())
    if ctor is None:
        raise ValueError(
            f"Unsupported calendar display name {display!r}. "
            f"Available: "
            + ", ".join(sorted(_CAL_DISP_FACTORY.keys())[:20])
            + ("..." if len(_CAL_DISP_FACTORY) > 20 else "")
        )
    return ctor()


def _try_get_market(c: ql.Calendar) -> int | None:
    try:
        return int(c.market())
    except Exception:
        return None


def _normalize_calendar_to_class_and_market(cal: ql.Calendar) -> dict[str, Any]:
    """
    Return the canonical JSON dict {"name": <class name>, "market": <int?>}
    even if 'cal' is a base Calendar proxy. We achieve this by re-instantiating
    a derived calendar from Calendar::name() (display name) when needed.
    """
    cls_name = cal.__class__.__name__
    # If SWIG gives a base proxy ('Calendar'), derive a concrete instance via display name
    if cls_name == "Calendar":
        try:
            derived = _calendar_from_display_name(cal.name())
            name = derived.__class__.__name__
            market = _try_get_market(derived)
        except Exception:
            # Fall back to best-effort: use display name as a string name (non-canonical)
            # but caller's calendar_from_json can still accept it as a display name.
            return {"name": cal.name()}
    else:
        name = cls_name
        market = _try_get_market(cal)

    out: dict[str, Any] = {"name": name}
    if market is not None:
        out["market"] = market
    return out


def calendar_to_json(cal: ql.Calendar) -> dict[str, Any]:
    """
    Encode a Calendar as canonical {"name": "<QuantLib class name>", "market": <int?>}.
    Robust to base Calendar proxies returned by SWIG.
    """
    return _normalize_calendar_to_class_and_market(cal)


def _calendar_from_class_and_market(name: str, market: int | None) -> ql.Calendar:
    """
    Construct a calendar from a QuantLib class name + optional market.
    Tries nested Market enums, then plain int, then no-arg.
    """
    cls = getattr(ql, name, None)
    if cls is None or not inspect.isclass(cls):
        raise ValueError(f"Unsupported calendar class {name!r}")
    # Try with market if provided
    if market is not None:
        # Most calendars expose a nested Market enum:
        try:
            enum_cls = cls.Market
            return cls(enum_cls(int(market)))
        except Exception:
            pass
        # Some accept a raw int:
        try:
            return cls(int(market))
        except Exception:
            pass
    # Fallback: no-arg constructor
    return cls()


def calendar_from_json(v: dict[str, Any] | str | ql.Calendar) -> ql.Calendar:
    """
    Decode dict or string into a Calendar instance. Accepts:
      - {"name": "<class name>", "market": <int?>}    (canonical)
      - "<class name>"                                (canonical string form)
      - {"name": "<display name>"} / "<display name>" (legacy interop)
    """
    if isinstance(v, ql.Calendar):
        return v

    # String input
    if isinstance(v, str):
        name = v
        # First try class name
        try:
            return _calendar_from_class_and_market(name, None)
        except Exception:
            # Then try display name
            return _calendar_from_display_name(name)

    # Dict input
    if isinstance(v, dict):
        name = v.get("name")
        if not name:
            raise ValueError("Calendar dict must contain 'name'.")
        market = v.get("market", None)

        # Prefer class name path; if that fails, treat 'name' as display name.
        try:
            return _calendar_from_class_and_market(name, market)
        except Exception:
            return _calendar_from_display_name(name)

    raise TypeError(f"Cannot decode calendar from {type(v).__name__}")


# ----------------------------- Business Day Convention helpers --------------

_BDC_TO_STR = {
    ql.Following: "Following",
    ql.ModifiedFollowing: "ModifiedFollowing",
    ql.Preceding: "Preceding",
    ql.ModifiedPreceding: "ModifiedPreceding",
    ql.Unadjusted: "Unadjusted",
    ql.HalfMonthModifiedFollowing: "HalfMonthModifiedFollowing",
    ql.Nearest: "Nearest",
}

_STR_TO_BDC = {v: k for k, v in _BDC_TO_STR.items()}


def bdc_to_json(bdc: int) -> int | str:
    """Encode BusinessDayConvention as a stable string (falls back to int if unknown)."""
    return _BDC_TO_STR.get(int(bdc), int(bdc))


def bdc_from_json(v: int | str) -> int:
    """Decode a BusinessDayConvention from string or int."""
    if isinstance(v, int):
        return int(v)
    if isinstance(v, str):
        if v in _STR_TO_BDC:
            return int(_STR_TO_BDC[v])
        # accept enum name variants like 'Following' / 'following'
        key = v[0].upper() + v[1:]
        if key in _STR_TO_BDC:
            return int(_STR_TO_BDC[key])
        try:
            return int(v)
        except Exception:
            pass
    raise ValueError(f"Unsupported business day convention '{v}'")


# ----------------------------- Date utils -----------------------------------


def _ql_date_to_iso(d: ql.Date) -> str:
    return f"{d.year():04d}-{int(d.month()):02d}-{d.dayOfMonth():02d}"


def _iso_to_ql_date(s: str) -> ql.Date:
    y, m, d = (int(x) for x in s.split("-"))
    return ql.Date(d, m, y)


# ----------------------------- Schedule codec --------------------------------

# Optional rule mapping (used only if you ever store rule-based schedules)
_RULE_TO_STR = {
    ql.DateGeneration.Backward: "Backward",
    ql.DateGeneration.Forward: "Forward",
    ql.DateGeneration.Zero: "Zero",
    ql.DateGeneration.Twentieth: "Twentieth",
    ql.DateGeneration.TwentiethIMM: "TwentiethIMM",
    ql.DateGeneration.ThirdWednesday: "ThirdWednesday",
    ql.DateGeneration.OldCDS: "OldCDS",
    ql.DateGeneration.CDS: "CDS",
    ql.DateGeneration.CDS2015: "CDS2015",
}
_STR_TO_RULE = {v: k for k, v in _RULE_TO_STR.items() if v != 9999}


def schedule_to_json(s: ql.Schedule | None) -> dict[str, Any] | None:
    """
    Encode a QuantLib Schedule. We always include the explicit 'dates' array so
    round-tripping never depends on rule/tenor reconstruction.
    """
    if s is None:
        return None

    # Extract dates as ISO strings
    try:
        dates_iso = [_ql_date_to_iso(d) for d in list(s.dates())]
    except Exception:
        # Some SWIG builds require iterating via size()/date(i)
        dates_iso = [_ql_date_to_iso(s.date(i)) for i in range(s.size())]

    payload: dict[str, Any] = {
        "dates": dates_iso,
    }

    # Include helpful metadata when available (not required for decoding)
    try:
        payload["calendar"] = calendar_to_json(s.calendar())
    except Exception:
        pass
    try:
        payload["business_day_convention"] = bdc_to_json(int(s.businessDayConvention()))
    except Exception:
        pass
    try:
        payload["termination_business_day_convention"] = bdc_to_json(
            int(s.terminationDateConvention())
        )
    except Exception:
        pass
    try:
        payload["end_of_month"] = bool(s.endOfMonth())
    except Exception:
        pass
    try:
        payload["tenor"] = period_to_json(s.tenor())
    except Exception:
        pass
    try:
        rule = s.rule()
        payload["rule"] = _RULE_TO_STR.get(int(rule), int(rule))
    except Exception:
        pass

    return payload


def schedule_from_json(
    v: None | ql.Schedule | dict[str, Any] | List[str] | List[ql.Date]
) -> ql.Schedule | None:
    """
    Decode a schedule. Supported forms:
      - None
      - ql.Schedule (returned as-is)
      - {"dates":[...], "calendar":{...}, "business_day_convention":"Following", ...}
      - ["2025-01-15", "2025-02-12", ...]  (ISO date list)
      - [ql.Date(...), ...]                 (rare, but supported)
    For explicit-date payloads we build: ql.Schedule(DateVector, calendar, bdc).
    """
    if v is None or isinstance(v, ql.Schedule):
        return v

    # List forms (explicit dates)
    if isinstance(v, list):
        if not v:
            return ql.Schedule()  # empty schedule
        # Accept ISO strings or ql.Date
        date_vec = ql.DateVector()
        if isinstance(v[0], ql.Date):
            for d in v:
                date_vec.push_back(d)
        else:
            for s in v:
                date_vec.push_back(_iso_to_ql_date(str(s)))
        # Defaults are conservative; your model field carries calendar/bdc anyway
        cal = ql.NullCalendar()
        bdc = ql.Following
        return ql.Schedule(date_vec, cal, bdc)

    # Dict form
    if isinstance(v, dict):
        dates = v.get("dates")
        if dates:
            date_vec = ql.DateVector()
            # Accept both ISO strings and ql.Date in the list
            for x in dates:
                if isinstance(x, ql.Date):
                    date_vec.push_back(x)
                else:
                    date_vec.push_back(_iso_to_ql_date(str(x)))
            cal_json = v.get("calendar", {"name": "NullCalendar"})
            cal = calendar_from_json(cal_json)
            bdc_json = v.get("business_day_convention", "Following")
            bdc = bdc_from_json(bdc_json)
            return ql.Schedule(date_vec, cal, bdc)

        # Optional: support rule-based reconstruction if no explicit dates provided
        start = v.get("start")
        end = v.get("end")
        tenor = v.get("tenor")
        if start and end and tenor:
            cal = calendar_from_json(v.get("calendar", {"name": "TARGET"}))
            bdc = bdc_from_json(v.get("business_day_convention", "Following"))
            term_bdc = bdc_from_json(v.get("termination_business_day_convention", bdc))
            rule_val = v.get("rule", "Forward")
            if isinstance(rule_val, str):
                rule = _STR_TO_RULE.get(rule_val, ql.DateGeneration.Forward)
            else:
                rule = int(rule_val)
            eom = bool(v.get("end_of_month", False))
            first_date = v.get("first_date")
            next_to_last = v.get("next_to_last_date")

            sd = _iso_to_ql_date(str(start))
            ed = _iso_to_ql_date(str(end))
            ten = ql.Period(str(tenor))
            fd = _iso_to_ql_date(str(first_date)) if first_date else ql.Date()
            ntl = _iso_to_ql_date(str(next_to_last)) if next_to_last else ql.Date()

            return ql.Schedule(sd, ed, ten, cal, bdc, term_bdc, rule, eom, fd, ntl)

    raise TypeError(f"Cannot decode Schedule from {type(v).__name__}")


# ----------------------------- ql.IborIndex ----------------------------------


def ibor_to_json(idx: ql.IborIndex | None) -> dict[str, Any] | None:
    """
    Encode an IborIndex without trying to serialize the curve handle:
    {"family": "USDLibor", "tenor": "3M"}  or {"family":"Euribor","tenor":"6M"}.
    """
    if idx is None:
        return None
    name_upper = idx.name().upper()
    if "TIIE" in name_upper or "MXNTIIE" in name_upper:
        return {"family": "TIIE-28D", "tenor": "28D"}

    family = getattr(idx, "familyName", lambda: None)() or idx.name()
    try:
        ten = period_to_json(idx.tenor())
    except Exception:
        ten = None
    out = {"family": str(family)}
    if ten:
        out["tenor"] = ten
    return out


def _construct_ibor(family: str, tenor: str) -> ql.IborIndex:
    p = ql.Period(tenor)
    # Common families—extend as needed
    if hasattr(ql, "USDLibor") and family == "USDLibor":
        return ql.USDLibor(p, ql.YieldTermStructureHandle())
    if hasattr(ql, "Euribor") and family == "Euribor":
        return ql.Euribor(p, ql.YieldTermStructureHandle())
    # Generic fallback if QuantLib exposes the family by name
    ctor = getattr(ql, family, None)
    if ctor:
        try:
            return ctor(p, ql.YieldTermStructureHandle())
        except TypeError:
            return ctor(p)
    # TIIE is not a built-in IborIndex; TIIE swaps build their own index later.
    raise ValueError(f"Unsupported Ibor index family '{family}'")


def ibor_from_json(v: None | str | dict[str, Any] | ql.IborIndex) -> ql.IborIndex | None:
    """
    Decode from JSON into a ql.IborIndex, delegating to the central factory when possible.
    Falls back to legacy parsing for 'USDLibor3M' / 'Euribor6M' styles.
    NOTE: TIIE for swaps remains handled in TIIESwap; this function does not change that flow.
    """
    if v is None or isinstance(v, ql.IborIndex):
        return v

    # 1) String form: try the factory first (supports: 'EURIBOR_6M', 'USD_LIBOR_3M', 'SOFOR'→'SOFR', etc.)
    if isinstance(v, str):
        if _index_by_name is not None:
            try:
                idx = _index_by_name(v)
                # For instruments here we expect an IborIndex; ignore overnight-only results.
                if isinstance(idx, ql.IborIndex):
                    return idx
            except Exception:
                pass  # fall back to legacy parser
        # Legacy fallback: 'USDLibor3M' / 'Euribor6M' / 'USDLibor' (defaults 3M)
        name = v
        tenor = "3M"
        for t in ("1M", "3M", "6M", "12M", "1Y", "28D"):
            if name.endswith(t):
                tenor = t
                family = name[: -len(t)]
                break
        else:
            family = name
        return _construct_ibor(family, tenor)

    # 2) Dict form: try the factory if we have family/tenor; else fallback
    if isinstance(v, dict):
        family = v.get("family") or v.get("name")
        tenor = v.get("tenor", "3M")
        if not family:
            return None
        if _index_by_name is not None:
            try:
                # Accept either {'family':'Euribor','tenor':'6M'} or {'name':'USD_LIBOR','tenor':'3M'}
                candidate = f"{family}_{tenor}" if tenor else family
                idx = _index_by_name(candidate)
                if isinstance(idx, ql.IborIndex):
                    return idx
            except Exception as e:
                raise e
        return _construct_ibor(family, tenor)

    raise TypeError(f"Cannot decode IborIndex from {type(v).__name__}")


def _fix_schedule_calendar_from_top_level(data: dict) -> dict:
    try:
        sched = data.get("schedule")
        top_cal = data.get("calendar")
        if isinstance(sched, dict) and isinstance(sched.get("calendar"), dict):
            if (
                sched["calendar"].get("name") == "Calendar"
                and isinstance(top_cal, dict)
                and top_cal.get("name")
            ):
                sched["calendar"] = {"name": top_cal["name"]}
    except Exception:
        pass
    return data


# ----------------------------- Generic mixin ---------------------------------


class JSONMixin:
    """
    Mixin to give Pydantic models convenient JSON round-trip helpers.
    Uses Pydantic's JSON mode (so field_serializers are honored).
    """

    def to_json_dict(self) -> dict[str, Any]:
        try:
            return self.model_dump(mode="json")
        except Exception as e:
            raise e

    def to_json(self, **json_kwargs: Any) -> str:
        return json.dumps(self.to_json_dict(), default=str, **json_kwargs)

    @classmethod
    def from_json_dict(cls, data: dict[str, Any]):
        data = _fix_schedule_calendar_from_top_level(data)
        return cls.model_validate(data)

    @classmethod
    def from_json(cls, payload: str | bytes | dict[str, Any]):  # <-- broadened
        if isinstance(payload, dict):
            return cls.from_json_dict(payload)
        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode("utf-8")
        return cls.from_json_dict(json.loads(payload))

    def to_canonical_json(self) -> str:
        """
        Canonical JSON used for hashing:
        - keys sorted
        - no extra whitespace
        - UTF-8 friendly (no ASCII escaping)
        """
        data = self.to_json_dict()
        return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)

    def content_hash(self, algorithm: str = "sha256") -> str:
        """
        Hash of the canonical JSON representation.
        `algorithm` must be a hashlib-supported name (e.g., 'sha256', 'sha1', 'md5', 'blake2b').
        """
        s = self.to_canonical_json().encode("utf-8")
        h = hashlib.new(algorithm)
        h.update(s)
        return h.hexdigest()

    @classmethod
    def hash_payload(cls, payload: str | bytes | dict[str, Any], algorithm: str = "sha256") -> str:
        """
        Hash an arbitrary JSON payload (str/bytes/dict) using the same canonicalization.
        Useful if you have serialized JSON already and want the same digest.
        """
        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode("utf-8")
        if isinstance(payload, str):
            obj = json.loads(payload)
        elif isinstance(payload, dict):
            obj = payload
        else:
            raise TypeError(f"Unsupported payload type: {type(payload).__name__}")
        s = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode(
            "utf-8"
        )
        h = hashlib.new(algorithm)
        h.update(s)
        return h.hexdigest()
