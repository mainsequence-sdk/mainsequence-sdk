# src/instruments/ql_fields.py
from __future__ import annotations

import inspect
from typing import Annotated, Any, Optional

import QuantLib as ql
from pydantic import BeforeValidator, Field, PlainSerializer, WithJsonSchema

# Reuse your existing codec helpers
from mainsequence.instruments.instruments.json_codec import (
    bdc_from_json,
    # BDC
    bdc_to_json,
    calendar_from_json,
    # Calendar
    daycount_from_json,
    # DayCounter
    daycount_to_json,
    period_from_json,
    # Period
    period_to_json,
    schedule_from_json,
    # Schedule
    schedule_to_json,
)

# ============================================================================
# Automatic Calendar Factory
# ============================================================================


def _build_fully_automatic_calendar_factory() -> dict[str, callable]:
    """
    Build a mapping: <calendar display name> -> zero-arg callable that returns ql.Calendar
    We try both no-arg constructors and Market-enum constructors.

    Examples of keys (display names):
      - "TARGET"
      - "Mexican stock exchange"      (ql.Mexico())
      - "New York stock exchange"     (ql.UnitedStates(ql.UnitedStates.NYSE))
      - "United States settlement"    (ql.UnitedStates(ql.UnitedStates.Settlement))
      - "London stock exchange"       (ql.UnitedKingdom(ql.UnitedKingdom.LSE))
    """
    factory: dict[str, callable] = {}

    def _try_register(ctor):
        try:
            inst = ctor()
            name = inst.name()
            # Keep first seen mapping; if duplicates exist, first one wins.
            factory.setdefault(name, ctor)
            return True
        except Exception:
            return False

    # Iterate over all QuantLib classes; find Calendar subclasses (excluding base Calendar)
    for _, cls in inspect.getmembers(ql, predicate=inspect.isclass):
        try:
            if not issubclass(cls, ql.Calendar) or cls is ql.Calendar:
                continue
        except TypeError:
            # Some SWIG artifacts aren't proper classes for issubclass; skip them
            continue

        # Case A: try no-arg constructor (e.g., TARGET, Mexico, Turkey, etc.)
        _try_register(lambda c=cls: c())

        # Case B: try int-valued attributes on the class (likely Market enums)
        for attr_name, attr_val in inspect.getmembers(cls):
            if attr_name.startswith("_"):
                continue
            if isinstance(attr_val, int):
                _try_register(lambda c=cls, e=attr_val: c(e))

        # Case C: nested 'Market' enum classes (common in recent QuantLib builds)
        for attr_name, attr_val in inspect.getmembers(cls):
            if not inspect.isclass(attr_val):
                continue
            if attr_name.lower().startswith("market"):
                for mname, mval in inspect.getmembers(attr_val):
                    if mname.startswith("_"):
                        continue
                    if isinstance(mval, int):
                        _try_register(lambda c=cls, e=mval: c(e))

    return factory


# Build once; also a case-insensitive mirror for defensive lookups.
_CAL_FACTORY: dict[str, callable] = _build_fully_automatic_calendar_factory()
_CAL_FACTORY_CI: dict[str, callable] = {k.casefold(): v for k, v in _CAL_FACTORY.items()}


def calendar_from_display_name(name: str) -> ql.Calendar:
    """
    Rebuild a QuantLib calendar from its display name (Calendar::name()).
    Example: "Mexican stock exchange" -> ql.Mexico()
    """
    ctor = _CAL_FACTORY.get(name) or _CAL_FACTORY_CI.get(name.casefold())
    if ctor is None:
        raise ValueError(
            f"Unsupported calendar display name {name!r}. "
            "Available: "
            + ", ".join(sorted(_CAL_FACTORY.keys())[:20])
            + ("..." if len(_CAL_FACTORY) > 20 else "")
        )
    return ctor()


# ============================================================================
# Strict serializers that force the real calendar name via virtual .name()
# ============================================================================


def _calendar_to_json_actual(cal: ql.Calendar) -> dict[str, Any]:
    """
    Serialize as {'name': cal.name()} using the virtual method.
    Works even if 'cal' is a base Calendar proxy returned by SWIG.
    """
    return {"name": cal.name()}


def _schedule_to_json_actual(s: ql.Schedule | None) -> dict[str, Any] | None:
    """
    Serialize a schedule; ensure its calendar is emitted with the true display name.
    """
    if s is None:
        return None
    data = schedule_to_json(s)  # keep your canonical fields (dates, BDCs, EOM, rule, etc.)
    try:
        data["calendar"] = {"name": s.calendar().name()}
    except Exception:
        pass
    return data


# ============================================================================
# Lenient deserializers that accept {'name': '<display name>'}
# ============================================================================


def _calendar_from_json_auto(v):
    """
    Accept:
      - ql.Calendar (pass-through)
      - {'name': '<display name>'}  -> rebuilt via factory
      - str '<display name>'        -> rebuilt via factory
      - else -> delegate to existing calendar_from_json
    """
    if isinstance(v, ql.Calendar):
        return v
    if isinstance(v, dict):
        nm = v.get("name")
        if isinstance(nm, str) and nm and nm != "Calendar":
            return calendar_from_display_name(nm)
    if isinstance(v, str) and v and v != "Calendar":
        return calendar_from_display_name(v)
    # Fallback to your existing helper (may accept other legacy formats)
    return calendar_from_json(v)


def _schedule_from_json_auto(v):
    """
    If schedule JSON contains {'calendar': {'name': '<display name>'}},
    rebuild a concrete ql.Calendar first, then delegate to schedule_from_json.
    """
    if v is None or isinstance(v, ql.Schedule):
        return v
    if isinstance(v, dict) and "calendar" in v:
        cal_spec = v["calendar"]
        # Rebuild calendar if we have a display name
        try:
            v = dict(v)
            v["calendar"] = _calendar_from_json_auto(cal_spec)
        except Exception:
            # Leave as-is; schedule_from_json may still handle it
            pass
    return schedule_from_json(v)


# ============================================================================
# Pydantic Annotated field types
# ============================================================================

# ---------- Period -----------------------------------------------------------
QuantLibPeriod = Annotated[
    ql.Period,
    BeforeValidator(period_from_json),
    PlainSerializer(period_to_json, return_type=str),
Field(
        description="QuantLib Period (tenor). Common formats: '1D', '1W', '3M', '6M', '1Y'.",
        examples=["3M", "6M", "1Y"],
        json_schema_extra={
            "quantlib_class": "Period",
            "accepted_json": ["string"],
            "notes": "Use uppercase units: D/W/M/Y.",
        },
    ),
]

# ---------- DayCounter -------------------------------------------------------
QuantLibDayCounter = Annotated[
    ql.DayCounter,
    BeforeValidator(daycount_from_json),
    PlainSerializer(daycount_to_json, return_type=str),
Field(
        description=(
            "QuantLib DayCounter (day count convention) used for year fractions/accrual. "
            "Provide a canonical name supported by your daycount_from_json parser."
        ),
        examples=["Actual/360", "30/360", "Actual/Actual (ISDA)"],
        json_schema_extra={
            "quantlib_class": "DayCounter",
            "accepted_json": ["string"],
            "synonyms": ["dcc", "day_count_convention"],
        },
    ),
]

# ---------- Calendar ---------------------------------------------------------
QuantLibCalendar = Annotated[
    ql.Calendar,
    BeforeValidator(_calendar_from_json_auto),  # <— use factory-based loader
    PlainSerializer(
        _calendar_to_json_actual, return_type=dict[str, Any]
    ),  # <— always emit true name

    Field(
        description=(
            "QuantLib Calendar used to adjust dates (holidays/weekends). "
            "Serialized as an object so the true calendar name is preserved."
        ),
        examples=[
            {"name": "TARGET"},
            {"name": "UnitedStates", "market": "GovernmentBond"},
        ],
        json_schema_extra={
            "quantlib_class": "Calendar",
            "accepted_json": ["object", "string"],
            "notes": "Prefer object form to preserve the exact calendar identity.",
        },
    ),
]


# ---------- Business Day Convention (BDC) -----------------------------------
def _bdc_from_any(v):
    return bdc_from_json(v)


def _bdc_to_str(v: int) -> str:
    return str(bdc_to_json(int(v)))


QuantLibBDC = Annotated[
    int,
    BeforeValidator(_bdc_from_any),
    PlainSerializer(_bdc_to_str, return_type=str),
Field(
        description=(
            "QuantLib BusinessDayConvention. Accepts either a convention name "
            "(e.g. 'Following') or the QuantLib enum integer."
        ),
        examples=["Following", "ModifiedFollowing", "Unadjusted"],
        json_schema_extra={
            "quantlib_enum": "BusinessDayConvention",
            "accepted_json": ["string", "integer"],
        },
    ),

    WithJsonSchema(
        {
            "type": ["string", "integer"],
            "examples": ["Following", "ModifiedFollowing", "Unadjusted"],
        },
        mode="validation",
    ),
    WithJsonSchema(
        {"type": "string", "examples": ["Following"]},
        mode="serialization",
    ),
]

# ---------- Schedule ---------------------------------------------------------
_SCHEDULE_SCHEMA: dict[str, Any] = {
    "type": ["object", "null"],
    "properties": {
        "dates": {
            "type": "array",
            "items": {"type": "string", "pattern": r"^\d{4}-\d{2}-\d{2}$"},
        },
        "calendar": {"type": "object"},  # {"name": "<display name from cal.name()>"}
        "business_day_convention": {"type": ["string", "integer"]},
        "termination_business_day_convention": {"type": ["string", "integer"]},
        "end_of_month": {"type": "boolean"},
        "tenor": {"type": "string"},
        "rule": {"type": ["string", "integer"]},
    },
    "required": ["dates"],
    "additionalProperties": True,
}
QuantLibSchedule = Annotated[
    ql.Schedule | None,
    BeforeValidator(_schedule_from_json_auto),  # <— rebuild calendar from display name first
    PlainSerializer(
        _schedule_to_json_actual, return_type=Optional[dict[str, Any]]
    ),  # <— emit true name
    Field(
        description=(
            "QuantLib Schedule definition. At minimum provide 'dates' as ISO strings. "
            "Optional keys like 'calendar', 'business_day_convention', 'tenor', 'rule', etc. "
            "are accepted and passed through your schedule codec."
        ),
        examples=[
            None,
            {
                "dates": ["2026-01-15", "2026-07-15", "2027-01-15"],
                "calendar": {"name": "TARGET"},
                "business_day_convention": "ModifiedFollowing",
                "termination_business_day_convention": "ModifiedFollowing",
                "end_of_month": False,
                "tenor": "6M",
                "rule": "Forward",
            },
        ],
        json_schema_extra={
            "quantlib_class": "Schedule",
            "accepted_json": ["object", "null"],
            "required_keys": ["dates"],
            "date_format": "YYYY-MM-DD",
            "notes": "calendar is expected as {'name': '<Calendar.name() display name>'} in the preferred form.",
        },
    ),
        # Make it appear in config_schema (validation mode)
    WithJsonSchema(_SCHEDULE_SCHEMA, mode="validation"),
        #  Keep serialization shape documented too (your serializer returns dict|null)
    WithJsonSchema(_SCHEDULE_SCHEMA, mode="serialization"),
]

__all__ = [
    "QuantLibPeriod",
    "QuantLibDayCounter",
    "QuantLibCalendar",
    "QuantLibBDC",
    "QuantLibSchedule",
]
