# streamlit_form_factory.py
from __future__ import annotations

import datetime as _dt
import types
import typing as _t
from dataclasses import dataclass
from enum import Enum

import streamlit as st
from pydantic import BaseModel, ValidationError

try:
    import pandas as pd
except Exception:  # pragma: no cover
    pd = None  # type: ignore

# Optional: AG Grid support
try:
    from st_aggrid import (
        AgGrid,
        DataReturnMode,
        GridOptionsBuilder,
        GridUpdateMode,
    )
    _HAS_AGGRID = True
except Exception:  # pragma: no cover
    _HAS_AGGRID = False


# ----------------------------
# Helpers: typing inspection
# ----------------------------
def _unwrap_annotated(tp: _t.Any) -> _t.Any:
    origin = _t.get_origin(tp)
    if origin is _t.Annotated:
        return _t.get_args(tp)[0]
    return tp


def _annotated_metadata(tp: _t.Any) -> list[_t.Any]:
    origin = _t.get_origin(tp)
    if origin is _t.Annotated:
        args = _t.get_args(tp)
        return list(args[1:])
    return []


def _unwrap_optional(tp: _t.Any) -> tuple[_t.Any, bool]:
    tp = _unwrap_annotated(tp)
    origin = _t.get_origin(tp)
    if origin in (_t.Union, types.UnionType):
        args = list(_t.get_args(tp))
        if type(None) in args:
            non_none = [a for a in args if a is not type(None)]
            if len(non_none) == 1:
                return _unwrap_annotated(non_none[0]), True
            # optional but ambiguous union
            return tp, True
    return tp, False


def _is_pydantic_model(tp: _t.Any) -> bool:
    try:
        return isinstance(tp, type) and issubclass(tp, BaseModel)
    except Exception:
        return False


def _schema_props_safe(model_cls: type[BaseModel]) -> dict[str, dict]:
    """
    Best-effort JSON-schema properties extraction.
    Falls back to empty dict when arbitrary runtime types (e.g. QuantLib) break schema generation.
    """
    try:
        schema = model_cls.model_json_schema()
        props = schema.get("properties") or {}
        return props if isinstance(props, dict) else {}
    except Exception:
        return {}


def _field_examples(finfo: _t.Any, prop_schema: dict) -> list[_t.Any]:
    ex = prop_schema.get("examples")
    if isinstance(ex, list):
        return ex
    finfo_examples = getattr(finfo, "examples", None)
    if isinstance(finfo_examples, (list, tuple)):
        return list(finfo_examples)
    return []


def _field_title(fname: str, finfo: _t.Any, prop_schema: dict) -> str:
    return (
        prop_schema.get("title")
        or getattr(finfo, "title", None)
        or _safe_title(fname)
    )


def _field_desc(finfo: _t.Any, prop_schema: dict) -> str:
    return (
        prop_schema.get("description")
        or getattr(finfo, "description", None)
        or ""
    )


def _field_json_extra(finfo: _t.Any) -> dict:
    extra = getattr(finfo, "json_schema_extra", None)
    return extra if isinstance(extra, dict) else {}


def _infer_quantlib_kind_from_annotation(ann: _t.Any) -> str | None:
    try:
        mod = getattr(ann, "__module__", "")
        name = getattr(ann, "__name__", "")
        if mod.startswith("QuantLib"):
            if name in ("Schedule", "Calendar", "DayCounter", "Period"):
                return name
    except Exception:
        pass
    return None


def _safe_title(name: str) -> str:
    return name.replace("_", " ").strip().title()


# ----------------------------
# Defaults/options for QL-like fields
# ----------------------------
_DEFAULT_BDC = [
    "Following",
    "ModifiedFollowing",
    "Preceding",
    "ModifiedPreceding",
    "Unadjusted",
]

_DEFAULT_DCC = [
    "Actual/360",
    "Actual/365 (Fixed)",
    "30/360",
    "Actual/Actual (ISDA)",
    "Actual/Actual (Bond)",
]

_DEFAULT_SCHED_RULES = [
    "Forward",
    "Backward",
    "Zero",
    "ThirdWednesday",
    "Twentieth",
    "TwentiethIMM",
    "CDS",
    "CDS2015",
]


def _available_calendar_names() -> list[str]:
    """
    Best-effort calendar names list using your ql_fields factory cache.
    Falls back to TARGET-only.
    """
    try:
        # This is in your snippet. It's "private", but it’s the cleanest way to get names.
        from mainsequence.instruments.instruments.ql_fields import _CAL_FACTORY  # type: ignore

        names = sorted(list(_CAL_FACTORY.keys()))
        return names if names else ["TARGET"]
    except Exception:
        return ["TARGET"]


# ----------------------------
# Grid editors
# ----------------------------
def _grid_edit_dates(
    *,
    key: str,
    value: list[_dt.date] | None,
    label: str = "Schedule dates",
    help_text: str | None = None,
) -> list[_dt.date]:
    """
    Editable list of dates with add/remove rows.
    - Prefers AgGrid if installed.
    - Falls back to st.data_editor (dynamic rows).
    """
    if value is None:
        value = []

    if pd is None:
        # absolute fallback without pandas: use text area
        txt = st.text_area(
            label,
            value="\n".join([d.isoformat() for d in value]),
            help=help_text or "One date per line (YYYY-MM-DD).",
            key=f"{key}.textarea",
        )
        out: list[_dt.date] = []
        for line in [x.strip() for x in txt.splitlines() if x.strip()]:
            out.append(_dt.date.fromisoformat(line))
        return out

    # Keep stateful df
    state_key = f"{key}.__dates_df"
    if state_key not in st.session_state:
        st.session_state[state_key] = pd.DataFrame({"date": value})

    st.caption(help_text or "Add/remove rows. Dates are used as-is to build the QuantLib Schedule JSON.")

    df = st.session_state[state_key]

    cols = st.columns([1, 1, 6])
    with cols[0]:
        if st.button("Add row", key=f"{key}.add"):
            df = pd.concat([df, pd.DataFrame({"date": [_dt.date.today()]})], ignore_index=True)
            st.session_state[state_key] = df
            st.rerun()
    with cols[1]:
        # Remove happens differently depending on editor; we implement after we know selection
        st.write("")  # spacer
    with cols[2]:
        st.write(f"**{label}**")

    if _HAS_AGGRID:
        gb = GridOptionsBuilder.from_dataframe(df)
        gb.configure_default_column(editable=True, resizable=True)
        gb.configure_column("date", header_name="date", editable=True)
        gb.configure_selection("multiple", use_checkbox=True)
        grid = AgGrid(
            df,
            gridOptions=gb.build(),
            data_return_mode=DataReturnMode.AS_INPUT,
            update_mode=GridUpdateMode.MODEL_CHANGED,
            fit_columns_on_grid_load=True,
            key=f"{key}.aggrid",
        )
        new_df = grid["data"]
        selected = grid.get("selected_rows", []) or []

        # Remove selected
        if st.button("Remove selected", key=f"{key}.remove"):
            if selected:
                # selected is list[dict] rows
                sel_dates = set()
                for r in selected:
                    v = r.get("date")
                    if isinstance(v, _dt.date):
                        sel_dates.add(v)
                    elif isinstance(v, str) and v:
                        try:
                            sel_dates.add(_dt.date.fromisoformat(v))
                        except Exception:
                            pass
                if "date" in new_df.columns:
                    new_df = new_df[~new_df["date"].apply(lambda x: (x in sel_dates) if isinstance(x, _dt.date) else False)]
                st.session_state[state_key] = new_df.reset_index(drop=True)
                st.rerun()

        # Normalize output
        out: list[_dt.date] = []
        for v in list(new_df["date"]) if "date" in new_df.columns else []:
            if isinstance(v, _dt.date):
                out.append(v)
            elif isinstance(v, str) and v:
                out.append(_dt.date.fromisoformat(v))
        out = [d for d in out if isinstance(d, _dt.date)]
        return out

    # Fallback to Streamlit native editor
    edited = st.data_editor(
        df,
        num_rows="dynamic",
        key=f"{key}.data_editor",
        column_config={
            "date": st.column_config.DateColumn("date", format="YYYY-MM-DD"),
        },
        use_container_width=True,
    )
    st.session_state[state_key] = edited

    out = []
    for v in list(edited["date"]) if "date" in edited.columns else []:
        if isinstance(v, _dt.date):
            out.append(v)
        elif isinstance(v, str) and v:
            out.append(_dt.date.fromisoformat(v))
    return out


def _grid_edit_model_list(
    *,
    item_model: type[BaseModel],
    key: str,
    value: list[dict] | list[BaseModel] | None,
    label: str,
    help_text: str | None = None,
) -> list[dict]:
    """
    Generic editor for list[BaseModel] using a grid (AgGrid if available, else st.data_editor).
    Returns list[dict] suitable to pass into parent model; Pydantic will validate/convert.
    """
    if pd is None:
        # fallback: JSON editor
        import json
        default_json = "[]"
        if value:
            rows = []
            for it in value:
                if isinstance(it, BaseModel):
                    rows.append(it.model_dump(mode="json"))
                else:
                    rows.append(dict(it))
            default_json = json.dumps(rows, indent=2, default=str)

        txt = st.text_area(label, value=default_json, help=help_text, key=f"{key}.json")
        try:
            parsed = json.loads(txt)
            if isinstance(parsed, list):
                return [dict(x) for x in parsed]
        except Exception:
            return []
        return []

    # Build initial rows
    rows: list[dict] = []
    if value:
        for it in value:
            if isinstance(it, BaseModel):
                rows.append(it.model_dump(mode="json"))
            else:
                rows.append(dict(it))

    # If empty, seed one row (often nicer UX)
    if not rows:
        seed: dict[str, _t.Any] = {}
        schema = {}
        try:
            schema = item_model.model_json_schema()
        except Exception:
            schema = {}
        for fn, fi in item_model.model_fields.items():
            prop = (schema.get("properties") or {}).get(fn, {})
            ex = (prop.get("examples") or [None])[0]
            if ex is None:
                fi_examples = getattr(fi, "examples", None)
                if isinstance(fi_examples, (list, tuple)) and fi_examples:
                    ex = fi_examples[0]
            if ex is not None:
                seed[fn] = ex
            else:
                # sensible minimal defaults
                ann, opt = _unwrap_optional(fi.annotation)
                if ann in (int, float):
                    seed[fn] = 0
                elif ann is bool:
                    seed[fn] = False
                elif ann is _dt.date:
                    seed[fn] = _dt.date.today().isoformat()
                else:
                    seed[fn] = None if opt else ""
        rows = [seed]

    df = pd.DataFrame(rows)

    st.caption(help_text or "Add/remove/edit rows.")

    cols = st.columns([1, 1, 6])
    with cols[0]:
        if st.button("Add row", key=f"{key}.add"):
            df = pd.concat([df, pd.DataFrame([{}])], ignore_index=True)
            st.session_state[f"{key}.__df"] = df
            st.rerun()
    with cols[1]:
        st.write("")
    with cols[2]:
        st.write(f"**{label}**")

    if _HAS_AGGRID:
        gb = GridOptionsBuilder.from_dataframe(df)
        gb.configure_default_column(editable=True, resizable=True)
        gb.configure_selection("multiple", use_checkbox=True)
        grid = AgGrid(
            df,
            gridOptions=gb.build(),
            data_return_mode=DataReturnMode.AS_INPUT,
            update_mode=GridUpdateMode.MODEL_CHANGED,
            fit_columns_on_grid_load=True,
            key=f"{key}.aggrid",
        )
        new_df = grid["data"]
        selected = grid.get("selected_rows", []) or []

        if st.button("Remove selected", key=f"{key}.remove"):
            if selected and len(new_df) > 0:
                # drop by index match if possible
                sel_idx = {r.get("_selectedRowNodeInfo", {}).get("nodeRowIndex") for r in selected}
                sel_idx = {i for i in sel_idx if isinstance(i, int)}
                if sel_idx:
                    new_df = new_df.drop(list(sel_idx), errors="ignore")
                else:
                    # fallback: drop exact dict rows
                    sel_as_tuples = {tuple(sorted(r.items())) for r in selected}
                    keep_rows = []
                    for _, row in new_df.iterrows():
                        d = row.to_dict()
                        if tuple(sorted(d.items())) not in sel_as_tuples:
                            keep_rows.append(d)
                    new_df = pd.DataFrame(keep_rows)
                st.session_state[f"{key}.__df"] = new_df.reset_index(drop=True)
                st.rerun()

        out_rows = []
        for _, row in new_df.iterrows():
            d = row.to_dict()
            # normalize dates
            for k, v in list(d.items()):
                if isinstance(v, (_dt.date, _dt.datetime)):
                    d[k] = v.date().isoformat() if isinstance(v, _dt.datetime) else v.isoformat()
            out_rows.append(d)
        return out_rows

    # Streamlit editor fallback
    edited = st.data_editor(df, num_rows="dynamic", key=f"{key}.data_editor", use_container_width=True)
    out_rows = []
    for _, row in edited.iterrows():
        d = row.to_dict()
        for k, v in list(d.items()):
            if isinstance(v, (_dt.date, _dt.datetime)):
                d[k] = v.date().isoformat() if isinstance(v, _dt.datetime) else v.isoformat()
        out_rows.append(d)
    return out_rows


# ----------------------------
# Schedule editor (QuantLibSchedule)
# ----------------------------
def schedule_editor(
    *,
    key: str,
    label: str,
    value: dict | None,
    calendar_names: list[str] | None = None,
) -> dict | None:
    """
    Produces a dict matching your QuantLibSchedule schema:

      {
        "dates": ["YYYY-MM-DD", ...],
        "calendar": {"name": "..."},
        "business_day_convention": "Following",
        "termination_business_day_convention": "Following",
        "end_of_month": false,
        "tenor": "6M",
        "rule": "Forward"
      }

    Pydantic will turn this into ql.Schedule via your BeforeValidator.
    """
    enabled = st.toggle(f"Provide explicit schedule for **{label}**", value=(value is not None), key=f"{key}.enabled")
    if not enabled:
        return None

    calendar_names = calendar_names or _available_calendar_names()

    # Seed defaults
    dates_in: list[_dt.date] = []
    cal_name = "TARGET"
    bdc = "Following"
    tbdc = "Following"
    eom = False
    tenor = ""
    rule = "Forward"

    if isinstance(value, dict):
        try:
            dates_in = [_dt.date.fromisoformat(x) for x in (value.get("dates") or []) if isinstance(x, str)]
        except Exception:
            dates_in = []
        cal_spec = value.get("calendar") or {}
        if isinstance(cal_spec, dict) and isinstance(cal_spec.get("name"), str):
            cal_name = cal_spec["name"]
        bdc = value.get("business_day_convention") or bdc
        tbdc = value.get("termination_business_day_convention") or tbdc
        eom = bool(value.get("end_of_month") or False)
        tenor = value.get("tenor") or ""
        rule = value.get("rule") or rule

    with st.expander(f"{label} details", expanded=True):
        cols = st.columns(2)
        with cols[0]:
            cal_name = st.selectbox(
                "Calendar",
                options=calendar_names,
                index=calendar_names.index(cal_name) if cal_name in calendar_names else 0,
                key=f"{key}.calendar",
            )
            bdc = st.selectbox("Business day convention", options=_DEFAULT_BDC, index=_DEFAULT_BDC.index(bdc) if bdc in _DEFAULT_BDC else 0, key=f"{key}.bdc")
            tbdc = st.selectbox("Termination business day convention", options=_DEFAULT_BDC, index=_DEFAULT_BDC.index(tbdc) if tbdc in _DEFAULT_BDC else 0, key=f"{key}.tbdc")
        with cols[1]:
            eom = st.checkbox("End of month", value=eom, key=f"{key}.eom")
            tenor = st.text_input("Tenor (optional, e.g. 6M)", value=tenor, key=f"{key}.tenor")
            rule = st.selectbox("Rule (optional)", options=_DEFAULT_SCHED_RULES, index=_DEFAULT_SCHED_RULES.index(rule) if rule in _DEFAULT_SCHED_RULES else 0, key=f"{key}.rule")

        dates = _grid_edit_dates(
            key=f"{key}.dates",
            value=dates_in,
            label="Dates",
            help_text="These are the exact schedule dates (ISO). Add/remove rows.",
        )

    # Clean + normalize
    dates = sorted({d for d in dates if isinstance(d, _dt.date)})
    if len(dates) < 2:
        st.warning("Schedule should typically contain at least 2 dates (start + end).")

    out = {
        "dates": [d.isoformat() for d in dates],
        "calendar": {"name": cal_name},
        "business_day_convention": bdc,
        "termination_business_day_convention": tbdc,
        "end_of_month": bool(eom),
    }
    if tenor:
        out["tenor"] = tenor
    if rule:
        out["rule"] = rule
    return out


# ----------------------------
# Factory
# ----------------------------
@dataclass
class RenderResult:
    raw_values: dict[str, _t.Any]
    instance: BaseModel | None
    errors: list[dict] | None


class StreamlitModelFormFactory:
    """
    Render Streamlit inputs for a Pydantic model and (optionally) build an instance.

    Strategy:
      - Use model_json_schema() property definitions for UI metadata:
          - description, examples
          - constraints (minimum/maximum)
          - your json_schema_extra fields (quantlib_class, ui, semantic_type, etc.)
      - Use model_fields annotation to detect nested models & lists of models.
    """

    def __init__(
        self,
        *,
        calendar_names: list[str] | None = None,
        day_count_names: list[str] | None = None,
    ):
        self.calendar_names = calendar_names or _available_calendar_names()
        self.day_count_names = day_count_names or _DEFAULT_DCC

    def render(
        self,
        model_cls: type[BaseModel],
        *,
        key_prefix: str,
        initial: dict[str, _t.Any] | None = None,
        build_instance: bool = True,
        show_advanced: bool = True,
    ) -> RenderResult:
        props = _schema_props_safe(model_cls)

        raw: dict[str, _t.Any] = {}
        for fname, finfo in model_cls.model_fields.items():
            prop = props.get(fname, {}) if isinstance(props, dict) else {}
            raw[fname] = self._render_field(
                model_cls=model_cls,
                fname=fname,
                finfo=finfo,
                prop_schema=prop,
                key_prefix=key_prefix,
                initial=initial or {},
                show_advanced=show_advanced,
            )

        instance = None
        errors = None
        if build_instance:
            try:
                instance = model_cls(**raw)
            except ValidationError as e:
                errors = e.errors()

        return RenderResult(raw_values=raw, instance=instance, errors=errors)

    def _render_field(
        self,
        *,
        model_cls: type[BaseModel],
        fname: str,
        finfo: _t.Any,
        prop_schema: dict,
        key_prefix: str,
        initial: dict[str, _t.Any],
        show_advanced: bool,
    ) -> _t.Any:
        key = f"{key_prefix}{fname}"
        label = _field_title(fname, finfo, prop_schema)
        desc = _field_desc(finfo, prop_schema)
        examples = _field_examples(finfo, prop_schema)
        extra = _field_json_extra(finfo)
        ui = prop_schema.get("ui") or extra.get("ui") or {}
        quantlib_class = (
            prop_schema.get("quantlib_class")
            or prop_schema.get("quantlib_enum")
            or extra.get("quantlib_class")
            or extra.get("quantlib_enum")
        )
        semantic_type = prop_schema.get("semantic_type") or extra.get("semantic_type")

        ann_meta = _annotated_metadata(finfo.annotation)
        ann, is_optional = _unwrap_optional(finfo.annotation)
        if not ann_meta:
            ann_meta = _annotated_metadata(ann)
        ann = _unwrap_annotated(ann)
        if not quantlib_class:
            quantlib_class = _infer_quantlib_kind_from_annotation(ann)
        if not quantlib_class:
            for meta in ann_meta:
                m_extra = getattr(meta, "json_schema_extra", None)
                if isinstance(m_extra, dict):
                    quantlib_class = (
                        m_extra.get("quantlib_class")
                        or m_extra.get("quantlib_enum")
                        or quantlib_class
                    )

        # Determine default
        has_initial = fname in initial
        default = initial.get(fname) if has_initial else None
        if not has_initial:
            # Pydantic v2: if required -> no default; else finfo.default/factory exists
            try:
                if not finfo.is_required():
                    if finfo.default_factory is not None:
                        default = finfo.default_factory()
                    else:
                        default = finfo.default
            except Exception:
                pass

        # Optional toggle UX (for None-able fields)
        if is_optional:
            enabled_default = default is not None
            enabled = st.toggle(f"Set **{label}**", value=enabled_default, key=f"{key}.enabled")
            if not enabled:
                return None

        # QuantLib-like special handling via schema tags
        if quantlib_class == "Schedule" or semantic_type == "schedule":
            return schedule_editor(
                key=key,
                label=label,
                value=default if isinstance(default, dict) else None,
                calendar_names=self.calendar_names,
            )

        if quantlib_class == "Calendar":
            # store as {"name": "..."} (your validator supports dict or str)
            cal_default = None
            if isinstance(default, dict) and isinstance(default.get("name"), str):
                cal_default = default["name"]
            elif isinstance(default, str):
                cal_default = default
            cal_default = cal_default or "TARGET"

            chosen = st.selectbox(
                label,
                options=self.calendar_names,
                index=self.calendar_names.index(cal_default) if cal_default in self.calendar_names else 0,
                help=desc,
                key=key,
            )
            return {"name": chosen}

        if quantlib_class == "DayCounter":
            dflt = default if isinstance(default, str) else (examples[0] if examples else self.day_count_names[0])
            chosen = st.selectbox(label, options=self.day_count_names, index=self.day_count_names.index(dflt) if dflt in self.day_count_names else 0, help=desc, key=key)
            return chosen

        if quantlib_class == "Period":
            dflt = default if isinstance(default, str) else (examples[0] if examples else "")
            return st.text_input(label, value=dflt, help=desc, key=key)

        if quantlib_class == "BusinessDayConvention":
            dflt = default if isinstance(default, str) else (examples[0] if examples else "Following")
            idx = _DEFAULT_BDC.index(dflt) if dflt in _DEFAULT_BDC else 0
            return st.selectbox(label, options=_DEFAULT_BDC, index=idx, help=desc, key=key)

        # Nested model
        if _is_pydantic_model(ann):
            with st.expander(label, expanded=False):
                sub = self.render(
                    ann,
                    key_prefix=f"{key}.",
                    initial=default if isinstance(default, dict) else None,
                    build_instance=False,
                    show_advanced=show_advanced,
                )
            return sub.raw_values

        # Lists / arrays
        origin = _t.get_origin(ann)
        if origin in (list, list):
            (item_tp,) = _t.get_args(ann) if _t.get_args(ann) else (None,)
            item_tp = _unwrap_annotated(item_tp)

            # list[BaseModel]
            if _is_pydantic_model(item_tp):
                return _grid_edit_model_list(
                    item_model=item_tp,
                    key=key,
                    value=default if isinstance(default, list) else None,
                    label=label,
                    help_text=desc,
                )

            # list of scalars: use text area (simple + robust)
            dflt_list = default if isinstance(default, list) else []
            txt_default = "\n".join([str(x) for x in dflt_list]) if dflt_list else ""
            txt = st.text_area(
                label,
                value=txt_default,
                help=(desc + "\nOne value per line.") if desc else "One value per line.",
                key=key,
            )
            items = [x.strip() for x in txt.splitlines() if x.strip()]
            # Let Pydantic handle conversion as much as possible, but do light parsing for numbers
            if item_tp in (int, float):
                out_num = []
                for x in items:
                    try:
                        out_num.append(item_tp(x))  # type: ignore[misc]
                    except Exception:
                        out_num.append(x)
                return out_num
            return items

        # Dates
        if ann is _dt.date or prop_schema.get("format") == "date":
            dflt = default if isinstance(default, _dt.date) else _dt.date.today()
            return st.date_input(label, value=dflt, help=desc, key=key)

        # Booleans
        if ann is bool:
            dflt = bool(default) if default is not None else False
            return st.checkbox(label, value=dflt, help=desc, key=key)

        # Enums from schema
        if "enum" in prop_schema and isinstance(prop_schema["enum"], list):
            enum_vals = prop_schema["enum"]
            dflt = default if default in enum_vals else (enum_vals[0] if enum_vals else None)
            return st.selectbox(label, options=enum_vals, index=enum_vals.index(dflt) if dflt in enum_vals else 0, help=desc, key=key)
        if isinstance(ann, type) and issubclass(ann, Enum):
            enum_vals = [x.value for x in ann]  # type: ignore[arg-type]
            dflt = default if default in enum_vals else (enum_vals[0] if enum_vals else None)
            return st.selectbox(label, options=enum_vals, index=enum_vals.index(dflt) if dflt in enum_vals else 0, help=desc, key=key)

        # Numbers (with percent UI hint)
        if ann in (int, float) or prop_schema.get("type") in ("integer", "number"):
            ui_format = (ui or {}).get("format")
            scale = float((ui or {}).get("scale", 1.0) or 1.0)

            # constraints
            minv = prop_schema.get("minimum")
            maxv = prop_schema.get("maximum")
            # exclusiveMinimum/exclusiveMaximum exist in JSON schema; Streamlit doesn't support exclusivity cleanly.
            if minv is None and "exclusiveMinimum" in prop_schema:
                minv = prop_schema.get("exclusiveMinimum")
            if maxv is None and "exclusiveMaximum" in prop_schema:
                maxv = prop_schema.get("exclusiveMaximum")

            # seed default from examples if missing
            if default is None and examples:
                default = examples[0]

            # percent display
            if ui_format == "percent":
                # stored -> shown
                stored_default = float(default) if default is not None else 0.0
                shown_default = stored_default * scale
                shown_min = (float(minv) * scale) if minv is not None else None
                shown_max = (float(maxv) * scale) if maxv is not None else None

                shown = st.number_input(
                    f"{label} (%)",
                    value=float(shown_default),
                    min_value=float(shown_min) if shown_min is not None else None,
                    max_value=float(shown_max) if shown_max is not None else None,
                    help=desc,
                    key=key,
                )
                # shown -> stored
                stored = float(shown) / scale
                return int(stored) if ann is int else float(stored)

            # normal numeric
            if ann is int or prop_schema.get("type") == "integer":
                dflt = int(default) if default is not None else 0
                return st.number_input(
                    label,
                    value=int(dflt),
                    min_value=int(minv) if minv is not None else None,
                    max_value=int(maxv) if maxv is not None else None,
                    step=1,
                    help=desc,
                    key=key,
                )

            dflt = float(default) if default is not None else 0.0
            return st.number_input(
                label,
                value=float(dflt),
                min_value=float(minv) if minv is not None else None,
                max_value=float(maxv) if maxv is not None else None,
                help=desc,
                key=key,
            )

        # Strings (default)
        dflt = default if isinstance(default, str) else (examples[0] if examples else "")
        return st.text_input(label, value=dflt, help=desc, key=key)
