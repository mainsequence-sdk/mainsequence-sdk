# dashboards/streamlit/components/asset_select.py
"""
import mainsequence.dashboards.streamlit.components.asset_select


"""
from __future__ import annotations

from typing import Any

import streamlit as st

import mainsequence.client as msc


@st.cache_data(show_spinner=False)
def _search_assets(q: str) -> list[dict[str, Any]]:
    """
    Search MainSequence assets by current_snapshot.name or unique_identifier.
    Returns [{'id', 'label', 'instance'}] with unique IDs.
    """
    if not q or len(q.strip()) < 3:
        return []
    out: list[dict[str, Any]] = []
    seen = set()

    try:
        by_name = msc.Asset.filter(current_snapshot__name__contains=q.strip())
    except Exception:
        by_name = []

    try:
        by_uid = msc.Asset.filter(unique_identifier__contains=q.strip())
    except Exception:
        by_uid = []

    for a in list(by_name or []) + list(by_uid or []):
        try:
            aid = getattr(a, "id", None)
            if aid is None or aid in seen:
                continue
            uid = getattr(a, "unique_identifier", None)
            snap = getattr(getattr(a, "current_snapshot", None), "name", None)
            label = f"{uid or '—'} · {snap or '—'} (id={aid})"
            out.append({"id": aid, "label": label, "instance": a})
            seen.add(aid)
        except Exception:
            continue
    return out


def sidebar_asset_single_select(
    *,
    title: str = "Find asset",
    key_prefix: str = "asset_select",
    min_chars: int = 3,
) -> Any | None:
    """
    Streamlit sidebar widget:
      - Text search (cached)
      - Single-select of results
      - Returns the loaded Asset instance and keeps it in session until cleared.
      - Only changes the loaded asset when "Load asset" is pressed.
    Session keys:
      • {key_prefix}_loaded_id
      • {key_prefix}_loaded_instance
      • {key_prefix}_q, {key_prefix}_results, {key_prefix}_sel  (UI state)
    """
    loaded_id_key = f"{key_prefix}_loaded_id"
    loaded_inst_key = f"{key_prefix}_loaded_instance"
    results_key = f"{key_prefix}_results"
    select_key = f"{key_prefix}_sel"
    query_key = f"{key_prefix}_q"

    with st.sidebar:
        st.markdown(f"### {title}")
        st.caption(f"Type at least **{min_chars}** characters to search.")

        # Show the currently loaded asset (if any)
        loaded_id = st.session_state.get(loaded_id_key)
        loaded_inst = st.session_state.get(loaded_inst_key)
        if loaded_inst is not None:
            try:
                uid = getattr(loaded_inst, "unique_identifier", None)
                snap = getattr(getattr(loaded_inst, "current_snapshot", None), "name", None)
                st.caption(f"Loaded asset: **{uid or '—'} · {snap or '—'} (id={loaded_id})**")
            except Exception:
                st.caption(f"Loaded asset: id={loaded_id}")

        # Search box
        def _do_search_from_state(prefix: str, min_chars_local: int) -> None:
            q_local = st.session_state.get(f"{prefix}_q", "")
            if q_local and len(q_local.strip()) >= min_chars_local:
                st.session_state[f"{prefix}_results"] = _search_assets(q_local.strip())
            else:
                st.session_state.pop(f"{prefix}_results", None)

        q = st.text_input(
            "Search assets",
            placeholder="e.g. BONOS 2030  or  MXN:... UID",
            key=query_key,
            on_change=_do_search_from_state,
            args=(key_prefix, min_chars),
        )

        if st.button("Search", key=f"{key_prefix}_btn") and q and len(q.strip()) >= min_chars:
            st.session_state[results_key] = _search_assets(q.strip())

        assets = st.session_state.get(results_key, [])
        if q and len(q.strip()) < min_chars:
            st.caption(f"Keep typing… need **{min_chars}+** characters to search.")

        if assets:
            id_to_label = {a["id"]: a["label"] for a in assets}
            id_to_inst = {a["id"]: a["instance"] for a in assets}
            # Preselect currently loaded id if visible in the options
            default_index = 0
            options = list(id_to_label.keys())
            if loaded_id in options:
                default_index = options.index(loaded_id)

            selected_id = st.selectbox(
                "Select an asset",
                options=options,
                index=default_index if options else 0,
                format_func=lambda x: id_to_label.get(x, str(x)),
                key=select_key,
            )

            col_load, col_clear = st.columns([3, 2])
            with col_load:
                if st.button(
                    "Load asset", type="primary", width='stretch', key=f"{key_prefix}_load"
                ):
                    inst = id_to_inst.get(selected_id)
                    # Persist loaded selection
                    st.session_state[loaded_id_key] = (
                        int(selected_id) if selected_id is not None else None
                    )
                    st.session_state[loaded_inst_key] = inst
                    return inst

            with col_clear:
                if st.button("Clear", width='stretch', key=f"{key_prefix}_clear"):
                    # Clear UI + loaded asset
                    st.session_state.pop(query_key, None)
                    st.session_state.pop(results_key, None)
                    st.session_state.pop(select_key, None)
                    st.session_state.pop(loaded_id_key, None)
                    st.session_state.pop(loaded_inst_key, None)
                    st.rerun()

    # Persisted behavior: if an asset is already loaded, return it on every rerun
    loaded_inst = st.session_state.get(loaded_inst_key)
    if loaded_inst is not None:
        return loaded_inst

    return None
