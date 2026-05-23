from __future__ import annotations

import datetime as dt
import json
import os
import time
from dataclasses import dataclass
from importlib import resources
from typing import TYPE_CHECKING, Any

import pandas as pd
import requests

if TYPE_CHECKING:
    from ..simple_tables import AssetSimpleTable
    from .simple_tables import OpenFigiDetails

OPENFIGI_MAPPING_URL = "https://api.openfigi.com/v3/mapping"
OPENFIGI_SEARCH_URL = "https://api.openfigi.com/v3/search"
OPENFIGI_API_KEY_ENV = "FIGI_API_KEY"
OPENFIGI_API_URL_ENV = "FIGI_API_URL"


@dataclass(frozen=True)
class OpenFigiAssetRows:
    """Client-owned rows derived from one OpenFIGI result."""

    asset: AssetSimpleTable
    open_figi_details: OpenFigiDetails
    snapshot_frame: pd.DataFrame


def normalize_openfigi_result(item: dict[str, Any]) -> dict[str, Any]:
    """Normalize one OpenFIGI response row to SDK snake_case fields."""

    return {
        "unique_identifier": item.get("figi"),
        "figi": item.get("figi"),
        "composite": item.get("compositeFIGI"),
        "share_class": item.get("shareClassFIGI"),
        "isin": item.get("isin"),
        "ticker": item.get("ticker"),
        "name": item.get("name"),
        "exchange_code": item.get("exchCode"),
        "security_type": item.get("securityType"),
        "security_type_2": item.get("securityType2"),
        "security_market_sector": item.get("marketSector"),
        "security_description": item.get("securityDescription"),
        "unique_id": item.get("uniqueID"),
        "unique_id_fut_opt": item.get("uniqueIDFutOpt"),
        "metadata": item.get("metadata"),
        "raw_payload": dict(item),
    }


def build_asset_rows_from_openfigi_result(
    item: dict[str, Any],
    *,
    asset_id: int | None = None,
    time_index: dt.datetime | pd.Timestamp | None = None,
) -> OpenFigiAssetRows:
    """Build client-owned asset rows from one OpenFIGI result.

    `asset_id` is the backend-managed row id returned after inserting or reading
    `AssetSimpleTable`. It is required before inserting `OpenFigiDetails`.
    """

    from ..simple_tables import AssetSimpleTable
    from .simple_tables import OpenFigiDetails

    normalized = normalize_openfigi_result(item)
    unique_identifier = normalized.get("unique_identifier")
    if not unique_identifier:
        raise ValueError("OpenFIGI result does not include `figi`.")
    if asset_id is None:
        raise ValueError("asset_id is required to build OpenFigiDetails.")

    asset = AssetSimpleTable(id=asset_id, unique_identifier=unique_identifier)
    open_figi_details = OpenFigiDetails(
        asset_id=asset_id,
        figi=normalized.get("figi"),
        composite=normalized.get("composite"),
        share_class=normalized.get("share_class"),
        isin=normalized.get("isin"),
        ticker=normalized.get("ticker"),
        name=normalized.get("name"),
        exchange_code=normalized.get("exchange_code"),
        security_type=normalized.get("security_type"),
        security_type_2=normalized.get("security_type_2"),
        security_market_sector=normalized.get("security_market_sector"),
        security_description=normalized.get("security_description"),
        unique_id=normalized.get("unique_id"),
        unique_id_fut_opt=normalized.get("unique_id_fut_opt"),
        metadata=normalized.get("metadata"),
        raw_payload=normalized.get("raw_payload"),
    )
    snapshot_frame = build_asset_snapshot_frame_from_openfigi_result(
        normalized,
        time_index=time_index,
    )
    return OpenFigiAssetRows(
        asset=asset,
        open_figi_details=open_figi_details,
        snapshot_frame=snapshot_frame,
    )


def build_asset_snapshot_frame_from_openfigi_result(
    item: dict[str, Any],
    *,
    time_index: dt.datetime | pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Build one AssetSnapshot DataNode frame row from an OpenFIGI result."""

    from ..data_nodes import AssetSnapshot

    normalized = (
        item
        if "unique_identifier" in item
        else normalize_openfigi_result(item)
    )
    unique_identifier = normalized.get("unique_identifier")
    if not unique_identifier:
        raise ValueError("OpenFIGI result does not include `figi`.")

    effective_time = time_index or dt.datetime.now(dt.UTC)
    frame = pd.DataFrame(
        [
            {
                "time_index": effective_time,
                "unique_identifier": unique_identifier,
                "name": normalized.get("name") or "",
                "ticker": normalized.get("ticker") or "",
                "exchange_code": normalized.get("exchange_code") or "",
                "asset_ticker_group_id": normalized.get("share_class") or "",
                "venue_specific_properties": {
                    "openfigi": {
                        key: value
                        for key, value in normalized.items()
                        if key not in {"raw_payload"}
                    }
                },
            }
        ]
    )
    return AssetSnapshot.validate_frame(frame)


def search_figi(
    query: str,
    *,
    market_sector: str = "All",
    exch_code: str | None = None,
    security_type: str | None = None,
    security_type_2: str | None = None,
    include_unlisted_equities: bool = False,
    api_key: str | None = None,
    api_url: str | None = None,
    safety_buffer: int = 2,
) -> list[dict[str, Any]]:
    """Search OpenFIGI and return normalized response rows."""

    headers = _openfigi_headers(api_key=api_key)
    payload: dict[str, Any] = {
        "query": query,
        "marketSecDes": market_sector,
        "includeUnlistedEquities": include_unlisted_equities,
    }
    if exch_code:
        payload["exchCode"] = exch_code
    if security_type:
        payload["securityType"] = security_type
    if security_type_2:
        payload["securityType2"] = security_type_2

    url = api_url or os.getenv(OPENFIGI_API_URL_ENV) or OPENFIGI_SEARCH_URL
    return _paged_openfigi_search(
        url=url,
        headers=headers,
        payload=payload,
        safety_buffer=safety_buffer,
    )


def query_figi(
    tickers: list[str],
    *,
    market_sector: str,
    exch_code: str | None = None,
    security_type: str | None = None,
    security_type_2: str | None = None,
    api_key: str | None = None,
    api_url: str | None = None,
    safety_buffer: int = 2,
) -> list[dict[str, Any]]:
    """Map ticker values through OpenFIGI and return normalized rows."""

    payload_items: list[dict[str, Any]] = []
    for ticker in tickers:
        item = {
            "idType": "TICKER",
            "idValue": ticker,
            "marketSecDes": market_sector,
        }
        if exch_code:
            item["exchCode"] = exch_code
        if security_type:
            item["securityType"] = security_type
        if security_type_2:
            item["securityType2"] = security_type_2
        payload_items.append(item)

    return _openfigi_mapping_batches(
        payload_items,
        api_key=api_key,
        api_url=api_url,
        safety_buffer=safety_buffer,
    )


def query_by_figi(
    figi_code: str,
    *,
    api_key: str | None = None,
    api_url: str | None = None,
) -> dict[str, Any]:
    """Resolve one FIGI through OpenFIGI."""

    rows = _openfigi_mapping_batches(
        [{"idType": "ID_BB_GLOBAL", "idValue": figi_code}],
        api_key=api_key,
        api_url=api_url,
    )
    if len(rows) != 1:
        raise ValueError(f"Expected one OpenFIGI row for FIGI {figi_code!r}, got {len(rows)}.")
    return rows[0]


def query_by_isin(
    isin_code: str,
    exchange_code: str,
    *,
    api_key: str | None = None,
    api_url: str | None = None,
) -> dict[str, Any]:
    """Resolve one ISIN/exchange pair through OpenFIGI."""

    rows = _openfigi_mapping_batches(
        [
            {
                "idType": "ID_ISIN",
                "idValue": isin_code,
                "exchCode": exchange_code,
            }
        ],
        api_key=api_key,
        api_url=api_url,
    )
    if len(rows) != 1:
        raise ValueError(
            "Expected one OpenFIGI row for "
            f"ISIN {isin_code!r}/{exchange_code!r}, got {len(rows)}."
        )
    return rows[0]


def load_openfigi_lists() -> dict[str, list[Any]]:
    """Load packaged OpenFIGI definition lists."""

    base = resources.files(__package__) / "open_figi_lists"
    lists: dict[str, list[Any]] = {}
    for name in ("security_type", "security_type_2", "market_sector"):
        path = base / f"{name}.json"
        with path.open("r", encoding="utf-8") as handle:
            lists[name] = json.load(handle)
    return lists


def get_open_figi_definitions() -> dict[str, list[Any]]:
    return load_openfigi_lists()


def _openfigi_headers(*, api_key: str | None = None) -> dict[str, str]:
    resolved_api_key = api_key or os.getenv(OPENFIGI_API_KEY_ENV)
    if not resolved_api_key:
        raise ValueError(f"{OPENFIGI_API_KEY_ENV} is required for OpenFIGI requests.")
    return {
        "Content-Type": "application/json",
        "X-OPENFIGI-APIKEY": resolved_api_key,
    }


def _openfigi_mapping_batches(
    payload_items: list[dict[str, Any]],
    *,
    api_key: str | None = None,
    api_url: str | None = None,
    safety_buffer: int = 2,
) -> list[dict[str, Any]]:
    headers = _openfigi_headers(api_key=api_key)
    url = api_url or os.getenv(OPENFIGI_API_URL_ENV) or OPENFIGI_MAPPING_URL
    min_interval = 6.0 / 25.0
    last_call = 0.0
    rows: list[dict[str, Any]] = []

    for start in range(0, len(payload_items), 100):
        batch = payload_items[start:start + 100]
        elapsed = time.time() - last_call
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

        while True:
            response = requests.post(url, headers=headers, json=batch)
            now = time.time()
            if response.status_code == 429:
                time.sleep(_rate_limit_wait(response, default=min_interval) + 0.1)
                continue
            response.raise_for_status()
            last_call = now
            break

        for result in response.json():
            for item in result.get("data", []):
                rows.append(normalize_openfigi_result(item))

        remaining = response.headers.get("X-RateLimit-Remaining") or response.headers.get(
            "ratelimit-remaining"
        )
        if remaining is not None and int(remaining) <= safety_buffer:
            time.sleep(_rate_limit_wait(response, default=min_interval) + 0.1)

    return rows


def _paged_openfigi_search(
    *,
    url: str,
    headers: dict[str, str],
    payload: dict[str, Any],
    safety_buffer: int,
) -> list[dict[str, Any]]:
    min_interval = 6.0 / 25.0
    last_call_time = 0.0
    rows: list[dict[str, Any]] = []

    while True:
        elapsed = time.time() - last_call_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

        response = requests.post(url, headers=headers, json=payload)
        now = time.time()
        if response.status_code == 429:
            time.sleep(_rate_limit_wait(response, default=min_interval) + 0.1)
            continue
        response.raise_for_status()
        last_call_time = now

        response_data = response.json()
        rows.extend(
            normalize_openfigi_result(item)
            for item in response_data.get("data", [])
        )

        next_cursor = response_data.get("next")
        if not next_cursor:
            break
        payload["start"] = next_cursor

        remaining = response.headers.get("X-RateLimit-Remaining")
        if remaining is not None and int(remaining) <= safety_buffer:
            time.sleep(_rate_limit_wait(response, default=min_interval) + 0.1)

    return rows


def _rate_limit_wait(response: requests.Response, *, default: float) -> float:
    reset = response.headers.get("X-RateLimit-Reset") or response.headers.get(
        "ratelimit-reset"
    )
    return float(reset) if reset else default


__all__ = [
    "OPENFIGI_API_KEY_ENV",
    "OPENFIGI_API_URL_ENV",
    "OPENFIGI_MAPPING_URL",
    "OPENFIGI_SEARCH_URL",
    "OpenFigiAssetRows",
    "build_asset_rows_from_openfigi_result",
    "build_asset_snapshot_frame_from_openfigi_result",
    "get_open_figi_definitions",
    "load_openfigi_lists",
    "normalize_openfigi_result",
    "query_by_figi",
    "query_by_isin",
    "query_figi",
    "search_figi",
]
