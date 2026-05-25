from __future__ import annotations

import datetime
import json
from decimal import Decimal

import mainsequence.client as client_mod
import mainsequence.client.base as base_mod
import mainsequence.markets.client.models as models_mod
import mainsequence.markets.client.models.accounts_and_portfolios as accounts_mod
from mainsequence.markets.client.models import (
    Account,
    AccountTargetPosition,
    VirtualFundHistoricalHoldings,
)


class _FakeResponse:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload
        self.content = b"{}"
        self.text = str(payload)

    def json(self):
        return self._payload


def _account_payload(uid: str) -> dict:
    return {
        "uid": uid,
        "account_name": "Main Account",
        "account_is_active": True,
        "holdings_data_source": {
            "uid": "11111111-1111-4111-8111-111111111111",
            "storage_hash": "account_holdings_hash",
            "identifier": "examples.account_holdings",
        },
        "labels": ["production"],
        "latest_holdings": {
            "holdings_set_uid": "665f8a92-4c7a-4c01-9a28-15744b31abbb",
            "is_trade_snapshot": False,
            "target_trade_time": None,
            "comments": None,
            "holdings_date": "2026-05-20T09:30:00Z",
            "holdings": [
                {
                    "time_index": "2026-05-20T09:30:00Z",
                    "unique_identifier": "ASSET:BTC",
                    "quantity": "2.000000000000000000",
                    "target_trade_time": "2026-05-20T09:35:00Z",
                    "extra_details": {"source": "api"},
                }
            ],
        },
        "is_paper": True,
    }


def test_account_model_matches_uid_based_server_contract():
    account = Account(**_account_payload("cbace713-dff7-42aa-a2d3-252ca7849ade"))

    assert account.uid == "cbace713-dff7-42aa-a2d3-252ca7849ade"
    assert "execution_venue" not in Account.model_fields
    assert account.holdings_data_source is not None
    assert account.holdings_data_source["uid"] == "11111111-1111-4111-8111-111111111111"
    assert account.latest_holdings is not None
    assert account.latest_holdings.comments is None
    assert account.latest_holdings.holdings[0].unique_identifier == "ASSET:BTC"
    assert account.latest_holdings.holdings[0].quantity == Decimal("2.000000000000000000")


def test_account_get_uses_uid_detail_route(monkeypatch):
    captured: dict[str, object] = {}
    uid = "cbace713-dff7-42aa-a2d3-252ca7849ade"

    def fake_make_request(*, s, loaders, r_type, url, payload, time_out):
        captured["url"] = url
        captured["r_type"] = r_type
        captured["payload"] = payload
        return _FakeResponse(200, _account_payload(uid))

    monkeypatch.setattr(base_mod, "make_request", fake_make_request)

    account = Account.get(uid=uid)

    assert account.uid == uid
    assert captured["r_type"] == "GET"
    assert str(captured["url"]).endswith(f"/orm/api/assets/account/{uid}/")


def test_account_get_holdings_uses_collection_route(monkeypatch):
    account = Account(**_account_payload("cbace713-dff7-42aa-a2d3-252ca7849ade"))
    captured: dict[str, object] = {}

    def fake_make_request(*, s, loaders, r_type, url, payload, time_out):
        captured["url"] = url
        captured["r_type"] = r_type
        captured["payload"] = payload
        return _FakeResponse(
            200,
            [
                {
                    "snapshot_uid": "dff6f5fb-8f0d-46f8-a397-2aa7d6e5f4fd",
                    "holdings_set_uid": "665f8a92-4c7a-4c01-9a28-15744b31abbb",
                    "holdings_date": "2026-05-20T09:30:00Z",
                    "related_account_uid": account.uid,
                    "is_trade_snapshot": False,
                    "target_trade_time": None,
                    "comments": None,
                    "holdings": [
                        {
                            "time_index": "2026-05-20T09:30:00Z",
                            "unique_identifier": "ASSET:BTC",
                            "quantity": "2.000000000000000000",
                            "target_trade_time": "2026-05-20T09:35:00Z",
                            "extra_details": {"source": "api"},
                        }
                    ],
                }
            ],
        )

    monkeypatch.setattr(accounts_mod, "make_request", fake_make_request)

    snapshots = account.get_holdings(order="desc", limit=1, include_asset_detail=False)

    assert len(snapshots) == 1
    assert snapshots[0].related_account_uid == account.uid
    assert snapshots[0].comments is None
    assert snapshots[0].holdings[0].quantity == Decimal("2.000000000000000000")
    assert str(captured["url"]).endswith(f"/orm/api/assets/account/{account.uid}/holdings/")
    assert captured["payload"] == {
        "params": {
            "order": "desc",
            "include_asset_detail": "false",
            "limit": 1,
        }
    }


def test_account_add_target_positions_posts_canonical_payload(monkeypatch):
    account = Account(**_account_payload("cbace713-dff7-42aa-a2d3-252ca7849ade"))
    captured: dict[str, object] = {}

    def fake_make_request(*, s, loaders, r_type, url, payload, time_out):
        captured["url"] = url
        captured["r_type"] = r_type
        captured["payload"] = payload
        return _FakeResponse(
            201,
            {
                "related_account_uid": account.uid,
                "target_positions_date": "2026-05-20T09:30:00Z",
                "position_set_uid": "86b20a07-e5c8-4c53-bd52-7e40ef2fc772",
                "positions": [
                    {
                        "unique_identifier": "ASSET:BTC",
                        "weight_notional_exposure": "0.550000000000000000",
                        "constant_notional_exposure": None,
                        "single_asset_quantity": None,
                    }
                ],
            },
        )

    monkeypatch.setattr(accounts_mod, "make_request", fake_make_request)

    response = account.add_target_positions(
        target_positions_date="2026-05-20T09:30:00Z",
        overwrite=False,
        positions=[
            AccountTargetPosition(
                unique_identifier="ASSET:BTC",
                weight_notional_exposure=Decimal("0.55"),
            )
        ],
    )

    assert response.related_account_uid == account.uid
    assert response.positions[0].weight_notional_exposure == Decimal("0.550000000000000000")
    assert str(captured["url"]).endswith(
        f"/orm/api/assets/account/{account.uid}/add-target-positions/"
    )
    assert captured["r_type"] == "POST"


def test_account_add_holdings_serializes_decimal_dict_positions(monkeypatch):
    account = Account(**_account_payload("cbace713-dff7-42aa-a2d3-252ca7849ade"))
    captured: dict[str, object] = {}

    def fake_make_request(*, s, loaders, r_type, url, payload, time_out):
        json.dumps(payload["json"], allow_nan=False)
        captured["url"] = url
        captured["r_type"] = r_type
        captured["payload"] = payload
        return _FakeResponse(
            201,
            {
                "related_account_uid": account.uid,
                "holdings_date": "2026-05-20T09:30:00Z",
                "holdings_set_uid": "665f8a92-e5c8-4c53-bd52-7e40ef2fc772",
                "positions": [
                    {
                        "unique_identifier": "ASSET:BTC",
                        "quantity": "1.234567890123456789",
                        "target_trade_time": "2026-05-20T09:30:00Z",
                        "extra_details": {},
                    }
                ],
            },
        )

    monkeypatch.setattr(accounts_mod, "make_request", fake_make_request)

    response = account.add_holdings(
        holdings_date=datetime.datetime(2026, 5, 20, 9, 30, tzinfo=datetime.UTC),
        overwrite=False,
        positions=[
            {
                "unique_identifier": "ASSET:BTC",
                "quantity": Decimal("1.234567890123456789"),
                "target_trade_time": datetime.datetime(2026, 5, 20, 9, 30, tzinfo=datetime.UTC),
                "extra_details": {},
            }
        ],
    )

    payload_json = captured["payload"]["json"]
    assert payload_json["positions"][0]["quantity"] == "1.234567890123456789"
    assert payload_json["positions"][0]["target_trade_time"] == "2026-05-20T09:30:00Z"
    assert response.positions[0].quantity == Decimal("1.234567890123456789")
    assert str(captured["url"]).endswith(f"/orm/api/assets/account/{account.uid}/add-holdings/")
    assert captured["r_type"] == "POST"


def test_virtual_fund_historical_holdings_matches_canonical_contract():
    snapshot = VirtualFundHistoricalHoldings(
        snapshot_uid="dff6f5fb-8f0d-46f8-a397-2aa7d6e5f4fd",
        holdings_set_uid="665f8a92-4c7a-4c01-9a28-15744b31abbb",
        holdings_date=datetime.datetime(2026, 5, 20, 9, 30, tzinfo=datetime.UTC),
        related_fund_uid="cbace713-dff7-42aa-a2d3-252ca7849ade",
        is_trade_snapshot=False,
        target_trade_time=None,
        holdings=[
            {
                "time_index": "2026-05-20T09:30:00Z",
                "unique_identifier": "ASSET:BTC",
                "quantity": "2.000000000000000000",
                "target_weight": "0.500000000000000000",
                "target_trade_time": "2026-05-20T09:35:00Z",
                "extra_details": {"source": "api"},
            }
        ],
    )

    assert snapshot.related_fund_uid == "cbace713-dff7-42aa-a2d3-252ca7849ade"
    assert snapshot.holdings[0].target_weight == Decimal("0.500000000000000000")


def test_removed_legacy_account_objects_are_not_exposed():
    assert not hasattr(client_mod, "ExecutionVenue")
    assert not hasattr(models_mod, "ExecutionVenue")
    assert not hasattr(models_mod, "AccountHistoricalHoldings")
    assert not hasattr(models_mod, "AccountPositionDetail")
    assert not hasattr(models_mod, "AccountRiskFactors")
    assert not hasattr(models_mod, "AccountExecutionConfiguration")
    assert not hasattr(models_mod, "AccountPortfolio")
    assert not hasattr(models_mod, "VirtualFundPositionDetail")
