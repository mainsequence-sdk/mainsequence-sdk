from __future__ import annotations

from sqlalchemy import select

from examples.meta_tables.common import (
    DEFAULT_TIMEOUT,
    PLACEHOLDER_ACCOUNT_META_TABLE_UID,
    PLACEHOLDER_ACCOUNT_ROW_UID,
    PLACEHOLDER_ASSET_META_TABLE_UID,
    env_flag,
    optional_env,
    print_json,
)
from mainsequence.client import MetaTable
from mainsequence.meta_tables.compiled_sql.v1 import compile_sqlalchemy_statement


def load_models(*, mode: str):
    if mode in {"external_managed", "external_registered"}:
        from examples.meta_tables.external_managed.account_asset import Account, Asset

        return Account, Asset
    if mode == "platform_managed":
        from examples.meta_tables.platform_managed.account_asset import Account, Asset

        return Account, Asset
    raise ValueError(
        "MAINSEQUENCE_META_TABLE_EXAMPLE_MODE must be platform_managed or external_managed."
    )


def build_asset_search_operation(
    *,
    account_model,
    asset_model,
    asset_meta_table_uid: str,
    account_meta_table_uid: str,
    account_row_uid: str,
    symbol_pattern: str,
):
    stmt = (
        select(asset_model.uid, asset_model.symbol)
        .join(account_model, asset_model.account_uid == account_model.uid)
        .where(account_model.uid == account_row_uid)
        .where(asset_model.symbol.ilike(symbol_pattern))
    )

    return compile_sqlalchemy_statement(
        stmt,
        operation="select",
        scope_tables=[
            {
                "metaTableUid": asset_meta_table_uid,
                "alias": "asset",
                "access": "read",
            },
            {
                "metaTableUid": account_meta_table_uid,
                "alias": "account",
                "access": "read",
            },
        ],
        limits={"max_rows": 1000, "statement_timeout_ms": 15000},
    )


def main() -> None:
    mode = optional_env("MAINSEQUENCE_META_TABLE_EXAMPLE_MODE", "platform_managed")
    account_model, asset_model = load_models(mode=mode)
    operation = build_asset_search_operation(
        account_model=account_model,
        asset_model=asset_model,
        asset_meta_table_uid=optional_env(
            "MAINSEQUENCE_META_TABLE_ASSET_UID",
            PLACEHOLDER_ASSET_META_TABLE_UID,
        ),
        account_meta_table_uid=optional_env(
            "MAINSEQUENCE_META_TABLE_ACCOUNT_UID",
            PLACEHOLDER_ACCOUNT_META_TABLE_UID,
        ),
        account_row_uid=optional_env(
            "MAINSEQUENCE_META_TABLE_ACCOUNT_ROW_UID",
            PLACEHOLDER_ACCOUNT_ROW_UID,
        ),
        symbol_pattern=optional_env("MAINSEQUENCE_META_TABLE_SYMBOL_PATTERN", "%BTC%"),
    )
    print_json(f"compiled-sql.v1 operation ({mode})", operation)

    if not env_flag("MAINSEQUENCE_META_TABLE_EXECUTE", default=False):
        print("\nDry run only. Set MAINSEQUENCE_META_TABLE_EXECUTE=1 to send it.")
        return

    result = MetaTable.execute_operation(operation, timeout=DEFAULT_TIMEOUT)
    print_json("Execution result", result)


if __name__ == "__main__":
    main()
