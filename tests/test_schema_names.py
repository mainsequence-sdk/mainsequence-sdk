from __future__ import annotations

from sqlalchemy import Column, ForeignKey, Index, Integer, MetaData, String, Table, UniqueConstraint

from mainsequence.meta_tables import (
    POSTGRES_IDENTIFIER_MAX_LENGTH,
    bounded_identifier,
    parse_schema_table_name,
    schema_foreign_key_name,
    schema_index_name,
    schema_table_name,
    sqlalchemy_naming_convention,
)


def test_schema_table_name_normalizes_project_prefixed_parts() -> None:
    assert schema_table_name("SDK Examples", "Account Holdings") == (
        "sdk_examples__account_holdings"
    )
    assert schema_table_name("ms-markets", "Asset", suffix="mainsequence.examples") == (
        "ms_markets__asset__mainsequence_examples"
    )


def test_schema_table_name_truncates_with_stable_project_prefix() -> None:
    table_name = schema_table_name(
        "sdk_examples",
        "very_long_concept_name_that_would_exceed_the_postgres_identifier_limit",
        suffix="mainsequence_examples",
    )

    assert len(table_name) <= POSTGRES_IDENTIFIER_MAX_LENGTH
    assert table_name.startswith("sdk_examples__")


def test_parse_schema_table_name_round_trips_suffix() -> None:
    parts = parse_schema_table_name("ms_markets__asset__mainsequence_examples")

    assert parts.app == "ms_markets"
    assert parts.concept == "asset"
    assert parts.suffix == "mainsequence_examples"


def test_bounded_identifier_uses_double_underscore_separator_and_hash_suffix() -> None:
    identifier = bounded_identifier(
        "ix",
        "sdk_examples__account_holdings",
        "very_long_column_name_that_would_exceed_postgres_identifier_limits",
    )

    assert len(identifier) <= POSTGRES_IDENTIFIER_MAX_LENGTH
    assert identifier.startswith("ix__sdk_examples__account_holdings__")


def test_schema_constraint_helpers() -> None:
    table_name = "sdk_examples__account_holding"

    assert schema_index_name(table_name, ["account_uid"]) == (
        "ix__sdk_examples__account_holding__account_uid"
    )
    assert schema_index_name(table_name, ["account_uid"], unique=True) == (
        "uix__sdk_examples__account_holding__account_uid"
    )
    fk_name = schema_foreign_key_name(
        table_name,
        ["account_uid"],
        "sdk_examples__account",
        ["uid"],
    )
    assert fk_name == "fk__sdk_examples__account_holding__account_uid__sdk_7620762ba5"
    assert len(fk_name) <= POSTGRES_IDENTIFIER_MAX_LENGTH


def test_sqlalchemy_naming_convention_generates_project_prefixed_names() -> None:
    metadata = MetaData(naming_convention=sqlalchemy_naming_convention())
    account = Table(
        "sdk_examples__account",
        metadata,
        Column("uid", Integer, primary_key=True),
        Column("name", String(255), nullable=False),
    )
    holding = Table(
        "sdk_examples__account_holding",
        metadata,
        Column("uid", Integer, primary_key=True),
        Column("account_uid", Integer, ForeignKey(f"{account.name}.uid"), nullable=False),
        Column("code", String(64), nullable=False),
        UniqueConstraint("code"),
        Index(None, "account_uid"),
    )

    account_pk = account.primary_key
    holding_pk = holding.primary_key
    holding_fk = next(iter(holding.foreign_key_constraints))
    holding_unique = next(
        constraint for constraint in holding.constraints if isinstance(constraint, UniqueConstraint)
    )
    holding_index = next(iter(holding.indexes))

    assert account_pk.name == "pk__sdk_examples__account"
    assert holding_pk.name == "pk__sdk_examples__account_holding"
    assert holding_fk.name == "fk__sdk_examples__account_holding__account_uid__sdk_7620762ba5"
    assert holding_unique.name == "uq__sdk_examples__account_holding__code"
    assert holding_index.name == "ix__sdk_examples__account_holding__account_uid"
