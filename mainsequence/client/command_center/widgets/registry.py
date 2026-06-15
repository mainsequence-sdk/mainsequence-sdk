from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from ..contracts.tabular import CORE_TABULAR_FRAME_CONTRACT


def iter_contract_strings(payload: Any) -> Iterator[str]:
    """Yield contract-looking strings from a registry or widget metadata payload."""

    if isinstance(payload, str):
        if "@v" in payload or payload.startswith("core."):
            yield payload
        return
    if isinstance(payload, Mapping):
        for value in payload.values():
            yield from iter_contract_strings(value)
        return
    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        for item in payload:
            yield from iter_contract_strings(item)


def declares_contract(payload: Mapping[str, Any], contract: str = CORE_TABULAR_FRAME_CONTRACT) -> bool:
    """Return whether a registry/detail payload declares a specific contract string."""

    return any(value == contract for value in iter_contract_strings(payload))


def require_declared_contract(
    payload: Mapping[str, Any],
    contract: str = CORE_TABULAR_FRAME_CONTRACT,
) -> Mapping[str, Any]:
    """Return the payload if it declares the contract, otherwise raise a clear error."""

    if not declares_contract(payload, contract):
        raise ValueError(f"Widget registry payload does not declare contract {contract!r}.")
    return payload


__all__ = [
    "declares_contract",
    "iter_contract_strings",
    "require_declared_contract",
]
