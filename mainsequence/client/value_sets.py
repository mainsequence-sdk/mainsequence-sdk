"""Open value sets: read a closed vocabulary without failing on an undeclared value.

ADR 0033. A response field whose vocabulary the backend is allowed to extend — a
state, a phase, a kind — is declared with the types here instead of a bare `Enum`
or `Literal`. A value this SDK release does not declare is kept as the backend
wrote it and reported once, so one new backend state does not fail the row that
carries it, nor the listing that row is in.

Values the SDK *sends* stay closed. A request parameter typed `Literal[...]` still
rejects a value the backend does not accept, because there the mistake is the
caller's and raising is what catches it.
"""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Any, Literal, get_args, get_origin

from pydantic import AfterValidator

from mainsequence.logconf import logger

# An unknown value is reported once per process, not once per row: a listing of
# 500 rows carrying the same new state is one backend change, so it is one line.
# The set is capped so a response cannot grow it without bound.
_MAX_REPORTED_UNKNOWN_VALUES = 512
_reported_unknown_values: set[tuple[str, str, str]] = set()


def _report_unknown_value(
    owner: str,
    field_name: str | None,
    value: str,
    known: tuple[str, ...],
) -> None:
    """Log an undeclared value once, naming the model, the field and the value."""
    key = (owner, field_name or "", value)
    if key in _reported_unknown_values:
        return
    if len(_reported_unknown_values) >= _MAX_REPORTED_UNKNOWN_VALUES:
        return
    _reported_unknown_values.add(key)
    where = f"{owner}.{field_name}" if field_name else owner
    logger.warning(
        f"{where} received {value!r}, which this mainsequence release does not declare. "
        f"The value is kept as sent. Declared values: {', '.join(known)}. "
        f"Upgrade the SDK to read it as a declared member."
    )


def _reset_reported_unknown_values() -> None:
    """Forget what has been reported. For tests that assert on the log line."""
    _reported_unknown_values.clear()


class OpenStrEnum(str, Enum):
    """A string enum that keeps a value it does not declare instead of raising.

    A known value validates to the declared member, so `kind is Kind.AGENT` and
    `kind == "agent"` both keep working. An undeclared value becomes a member-like
    object carrying that exact string: it compares equal to the string, serializes
    as the string, and is not equal to any declared member. It is deliberately not
    registered on the class, so iterating the enum still lists only what this
    release declares.
    """

    @classmethod
    def _missing_(cls, value: object) -> OpenStrEnum | None:
        if not isinstance(value, str):
            # Not a string: let Enum raise its own error, as before.
            return None
        undeclared = str.__new__(cls, value)
        undeclared._name_ = value
        undeclared._value_ = value
        _report_unknown_value(
            cls.__name__,
            None,
            value,
            tuple(member.value for member in cls),
        )
        return undeclared


class _KeepUndeclaredValue:
    """Validator behind `OpenValueSet`: keep any string, report the undeclared ones."""

    __slots__ = ("known",)

    def __init__(self, known: tuple[str, ...]) -> None:
        self.known = known

    def __call__(self, value: str, info: Any) -> str:
        if value in self.known:
            return value
        owner = (info.config or {}).get("title") or "response"
        _report_unknown_value(owner, info.field_name or "value", value, self.known)
        return value

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _KeepUndeclaredValue) and other.known == self.known

    def __hash__(self) -> int:
        return hash((type(self).__name__, self.known))


def _declared_values(item: object) -> tuple[str, ...]:
    """The values the wrapped `Literal[...]` declares."""
    candidates = item if isinstance(item, tuple) else (item,)
    values: list[str] = []
    for candidate in candidates:
        if get_origin(candidate) is not Literal:
            raise TypeError(
                "OpenValueSet wraps a Literal, e.g. OpenValueSet[Literal['a', 'b']] "
                f"or OpenValueSet[PublicLogLevel]; got {candidate!r}"
            )
        values.extend(get_args(candidate))
    if not values:
        raise TypeError("OpenValueSet requires at least one declared value")
    for value in values:
        if not isinstance(value, str):
            raise TypeError(f"OpenValueSet takes string values; got {value!r}")
    return tuple(dict.fromkeys(values))


class OpenValueSet:
    """Wraps a `Literal[...]` for a response field whose vocabulary may grow.

    Reads as a `str` that keeps an undeclared value instead of raising:

        state: OpenValueSet[Literal["routable", "unavailable"]]
        level: OpenValueSet[PublicLogLevel]

    The values stay inside a real `Literal`, so they keep one spelling for readers,
    for `Literal` aliases reused between a request parameter and a response field,
    and for tools that rewrite annotations. `declared_values()` reads them back.
    """

    def __class_getitem__(cls, item: object) -> Any:
        known = _declared_values(item)
        return Annotated[str, AfterValidator(_KeepUndeclaredValue(known))]


def declared_values(annotation: Any) -> tuple[str, ...]:
    """The values an `OpenValueSet` declares, in declaration order.

    Takes the annotation itself, or the `FieldInfo` pydantic built from it —
    `Model.model_fields["state"]` — because pydantic moves the metadata there.
    """
    candidates = (
        *getattr(annotation, "__metadata__", ()),
        *getattr(annotation, "metadata", ()),
    )
    for meta in candidates:
        func = getattr(meta, "func", None)
        if isinstance(func, _KeepUndeclaredValue):
            return func.known
    raise TypeError(f"{annotation!r} does not declare an OpenValueSet")


__all__ = [
    "OpenStrEnum",
    "OpenValueSet",
    "declared_values",
]
