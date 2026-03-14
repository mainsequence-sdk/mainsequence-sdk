from __future__ import annotations

import ast
import json
import pathlib
from dataclasses import dataclass
from functools import cache
from typing import Any

import typer


@dataclass(frozen=True)
class CliFieldMetadata:
    field_name: str
    label: str
    description: str
    examples: tuple[str, ...]
    required: bool

    def build_help(
        self,
        *,
        extra_help: str | None = None,
        include_examples: bool = True,
    ) -> str:
        parts: list[str] = []
        if self.description:
            parts.append(self.description.rstrip("."))
        if include_examples and self.examples:
            prefix = "Example" if len(self.examples) == 1 else "Examples"
            parts.append(f"{prefix}: {', '.join(self.examples)}")
        if extra_help:
            parts.append(extra_help.rstrip("."))
        if not parts:
            return self.label
        return ". ".join(parts) + "."

    def build_prompt(
        self,
        *,
        optional: bool = False,
        example_override: str | None = None,
        extra_hint: str | None = None,
    ) -> str:
        suffixes: list[str] = []
        if self.description:
            suffixes.append(self.description.rstrip("."))

        example_value = example_override or (self.examples[0] if self.examples else None)
        if example_value:
            suffixes.append(f"example: {example_value}")
        if optional:
            suffixes.append("optional")
        if extra_hint:
            suffixes.append(extra_hint.rstrip("."))

        if not suffixes:
            return self.label
        return f"{self.label} ({'; '.join(suffixes)})"


def _format_example(value: Any) -> str:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, sort_keys=True)
    except TypeError:
        return str(value)


def _humanize_label(field_name: str) -> str:
    label = field_name.replace("_", " ").strip()
    if label.endswith(" id"):
        label = label[:-3] + " ID"
    return label[:1].upper() + label[1:] if label else field_name


def _split_model_ref(model_ref: str) -> tuple[str, str]:
    module_name, _, class_name = model_ref.rpartition(".")
    if not module_name or not class_name:
        raise ValueError(f"Invalid model reference: {model_ref!r}")
    return module_name, class_name


def _module_path_from_ref(module_name: str) -> pathlib.Path:
    package_root = pathlib.Path(__file__).resolve().parents[1]
    prefix = "mainsequence."
    relative_module = module_name[len(prefix) :] if module_name.startswith(prefix) else module_name
    return package_root / pathlib.Path(relative_module.replace(".", "/") + ".py")


def _literal_or_none(node: ast.AST | None) -> Any:
    if node is None:
        return None
    try:
        return ast.literal_eval(node)
    except Exception:
        return None


@cache
def _load_field_metadata_from_source(model_ref: str, field_name: str) -> CliFieldMetadata:
    module_name, class_name = _split_model_ref(model_ref)
    module_path = _module_path_from_ref(module_name)
    tree = ast.parse(module_path.read_text(encoding="utf-8"), filename=str(module_path))

    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or node.name != class_name:
            continue
        for stmt in node.body:
            if not isinstance(stmt, ast.AnnAssign) or not isinstance(stmt.target, ast.Name):
                continue
            if stmt.target.id != field_name:
                continue

            label = _humanize_label(field_name)
            description = ""
            examples: tuple[str, ...] = ()
            required = stmt.value is None

            if isinstance(stmt.value, ast.Call) and getattr(stmt.value.func, "id", None) == "Field":
                kwargs = {kw.arg: kw.value for kw in stmt.value.keywords if kw.arg}
                description = (_literal_or_none(kwargs.get("description")) or "").strip()
                raw_examples = _literal_or_none(kwargs.get("examples")) or ()
                examples = tuple(_format_example(v) for v in raw_examples)

                if stmt.value.args:
                    first_arg = stmt.value.args[0]
                    required = isinstance(first_arg, ast.Constant) and first_arg.value is Ellipsis

            return CliFieldMetadata(
                field_name=field_name,
                label=label,
                description=description,
                examples=examples,
                required=required,
            )

    raise KeyError(f"{model_ref}.{field_name} is not a valid Pydantic field reference.")


@cache
def get_cli_field_metadata(model_ref: type | str, field_name: str) -> CliFieldMetadata:
    if isinstance(model_ref, str):
        return _load_field_metadata_from_source(model_ref, field_name)

    try:
        field = model_ref.model_fields[field_name]
    except KeyError as exc:
        raise KeyError(f"{model_ref.__name__}.{field_name} is not a valid Pydantic field.") from exc

    description = (field.description or "").strip()
    examples = tuple(_format_example(v) for v in (getattr(field, "examples", None) or ()))
    label = _humanize_label(field_name)

    return CliFieldMetadata(
        field_name=field_name,
        label=label,
        description=description,
        examples=examples,
        required=field.is_required(),
    )


def pydantic_option(
    model_cls: type | str,
    field_name: str,
    default: Any = ...,
    *param_decls: str,
    extra_help: str | None = None,
    include_examples: bool = True,
    **kwargs: Any,
):
    if not kwargs.get("help"):
        meta = get_cli_field_metadata(model_cls, field_name)
        kwargs["help"] = meta.build_help(extra_help=extra_help, include_examples=include_examples)
    return typer.Option(default, *param_decls, **kwargs)


def pydantic_argument(
    model_cls: type | str,
    field_name: str,
    default: Any = ...,
    *param_decls: str,
    extra_help: str | None = None,
    include_examples: bool = True,
    **kwargs: Any,
):
    if not kwargs.get("help"):
        meta = get_cli_field_metadata(model_cls, field_name)
        kwargs["help"] = meta.build_help(extra_help=extra_help, include_examples=include_examples)
    return typer.Argument(default, *param_decls, **kwargs)


def pydantic_prompt_text(
    model_cls: type | str,
    field_name: str,
    *,
    optional: bool = False,
    example_override: str | None = None,
    extra_hint: str | None = None,
) -> str:
    meta = get_cli_field_metadata(model_cls, field_name)
    return meta.build_prompt(
        optional=optional,
        example_override=example_override,
        extra_hint=extra_hint,
    )
