from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Literal

CPU_MIN = Decimal("0.25")
CPU_MAX = Decimal("30")
MEMORY_MIN = Decimal("0.5")
MEMORY_MAX = Decimal("110")
MEMORY_PER_CPU_MIN = Decimal("1")
MEMORY_PER_CPU_MAX = Decimal("6.5")
GPU_MIN = 1
GPU_MAX = 8


def normalize_string(value: Any) -> str | None:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def decimal_to_storage(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value.normalize(), "f")


def _parse_decimal(raw: str, *, field_name: str, error_hint: str) -> Decimal:
    try:
        dec = Decimal(raw)
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be {error_hint}.") from exc

    if not dec.is_finite():
        raise ValueError(f"{field_name} must be {error_hint}.")
    return dec


def parse_cpu_request(value: Any, *, field_name: str = "cpu_request") -> Decimal | None:
    if value in (None, ""):
        return None

    raw = str(value).strip()
    if not raw:
        return None

    lowered = raw.lower()
    if lowered.endswith("m"):
        milli = _parse_decimal(
            lowered[:-1].strip(),
            field_name=field_name,
            error_hint="a valid decimal value or milliCPU quantity like 500m",
        )
        if milli.as_tuple().exponent < 0:
            raise ValueError(f"{field_name} milliCPU quantities must use whole milliCPU values.")
        return milli / Decimal("1000")

    cpu = _parse_decimal(
        raw,
        field_name=field_name,
        error_hint="a valid decimal value or milliCPU quantity like 500m",
    )
    if cpu.as_tuple().exponent < -3:
        raise ValueError(f"{field_name} must have at most 3 decimal places.")
    return cpu


def parse_memory_request(value: Any, *, field_name: str = "memory_request") -> Decimal | None:
    if value in (None, ""):
        return None

    raw = str(value).strip()
    if not raw:
        return None

    lowered = raw.lower()
    if lowered.endswith("gi"):
        memory = _parse_decimal(
            lowered[:-2].strip(),
            field_name=field_name,
            error_hint="a valid decimal GiB value or memory quantity like 1Gi or 512Mi",
        )
        if memory.as_tuple().exponent < -3:
            raise ValueError(f"{field_name} must have at most 3 decimal places.")
        return memory

    if lowered.endswith("mi"):
        mebibytes = _parse_decimal(
            lowered[:-2].strip(),
            field_name=field_name,
            error_hint="a valid decimal GiB value or memory quantity like 1Gi or 512Mi",
        )
        if mebibytes.as_tuple().exponent < 0:
            raise ValueError(f"{field_name} Mi quantities must use whole Mi values.")
        return mebibytes / Decimal("1024")

    memory = _parse_decimal(
        raw,
        field_name=field_name,
        error_hint="a valid decimal GiB value or memory quantity like 1Gi or 512Mi",
    )
    if memory.as_tuple().exponent < -3:
        raise ValueError(f"{field_name} must have at most 3 decimal places.")
    return memory


def parse_gpu_request(value: Any, *, field_name: str = "gpu_request") -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a valid integer.") from exc


def format_cpu_request(value: Decimal | None, *, output_format: Literal["decimal", "k8s"] = "decimal") -> str | None:
    if value is None:
        return None
    if output_format == "decimal":
        return decimal_to_storage(value)

    milli = value * Decimal("1000")
    if milli == milli.to_integral_value():
        milli_int = int(milli)
        if milli_int % 1000 == 0:
            return str(milli_int // 1000)
        return f"{milli_int}m"
    return decimal_to_storage(value)


def format_memory_request(value: Decimal | None, *, output_format: Literal["decimal", "k8s"] = "decimal") -> str | None:
    if value is None:
        return None
    if output_format == "decimal":
        return decimal_to_storage(value)

    mebibytes = value * Decimal("1024")
    if mebibytes == mebibytes.to_integral_value():
        mebibytes_int = int(mebibytes)
        if mebibytes_int % 1024 == 0:
            return f"{mebibytes_int // 1024}Gi"
        return f"{mebibytes_int}Mi"
    return f"{decimal_to_storage(value)}Gi"


def validate_and_normalize_compute_fields(
    *,
    cpu_request: Any,
    memory_request: Any,
    gpu_request: Any,
    gpu_type: Any,
    require_cpu_and_memory: bool = True,
    output_format: Literal["decimal", "k8s"] = "decimal",
) -> dict[str, str | None]:
    cpu = parse_cpu_request(cpu_request)
    memory = parse_memory_request(memory_request)

    if require_cpu_and_memory:
        if cpu is None:
            raise ValueError("cpu_request is required.")
        if memory is None:
            raise ValueError("memory_request is required.")
    else:
        if (cpu is None) ^ (memory is None):
            raise ValueError("cpu_request and memory_request must be provided together.")

    if cpu is not None and (cpu < CPU_MIN or cpu > CPU_MAX):
        raise ValueError(f"cpu_request must be between {CPU_MIN} and {CPU_MAX} vCPU.")

    if memory is not None and (memory < MEMORY_MIN or memory > MEMORY_MAX):
        raise ValueError(f"memory_request must be between {MEMORY_MIN} and {MEMORY_MAX} GiB.")

    if cpu is not None and memory is not None:
        ratio = memory / cpu
        if ratio < MEMORY_PER_CPU_MIN or ratio > MEMORY_PER_CPU_MAX:
            raise ValueError("memory_request must be between 1x and 6.5x cpu_request.")

    gpu_count = parse_gpu_request(gpu_request)
    normalized_gpu_type = normalize_string(gpu_type)

    if gpu_count is None and normalized_gpu_type is None:
        pass
    else:
        if gpu_count is None:
            raise ValueError("gpu_request is required when gpu_type is set.")
        if normalized_gpu_type is None:
            raise ValueError("gpu_type is required when gpu_request is set.")
        if gpu_count < GPU_MIN or gpu_count > GPU_MAX:
            raise ValueError(f"gpu_request must be between {GPU_MIN} and {GPU_MAX}.")

    return {
        "cpu_request": format_cpu_request(cpu, output_format=output_format),
        "memory_request": format_memory_request(memory, output_format=output_format),
        "gpu_request": str(gpu_count) if gpu_count is not None else None,
        "gpu_type": normalized_gpu_type,
    }
