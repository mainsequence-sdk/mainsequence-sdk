# cli.py
import importlib
import json
import os
import sys


def load_class(entry_point: str):
    entry_point = entry_point.strip()
    if ":" in entry_point:  # preferred standard
        module_path, class_name = entry_point.split(":", 1)
    else:  # also accept your requested dotted form
        module_path, class_name = entry_point.rsplit(".", 1)

    module = importlib.import_module(module_path)
    return getattr(module, class_name)


def read_configuration() -> dict:
    # Priority: argv[1] -> $CONFIGURATION -> stdin (if piped)
    if len(sys.argv) >= 2 and sys.argv[1].strip():
        raw = sys.argv[1]
    else:
        raw = os.getenv("TOOL_CONFIGURATION")
        if (raw is None or not raw.strip()) and not sys.stdin.isatty():
            raw = sys.stdin.read()

    if raw is None or not raw.strip():
        return {}

    cfg = json.loads(raw)
    if not isinstance(cfg, dict):
        raise SystemExit("CONFIGURATION must be a JSON object (dict).")
    return cfg


def main() -> None:
    entry_point = os.getenv("TOOL_ENTRY_POINT")
    if not entry_point:
        raise SystemExit("Missing TOOL_ENTRY_POINT env var.")

    ToolClass = load_class(entry_point)
    cfg = read_configuration()

    # What you asked for:

    tool = ToolClass(**cfg)
    tool.run_and_response()




if __name__ == "__main__":
    main()
