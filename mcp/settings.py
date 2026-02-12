from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _csv_set(value: str | None) -> tuple[str, ...]:
    if not value:
        return tuple()
    parts = [p.strip() for p in value.split(",")]
    return tuple(p for p in parts if p)


@dataclass(frozen=True)
class MCPSettings:
    # Identity
    server_name: str = "mainsequence"

    # Toolsets
    toolsets: tuple[str, ...] = ("core", "client", "datanodes", "instruments")

    # Transport
    transport: str = "stdio"  # "stdio" | "streamable-http"
    host: str = "127.0.0.1"
    port: int = 8000

    # Recommended for production HTTP deployments
    stateless_http: bool = True
    json_response: bool = True

    # Artifacts
    artifacts_dir: Path = Path.home() / ".mainsequence_mcp" / "artifacts"
    max_preview_rows: int = 50

    # Safety / import allowlist for dynamic imports (DataNodes, instruments call wrapper)
    allowed_import_prefixes: tuple[str, ...] = ("mainsequence",)

    @classmethod
    def from_env(cls) -> MCPSettings:
        toolsets = _csv_set(os.getenv("MS_MCP_TOOLSETS")) or ("core", "client", "datanodes", "instruments")

        transport = os.getenv("MS_MCP_TRANSPORT", "stdio").strip()
        host = os.getenv("MS_MCP_HOST", "127.0.0.1").strip()
        port = int(os.getenv("MS_MCP_PORT", "8000"))

        stateless_http = os.getenv("MS_MCP_STATELESS_HTTP", "true").lower() == "true"
        json_response = os.getenv("MS_MCP_JSON_RESPONSE", "true").lower() == "true"

        artifacts_dir = Path(os.getenv("MS_MCP_ARTIFACTS_DIR", str(Path.home() / ".mainsequence_mcp" / "artifacts")))
        max_preview_rows = int(os.getenv("MS_MCP_MAX_PREVIEW_ROWS", "50"))

        allowed_import_prefixes = _csv_set(os.getenv("MS_MCP_ALLOWED_IMPORT_PREFIXES")) or ("mainsequence",)

        return cls(
            server_name=os.getenv("MS_MCP_SERVER_NAME", "mainsequence").strip(),
            toolsets=toolsets,
            transport=transport,
            host=host,
            port=port,
            stateless_http=stateless_http,
            json_response=json_response,
            artifacts_dir=artifacts_dir,
            max_preview_rows=max_preview_rows,
            allowed_import_prefixes=allowed_import_prefixes,
        )

    def is_toolset_enabled(self, name: str) -> bool:
        return name in set(self.toolsets)

    def ensure_dirs(self) -> None:
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
