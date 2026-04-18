from __future__ import annotations

import base64
import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from ..client import MainSequenceClientConfig
from ..utils import AuthLoaders

_AUTH_STORAGE_KEY = "command-center.jwt-auth"
_DEFAULT_PROFILE = "full-data"
_DEFAULT_TIMEOUT_MS = 300_000
_DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024


class _WorkspaceSnapshotError(RuntimeError):
    """Raised when the Command Center client cannot produce a workspace snapshot."""


def _decode_jwt_exp_ms(access_token: str) -> int | None:
    try:
        parts = access_token.split(".")
        if len(parts) < 2:
            return None
        payload = parts[1]
        payload += "=" * (-len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")).decode("utf-8"))
        exp = claims.get("exp")
        if isinstance(exp, str):
            exp = int(exp)
        if not isinstance(exp, int):
            return None
        return exp * 1000
    except Exception:
        return None


def _refresh_and_collect_tokens() -> dict[str, Any]:
    auth_mode = (os.getenv("MAINSEQUENCE_AUTH_MODE") or "jwt").strip().lower()
    if auth_mode == "session_jwt":
        raise _WorkspaceSnapshotError(
            "MAINSEQUENCE_AUTH_MODE=session_jwt is not supported for Command Center workspace snapshots."
        )

    loaders = AuthLoaders()
    loaders.refresh_headers(force=False)

    access_token = os.getenv("MAINSEQUENCE_ACCESS_TOKEN")
    refresh_token = os.getenv("MAINSEQUENCE_REFRESH_TOKEN")

    if not access_token:
        raise _WorkspaceSnapshotError(
            "MAINSEQUENCE_ACCESS_TOKEN is not available after auth refresh. "
            "Set MAINSEQUENCE_ACCESS_TOKEN and MAINSEQUENCE_REFRESH_TOKEN first."
        )

    return {
        "accessToken": access_token,
        "refreshToken": refresh_token or None,
        "tokenType": "Bearer",
        "expiresAt": _decode_jwt_exp_ms(access_token),
    }


def _infer_command_center_url_from_endpoint(endpoint: str) -> str:
    parsed = urlparse(endpoint.rstrip("/"))
    host = (parsed.hostname or "").strip().lower()
    scheme = parsed.scheme or "https"

    if not host:
        raise _WorkspaceSnapshotError(
            "Cannot infer Command Center URL because MAINSEQUENCE_ENDPOINT is empty."
        )

    if host in {"localhost", "127.0.0.1"}:
        return f"{scheme}://localhost:5173"

    if host == "api.main-sequence.app":
        return f"{scheme}://main-sequence.app"

    if host.startswith("api."):
        frontend_host = host.removeprefix("api.")
        return f"{scheme}://{frontend_host}"

    raise _WorkspaceSnapshotError(
        "Unable to infer Command Center URL from "
        f"{endpoint!r}. Set MAINSEQUENCE_COMMAND_CENTER_URL explicitly."
    )


def _resolve_command_center_url() -> str:
    explicit_url = (
        os.getenv("MAINSEQUENCE_COMMAND_CENTER_URL")
        or os.getenv("COMMAND_CENTER_URL")
        or ""
    ).strip()
    if explicit_url:
        return explicit_url.rstrip("/")

    config = MainSequenceClientConfig()
    return _infer_command_center_url_from_endpoint(config.endpoint)


def _build_storage_payload(tokens: dict[str, Any]) -> dict[str, Any]:
    return {"tokens": tokens}


def _build_snapshot_url(base_url: str, workspace_id: int | str) -> str:
    return (
        f"{base_url.rstrip('/')}/app/workspace-studio/workspaces"
        f"?workspace={workspace_id}&snapshot=true&snapshotProfile={_DEFAULT_PROFILE}"
    )


def _read_snapshot_state(page: Any) -> dict[str, Any] | None:
    return page.evaluate(
        """
        () => {
          const state = window.__COMMAND_CENTER_SNAPSHOT__;
          if (!state) {
            return null;
          }

          return {
            status: state.status ?? null,
            error: state.error ?? null,
            warnings: Array.isArray(state.warnings) ? state.warnings : [],
            errors: Array.isArray(state.errors) ? state.errors : [],
            archiveName: state.archiveName ?? null,
            archiveSizeBytes: state.archiveSizeBytes ?? null,
            manifest: state.manifest ?? null,
            workspaceId: state.workspaceId ?? null,
          };
        }
        """
    )


def _wait_for_snapshot_ready(page: Any, timeout_ms: int) -> dict[str, Any]:
    try:
        page.wait_for_function(
            """
            () => {
              const state = window.__COMMAND_CENTER_SNAPSHOT__;
              return Boolean(
                state && (state.status === "ready" || state.status === "error")
              );
            }
            """,
            timeout=timeout_ms,
        )
    except Exception as exc:
        raise _WorkspaceSnapshotError(
            "Timed out waiting for the Command Center workspace snapshot to complete. "
            f"Current page: {page.url}"
        ) from exc

    state = _read_snapshot_state(page)
    if not state:
        raise _WorkspaceSnapshotError(
            "Command Center did not expose window.__COMMAND_CENTER_SNAPSHOT__."
        )

    if state.get("status") == "error":
        error_message = state.get("error") or "Workspace snapshot failed in the browser."
        warnings = state.get("warnings") or []
        errors = state.get("errors") or []
        details = []
        if warnings:
            details.append(f"warnings={warnings}")
        if errors:
            details.append(f"errors={errors}")
        detail_text = f" ({'; '.join(details)})" if details else ""
        raise _WorkspaceSnapshotError(f"{error_message}{detail_text}")

    if state.get("status") != "ready":
        raise _WorkspaceSnapshotError(
            "Command Center snapshot finished in an unexpected state: "
            f"{state.get('status')!r}"
        )

    return state


def _read_blob_size(page: Any) -> int:
    size = page.evaluate(
        """
        () => {
          const blob = window.__COMMAND_CENTER_SNAPSHOT__?.archiveBlob;
          return blob ? blob.size : null;
        }
        """
    )
    if not isinstance(size, int) or size < 0:
        raise _WorkspaceSnapshotError(
            "Command Center snapshot completed but archiveBlob is missing."
        )
    return size


def _read_blob_chunk_base64(page: Any, start: int, end: int) -> str:
    encoded = page.evaluate(
        """
        async ({ start, end }) => {
          const blob = window.__COMMAND_CENTER_SNAPSHOT__?.archiveBlob;
          if (!blob) {
            throw new Error("Snapshot archive blob is not available.");
          }

          const buffer = await blob.slice(start, end).arrayBuffer();
          const bytes = new Uint8Array(buffer);
          let binary = "";
          const segmentSize = 0x8000;

          for (let index = 0; index < bytes.length; index += segmentSize) {
            const segment = bytes.subarray(index, index + segmentSize);
            binary += String.fromCharCode(...segment);
          }

          return btoa(binary);
        }
        """,
        {"start": start, "end": end},
    )

    if not isinstance(encoded, str) or not encoded:
        raise _WorkspaceSnapshotError("Failed to read a workspace snapshot archive chunk.")

    return encoded


def _read_blob_bytes_in_chunks(page: Any, chunk_size: int) -> bytes:
    size = _read_blob_size(page)
    archive = bytearray()

    for start in range(0, size, chunk_size):
        end = min(size, start + chunk_size)
        archive.extend(base64.b64decode(_read_blob_chunk_base64(page, start, end)))

    return bytes(archive)


def _write_output_path(output_path: str | os.PathLike[str], payload: bytes) -> None:
    destination = Path(output_path).expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(payload)


def get_workspace_snapshot(
    workspace_id: int | str,
    output_path: str | os.PathLike[str] | None = None,
) -> bytes:
    """
    Capture a live Command Center workspace snapshot archive through the real browser client.

    The Command Center frontend already knows how to assemble the ZIP archive. This helper
    authenticates a headless browser using the SDK's JWT environment variables, navigates to
    the existing `snapshot=true` route, waits for the client to finish capture, and returns the
    resulting ZIP bytes. When `output_path` is provided, the ZIP is also written to disk.
    """

    tokens = _refresh_and_collect_tokens()
    command_center_url = _resolve_command_center_url()
    storage_payload = _build_storage_payload(tokens)
    snapshot_url = _build_snapshot_url(command_center_url, workspace_id)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise _WorkspaceSnapshotError(
            "Playwright is required for workspace snapshots. Install the SDK extra "
            "`workspace-snapshots` and ensure the browser binaries are installed."
        ) from exc

    archive_bytes: bytes

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            context = browser.new_context(
                viewport={"width": 1600, "height": 1200},
                device_scale_factor=1,
            )
            context.add_init_script(
                """
                ({ key, payload }) => {
                  window.localStorage.setItem(key, JSON.stringify(payload));
                }
                """,
                {"key": _AUTH_STORAGE_KEY, "payload": storage_payload},
            )
            page = context.new_page()
            page.goto(snapshot_url, wait_until="domcontentloaded")
            _wait_for_snapshot_ready(page, timeout_ms=_DEFAULT_TIMEOUT_MS)
            archive_bytes = _read_blob_bytes_in_chunks(page, chunk_size=_DEFAULT_CHUNK_SIZE)
            context.close()
            browser.close()
    except _WorkspaceSnapshotError:
        raise
    except Exception as exc:
        raise _WorkspaceSnapshotError(
            "Failed to capture the workspace snapshot through Playwright. "
            "Ensure the Command Center frontend is reachable and Playwright Chromium is installed."
        ) from exc

    if output_path is not None:
        _write_output_path(output_path, archive_bytes)

    return archive_bytes


__all__ = ["get_workspace_snapshot"]
