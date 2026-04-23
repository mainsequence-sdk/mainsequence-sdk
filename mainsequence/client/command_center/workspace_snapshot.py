from __future__ import annotations

import base64
import io
import json
import os
import re
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from mainsequence.defaults import STANDARD_BACKEND_URL

from ..client import MainSequenceClientConfig
from ..utils import AuthError, AuthLoaders, _default_auth_provider_kind

_AUTH_STORAGE_KEY = "command-center.jwt-auth"
_DEFAULT_PROFILE = "full-data"
_DEFAULT_TIMEOUT_MS = 300_000
_DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024
_DEFAULT_OUTPUT_DIR_PARTS = ("mainsequence", "workspaces")
_DEFAULT_ARCHIVE_FILENAME = "snapshot.zip"
_SCROLL_CONTAINER_SELECTOR = '[data-workspace-canvas-scroll-container="true"]'
_SCROLL_CONTENT_SELECTOR = '[data-workspace-canvas-content="true"]'
_SCROLL_SCREENSHOT_DIR = "screenshots/playwright-scroll"
_MAX_SCROLL_SCREENSHOTS = 24
_SCROLL_SCREENSHOT_OVERLAP_PX = 160


class _WorkspaceSnapshotError(RuntimeError):
    """Raised when the Command Center client cannot produce a workspace snapshot."""


def _refresh_and_collect_tokens() -> dict[str, Any]:
    loaders = AuthLoaders()
    try:
        loaders.refresh_headers(force=False)
    except AuthError as exc:
        raise _WorkspaceSnapshotError(f"Could not refresh SDK authentication: {exc}") from exc

    access_token = os.getenv("MAINSEQUENCE_ACCESS_TOKEN")
    refresh_token = os.getenv("MAINSEQUENCE_REFRESH_TOKEN")

    if not access_token:
        raise _WorkspaceSnapshotError(
            "MAINSEQUENCE_ACCESS_TOKEN is not available after auth refresh. "
            "Configure SDK authentication so MAINSEQUENCE_ACCESS_TOKEN can be produced first."
        )

    return {
        "accessToken": access_token,
        "tokenType": "Bearer",
        "refreshToken": refresh_token or None,
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

    if host == (urlparse(STANDARD_BACKEND_URL).hostname or ""):
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


def _resolve_browser_auth_mode(auth_mode: str | None) -> str:
    if auth_mode is not None:
        browser_auth_mode = auth_mode.strip() if isinstance(auth_mode, str) else ""
        if not browser_auth_mode:
            raise _WorkspaceSnapshotError("auth_mode is required for Command Center workspace snapshots.")
        return browser_auth_mode

    provider_kind = _default_auth_provider_kind()
    if not provider_kind:
        raise _WorkspaceSnapshotError(
            "No SDK authentication provider is configured for Command Center workspace snapshots. "
            "Run `mainsequence login` first."
        )

    return "runtime_credential" if provider_kind == "runtime_credential" else "jwt"


def _build_storage_payload(auth_mode: str | None, tokens: dict[str, Any]) -> dict[str, Any]:
    browser_auth_mode = _resolve_browser_auth_mode(auth_mode)

    return {"authMode": browser_auth_mode, "tokens": tokens}


def _build_snapshot_url(base_url: str, workspace_id: int | str) -> str:
    return (
        f"{base_url.rstrip('/')}/app/workspace-studio/workspaces"
        f"?workspace={workspace_id}&snapshot=true&snapshotProfile={_DEFAULT_PROFILE}"
    )


def _default_snapshot_output_dir() -> Path:
    return Path.home().joinpath(*_DEFAULT_OUTPUT_DIR_PARTS)


def _safe_path_fragment(value: Any) -> str:
    fragment = str(value).strip()
    fragment = fragment.replace("\\", "/").split("/")[-1]
    fragment = re.sub(r"[^A-Za-z0-9._ -]+", "-", fragment)
    fragment = re.sub(r"\s+", " ", fragment).strip(" .")
    return fragment or "unknown"


def _snapshot_timestamp() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _default_snapshot_output_path(workspace_id: int | str) -> Path:
    workspace_fragment = _safe_path_fragment(workspace_id)
    snapshot_dir = f"workspace-{workspace_fragment}-{_snapshot_timestamp()}"
    return _default_snapshot_output_dir() / snapshot_dir / _DEFAULT_ARCHIVE_FILENAME


def _safe_archive_filename(value: Any) -> str | None:
    if not isinstance(value, str):
        return None

    filename = value.strip()
    if not filename:
        return None

    filename = filename.replace("\\", "/").split("/")[-1]
    filename = re.sub(r"[^A-Za-z0-9._ -]+", "-", filename)
    filename = re.sub(r"\s+", " ", filename).strip(" .")
    if not filename:
        return None

    if not filename.lower().endswith(".zip"):
        filename = f"{filename}.zip"

    return filename


def _default_snapshot_filename(workspace_id: int | str, archive_name: Any) -> str:
    archive_filename = _safe_archive_filename(archive_name)
    if archive_filename:
        return archive_filename

    workspace_fragment = _safe_archive_filename(f"workspace-{workspace_id}-snapshot")
    return workspace_fragment or "workspace-snapshot.zip"


def _resolve_snapshot_output_path(
    output_path: str | os.PathLike[str] | None,
    workspace_id: int | str,
    snapshot_state: dict[str, Any],
) -> Path:
    archive_name = snapshot_state.get("archiveName")
    archive_filename = _default_snapshot_filename(workspace_id, archive_name)

    if output_path is None:
        return _default_snapshot_output_path(workspace_id)

    raw_output_path = os.fspath(output_path)
    destination = Path(raw_output_path).expanduser()

    separators = [os.sep]
    if os.altsep:
        separators.append(os.altsep)

    if destination.exists() and destination.is_dir():
        return destination / archive_filename

    if any(raw_output_path.endswith(separator) for separator in separators):
        return destination / archive_filename

    return destination


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
                (state && (state.status === "ready" || state.status === "error")) ||
                window.location.pathname.includes("/login")
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
        if "/login" in page.url:
            raise _WorkspaceSnapshotError(
                "Command Center redirected to login while bootstrapping snapshot auth. "
                "The injected SDK access token was not accepted by the frontend/backend."
            )
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


def _resolve_scroll_screenshot_positions(page: Any) -> list[dict[str, Any]]:
    return page.evaluate(
        """
        ({ containerSelector, contentSelector, maxScreenshots, overlapPx }) => {
          const container = document.querySelector(containerSelector);
          if (!container) {
            throw new Error(`Workspace canvas scroll container not found: ${containerSelector}`);
          }

          const content = document.querySelector(contentSelector);
          const containerRect = container.getBoundingClientRect();
          const contentRect = content?.getBoundingClientRect();
          const viewportHeight = Math.max(
            1,
            Math.floor(container.clientHeight || containerRect.height || window.innerHeight)
          );
          const scrollHeight = Math.max(
            container.scrollHeight || 0,
            content?.scrollHeight || 0,
            contentRect?.height || 0,
            viewportHeight
          );
          const maxScrollTop = Math.max(0, Math.ceil(scrollHeight - viewportHeight));
          const step = Math.max(1, viewportHeight - overlapPx);
          const requestedCount = Math.max(1, Math.ceil(maxScrollTop / step) + 1);
          const captureCount = Math.min(maxScreenshots, requestedCount);
          const tops = [];

          if (captureCount === 1) {
            tops.push(0);
          } else if (requestedCount <= maxScreenshots) {
            for (let top = 0; top < maxScrollTop; top += step) {
              tops.push(Math.round(top));
            }
            tops.push(maxScrollTop);
          } else {
            for (let index = 0; index < captureCount; index += 1) {
              tops.push(Math.round((maxScrollTop * index) / (captureCount - 1)));
            }
          }

          return Array.from(new Set(tops)).map((top, index) => ({
            index: index + 1,
            scrollTop: top,
            viewportHeight,
            scrollHeight,
            maxScrollTop,
            isSampled: requestedCount > maxScreenshots,
          }));
        }
        """,
        {
            "containerSelector": _SCROLL_CONTAINER_SELECTOR,
            "contentSelector": _SCROLL_CONTENT_SELECTOR,
            "maxScreenshots": _MAX_SCROLL_SCREENSHOTS,
            "overlapPx": _SCROLL_SCREENSHOT_OVERLAP_PX,
        },
    )


def _capture_workspace_scroll_screenshots(page: Any) -> list[dict[str, Any]]:
    positions = _resolve_scroll_screenshot_positions(page)
    container = page.locator(_SCROLL_CONTAINER_SELECTOR).first
    captures: list[dict[str, Any]] = []

    for position in positions:
        page.evaluate(
            """
            ({ selector, scrollTop }) => {
              const container = document.querySelector(selector);
              if (!container) {
                throw new Error(`Workspace canvas scroll container not found: ${selector}`);
              }
              container.scrollTop = scrollTop;
              container.scrollLeft = 0;
            }
            """,
            {
                "selector": _SCROLL_CONTAINER_SELECTOR,
                "scrollTop": position["scrollTop"],
            },
        )
        page.wait_for_timeout(150)
        filename = f"workspace-scroll-{position['index']:03d}.png"
        captures.append(
            {
                "path": f"{_SCROLL_SCREENSHOT_DIR}/{filename}",
                "bytes": container.screenshot(type="png"),
                "metadata": {
                    "path": f"{_SCROLL_SCREENSHOT_DIR}/{filename}",
                    **position,
                },
            }
        )

    return captures


def _append_scroll_screenshots_to_archive(
    archive_bytes: bytes,
    captures: list[dict[str, Any]],
) -> bytes:
    if not captures:
        return archive_bytes

    archive_buffer = io.BytesIO(archive_bytes)
    index_payload = {
        "selector": _SCROLL_CONTAINER_SELECTOR,
        "contentSelector": _SCROLL_CONTENT_SELECTOR,
        "maxScreenshots": _MAX_SCROLL_SCREENSHOTS,
        "screenshots": [capture["metadata"] for capture in captures],
    }

    with zipfile.ZipFile(archive_buffer, mode="a", compression=zipfile.ZIP_DEFLATED) as archive:
        for capture in captures:
            archive.writestr(capture["path"], capture["bytes"])
        archive.writestr(
            f"{_SCROLL_SCREENSHOT_DIR}/index.json",
            json.dumps(index_payload, indent=2, sort_keys=True),
        )

    return archive_buffer.getvalue()


def _write_output_path(output_path: str | os.PathLike[str], payload: bytes) -> None:
    destination = Path(output_path).expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(payload)


def get_workspace_snapshot(
    workspace_id: int | str,
    output_path: str | os.PathLike[str] | None = None,
    *,
    auth_mode: str | None = None,
) -> bytes:
    """
    Capture a live Command Center workspace snapshot archive through the real browser client.

    The Command Center frontend already knows how to assemble the ZIP archive. This helper
    authenticates a headless browser using the SDK's configured auth provider and refreshed
    environment tokens, navigates to the existing `snapshot=true` route, waits for the client
    to finish capture, and returns the resulting ZIP bytes. The ZIP is written to `output_path`
    when provided; otherwise it is written to
    `~/mainsequence/workspaces/workspace-<workspace_id>-<timestamp>/snapshot.zip`.
    """

    tokens = _refresh_and_collect_tokens()
    command_center_url = _resolve_command_center_url()
    storage_payload = _build_storage_payload(auth_mode=auth_mode, tokens=tokens)
    snapshot_url = _build_snapshot_url(command_center_url, workspace_id)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise _WorkspaceSnapshotError(
            "Playwright is required for workspace snapshots. Install the SDK extra "
            "`workspace-snapshots` and ensure the browser binaries are installed."
        ) from exc

    archive_bytes: bytes
    snapshot_state: dict[str, Any]

    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            context = browser.new_context(
                viewport={"width": 1600, "height": 1200},
                device_scale_factor=1,
            )
            init_payload = json.dumps({"key": _AUTH_STORAGE_KEY, "payload": storage_payload})
            context.add_init_script(
                f"""
                (() => {{
                  const data = {init_payload};
                  window.localStorage.setItem(data.key, JSON.stringify(data.payload));
                }})();
                """,
            )
            page = context.new_page()
            page.goto(snapshot_url, wait_until="domcontentloaded")
            snapshot_state = _wait_for_snapshot_ready(page, timeout_ms=_DEFAULT_TIMEOUT_MS)
            archive_bytes = _read_blob_bytes_in_chunks(page, chunk_size=_DEFAULT_CHUNK_SIZE)
            scroll_screenshots = _capture_workspace_scroll_screenshots(page)
            archive_bytes = _append_scroll_screenshots_to_archive(archive_bytes, scroll_screenshots)
            context.close()
            browser.close()
    except _WorkspaceSnapshotError:
        raise
    except Exception as exc:
        raise _WorkspaceSnapshotError(
            "Failed to capture the workspace snapshot through Playwright. "
            "Ensure the Command Center frontend is reachable and Playwright Chromium is installed. "
            f"Snapshot URL: {snapshot_url}. Underlying error: {type(exc).__name__}: {exc}"
        ) from exc

    resolved_output_path = _resolve_snapshot_output_path(output_path, workspace_id, snapshot_state)
    _write_output_path(resolved_output_path, archive_bytes)

    return archive_bytes


__all__ = ["get_workspace_snapshot"]
