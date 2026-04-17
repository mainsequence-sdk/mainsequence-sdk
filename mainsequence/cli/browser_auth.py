from __future__ import annotations

"""
mainsequence.cli.browser_auth
=============================

Browser-based authentication helpers for MainSequence CLI login.
"""

import base64
import hashlib
import html
import secrets
import threading
import webbrowser
from collections.abc import Callable
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .api import CLI_BROWSER_CLIENT_ID, build_cli_authorize_url, exchange_cli_authorization_code


class BrowserAuthError(RuntimeError):
    """Raised when browser-based authentication fails."""


@dataclass
class BrowserAuthCallback:
    """Authorization callback payload captured from the local loopback receiver."""

    code: str | None = None
    state: str | None = None
    error: str | None = None
    error_description: str | None = None


def _first(values: list[str] | None) -> str | None:
    if not values:
        return None
    value = (values[0] or "").strip()
    return value or None


def _pkce_code_verifier() -> str:
    """
    Generate a RFC 7636 PKCE code verifier (43-128 chars, URL-safe).
    """
    verifier = secrets.token_urlsafe(64).rstrip("=")
    if len(verifier) < 43:
        verifier = verifier + ("a" * (43 - len(verifier)))
    return verifier[:128]


def _pkce_code_challenge(code_verifier: str) -> str:
    digest = hashlib.sha256(code_verifier.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")


def _state_token() -> str:
    return secrets.token_urlsafe(32)


class _CallbackState:
    def __init__(self) -> None:
        self.event = threading.Event()
        self.callback = BrowserAuthCallback()


class _AuthorizationCallbackHandler(BaseHTTPRequestHandler):
    state: _CallbackState | None = None

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        callback = BrowserAuthCallback(
            code=_first(query.get("code")),
            state=_first(query.get("state")),
            error=_first(query.get("error")),
            error_description=_first(query.get("error_description")),
        )

        shared = self.__class__.state
        if shared is not None:
            shared.callback = callback
            shared.event.set()

        ok = bool(callback.code) and not callback.error
        title = "Main Sequence Login Complete" if ok else "Main Sequence Login Failed"
        body = "You can close this window and return to the CLI."
        if not ok:
            detail = callback.error_description or callback.error or "Missing authorization code in callback."
            body = f"Authentication failed: {html.escape(detail)}"

        payload = (
            "<html><head><meta charset='utf-8'><title>"
            + title
            + "</title></head><body><h1>"
            + title
            + "</h1><p>"
            + body
            + "</p></body></html>"
        ).encode("utf-8")

        self.send_response(200 if ok else 400)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        # Keep CLI login output clean.
        return


class _LoopbackAuthServer:
    def __init__(self, *, host: str = "127.0.0.1") -> None:
        self.host = host
        self._state = _CallbackState()
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None

    def __enter__(self) -> _LoopbackAuthServer:
        handler_cls = type("_MainSequenceAuthCallbackHandler", (_AuthorizationCallbackHandler,), {})
        handler_cls.state = self._state
        self._server = ThreadingHTTPServer((self.host, 0), handler_cls)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._server is not None:
            try:
                self._server.shutdown()
            finally:
                self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=1.0)

    @property
    def redirect_uri(self) -> str:
        if self._server is None:
            raise RuntimeError("Loopback auth server is not running.")
        port = self._server.server_address[1]
        return f"http://{self.host}:{port}/callback"

    def wait_for_callback(self, *, timeout_seconds: int) -> BrowserAuthCallback | None:
        if not self._state.event.wait(timeout=timeout_seconds):
            return None
        return self._state.callback


def login_via_browser(
    *,
    no_open: bool = False,
    timeout_seconds: int = 300,
    on_authorize_url: Callable[[str], None] | None = None,
) -> dict:
    """
    Execute browser-based CLI login and return access/refresh JWT tokens.

    Raises:
        BrowserAuthError: if browser flow fails or callback validation fails.
    """
    state = _state_token()
    client_id = CLI_BROWSER_CLIENT_ID
    code_verifier = _pkce_code_verifier()
    code_challenge = _pkce_code_challenge(code_verifier)

    with _LoopbackAuthServer() as callback_server:
        authorize_url = build_cli_authorize_url(
            redirect_uri=callback_server.redirect_uri,
            state=state,
            code_challenge=code_challenge,
            client_id=client_id,
        )
        if on_authorize_url is not None:
            on_authorize_url(authorize_url)

        if not no_open:
            opened = False
            try:
                opened = bool(webbrowser.open(authorize_url, new=2, autoraise=True))
            except Exception:
                opened = False
            if not opened:
                raise BrowserAuthError(
                    "Could not open a browser automatically. Re-run with --no-open and open this URL manually: "
                    f"{authorize_url}"
                )

        callback = callback_server.wait_for_callback(timeout_seconds=timeout_seconds)
        if callback is None:
            raise BrowserAuthError(
                "Timed out waiting for browser login callback. "
                "Re-run `mainsequence login` and complete the browser flow."
            )
        if callback.state != state:
            raise BrowserAuthError("Browser callback state mismatch.")
        if callback.error:
            detail = callback.error_description or callback.error
            raise BrowserAuthError(f"Browser authorization failed: {detail}")
        if not callback.code:
            raise BrowserAuthError("Authorization code missing in callback.")

        tokens = exchange_cli_authorization_code(
            code=callback.code,
            code_verifier=code_verifier,
            redirect_uri=callback_server.redirect_uri,
            client_id=client_id,
        )

    return tokens | {"authorize_url": authorize_url}
