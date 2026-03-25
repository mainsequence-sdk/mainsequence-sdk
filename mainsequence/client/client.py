# mainsequence/client/client.py
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

import requests

from .exceptions import raise_for_response
from .utils import AuthLoaders, build_session
from .utils import loaders as _global_loaders
from .utils import session as _global_session


@dataclass(frozen=True)
class MainSequenceClientConfig:
    # Prefer MAINSEQUENCE_ENDPOINT, keep TDAG_ENDPOINT for backward compat
    endpoint: str = (
        os.getenv("MAINSEQUENCE_ENDPOINT")
        or os.getenv("TDAG_ENDPOINT")
        or "https://api.main-sequence.app"
    )
    api_prefix: str = "/orm/api"
    # (connect, read) — align with your utils DEFAULT_TIMEOUT behavior
    timeout: tuple[float, float] = (5.0, 120.0)
    retries: int = 3
    backoff_factor: float = 0.5

    @property
    def api_root(self) -> str:
        return self.endpoint.rstrip("/") + self.api_prefix


class MainSequenceClient:
    """
    Official singleton client.

    - Uses ONE shared loaders + session (from utils) by default
    - Can be reconfigured once at startup
    """
    _default: MainSequenceClient | None = None

    def __init__(
        self,
        config: MainSequenceClientConfig,
        loaders: AuthLoaders | None = None,
        session: requests.Session | None = None,
    ):
        self.config = config
        self.loaders = loaders or AuthLoaders()

        if session is None:
            # Single place to build sessions: utils.build_session
            self.session = build_session(
                loaders=self.loaders,
                retries=self.config.retries,
                backoff_factor=self.config.backoff_factor,
                accept_gzip=True,
            )
        else:
            # Reuse an existing session (global singleton path)
            self.session = session
            # Ensure headers are up-to-date
            self.session.headers.update(self.loaders.auth_headers)
            self.session.headers.setdefault("Accept-Encoding", "gzip")

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        json: dict | None = None,
        files: dict | None = None,
        timeout: tuple[float, float] | float | None = None,
    ) -> Any:
        url = self.config.api_root.rstrip("/") + "/" + path.lstrip("/")
        to = timeout or self.config.timeout

        r = self.session.request(method.upper(), url, params=params, json=json, files=files, timeout=to)

        # Refresh auth once if needed
        if r.status_code in (401, 403):
            self.loaders.refresh_headers()
            self.session.headers.update(self.loaders.auth_headers)
            r = self.session.request(method.upper(), url, params=params, json=json, files=files, timeout=to)

        raise_for_response(r, payload={"params": params, "json": json})

        if r.status_code == 204 or not r.content:
            return None
        return r.json()

    @classmethod
    def default(cls) -> MainSequenceClient:
        """
        Single shared client used by models. No passing clients around.
        """
        if cls._default is None:
            # Reuse the existing global singletons from utils to avoid two sessions in prod.
            cls._default = cls(MainSequenceClientConfig(), loaders=_global_loaders, session=_global_session)
        return cls._default

    @classmethod
    def configure_default(cls, **kwargs) -> MainSequenceClient:
        """
        Startup-only configuration. Keeps a single default instance.

        Example:
            MainSequenceClient.configure_default(endpoint="https://staging...", timeout=(5,120))
        """
        cfg = MainSequenceClientConfig(**kwargs)
        if cls._default is None:
            cls._default = cls(cfg, loaders=_global_loaders, session=_global_session)
        else:
            # mutate config; keep same session reference (prod-safe)
            cls._default.config = cfg  # type: ignore[misc]
        return cls._default
