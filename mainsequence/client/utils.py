# mainsequence/client/utils.py
import base64
import datetime
import json
import os
import pathlib
import shutil
import socket
import subprocess
import threading
import time
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import TypedDict
from uuid import UUID, getnode

import psutil
import requests
from requests.structures import CaseInsensitiveDict

from mainsequence.defaults import resolve_backend_endpoint
from mainsequence.logconf import logger

from ._transport import DEFAULT_ALLOWED_METHODS as DEFAULT_ALLOWED_METHODS
from ._transport import DEFAULT_STATUS_FORCELIST as DEFAULT_STATUS_FORCELIST
from ._transport import DEFAULT_TIMEOUT as DEFAULT_TIMEOUT
from ._transport import DeadlineHTTPAdapter, remaining_timeout, request_budget

# ---- Backend defaults (single source of truth) ----
MAINSEQUENCE_ENDPOINT = resolve_backend_endpoint()
API_ENDPOINT = f"{MAINSEQUENCE_ENDPOINT}/api/v1"
AUTH_ENDPOINT = MAINSEQUENCE_ENDPOINT.rstrip("/")

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"


class DataFrequency(str, Enum):  # noqa: UP042 - preserve the published Enum string form
    one_m = "1m"
    one_min = "1m"
    five_m = "5m"
    one_d = "1d"
    one_w = "1w"
    one_year = "1y"
    one_month = "1mo"
    one_quarter = "1q"


class DateInfo(TypedDict, total=False):
    start_date: datetime.datetime | None
    start_date_operand: str | None
    end_date: datetime.datetime | None
    end_date_operand: str | None


class AuthError(Exception):
    pass


def set_mainsequence_endpoint(endpoint: str) -> None:
    global MAINSEQUENCE_ENDPOINT, API_ENDPOINT, AUTH_ENDPOINT
    normalized = endpoint.rstrip("/")
    MAINSEQUENCE_ENDPOINT = normalized
    API_ENDPOINT = f"{normalized}/api/v1"
    AUTH_ENDPOINT = normalized


_SESSION_TOKEN_VARIABLES = ("MAINSEQUENCE_ACCESS_TOKEN", "MAINSEQUENCE_REFRESH_TOKEN")
# Renewal answers that refuse the credentials, as opposed to a backend that failed.
_REFUSED_RENEWAL_STATUSES = frozenset({400, 401, 403})


def _format_utc(epoch: int) -> str:
    return datetime.datetime.fromtimestamp(epoch, tz=datetime.UTC).strftime(
        "%Y-%m-%d %H:%M UTC"
    )


def _session_backend() -> str:
    return (AUTH_ENDPOINT or "").rstrip("/")


def _login_command(backend: str) -> str:
    """
    Return the CLI command that signs in to `backend`.

    `mainsequence login` alone signs in to the configured backend. Another
    backend needs its address and the folder that holds its repositories.
    """
    try:
        from mainsequence.cli import config as cli_config

        configured = cli_config.normalize_backend_url(
            cli_config.get_persistent_config().get("backend_url")
        )
    except Exception:
        configured = ""
    if not backend or backend == configured:
        return "`mainsequence login`"
    return f"`mainsequence login {backend} <base folder>`"


def _env_file_holding_process_credentials() -> pathlib.Path | None:
    """
    Return the working directory's `.env` when it holds this process's tokens.

    IDE run configurations, launchers and dotenv calls load that file into a
    process's environment. Values are compared here and never shown.
    """
    from mainsequence.cli import config as cli_config

    env_path = pathlib.Path.cwd() / ".env"
    try:
        lines = env_path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError):
        return None
    for line in lines:
        key = cli_config.env_line_key(line)
        if key not in _SESSION_TOKEN_VARIABLES:
            continue
        value = line.split("=", 1)[1].split(" #", 1)[0].strip().strip("\"'")
        if value and value == (os.environ.get(key) or "").strip():
            return env_path
    return None


def _jwt_reauth_hint(refresh_token: str | None = None) -> str:
    """
    Say why the backend refused this process's credentials and how to repair them.

    The facts come from this process and this machine: the refresh token's own
    expiry, whether the credentials were already in the environment when the SDK
    started (and which `.env` holds them, when one does), and the saved session
    of the backend. A pair from the environment wins over the saved session, so
    the saved session is named and not used: the pair may belong to another user
    or backend. No token value is included.
    """
    backend = _session_backend()
    refresh = refresh_token if refresh_token is not None else os.getenv("MAINSEQUENCE_REFRESH_TOKEN")
    expiry = _decode_jwt_exp(refresh)
    try:
        from mainsequence import bootstrap

        source = bootstrap.credential_source()
        from_environment = source == bootstrap.CREDENTIALS_FROM_ENVIRONMENT
        from_store = source == bootstrap.CREDENTIALS_FROM_STORE
    except Exception:
        from_environment = from_store = False
    login = _login_command(backend)

    parts = []
    if expiry is not None and expiry <= int(time.time()):
        parts.append(f" The refresh token expired on {_format_utc(expiry)}.")
    elif expiry is not None:
        parts.append(
            " The refresh token has not expired: the backend revoked it, or it was "
            "issued by another backend."
        )

    if from_environment:
        try:
            env_file = _env_file_holding_process_credentials()
        except Exception:
            env_file = None
        try:
            from mainsequence.cli import config as cli_config

            saved = cli_config.saved_session_summary(backend)
        except Exception:
            saved = {"usable": False, "username": "", "expires_at": None}
        if env_file is not None:
            parts.append(
                f" These credentials come from the token lines of {env_file}, not from "
                "your saved session."
            )
            cleanup = f"run `mainsequence refresh-token` in {env_file.parent} to remove those lines"
        else:
            parts.append(
                " These credentials were in this process's environment "
                "(MAINSEQUENCE_ACCESS_TOKEN / MAINSEQUENCE_REFRESH_TOKEN) when it started, "
                "set by the program that started it, not taken from your saved session."
            )
            cleanup = "start the process without those two variables"
        if saved["usable"]:
            who = f" ({saved['username']})" if saved["username"] else ""
            until = (
                f", valid until {_format_utc(saved['expires_at'])}"
                if saved["expires_at"] is not None
                else ""
            )
            parts.append(
                f" Your saved session for {backend}{who} is usable{until}: "
                f"{cleanup}, and the next start uses it."
            )
        else:
            parts.append(
                f" There is no usable saved session for {backend}. Sign in with {login}, "
                f"then {cleanup}."
            )
    elif from_store:
        parts.append(
            f" These credentials are your saved session for {backend}. Sign in again with {login}."
        )
    else:
        parts.append(f" Sign in with {login}.")
    return "".join(parts)


def _env_has_value(name: str) -> bool:
    return bool((os.getenv(name) or "").strip())


def _default_auth_provider_kind() -> str | None:
    mode = (os.getenv("MAINSEQUENCE_AUTH_MODE") or "jwt").strip().lower()
    has_access = _env_has_value("MAINSEQUENCE_ACCESS_TOKEN")
    has_refresh = _env_has_value("MAINSEQUENCE_REFRESH_TOKEN")

    if mode == "runtime_credential":
        return "runtime_credential"

    if mode == "session_jwt":
        if has_access or has_refresh:
            return "session_jwt"
        return None

    if mode == "jwt":
        if has_access or has_refresh:
            return "jwt"
        return None

    if has_access or has_refresh:
        return "jwt"

    return None


def _decode_jwt_exp(token: str | None) -> int | None:
    """
    Decode JWT payload without signature verification.
    Used ONLY to decide whether to refresh early.
    """
    if not token:
        return None

    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None

        payload = parts[1] + "=" * (-len(parts[1]) % 4)
        data = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")).decode("utf-8"))
        exp = data.get("exp")
        return int(exp) if exp is not None else None
    except Exception:
        return None


class BaseAuthProvider:
    def get_headers(self) -> CaseInsensitiveDict:
        raise NotImplementedError

    def refresh(
        self,
        *,
        force: bool = False,
        session: requests.Session | None = None,
    ) -> None:
        return None


@dataclass
class SessionJWTAuthProvider(BaseAuthProvider):
    access_token: str | None = None
    refresh_token: str | None = None
    header_keyword: str = "Bearer"

    def __post_init__(self):
        if self.access_token is None:
            self.access_token = os.getenv("MAINSEQUENCE_ACCESS_TOKEN")
        if self.refresh_token is None:
            self.refresh_token = os.getenv("MAINSEQUENCE_REFRESH_TOKEN")
        if self.refresh_token:
            raise AuthError(
                "MAINSEQUENCE_REFRESH_TOKEN is not allowed when MAINSEQUENCE_AUTH_MODE=session_jwt."
            )

    def get_headers(self) -> CaseInsensitiveDict:
        if not self.access_token:
            raise AuthError(
                "MAINSEQUENCE_ACCESS_TOKEN is required when MAINSEQUENCE_AUTH_MODE=session_jwt."
            )

        return CaseInsensitiveDict(
            {
                "Authorization": f"{self.header_keyword} {self.access_token}",
            }
        )

    def refresh(
        self,
        *,
        force: bool = False,
        session: requests.Session | None = None,
    ) -> None:
        if force:
            raise AuthError("Refresh is not allowed when MAINSEQUENCE_AUTH_MODE=session_jwt.")
        return None


@dataclass
class RuntimeCredentialAuthProvider(BaseAuthProvider):
    credential_id: str | None = None
    credential_secret: str | None = None
    token_url: str | None = None
    token_type: str = "Bearer"
    refresh_skew_seconds: int = 30
    timeout: tuple[float, float] = DEFAULT_TIMEOUT
    expires_at: float | None = None
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False, repr=False)

    def __post_init__(self):
        if self.credential_id is None:
            self.credential_id = os.getenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_ID")
        if self.credential_secret is None:
            self.credential_secret = os.getenv("MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET")
        if not self.token_url:
            self.token_url = f"{API_ENDPOINT}/runtime-credentials/token/"

    def _current_access_token(self) -> str | None:
        return (os.getenv("MAINSEQUENCE_ACCESS_TOKEN") or "").strip() or None

    def _needs_exchange(self) -> bool:
        access_token = self._current_access_token()
        if not access_token:
            return True

        if self.expires_at is not None:
            return self.expires_at <= time.time() + self.refresh_skew_seconds

        exp = _decode_jwt_exp(access_token)
        if exp is None:
            # Access-only JWT behavior: use opaque/uninspectable access until a 401 forces exchange.
            return False

        return exp <= int(time.time()) + self.refresh_skew_seconds

    def _require_credentials(self) -> tuple[str, str]:
        credential_id = (self.credential_id or "").strip()
        credential_secret = (self.credential_secret or "").strip()
        if not credential_id:
            raise AuthError(
                "MAINSEQUENCE_RUNTIME_CREDENTIAL_ID is required when "
                "MAINSEQUENCE_AUTH_MODE=runtime_credential."
            )
        if not credential_secret:
            raise AuthError(
                "MAINSEQUENCE_RUNTIME_CREDENTIAL_SECRET is required when "
                "MAINSEQUENCE_AUTH_MODE=runtime_credential."
            )
        return credential_id, credential_secret

    def refresh(
        self,
        *,
        force: bool = False,
        session: requests.Session | None = None,
    ) -> None:
        _ = session
        with self._lock:
            if not force and not self._needs_exchange():
                return

            credential_id, credential_secret = self._require_credentials()
            response = requests.post(
                self.token_url,
                json={
                    "credential_id": credential_id,
                    "credential_secret": credential_secret,
                },
                headers={"Content-Type": "application/json"},
                timeout=remaining_timeout(self.timeout),
                allow_redirects=False,
            )
            if response.status_code < 200 or response.status_code >= 300:
                raise AuthError(
                    f"Runtime credential exchange failed with status {response.status_code}."
                )

            data = response.json()
            access = str(data.get("access") or "").strip()
            if not access:
                raise AuthError(
                    "Runtime credential exchange response did not include access token."
                )

            runtime_code_repository_context = data.get("runtime_code_repository_context")
            if runtime_code_repository_context is not None:
                if not isinstance(runtime_code_repository_context, dict):
                    raise AuthError("Runtime credential exchange returned invalid CodeRepository context.")
                from mainsequence.code_repository_context import (
                    CodeRepositoryContextError,
                    _install_authenticated_runtime_code_repository_context,
                )

                try:
                    _install_authenticated_runtime_code_repository_context(runtime_code_repository_context)
                except CodeRepositoryContextError as exc:
                    raise AuthError(
                        "Runtime credential exchange returned unusable CodeRepository context."
                    ) from exc

            token_type = str(data.get("token_type") or self.token_type or "Bearer").strip()
            self.token_type = token_type or "Bearer"

            expires_in_raw = data.get("expires_in")
            try:
                expires_in = int(expires_in_raw)
            except (TypeError, ValueError):
                expires_in = None
            self.expires_at = time.time() + expires_in if expires_in and expires_in > 0 else None
            os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = access

    def get_headers(self) -> CaseInsensitiveDict:
        if self._needs_exchange():
            self.refresh(force=False)

        access_token = self._current_access_token()
        if not access_token:
            raise AuthError(
                "MAINSEQUENCE_ACCESS_TOKEN is missing after runtime credential exchange."
            )

        return CaseInsensitiveDict(
            {
                "Authorization": f"{self.token_type} {access_token}",
            }
        )


@dataclass
class JWTAuthProvider(BaseAuthProvider):
    access_token: str | None = None
    refresh_token: str | None = None
    refresh_url: str | None = None
    obtain_url: str | None = None
    header_keyword: str = "Bearer"
    refresh_skew_seconds: int = 60
    timeout: tuple[float, float] = DEFAULT_TIMEOUT
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False, repr=False)

    def __post_init__(self):
        if self.access_token is None:
            self.access_token = os.getenv("MAINSEQUENCE_ACCESS_TOKEN")
        if self.refresh_token is None:
            self.refresh_token = os.getenv("MAINSEQUENCE_REFRESH_TOKEN")
        if not self.refresh_url:
            self.refresh_url = f"{AUTH_ENDPOINT}/auth/jwt-token/token/refresh/"
        if not self.obtain_url:
            self.obtain_url = f"{AUTH_ENDPOINT}/auth/jwt-token/token/"

    def set_tokens(self, *, access: str | None = None, refresh: str | None = None) -> None:
        with self._lock:
            if access is not None:
                self.access_token = access
                os.environ["MAINSEQUENCE_ACCESS_TOKEN"] = access
            if refresh is not None:
                self.refresh_token = refresh
                os.environ["MAINSEQUENCE_REFRESH_TOKEN"] = refresh

    def clear(self) -> None:
        with self._lock:
            self.access_token = None
            self.refresh_token = None
            os.environ.pop("MAINSEQUENCE_ACCESS_TOKEN", None)
            os.environ.pop("MAINSEQUENCE_REFRESH_TOKEN", None)

    def _needs_refresh(self) -> bool:
        if not self.access_token:
            return True

        exp = _decode_jwt_exp(self.access_token)
        if exp is None:
            # If we cannot inspect exp, just use the token until server says no.
            return False

        return exp <= int(time.time()) + self.refresh_skew_seconds

    def refresh(
        self,
        *,
        force: bool = False,
        session: requests.Session | None = None,
    ) -> None:
        with self._lock:
            if not force and not self._needs_refresh():
                return

            backend = _session_backend()
            if not self.refresh_token:
                if self.access_token and not force:
                    return
                raise AuthError(
                    f"Main Sequence cannot renew the session of this process for {backend}: "
                    "it has no refresh token." + _jwt_reauth_hint(self.refresh_token)
                )

            http_client = session or requests

            r = http_client.post(
                self.refresh_url,
                json={"refresh": self.refresh_token},
                headers={"Content-Type": "application/json"},
                timeout=remaining_timeout(self.timeout),
            )

            if r.status_code in _REFUSED_RENEWAL_STATUSES:
                raise AuthError(
                    f"Main Sequence refused to renew the session of this process for {backend} "
                    f"(HTTP {r.status_code})." + _jwt_reauth_hint(self.refresh_token)
                )
            if r.status_code != 200:
                # The backend failed; the credentials were not judged.
                raise AuthError(
                    f"Main Sequence could not renew the session of this process for {backend}: "
                    f"the backend answered HTTP {r.status_code}. Try again when it is available."
                )

            data = r.json()
            access = data.get("access")
            if not access:
                raise AuthError(
                    f"Main Sequence renewed the session for {backend} without an access token."
                    + _jwt_reauth_hint(self.refresh_token)
                )

            # Important if ROTATE_REFRESH_TOKENS=True
            new_refresh = data.get("refresh")
            self.set_tokens(access=access, refresh=new_refresh)

    def get_headers(self) -> CaseInsensitiveDict:
        if not self.access_token:
            raise AuthError("JWT access token is missing")

        return CaseInsensitiveDict(
            {
                "Authorization": f"{self.header_keyword} {self.access_token}",
            }
        )

    def login(
        self,
        username: str,
        password: str,
        session: requests.Session | None = None,
    ) -> dict:
        http_client = session or requests

        r = http_client.post(
            self.obtain_url,
            json={"username": username, "password": password},
            headers={"Content-Type": "application/json"},
            timeout=remaining_timeout(self.timeout),
        )
        r.raise_for_status()
        data = r.json()
        self.set_tokens(access=data.get("access"), refresh=data.get("refresh"))
        return data


def request_to_datetime(string_date: str):
    if "+" in string_date:
        string_date = datetime.datetime.fromisoformat(string_date.replace("T", " ")).replace(
            tzinfo=datetime.UTC
        )
        return string_date
    try:
        date = datetime.datetime.strptime(string_date, DATE_FORMAT).replace(tzinfo=datetime.UTC)
    except ValueError:
        date = datetime.datetime.strptime(string_date, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=datetime.UTC
        )
    return date


def build_default_auth_provider() -> BaseAuthProvider:
    provider_kind = _default_auth_provider_kind()

    if provider_kind == "session_jwt":
        return SessionJWTAuthProvider()

    if provider_kind == "runtime_credential":
        return RuntimeCredentialAuthProvider()

    if provider_kind == "jwt":
        return JWTAuthProvider()

    raise AuthError(
        "No auth configured. Set MAINSEQUENCE_ACCESS_TOKEN / MAINSEQUENCE_REFRESH_TOKEN."
    )


class DoesNotExist(Exception):
    pass


class AuthLoaders:
    def __init__(self, provider: BaseAuthProvider | None = None):
        self.provider = provider

    def _provider(self) -> BaseAuthProvider:
        provider_kind = _default_auth_provider_kind()

        if provider_kind == "runtime_credential" and not isinstance(
            self.provider, RuntimeCredentialAuthProvider
        ):
            self.provider = RuntimeCredentialAuthProvider()
        elif provider_kind == "session_jwt" and not isinstance(
            self.provider, SessionJWTAuthProvider
        ):
            self.provider = SessionJWTAuthProvider()
        elif provider_kind == "jwt" and not isinstance(self.provider, JWTAuthProvider):
            self.provider = JWTAuthProvider()
        elif self.provider is None:
            self.provider = build_default_auth_provider()
        return self.provider

    @property
    def auth_headers(self):
        return self._provider().get_headers()

    def refresh_headers(
        self,
        force: bool = False,
        session: requests.Session | None = None,
    ):
        provider = self._provider()
        provider.refresh(force=force, session=session)
        return provider.get_headers()

    def use_jwt(self, *, access: str | None = None, refresh: str | None = None):
        self.provider = JWTAuthProvider(access_token=access, refresh_token=refresh)

    def use_session_jwt(self, *, access: str | None = None):
        self.provider = SessionJWTAuthProvider(access_token=access, refresh_token=None)

    def clear_auth(self):
        self.provider = None


def get_rest_token_header():
    return loaders.refresh_headers()


def get_authorization_headers():
    return loaders.refresh_headers()


def make_request(
    s,
    r_type: str,
    url: str,
    loaders: AuthLoaders | None,
    payload: dict | None = None,
    time_out=None,
    accept_gzip: bool = True,
):
    from requests.models import Response

    timeout = DEFAULT_TIMEOUT if time_out is None else time_out
    payload = {} if payload is None else payload
    r_type = r_type.upper()
    if r_type not in DEFAULT_ALLOWED_METHODS | {"POST", "PUT", "PATCH", "DELETE"}:
        raise NotImplementedError(f"Unsupported method: {r_type}")

    request_kwargs = {}
    if r_type in ("POST", "PATCH") and "files" in payload:
        request_kwargs = dict(payload)
        request_kwargs["data"] = request_kwargs.pop("json", {})
        s.headers.pop("Content-Type", None)
    else:
        request_kwargs = dict(payload)
    request_kwargs.setdefault("allow_redirects", False)
    req = getattr(s, r_type.lower())

    if accept_gzip:
        s.headers.setdefault("Accept-Encoding", "gzip")

    with request_budget(timeout) as budget:
        try:
            for auth_attempt in range(2):
                if loaders is not None:
                    budget.timeout()
                    s.headers.update(loaders.refresh_headers(force=bool(auth_attempt), session=s))

                start_time = time.perf_counter()
                logger.debug(f"Requesting {r_type} from {url}")
                r = req(url, timeout=budget.timeout(), **request_kwargs)
                duration = time.perf_counter() - start_time
                logger.debug(f"{url} took {duration:.4f} seconds.")
                if (
                    r.status_code != 401
                    or loaders is None
                    or auth_attempt
                    or r_type not in DEFAULT_ALLOWED_METHODS
                ):
                    return r
                r.close()
                logger.warning("Error 401; forcing auth refresh once within the request budget")

        except AuthError as e:
            logger.warning(f"Auth error for {url}: {e}")
            r = Response()
            r.code = "auth_error"
            r.error_type = "auth_error"
            r.status_code = 401
            r._content = str(e).encode("utf-8")
            return r
        except requests.RequestException as e:
            logger.warning(f"Request {r_type} to {url} failed: {e}")
            r = Response()
            r.code = "expired"
            r.error_type = "expired"
            r.status_code = 500
            r._content = str(e).encode("utf-8")
            return r


def build_session(
    *,
    loaders: AuthLoaders | None = None,
    retries: int = 3,
    backoff_factor: float = 0.5,
    accept_gzip: bool = True,
) -> requests.Session:
    s = requests.Session()

    # Do not pin auth headers here.
    # Auth is attached per request inside make_request().

    if accept_gzip:
        s.headers.setdefault("Accept-Encoding", "gzip")

    adapter = DeadlineHTTPAdapter(retries=retries, backoff_factor=backoff_factor)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


# ---- Shared backend (import this in base/models) ----
loaders = AuthLoaders()
session = build_session(loaders=loaders)


def get_network_ip() -> str:
    """Return the local address selected for an outbound network route."""

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.connect(("8.8.8.8", 80))
        return sock.getsockname()[0]


def is_process_running(pid: int) -> bool:
    """Return whether ``pid`` identifies a live, non-zombie process."""

    try:
        process = psutil.Process(pid)
        return process.is_running() and process.status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False


def serialize_to_json(kwargs):
    def to_jsonable(v):
        if isinstance(v, Decimal):
            return str(v)

        if isinstance(v, UUID):
            return str(v)

        if isinstance(v, datetime.datetime):
            dt = v
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=datetime.UTC)
            else:
                dt = dt.astimezone(datetime.UTC)
            return dt.isoformat().replace("+00:00", "Z")

        if hasattr(v, "model_dump"):
            try:
                return v.model_dump(mode="json", exclude_none=True)
            except TypeError:
                return v.model_dump()

        if isinstance(v, dict):
            return {to_json_key(k): to_jsonable(x) for k, x in v.items()}
        if isinstance(v, (list, tuple)):
            return [to_jsonable(x) for x in v]

        return v

    def to_json_key(value):
        key = to_jsonable(value)
        if key is None or isinstance(key, str | int | float | bool):
            return key
        return str(key)

    return {to_json_key(k): to_jsonable(v) for k, v in kwargs.items()}


def _linux_machine_id() -> str | None:
    """Return the OS machine‑id if readable (many distros make this 0644)."""
    for p in ("/etc/machine-id", "/var/lib/dbus/machine-id"):
        path = pathlib.Path(p)
        if path.is_file():
            try:
                return path.read_text().strip().lower()
            except PermissionError:
                continue
    return None


def bios_uuid() -> str:
    """Best‑effort hardware/OS identifier that never returns None.

    Order of preference
    -------------------
    1. `/sys/class/dmi/id/product_uuid`          (kernel‑exported, no root)
    2. `dmidecode -s system-uuid`                (requires root *and* dmidecode)
    3. `/etc/machine-id` or `/var/lib/dbus/machine-id`
    4. `uuid.getnode()` (MAC address as 48‑bit int, zero‑padded hex)

    The value is always lower‑case and stripped of whitespace.
    """
    # Tier 1 – kernel DMI file
    path = pathlib.Path("/sys/class/dmi/id/product_uuid")
    if path.is_file():
        try:
            val = path.read_text().strip().lower()
            if val:
                return val
        except PermissionError:
            pass

    # Tier 2 – dmidecode, but only if available *and* running as root
    if shutil.which("dmidecode") and os.geteuid() == 0:
        try:
            out = subprocess.check_output(
                ["dmidecode", "-s", "system-uuid"],
                text=True,
                stderr=subprocess.DEVNULL,
            )
            val = out.splitlines()[0].strip().lower()
            if val:
                return val
        except subprocess.SubprocessError:
            pass

    # Tier 3 – machine‑id
    mid = _linux_machine_id()
    if mid:
        return mid

    # Tier 4 – MAC address (uuid.getnode). Always available.
    return f"{getnode():012x}"


def _install_retry_adapters_in_place(
    s: requests.Session,
    *,
    retries: int,
    backoff_factor: float,
) -> None:
    """
    Configure retry adapters on an EXISTING session object (do not rebind 'session').
    This is critical so 'from utils import session' users still get the updated behavior.
    """
    adapter = DeadlineHTTPAdapter(retries=retries, backoff_factor=backoff_factor)

    # Close old adapters' pools (best-effort), then mount new ones
    for prefix in ("https://", "http://"):
        old = s.adapters.get(prefix)
        if old is not None:
            try:
                old.close()
            except Exception:
                pass

    s.mount("https://", adapter)
    s.mount("http://", adapter)
