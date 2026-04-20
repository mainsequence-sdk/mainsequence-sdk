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
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import TypedDict

import psutil
import pytz
import requests
from requests.adapters import HTTPAdapter
from requests.structures import CaseInsensitiveDict
from urllib3.util.retry import Retry

from mainsequence.logconf import logger

# ---- Backend defaults (single source of truth) ----
TDAG_ENDPOINT = (
    os.environ.get("TDAG_ENDPOINT")
    or os.environ.get("MAINSEQUENCE_ENDPOINT")
    or "https://api.main-sequence.app"
)
API_ENDPOINT = f"{TDAG_ENDPOINT}/orm/api"
AUTH_ENDPOINT = TDAG_ENDPOINT.rstrip("/")

DEFAULT_STATUS_FORCELIST = (429, 500, 502, 503, 504)
DEFAULT_ALLOWED_METHODS = frozenset(["HEAD", "GET", "OPTIONS", "POST", "PUT", "PATCH", "DELETE"])

# requests supports either a float or (connect, read). Preserve previous ~120s read behavior,
# but add a sane connect timeout.
DEFAULT_TIMEOUT: tuple[float, float] = (5.0, 120.0)



DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"



class DataFrequency(str, Enum):
    one_m = "1m"
    one_min="1m"
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


UniqueIdentifierRangeMap = dict[str, DateInfo]

class AuthError(Exception):
    pass


def _jwt_reauth_hint() -> str:
    return (
        " Refresh your credentials with `mainsequence logout` and "
        "`mainsequence login <email>`. If this code runs in a separate shell or IDE, "
        "use `mainsequence login <email> --export` and load the exported env vars there."
    )


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
            raise AuthError(
                "Refresh is not allowed when MAINSEQUENCE_AUTH_MODE=session_jwt."
            )
        return None


@dataclass
class RuntimeCredentialAuthProvider(BaseAuthProvider):
    credential_id: str | None = None
    credential_secret: str | None = None
    token_url: str = f"{API_ENDPOINT}/pods/runtime-credentials/token/"
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
                timeout=self.timeout,
            )
            if response.status_code < 200 or response.status_code >= 300:
                raise AuthError(
                    "Runtime credential exchange failed with status "
                    f"{response.status_code}."
                )

            data = response.json()
            access = str(data.get("access") or "").strip()
            if not access:
                raise AuthError("Runtime credential exchange response did not include access token.")

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
            raise AuthError("MAINSEQUENCE_ACCESS_TOKEN is missing after runtime credential exchange.")

        return CaseInsensitiveDict(
            {
                "Authorization": f"{self.token_type} {access_token}",
            }
        )


@dataclass
class JWTAuthProvider(BaseAuthProvider):
    access_token: str | None = None
    refresh_token: str | None = None
    refresh_url: str = f"{AUTH_ENDPOINT}/auth/jwt-token/token/refresh/"
    obtain_url: str = f"{AUTH_ENDPOINT}/auth/jwt-token/token/"
    header_keyword: str = "Bearer"
    refresh_skew_seconds: int = 60
    timeout: tuple[float, float] = DEFAULT_TIMEOUT
    _lock: threading.RLock = field(default_factory=threading.RLock, init=False, repr=False)

    def __post_init__(self):
        if self.access_token is None:
            self.access_token = os.getenv("MAINSEQUENCE_ACCESS_TOKEN")
        if self.refresh_token is None:
            self.refresh_token = os.getenv("MAINSEQUENCE_REFRESH_TOKEN")

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

            if not self.refresh_token:
                if self.access_token and not force:
                    return
                raise AuthError("JWT refresh token is missing." + _jwt_reauth_hint())

            http_client = session or requests

            r = http_client.post(
                self.refresh_url,
                json={"refresh": self.refresh_token},
                headers={"Content-Type": "application/json"},
                timeout=self.timeout,
            )

            if r.status_code != 200:
                raise AuthError(
                    f"JWT refresh failed with status {r.status_code}." + _jwt_reauth_hint()
                )

            data = r.json()
            access = data.get("access")
            if not access:
                raise AuthError(
                    "JWT refresh response did not include access token." + _jwt_reauth_hint()
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
            timeout=self.timeout,
        )
        r.raise_for_status()
        data = r.json()
        self.set_tokens(access=data.get("access"), refresh=data.get("refresh"))
        return data


def request_to_datetime(string_date: str):
    if "+" in string_date:
        string_date = datetime.datetime.fromisoformat(string_date.replace("T", " ")).replace(
            tzinfo=pytz.utc
        )
        return string_date
    try:
        date = datetime.datetime.strptime(string_date, DATE_FORMAT).replace(tzinfo=pytz.utc)
    except ValueError:
        date = datetime.datetime.strptime(string_date, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=pytz.utc
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
        "No auth configured. Set MAINSEQUENCE_ACCESS_TOKEN / "
        "MAINSEQUENCE_REFRESH_TOKEN."
    )


class DoesNotExist(Exception):
    pass


class AuthLoaders:
    def __init__(self, provider: BaseAuthProvider | None = None):
        self.provider = provider

    def _provider(self) -> BaseAuthProvider:
        provider_kind = _default_auth_provider_kind()

        if provider_kind == "runtime_credential" and not isinstance(self.provider, RuntimeCredentialAuthProvider):
            self.provider = RuntimeCredentialAuthProvider()
        elif provider_kind == "session_jwt" and not isinstance(self.provider, SessionJWTAuthProvider):
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

    TIMEOFF = 0.25
    TRIES = int(15 // TIMEOFF)
    timeout = DEFAULT_TIMEOUT if time_out is None else time_out
    payload = {} if payload is None else payload

    def get_req(session):
        if r_type == "GET":
            return session.get
        elif r_type == "POST":
            return session.post
        elif r_type == "PUT":
            return session.put
        elif r_type == "PATCH":
            return session.patch
        elif r_type == "DELETE":
            return session.delete
        else:
            raise NotImplementedError(f"Unsupported method: {r_type}")

    request_kwargs = {}
    if r_type in ("POST", "PATCH") and "files" in payload:
        request_kwargs["data"] = payload.get("json", {})
        request_kwargs["files"] = payload["files"]
        s.headers.pop("Content-Type", None)
    else:
        request_kwargs = payload

    req = get_req(session=s)
    keep_request = True
    counter = 0
    auth_retried = False

    if accept_gzip:
        s.headers.setdefault("Accept-Encoding", "gzip")

    while keep_request:
        try:
            if loaders is not None:
                s.headers.update(loaders.refresh_headers(force=False, session=s))

            start_time = time.perf_counter()
            logger.debug(f"Requesting {r_type} from {url}")
            r = req(url, timeout=timeout, **request_kwargs)
            duration = time.perf_counter() - start_time
            logger.debug(f"{url} took {duration:.4f} seconds.")

            if r.status_code == 401 and loaders is not None and not auth_retried:
                logger.warning(f"Error {r.status_code}; forcing auth refresh once")
                try:
                    s.headers.update(loaders.refresh_headers(force=True, session=s))
                    req = get_req(session=s)
                    auth_retried = True
                    continue
                except AuthError:
                    logger.exception("Auth refresh failed")
                    keep_request = False
                    break

            keep_request = False
            break

        except AuthError as e:
            logger.warning(f"Auth error for {url}: {e}")
            r = Response()
            r.code = "auth_error"
            r.error_type = "auth_error"
            r.status_code = 401
            r._content = str(e).encode("utf-8")
            keep_request = False
            break

        except requests.exceptions.ConnectionError:
            logger.exception(f"Error connecting {url}")
        except TypeError as e:
            logger.exception(f"Type error for {url} exception {e}")
            raise e
        except Exception as e:
            logger.exception(f"Error connecting {url} exception {e}")

        counter += 1
        if counter >= TRIES:
            keep_request = False
            r = Response()
            r.code = "expired"
            r.error_type = "expired"
            r.status_code = 500
            break

        logger.debug(
            f"Trying request again after {TIMEOFF}s "
            f"- Counter: {counter}/{TRIES} - URL: {url}"
        )
        time.sleep(TIMEOFF)

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

    retry_kwargs = dict(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=backoff_factor,
        status_forcelist=DEFAULT_STATUS_FORCELIST,
        respect_retry_after_header=True,
        raise_on_status=False,
    )

    try:
        retry_cfg = Retry(allowed_methods=DEFAULT_ALLOWED_METHODS, **retry_kwargs)
    except TypeError:
        retry_cfg = Retry(method_whitelist=DEFAULT_ALLOWED_METHODS, **retry_kwargs)

    adapter = HTTPAdapter(max_retries=retry_cfg)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

# ---- Shared backend (import this in base/models) ----
loaders = AuthLoaders()
session = build_session(loaders=loaders)

def get_constants_tdag():
    url = f"{TDAG_ENDPOINT}/orm/api/ts_manager/api/constants"
    r = make_request(s=session, loaders=loaders, r_type="GET", url=url)
    return r.json()


def get_constants_vam():
    url = f"{TDAG_ENDPOINT}/orm/api/assets/api/constants"
    r = make_request(s=session, loaders=loaders, r_type="GET", url=url)
    return r.json()




class LazyConstants(dict):
    """
    Class Method to load constants only once they are called. this minimizes the calls to the API
    """

    def __init__(self, constant_type: str):
        if constant_type == "tdag":
            self.CONSTANTS_METHOD = get_constants_tdag
        elif constant_type == "vam":
            self.CONSTANTS_METHOD = get_constants_vam
        else:
            raise NotImplementedError(f"{constant_type} not implemented")
        self._initialized = False

    def __getattr__(self, key):
        if not self._initialized:
            self._load_constants()
        return self.__dict__[key]

    def _load_constants(self):
        # 1) call the method that returns your top-level dict
        raw_data = self.CONSTANTS_METHOD()
        # 2) Convert nested dicts to an "object" style
        nested = self.to_attr_dict(raw_data)
        # 3) Dump everything into self.__dict__ so it's dot-accessible
        for k, v in nested.items():
            self.__dict__[k] = v
        self._initialized = True

    def to_attr_dict(self, data):
        """
        Recursively convert a Python dict into an object that allows dot-notation access.
        Non-dict values (e.g., int, str, list) are returned as-is; dicts become _AttrDict.
        """
        if not isinstance(data, dict):
            return data

        class _AttrDict(dict):
            def __getattr__(self, name):
                return self[name]

            def __setattr__(self, name, value):
                self[name] = value

        out = _AttrDict()
        for k, v in data.items():
            out[k] = self.to_attr_dict(v)  # recursively transform
        return out


if "TDAG_CONSTANTS" not in locals():
    TDAG_CONSTANTS = LazyConstants("tdag")

if "MARKETS_CONSTANTS" not in locals():
    MARKETS_CONSTANTS = LazyConstants("vam")





def get_network_ip():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
        # Connect to a well-known external host (Google DNS) on port 80
        s.connect(("8.8.8.8", 80))
        # Get the local IP address used to make the connection
        network_ip = s.getsockname()[0]
    return network_ip


def is_process_running(pid: int) -> bool:
    """
    Check if a process with the given PID is running.

    Args:
        pid (int): The process ID to check.

    Returns:
        bool: True if the process is running, False otherwise.
    """
    try:
        # Check if the process with the given PID is running
        process = psutil.Process(pid)
        return process.is_running() and process.status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        # Process with the given PID does not exist
        return False


def set_types_in_table(df, column_types):
    index_cols = [name for name in df.index.names if name is not None]
    if index_cols:
        df = df.reset_index()

    for c, col_type in column_types.items():
        if c in df.columns:
            if col_type == "object":
                df[c] = df[c].astype(str)
            else:
                df[c] = df[c].astype(col_type)

    if index_cols:
        df = df.set_index(index_cols)
    return df


def serialize_to_json(kwargs):
    def to_jsonable(v):
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
            return {k: to_jsonable(x) for k, x in v.items()}
        if isinstance(v, (list, tuple)):
            return [to_jsonable(x) for x in v]

        return v

    return {k: to_jsonable(v) for k, v in kwargs.items()}




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
    return f"{uuid.getnode():012x}"


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
    retry_kwargs = dict(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=backoff_factor,
        status_forcelist=DEFAULT_STATUS_FORCELIST,
        respect_retry_after_header=True,
        raise_on_status=False,
    )

    # urllib3 compatibility across versions
    try:
        retry_cfg = Retry(allowed_methods=DEFAULT_ALLOWED_METHODS, **retry_kwargs)
    except TypeError:
        retry_cfg = Retry(method_whitelist=DEFAULT_ALLOWED_METHODS, **retry_kwargs)

    adapter = HTTPAdapter(max_retries=retry_cfg)

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
