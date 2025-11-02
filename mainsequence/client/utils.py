import copy
import datetime
import os
import time
from enum import Enum
from socket import socket
from typing import TypedDict

import psutil
import pytz
import requests
from requests.structures import CaseInsensitiveDict

from mainsequence.logconf import logger

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


class DoesNotExist(Exception):
    pass


class AuthLoaders:
    @property
    def auth_headers(self):
        if not hasattr(self, "_auth_headers"):
            self.refresh_headers()
        return self._auth_headers

    def refresh_headers(self):
        logger.debug("Getting Auth Headers ASSETS_ORM")
        self._auth_headers = get_authorization_headers()


def get_authorization_headers():
    headers = get_rest_token_header()
    return headers


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
    timeout = 120 if time_out is None else time_out
    payload = {} if payload is None else payload

    def get_req(session):
        if r_type == "GET":
            return session.get
        elif r_type == "POST":
            return session.post
        elif r_type == "PATCH":
            return session.patch
        elif r_type == "DELETE":
            return session.delete
        else:
            raise NotImplementedError(f"Unsupported method: {r_type}")

    # --- Prepare kwargs for requests call ---
    request_kwargs = {}
    if r_type in ("POST", "PATCH") and "files" in payload:
        # We have file uploads → use multipart form data
        request_kwargs["data"] = payload.get("json", {})  # form fields
        request_kwargs["files"] = payload["files"]  # actual files
        s.headers.pop("Content-Type", None)
    else:
        # Fallback: no files, no json → just form fields
        request_kwargs = payload

    req = get_req(session=s)
    keep_request = True
    counter = 0
    headers_refreshed = False

    if accept_gzip:
        # Don't clobber other headers; just ensure the key exists.
        # Keep it simple: gzip covers Django's GZipMiddleware.
        s.headers.setdefault("Accept-Encoding", "gzip")

    # Now loop with retry logic
    while keep_request:
        try:
            start_time = time.perf_counter()
            logger.debug(f"Requesting {r_type} from {url}")
            r = req(url, timeout=timeout, **request_kwargs)
            duration = time.perf_counter() - start_time
            logger.debug(f"{url} took {duration:.4f} seconds.")

            if r.status_code in [403, 401] and not headers_refreshed:
                logger.warning(f"Error {r.status_code} Refreshing headers")
                loaders.refresh_headers()
                s.headers.update(loaders.auth_headers)
                req = get_req(session=s)
                headers_refreshed = True
            else:
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
            f"Trying request again after {TIMEOFF}s " f"- Counter: {counter}/{TRIES} - URL: {url}"
        )
        time.sleep(TIMEOFF)
    return r


def build_session():
    from requests.adapters import HTTPAdapter, Retry

    s = requests.Session()
    retries = Retry(
        total=2,
        backoff_factor=2,
    )
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s


def get_constants_tdag():
    url = f"{os.getenv('TDAG_ENDPOINT')}/orm/api/ts_manager/api/constants"
    loaders = AuthLoaders()
    s = build_session()
    s.headers.update(loaders.auth_headers)
    r = make_request(s=s, loaders=loaders, r_type="GET", url=url)
    return r.json()


def get_constants_vam():
    url = f"{os.getenv('TDAG_ENDPOINT')}/orm/api/assets/api/constants"
    loaders = AuthLoaders()
    s = build_session()
    s.headers.update(loaders.auth_headers)
    r = make_request(s=s, loaders=loaders, r_type="GET", url=url)
    return r.json()


def get_binance_constants():
    url = f"{os.getenv('TDAG_ENDPOINT')}/orm/api/binance/constants"
    loaders = AuthLoaders()
    s = build_session()
    s.headers.update(loaders.auth_headers)
    r = make_request(s=s, loaders=loaders, r_type="GET", url=url)
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
        elif constant_type == "binance":
            self.CONSTANTS_METHOD = get_binance_constants
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

if "BINANCE_CONSTANTS" not in locals():
    BINANCE_CONSTANTS = LazyConstants("binance")


def get_rest_token_header():
    headers = CaseInsensitiveDict()
    headers["Content-Type"] = "application/json"

    if os.getenv("MAINSEQUENCE_TOKEN"):
        headers["Authorization"] = "Token " + os.getenv("MAINSEQUENCE_TOKEN")
        return headers
    else:
        raise Exception("MAINSEQUENCE_TOKEN is not set in env")


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
    new_data = {}
    for key, value in kwargs.items():
        new_value = copy.deepcopy(value)
        if isinstance(value, datetime.datetime):
            new_value = str(value)

        new_data[key] = new_value
    return new_data


import pathlib
import shutil
import subprocess
import uuid


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
