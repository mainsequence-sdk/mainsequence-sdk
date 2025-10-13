# mainsequence/cli/api.py
from __future__ import annotations

import json
import os
import re

import requests

from .config import backend_url, get_tokens, save_tokens, set_env_access

AUTH_PATHS = {
    "obtain": "/auth/jwt-token/token/",
    "refresh": "/auth/jwt-token/token/refresh/",
    "ping": "/auth/rest-auth/user/",
}

S = requests.Session()
S.headers.update({"Content-Type": "application/json"})


class ApiError(RuntimeError): ...


class NotLoggedIn(ApiError): ...


def _full(path: str) -> str:
    p = "/" + path.lstrip("/")
    return backend_url() + p


def _normalize_api_path(p: str) -> str:
    p = "/" + (p or "").lstrip("/")
    if not re.match(r"^/(api|auth|pods|orm|user)(/|$)", p):
        raise ApiError("Only /api/*, /auth/*, /pods/*, /orm/*, /user/* allowed")
    return p


def _access_token() -> str | None:
    t = os.environ.get("MAIN_SEQUENCE_USER_TOKEN")
    if t:
        return t
    tok = get_tokens()
    return tok.get("access")


def _refresh_token() -> str | None:
    tok = get_tokens()
    return tok.get("refresh")


def login(email: str, password: str) -> dict:
    email = (email or "").strip()
    password = (password or "").rstrip("\r\n")

    url = _full(AUTH_PATHS["obtain"])
    payload = {"email": email, "password": password}  # server expects 'email'
    r = S.post(url, data=json.dumps(payload))
    try:
        data = r.json()
    except Exception:
        data = {}
    if not r.ok:
        msg = data.get("detail") or data.get("message") or r.text
        raise ApiError(f"{msg}")
    access = data.get("access") or data.get("token") or data.get("jwt") or data.get("access_token")
    refresh = data.get("refresh") or data.get("refresh_token")
    if not access or not refresh:
        raise ApiError("Server did not return expected tokens.")
    save_tokens(email, access, refresh)
    set_env_access(access)
    return {"username": email, "backend": backend_url()}


def refresh_access() -> str:
    refresh = _refresh_token()
    if not refresh:
        raise NotLoggedIn("Not logged in. Run `mainsequence login <email>`.")
    r = S.post(_full(AUTH_PATHS["refresh"]), data=json.dumps({"refresh": refresh}))
    data = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    if not r.ok:
        raise NotLoggedIn(data.get("detail") or "Token refresh failed.")
    access = data.get("access")
    if not access:
        raise NotLoggedIn("Refresh succeeded but no access token returned.")
    tokens = get_tokens()
    save_tokens(tokens.get("username") or "", access, refresh)
    set_env_access(access)
    return access


def authed(method: str, api_path: str, body: dict | None = None) -> requests.Response:
    api_path = _normalize_api_path(api_path)
    access = _access_token()
    if not access:
        # try to refresh once
        access = refresh_access()
    headers = {"Authorization": f"Bearer {access}"}
    r = S.request(
        method.upper(),
        _full(api_path),
        headers=headers,
        data=None if method.upper() in {"GET", "HEAD"} else json.dumps(body or {}),
    )
    if r.status_code == 401:
        # retry after refresh
        access = refresh_access()
        headers = {"Authorization": f"Bearer {access}"}
        r = S.request(
            method.upper(),
            _full(api_path),
            headers=headers,
            data=None if method.upper() in {"GET", "HEAD"} else json.dumps(body or {}),
        )
    if r.status_code == 401:
        raise NotLoggedIn("Not logged in.")
    return r


# ---------- Helper APIs ----------


def safe_slug(s: str) -> str:
    x = re.sub(r"[^a-z0-9-_]+", "-", (s or "project").lower()).strip("-")
    return x[:64] or "project"


def repo_name_from_git_url(url: str | None) -> str | None:
    if not url:
        return None
    s = re.sub(r"[?#].*$", "", url.strip())
    last = s.split("/")[-1] if "/" in s else s
    if last.lower().endswith(".git"):
        last = last[:-4]
    return re.sub(r"[^A-Za-z0-9._-]+", "-", last)


def deep_find_repo_url(extra) -> str | None:
    if not isinstance(extra, dict):
        return None
    cand = ["ssh_url", "git_ssh_url", "repo_ssh_url", "git_url", "repo_url", "repository", "url"]
    for k in cand:
        v = extra.get(k)
        if isinstance(v, str) and (v.startswith("git@") or re.search(r"\.git($|\?)", v)):
            return v
        if isinstance(v, dict):
            for vv in v.values():
                if isinstance(vv, str) and (vv.startswith("git@") or re.search(r"\.git($|\?)", vv)):
                    return vv
    for v in extra.values():
        if isinstance(v, dict):
            found = deep_find_repo_url(v)
            if found:
                return found
    return None


def get_current_user_profile() -> dict:
    who = authed("GET", AUTH_PATHS["ping"])
    d = who.json() if who.ok else {}
    uid = d.get("id") or d.get("pk") or (d.get("user") or {}).get("id") or d.get("user_id")
    if not uid:
        return {}
    full = authed("GET", f"/user/api/user/{uid}/")
    u = full.json() if full.ok else {}
    org_name = (u.get("organization") or {}).get("name") or u.get("organization_name") or ""
    return {"username": u.get("username") or "", "organization": org_name}


def get_projects() -> list[dict]:
    r = authed("GET", "/orm/api/pods/projects/")
    # If the API shape ever changes, still try to pull a list.
    if not r.ok:
        raise ApiError(f"Projects fetch failed ({r.status_code}).")
    data = (
        r.json()
        if r.headers.get(
            "content-type",
            "",
        ).startswith("application/json")
        else {}
    )
    if isinstance(data, list):
        return data
    return data.get("results") or []


def fetch_project_env_text(project_id: int | str) -> str:
    r = authed("GET", f"/orm/api/pods/projects/{project_id}/get_environment/")
    raw = r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text
    if isinstance(raw, dict):
        raw = (
            raw.get("environment") or raw.get("env") or raw.get("content") or raw.get("text") or ""
        )
    return raw or ""


def add_deploy_key(project_id: int | str, key_title: str, public_key: str) -> None:
    try:
        authed(
            "POST",
            f"/orm/api/pods/projects/{project_id}/add_deploy_key/",
            {"key_title": key_title, "public_key": public_key},
        )
    except Exception:
        pass


def get_project_token(project_id: int | str) -> str:
    """
    Fetch the project's token using the current access token.
    If the access token is expired or missing, authed() will refresh once.
    If refresh also fails, NotLoggedIn is raised so the caller can prompt re-login.
    """
    r = authed("GET", f"/orm/api/pods/projects/{project_id}/get_project_token/")
    if not r.ok:
        # authed() already tried refresh on 401;
        # at this point treat as API error with server message.
        msg = r.text
        try:
            if r.headers.get("content-type", "").startswith("application/json"):
                data = r.json()
                msg = data.get("detail") or data.get("message") or msg
        except Exception:
            pass
        raise ApiError(f"Project token fetch failed ({r.status_code}). {msg}")

    token = None
    if r.headers.get("content-type", "").startswith("application/json"):
        data = r.json()
        token = data["development"][0]

    if not token:
        raise ApiError("Project token response did not include a token.")
    return token
