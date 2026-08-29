from __future__ import annotations

import hashlib
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
from urllib.parse import urlsplit

_SUPPORTED_GIT_ORIGIN_SCHEMES = {"git", "git+ssh", "http", "https", "ssh"}
_SSH_GIT_ORIGIN_SCHEMES = {"git+ssh", "ssh"}
_DEFAULT_GIT_ORIGIN_PORTS = {"http": 80, "https": 443, "ssh": 22, "git+ssh": 22}
_SCP_GIT_ORIGIN_PATTERN = re.compile(
    r"^(?:[^@/\s]+@)?(?P<host>[^:/\s]+):(?P<path>.+)$"
)


def which(cmd: str) -> str | None:
    p = shutil.which(cmd)
    return p


def run(cmd, *args, env=None, cwd=None) -> tuple[int, str, str]:
    proc = subprocess.Popen(
        [cmd, *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env, cwd=cwd
    )
    out, err = proc.communicate()
    return proc.returncode, out, err


def _clean_github_repository_binding_path(value: str) -> str:
    if "\\" in value:
        raise ValueError("Git origin repository paths must use forward slashes.")
    path = re.sub(r"/+", "/", value).strip("/")
    if path.lower().endswith(".git"):
        path = path[:-4]
    path = path.rstrip("/")
    if not path:
        raise ValueError("Git origin must include a repository path.")
    return path


def repository_ssh_key_identity(repo_url: str) -> tuple[str, bool]:
    """Return the transport-neutral repository identity and whether the origin uses SSH."""
    candidate = re.sub(r"[?#].*$", "", str(repo_url or "").strip())
    if not candidate:
        raise ValueError("Git origin must be non-empty.")
    if "\n" in candidate or "\r" in candidate:
        raise ValueError("Git origin must contain one non-empty line.")

    scheme_match = re.match(r"^(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*)://", candidate)
    if scheme_match:
        scheme = scheme_match.group("scheme").lower()
        if scheme not in _SUPPORTED_GIT_ORIGIN_SCHEMES:
            raise ValueError(f"Unsupported Git origin scheme: {scheme}.")
        try:
            parsed = urlsplit(candidate)
            port = parsed.port
        except ValueError as exc:
            raise ValueError(f"Invalid Git origin: {repo_url!r}.") from exc
        host = (parsed.hostname or "").lower().rstrip(".")
        if not host:
            raise ValueError("Git origin must include a hostname.")
        path = _clean_github_repository_binding_path(parsed.path)
        default_port = _DEFAULT_GIT_ORIGIN_PORTS.get(scheme)
        host_identity = host if port in (None, default_port) else f"{host}:{port}"
        return f"{host_identity}/{path}", scheme in _SSH_GIT_ORIGIN_SCHEMES

    scp_match = _SCP_GIT_ORIGIN_PATTERN.fullmatch(candidate)
    if not scp_match:
        raise ValueError("Git origin must be an SSH, Git, HTTP, or HTTPS repository URL.")
    host = scp_match.group("host").lower().rstrip(".")
    path = _clean_github_repository_binding_path(scp_match.group("path"))
    return f"{host}/{path}", True


def repository_ssh_key_name(repo_url: str) -> str:
    """Derive the cross-CLI Repository SSH Key Identity v1 filename."""
    identity, _uses_ssh = repository_ssh_key_identity(repo_url)
    repository_name = identity.rsplit("/", 1)[-1]
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", repository_name).strip("._-").lower()
    slug = slug[:48].rstrip("._-") or "repository"
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    return f"mainsequence-{slug}-{digest}"


def repository_ssh_key_paths(repo_url: str) -> tuple[pathlib.Path, pathlib.Path]:
    key = pathlib.Path.home() / ".ssh" / repository_ssh_key_name(repo_url)
    return key, pathlib.Path(f"{key}.pub")


def git_ssh_environment(
    key_path: pathlib.Path,
    *,
    base_env: dict[str, str] | None = None,
) -> dict[str, str]:
    env = dict(base_env or os.environ)
    env["GIT_SSH_COMMAND"] = f'ssh -i "{str(key_path)}" -o IdentitiesOnly=yes'
    return env


def require_ssh_git_origin(repo_url: str) -> str:
    identity, uses_ssh = repository_ssh_key_identity(repo_url)
    if not uses_ssh:
        raise ValueError(
            "Git origin must use SSH before a repository deploy key can be selected."
        )
    return identity


def ensure_key_for_repo(repo_url: str) -> tuple[pathlib.Path, pathlib.Path, str]:
    require_ssh_git_origin(repo_url)
    key, pub = repository_ssh_key_paths(repo_url)
    key.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    private_key_exists = key.exists()
    public_key_exists = pub.exists()
    if private_key_exists != public_key_exists:
        missing_path = pub if private_key_exists else key
        raise RuntimeError(f"Repository SSH keypair is incomplete; missing: {missing_path}")
    if not private_key_exists:
        rc, _out, err = run(
            "ssh-keygen",
            "-t",
            "ed25519",
            "-C",
            "mainsequence@main-sequence.io",
            "-f",
            str(key),
            "-N",
            "",
        )
        if rc != 0:
            detail = err.strip()
            raise RuntimeError(
                f"ssh-keygen failed for {key}" + (f": {detail}" if detail else ".")
            )
    if not key.is_file() or not pub.is_file():
        raise RuntimeError(f"Repository SSH keypair was not created: {key}")
    public_key = pub.read_text(encoding="utf-8").strip()
    if not public_key or "\n" in public_key or "\r" in public_key:
        raise RuntimeError(f"Repository SSH public key must contain one non-empty line: {pub}")
    return key, pub, public_key


def verify_git_remote_access(repo_url: str, env: dict[str, str]) -> None:
    rc, out, err = run("git", "ls-remote", repo_url, "HEAD", env=env)
    if rc != 0:
        detail = (err or out).strip()
        raise RuntimeError(
            "Git remote access verification failed" + (f": {detail}" if detail else ".")
        )


def verify_git_push_access(
    repo_dir: str | pathlib.Path,
    branch: str,
    env: dict[str, str],
) -> None:
    rc, out, err = run(
        "git",
        "push",
        "--dry-run",
        "--follow-tags",
        "origin",
        f"HEAD:refs/heads/{branch}",
        env=env,
        cwd=str(repo_dir),
    )
    if rc != 0:
        detail = (err or out).strip()
        raise RuntimeError(
            "Git push access verification failed" + (f": {detail}" if detail else ".")
        )


def verify_git_tag_absent(repo_dir: str | pathlib.Path, tag_name: str) -> None:
    """Reject an invalid tag or one that already exists in the local repository."""
    tag = str(tag_name or "").strip()
    if not tag or "\n" in tag or "\r" in tag:
        raise RuntimeError("Git tag must contain one non-empty line.")
    ref = f"refs/tags/{tag}"
    rc, out, err = run("git", "check-ref-format", ref, cwd=str(repo_dir))
    if rc != 0:
        detail = (err or out).strip()
        raise RuntimeError(
            f"Invalid Git tag: {tag}" + (f": {detail}" if detail else ".")
        )
    rc, out, err = run("git", "show-ref", "--verify", "--quiet", ref, cwd=str(repo_dir))
    if rc == 0:
        raise RuntimeError(f"Git tag already exists locally: {tag}")
    if rc != 1:
        detail = (err or out).strip()
        raise RuntimeError(
            f"Could not check whether Git tag already exists: {tag}"
            + (f": {detail}" if detail else ".")
        )


def verify_git_remote_tag_absent(
    repo_dir: str | pathlib.Path,
    tag_name: str,
    env: dict[str, str],
) -> None:
    """Reject an exact tag that already exists on origin using the forced identity."""
    tag = str(tag_name or "").strip()
    if not tag or "\n" in tag or "\r" in tag:
        raise RuntimeError("Git tag must contain one non-empty line.")
    ref = f"refs/tags/{tag}"
    rc, out, err = run(
        "git",
        "ls-remote",
        "--exit-code",
        "--refs",
        "--tags",
        "origin",
        ref,
        env=env,
        cwd=str(repo_dir),
    )
    if rc == 0:
        raise RuntimeError(f"Git tag already exists remotely: {tag}")
    if rc != 2:
        detail = (err or out).strip()
        raise RuntimeError(
            f"Could not check whether Git tag exists remotely: {tag}"
            + (f": {detail}" if detail else ".")
        )


def start_agent_and_add_key(key_path: pathlib.Path) -> dict:
    env = os.environ.copy()
    # try existing agent
    rc, _, _ = run("ssh-add", "-l")
    if rc != 0:
        # start agent
        rc, out, _ = run("ssh-agent", "-s")
        if rc == 0:
            m1 = re.search(r"SSH_AUTH_SOCK=([^;]+)", out)
            m2 = re.search(r"SSH_AGENT_PID=([^;]+)", out)
            if m1:
                env["SSH_AUTH_SOCK"] = m1.group(1)
            if m2:
                env["SSH_AGENT_PID"] = m2.group(1)
    # add key with updated env
    run("ssh-add", str(key_path), env=env)
    return env


def open_folder(path: str) -> None:
    if sys.platform == "win32":
        os.startfile(path)  # type: ignore[attr-defined]
    elif sys.platform == "darwin":
        subprocess.Popen(["open", path])
    else:
        if which("xdg-open"):
            subprocess.Popen(["xdg-open", path])
        else:
            # best effort
            subprocess.Popen(["sh", "-c", f'echo "{path}"'])


def pick_linux_terminal() -> tuple[str, list[str]] | None:
    candidates = [
        ("x-terminal-emulator", ["-e", "bash", "-lc"]),
        ("gnome-terminal", ["--", "bash", "-lc"]),
        ("konsole", ["-e", "bash", "-lc"]),
        ("xfce4-terminal", ["-e", "bash", "-lc"]),
        ("tilix", ["-e", "bash", "-lc"]),
        ("mate-terminal", ["-e", "bash", "-lc"]),
        ("alacritty", ["-e", "bash", "-lc"]),
        ("kitty", ["-e", "bash", "-lc"]),
        ("xterm", ["-e", "bash", "-lc"]),
    ]
    for cmd, args in candidates:
        p = which(cmd)
        if p:
            return p, args
    return None


def quote_bash(s: str) -> str:
    return (
        '"'
        + s.replace("\\", "\\\\").replace('"', '\\"').replace("$", "\\$").replace("`", "\\`")
        + '"'
    )


def quote_pwsh(s: str) -> str:
    return '"' + s.replace('"', '``"') + '"'


def open_signed_terminal(repo_dir: str, key_path: pathlib.Path, repo_name: str) -> None:
    # Windows
    if sys.platform == "win32":
        ps = "; ".join(
            [
                "$ErrorActionPreference='Stop'",
                # Check if ssh-agent service is running and start with admin privileges if not
                "$svc = Get-Service ssh-agent",
                "if ($svc.Status -ne 'Running') {",
                "  Write-Host 'SSH agent service is not running. Starting admin PowerShell to configure it...' -ForegroundColor Yellow",
                "  $adminScript = 'Set-Service ssh-agent -StartupType Automatic; Start-Service ssh-agent; Write-Host \"SSH agent configured successfully!\" -ForegroundColor Green; Start-Sleep -Seconds 2'",
                "  Start-Process powershell -ArgumentList '-NoProfile','-Command',$adminScript -Verb RunAs -Wait",
                "  Write-Host 'Service configured. Continuing...' -ForegroundColor Green",
                "}",
                # ensure key exists and add to agent
                f"if (!(Test-Path -Path {quote_pwsh(str(key_path))})) {{ ssh-keygen -t ed25519 -C 'mainsequence@main-sequence.io' -f {quote_pwsh(str(key_path))} -N '' }}",
                f"ssh-add {quote_pwsh(str(key_path))}",
                "ssh-add -l",
                # Set GIT_SSH_COMMAND to use the specific key (in set-up-locally we also add key to ssh-agent but use this environment variable as well to be sure)
                f"$env:GIT_SSH_COMMAND = 'ssh -i {quote_pwsh(str(key_path))} -o IdentitiesOnly=yes'",
                f"Set-Location {quote_pwsh(repo_dir)}",
                f"Write-Host 'SSH agent ready for {repo_name}. You can now run git.' -ForegroundColor Green",
            ]
        )
        subprocess.Popen(
            ["powershell.exe", "-NoExit", "-Command", ps],
            creationflags=subprocess.CREATE_NEW_CONSOLE,
        )
        return
    # macOS
    if sys.platform == "darwin":
        bash = " && ".join(
            [
                f"cd {quote_bash(repo_dir)}",
                f"[ -f {quote_bash(str(key_path))} ] || ssh-keygen -t ed25519 -C \"mainsequence@main-sequence.io\" -f {quote_bash(str(key_path))} -N ''",
                'eval "$(ssh-agent -s)"',
                f"ssh-add {quote_bash(str(key_path))}",
                "ssh-add -l",
                f"echo 'SSH agent ready for {repo_name}. You can now run git.'",
                'exec "$SHELL" -l',
            ]
        )

        # Let json.dumps handle the quoting for AppleScript string literal
        osa = [
            "osascript",
            "-e",
            'tell application "Terminal" to activate',
            "-e",
            f'tell application "Terminal" to do script {json.dumps(bash)}',
        ]
        subprocess.Popen(osa)
        return
    # Linux
    term = pick_linux_terminal()
    if not term:
        raise RuntimeError("No terminal emulator found (x-terminal-emulator, gnome-terminal, …)")
    cmd, args = term
    bash = " && ".join(
        [
            f"cd {quote_bash(repo_dir)}",
            f"[ -f {quote_bash(str(key_path))} ] || ssh-keygen -t ed25519 -C \"mainsequence@main-sequence.io\" -f {quote_bash(str(key_path))} -N ''",
            'eval "$(ssh-agent -s)"',
            f"ssh-add {quote_bash(str(key_path))}",
            "ssh-add -l",
            f"echo 'SSH agent ready for {repo_name}. You can now run git.'",
            'exec "$SHELL" -l',
        ]
    )
    subprocess.Popen([cmd, *args, bash])
