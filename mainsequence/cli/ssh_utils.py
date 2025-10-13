from __future__ import annotations

import json
import os
import pathlib
import re
import shutil
import subprocess
import sys


def which(cmd: str) -> str | None:
    p = shutil.which(cmd)
    return p


def run(cmd, *args, env=None, cwd=None) -> tuple[int, str, str]:
    proc = subprocess.Popen(
        [cmd, *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env, cwd=cwd
    )
    out, err = proc.communicate()
    return proc.returncode, out, err


def ensure_key_for_repo(repo_url: str) -> tuple[pathlib.Path, pathlib.Path, str]:
    home = pathlib.Path.home()
    key_dir = home / ".ssh"
    key_dir.mkdir(parents=True, exist_ok=True)
    # derive safe name
    last = re.sub(r"[?#].*$", "", repo_url).split("/")[-1]
    if last.lower().endswith(".git"):
        last = last[:-4]
    safe = re.sub(r"[^A-Za-z0-9._-]+", "-", last)
    key = key_dir / safe
    pub = key.with_suffix(key.suffix + ".pub")
    if not key.exists():
        run(
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
    public_key = pub.read_text(encoding="utf-8")
    return key, pub, public_key


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
