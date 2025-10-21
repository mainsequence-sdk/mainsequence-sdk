# jupyter_server_config.py
from pathlib import Path

c = get_config()

def _cmd_factory(app_path: Path, key: str):
    def _cmd(port, base_url):
        base = base_url.rstrip("/")
        return [
            "streamlit", "run", str(app_path),
            "--server.headless=true",
            "--server.address=127.0.0.1",
            f"--server.port={port}",
            f"--server.baseUrlPath={base}/{key}/",  # trailing slash matters
        ]
    return _cmd

servers = {}
home = Path.home()

EXCLUDE_DIRS = {"__pycache__", ".git", "node_modules", ".venv", "venv"}

for project_dir in home.iterdir():
    dashboards_dir = project_dir / "dashboards"
    if not dashboards_dir.is_dir():
        continue

    # discover apps at ANY depth
    for app_py in dashboards_dir.rglob("app.py"):
        # skip unwanted directories
        if any(part in EXCLUDE_DIRS for part in app_py.parts):
            continue

        app_slug = app_py.parent.name.replace("_", "-")
        key = f"streamlit-{app_slug}"

        # Optional: detect duplicates (same app name in multiple places)
        if key in servers:
            print(f"[jupyter-server-config] WARNING: duplicate key '{key}' "
                  f"for {app_py}. Keeping the first one and skipping this.")
            continue

        servers[key] = {
            "command": _cmd_factory(app_py, key),
            "absolute_url": True,
            "timeout": 60,  # gives Streamlit time to boot on cold start
            "launcher_entry": {
                "title": f"Streamlit — {app_slug.replace('-', ' ').title()}",
            },
        }

c.ServerProxy.servers = servers
