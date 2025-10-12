# jupyter_server_config.py
from pathlib import Path

c = get_config()

def _cmd_factory(app_path: Path, key: str):
    """Return a callable (port, base_url) -> argv list for jupyter-server-proxy."""
    def _cmd(port, base_url):
        return [
            "streamlit", "run", str(app_path),
            "--server.headless=true",
            "--server.address=127.0.0.1",
            f"--server.port={port}",
            f"--server.baseUrlPath={base_url.rstrip('/')}/{key}/",
        ]
    return _cmd

servers = {}
home = Path.home()

# Discover: ~/<project>/dashboards/<app>/app.py   (project folder name is dynamic)
for project_dir in home.iterdir():
    dashboards_dir = project_dir / "dashboards"
    if not dashboards_dir.is_dir():
        continue
    for app_py in dashboards_dir.glob("*/app.py"):
        app_slug = app_py.parent.name.replace("_", "-")   # e.g. companies_overview -> companies-overview
        key = f"streamlit-{app_slug}"                     # URL segment you wanted
        servers[key] = {"command": _cmd_factory(app_py, key),
                        "absolute_url": True

                        }

c.ServerProxy.servers = servers
