# scripts/jupyter_server_config.py
from pathlib import Path

c = get_config()

def _cmd_factory(app_path: Path, mount: str):
    """Return a callable (port, base_url) -> argv list for jupyter-server-proxy."""
    def _cmd(port, base_url):
        return [
            "streamlit", "run", str(app_path),
            "--server.headless=true",
            "--server.address=127.0.0.1",
            f"--server.port={port}",
            f"--server.baseUrlPath={base_url.rstrip('/')}/{mount.strip('/')}/",
        ]
    return _cmd

servers = {}
home = Path.home()

# Discover: ~/<project>/dashboards/<app>/app.py   (project is dynamic)
for project_dir in home.iterdir():
    dashboards_dir = project_dir / "dashboards"
    if not dashboards_dir.is_dir():
        continue
    for app_py in dashboards_dir.glob("*/app.py"):
        project_slug = project_dir.name.replace("_", "-")
        app_slug = app_py.parent.name.replace("_", "-")
        mount = f"apps/{project_slug}/{app_slug}"  # URL path (after the user prefix)

        servers[f"streamlit-{project_slug}-{app_slug}"] = {
            "command": _cmd_factory(app_py, mount)
        }

c.ServerProxy.servers = servers
