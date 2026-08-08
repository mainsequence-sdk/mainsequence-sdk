import ast
import pathlib

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
ROUTE_SOURCE_FILES = [
    *sorted((REPO_ROOT / "mainsequence" / "client").rglob("*.py")),
    REPO_ROOT / "mainsequence" / "cli" / "api.py",
    REPO_ROOT / "mainsequence" / "cli" / "migrations.py",
    REPO_ROOT / "mainsequence" / "logconf.py",
]
REMOVED_API_ROOTS = (
    "/orm/api/",
    "/user/api/",
    "/customer-success/api/",
    "/api/v1/command_center/",
)


def test_maintained_clients_do_not_reference_removed_api_roots():
    violations = []
    for source_file in ROUTE_SOURCE_FILES:
        source = source_file.read_text(encoding="utf-8")
        for removed_root in REMOVED_API_ROOTS:
            if removed_root in source:
                violations.append(f"{source_file.relative_to(REPO_ROOT)}: {removed_root}")

    assert violations == []


def test_literal_action_path_segments_are_kebab_case():
    violations = []
    for source_file in ROUTE_SOURCE_FILES:
        tree = ast.parse(source_file.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            value = node.value.strip()
            if not value.startswith("/"):
                continue
            path = value.split("?", 1)[0]
            if not (path.endswith("/") or "?" in value):
                continue
            invalid_segments = [segment for segment in path.split("/") if "_" in segment]
            if invalid_segments:
                violations.append(
                    f"{source_file.relative_to(REPO_ROOT)}:{node.lineno}: {value}"
                )

    assert violations == []


def test_cli_auth_and_resource_roots_match_backend_contract():
    from mainsequence.cli.api import AUTH_PATHS
    from mainsequence.client.agent_runtime_models import Agent, AgentSession, CodingAgentService
    from mainsequence.client.base import BaseObjectOrm
    from mainsequence.client.metatables import MetaTable, TimeIndexMetaTable
    from mainsequence.client.models_user import Notification, Organization, Team

    assert AUTH_PATHS == {
        "authorize": "/auth/cli/authorize/",
        "cli_token": "/auth/cli/token/",
        "cli_revoke": "/auth/cli/revoke/",
        "refresh": "/auth/jwt-token/token/refresh/",
        "logout": "/auth/jwt-token/logout/",
        "ping": "/auth/rest-auth/user/",
        "mcp_cli_handoff_start": "/auth/mcp/cli-handoff/start/",
    }
    resource_endpoints = set(BaseObjectOrm.END_POINTS.values()) | {
        Agent.ENDPOINT,
        AgentSession.ENDPOINT,
        CodingAgentService.ENDPOINT,
        MetaTable.ENDPOINT,
        TimeIndexMetaTable.ENDPOINT,
        Notification.ENDPOINT,
        Organization.ENDPOINT,
        Team.ENDPOINT,
    }
    assert resource_endpoints == {
        "agent-sessions",
        "agents",
        "artifacts",
        "buckets",
        "coding-agent-services",
        "constants",
        "data-sources",
        "deployment-runs",
        "github-organizations",
        "job-runs",
        "jobs",
        "local-time-series",
        "local-time-series-historical-updates",
        "local-time-series-update-details",
        "meta-tables",
        "notifications",
        "organizations",
        "project-base-images",
        "project-branches",
        "project-images",
        "project-repositories",
        "project-resources",
        "projects",
        "resource-releases",
        "schedulers",
        "secrets",
        "time-index-meta-tables",
        "teams",
        "users",
    }


def test_client_auth_provider_routes_match_backend_contract(monkeypatch):
    from mainsequence.client import utils

    monkeypatch.setattr(utils, "API_ENDPOINT", "https://backend.example/api/v1")
    monkeypatch.setattr(utils, "AUTH_ENDPOINT", "https://backend.example")

    jwt_provider = utils.JWTAuthProvider(
        access_token="access",
        refresh_token="refresh",
    )
    runtime_provider = utils.RuntimeCredentialAuthProvider(
        credential_id="credential-id",
        credential_secret="credential-secret",
    )

    assert jwt_provider.obtain_url == "https://backend.example/auth/jwt-token/token/"
    assert (
        jwt_provider.refresh_url
        == "https://backend.example/auth/jwt-token/token/refresh/"
    )
    assert (
        runtime_provider.token_url
        == "https://backend.example/api/v1/runtime-credentials/token/"
    )


def test_removed_backend_actions_are_not_exposed_by_client_models():
    from mainsequence.client.agent_runtime_models import CodingAgentService
    from mainsequence.client.metatables import DataNodeUpdate, DataSource
    from mainsequence.client.models_helpers import Job

    removed_actions = {
        DataSource: ("get_or_create_sqlite", "create_sqlite"),
        DataNodeUpdate: ("add_tags", "filter_by_hash_id", "get_upstream_nodes"),
        Job: ("bulk_get_or_create", "create_from_configuration", "sync_jobs"),
        CodingAgentService: ("reconcile_runtime",),
    }
    violations = [
        f"{model.__name__}.{method_name}"
        for model, method_names in removed_actions.items()
        for method_name in method_names
        if hasattr(model, method_name)
    ]

    assert violations == []


def test_cli_does_not_reintroduce_legacy_numeric_detail_routes_or_agent_runs():
    from mainsequence.cli import api
    from mainsequence.client import agent_runtime_models

    cli_sources = "\n".join(
        (REPO_ROOT / "mainsequence" / "cli" / name).read_text(encoding="utf-8")
        for name in ("api.py", "cli.py")
    )
    assert "pk=int(" not in cli_sources
    assert '"job__id"' not in cli_sources
    assert '"data_source__id"' not in cli_sources
    assert "--data-source-id" not in cli_sources
    assert "AgentRun" not in cli_sources
    assert not hasattr(api, "list_agent_runs")
    assert not hasattr(api, "get_agent_run")
    assert not hasattr(agent_runtime_models, "AgentRun")
