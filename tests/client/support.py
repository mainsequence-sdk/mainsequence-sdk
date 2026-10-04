import ast
import pathlib

from pydantic import ConfigDict, Field

from mainsequence.client.base import BaseObjectOrm, BasePydanticModel, ShareableObjectMixin

CODE_REPOSITORY_UID = "1d0530c0-65d1-4db0-856b-dc29d8260a09"


CODE_REPOSITORY_BRANCH_UID = "5a28020a-0f1b-47ee-aab8-334286234bea"


ENVIRONMENT_UID = "58218213-5e4e-43de-a5bd-6757f4e1c8f6"


def _agent_runtime_update_contract(state: str = "current") -> dict:
    values = {
        "current": (False, None),
        "update_required": (True, {"tool": "agent.update_runtime"}),
        "updating": (True, None),
        "update_failed": (True, {"tool": "agent.update_runtime"}),
        "not_deployed": (False, None),
        "unknown": (None, None),
    }
    needs_redeploy, remediation = values[state]
    return {
        "state": state,
        "needs_redeploy": needs_redeploy,
        "remediation": remediation,
    }


class _IdRef:
    def __init__(self, value: int):
        self.id = value


class _UidRef:
    def __init__(self, value: str):
        self.uid = value


class DemoFilterModel(BaseObjectOrm):
    FILTERSET_FIELDS = {
        "name": ["exact", "contains", "in"],
        "parent__id": ["exact", "in"],
        "active": ["isnull"],
    }
    FILTER_VALUE_NORMALIZERS = {
        "parent__id": "id",
    }


class DemoDestroyModel(BaseObjectOrm):
    DESTROY_QUERY_PARAMS = {
        "full_delete_selected": "bool",
        "override_protection": "bool",
    }

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        return "https://backend.test/demo"

    @classmethod
    def build_session(cls):
        return object()


class DemoReadModel(BaseObjectOrm):
    FILTERSET_FIELDS = {
        "id": ["exact"],
    }
    FILTER_VALUE_NORMALIZERS = {
        "id": "id",
    }
    READ_QUERY_PARAMS = {
        "include_relations_detail": "bool",
    }

    def __init__(self, **kwargs):
        self.id = kwargs.get("id")
        self.payload = kwargs

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        return "https://backend.test/demo-read"

    @classmethod
    def build_session(cls):
        return object()


class DemoShareableModel(ShareableObjectMixin, BaseObjectOrm):
    def __init__(self, uid: str, internal_id: int | None = None):
        self.uid = uid
        self.id = internal_id

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        return "https://backend.test/demo-shareable"

    @classmethod
    def build_session(cls):
        return object()


class DemoIdOnlyResource(ShareableObjectMixin, BaseObjectOrm):
    def __init__(self, internal_id: int):
        self.id = internal_id

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        return "https://backend.test/demo-shareable"

    @classmethod
    def build_session(cls):
        return object()


class DemoPatchModel(BasePydanticModel, BaseObjectOrm):
    id: int
    label: str | None = None

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        return "https://backend.test/demo-patch"

    @classmethod
    def build_session(cls):
        return object()


class DemoAliasedPatchModel(BasePydanticModel, BaseObjectOrm):
    model_config = ConfigDict(populate_by_name=True)

    id: int
    schema_payload: dict | None = Field(default=None, alias="schema")

    @classmethod
    def get_object_url(cls, custom_endpoint_name=None):
        return "https://backend.test/demo-patch"

    @classmethod
    def build_session(cls):
        return object()


def _code_repository_image_response(*, uid: str, build_error: bool, is_ready: bool = False):
    return {
        "uid": uid,
        "code_repository_commit_hash": "abc123abc123abc123abc123abc123abc123abcd",
        "related_code_repository_branch_uid": "5a28020a-0f1b-47ee-aab8-334286234bea",
        "base_image": None,
        "tags": [],
        "build_error": build_error,
        "is_ready": is_ready,
        "source_provenance": {
            "verification_state": "verified",
            "code_repository_uid": "1d0530c0-65d1-4db0-856b-dc29d8260a09",
            "code_repository_branch_uid": "5a28020a-0f1b-47ee-aab8-334286234bea",
            "github_repository_binding_uid": "3c2113e7-40ba-4d8c-ad65-51ca236c3b0c",
            "repository_branch": "main",
            "repository_ref": "refs/heads/main",
            "commit_sha": "abc123abc123abc123abc123abc123abc123abcd",
            "source_archive_sha256": "a" * 64,
            "build_context_checksum": "b" * 64,
            "base_image_uid": "c3ddb792-a3c0-428c-b5cf-31ed99dad10f",
            "output_image_digest": "sha256:" + "c" * 64,
        },
        "creation_date": "2026-04-07T09:00:00Z",
    }


def _runtime_access_payload(*, state: str, can_request: bool, retry_after_ms=None):
    release_uid = "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed"
    waking = state == "waking"
    return {
        "resource_release_uid": release_uid,
        "release_kind": "fastapi",
        "routing": {
            "state": "routable",
            "active_revision_uid": "19128ab6-d72f-460c-8525-d758fa92676a",
        },
        "runtime_access": {
            "state": state,
            "can_request": can_request,
            "notice": (
                {
                    "code": "runtime_waking",
                    "severity": "info",
                    "title": "Starting deployed runtime",
                    "message": "The deployed runtime is starting from idle.",
                }
                if waking
                else None
            ),
            "operation": (
                {
                    "uid": "00000000-0000-4000-8000-000000000002",
                    "status": "running",
                    "created_at": "2026-09-17T12:00:00Z",
                    "started_at": "2026-09-17T12:00:01Z",
                    "finished_at": None,
                    "support_reference": "00000000-0000-4000-8000-000000000002",
                }
                if waking
                else None
            ),
            "retry_after_ms": retry_after_ms,
        },
        "runtime_presence": {
            "phase": "starting" if waking else "serving",
            "replicas": {"desired": 1, "actual": 1},
            "detail": "Starting." if waking else "Ready.",
            "observed_at": "2026-09-17T12:00:02Z",
            "wake": (
                {
                    "operation_uid": "00000000-0000-4000-8000-000000000002",
                    "state": "in_progress",
                    "requested_at": "2026-09-17T12:00:00Z",
                    "deadline_at": "2026-09-17T12:10:00Z",
                }
                if waking
                else None
            ),
        },
        "access": (
            {
                "release_kind": "fastapi",
                "mode": "token",
                "token": "runtime-token",
                "rpc_url": "https://runtime.example.test/",
                "resource_release_uid": release_uid,
            }
            if can_request
            else None
        ),
    }


def _resource_release_pipeline_payload(*, running_step: str | None = None):
    keys = (
        "resolve_revision",
        "validate_deployment",
        "resolve_resource",
        "build_code_repository_image",
        "deploy_runtime",
        "verify_runtime_readiness",
    )
    current_index = keys.index(running_step) if running_step else None
    steps = []
    for index, key in enumerate(keys, start=1):
        if current_index is None or index - 1 < current_index:
            step_state = "succeeded"
            outcome = "completed"
        elif index - 1 == current_index:
            step_state = "running"
            outcome = ""
        else:
            step_state = "pending"
            outcome = ""
        steps.append(
            {
                "uid": f"00000000-0000-4000-8000-{index:012d}",
                "sequence": index,
                "key": key,
                "name": key.replace("_", " ").title(),
                "kind": "orchestration",
                "required": True,
                "state": step_state,
                "outcome": outcome,
                "artifact_context": {},
                "started_at": None,
                "finished_at": None,
                "error": None,
            }
        )
    return {
        "key": "resource_release.fastapi.build_and_deploy",
        "version": 1,
        "current_step_key": running_step,
        "steps": steps,
    }


def _deployment_run_billing_payload(
    *,
    pricing_state: str = "priced",
    total_cost: str | None = "0.000000",
) -> dict:
    return {
        "scope": "image_lifecycle",
        "total_cost": total_cost,
        "currency": "USD",
        "pricing_state": pricing_state,
        "components": {
            "image_build": total_cost,
            "image_registry_storage": "0.000000" if total_cost is not None else None,
            "image_registry_service": "0.000000" if total_cost is not None else None,
        },
        "priced_rows": 3 if pricing_state == "priced" else 0,
        "unpriced_rows": 0 if pricing_state == "priced" else 3,
        "reused_image_count": 0,
    }


def _deployment_run_runtime_billing_payload(
    *,
    pricing_state: str = "priced",
    total_cost: str | None = "0.000000",
    base_cost: str | None = "0.000000",
    is_complete: bool = True,
) -> dict:
    return {
        "scope": "knative_runtime",
        "total_cost": total_cost,
        "base_cost": base_cost,
        "currency": "USD",
        "pricing_state": pricing_state,
        "priced_rows": 3 if pricing_state == "priced" else 0,
        "unpriced_rows": 0 if pricing_state == "priced" else 3,
        "is_complete": is_complete,
    }


def _deployment_run_cost_summary_payload(
    *,
    total_cost: str | None = "0.000000",
    is_complete: bool = True,
) -> dict:
    return {
        "total_cost": total_cost,
        "currency": "USD",
        "is_complete": is_complete,
    }


def _deployment_run_cost_projections(
    *,
    pricing_state: str = "priced",
    total_cost: str | None = "0.000000",
    is_complete: bool = True,
) -> dict:
    return {
        "billing": _deployment_run_billing_payload(
            pricing_state=pricing_state,
            total_cost=total_cost,
        ),
        "runtime_billing": _deployment_run_runtime_billing_payload(
            pricing_state=pricing_state,
            total_cost=total_cost,
            base_cost=total_cost,
            is_complete=is_complete,
        ),
        "cost_summary": _deployment_run_cost_summary_payload(
            total_cost=total_cost,
            is_complete=is_complete,
        ),
    }


def _ready_runtime_contract():
    return {
        "runtime_interaction": {
            "state": "ready",
            "can_submit": True,
            "notice": None,
            "operation": None,
            "retry_after_ms": None,
        },
        "runtime_presence": {
            "phase": "serving",
            "replicas": {"desired": 1, "actual": 1},
            "detail": "The runtime is serving requests.",
            "observed_at": "2026-09-03T10:00:00Z",
            "wake": None,
        },
    }


def _class_base_names_from_source(path: pathlib.Path) -> dict[str, list[str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    out: dict[str, list[str]] = {}
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            base_names: list[str] = []
            for base in node.bases:
                if isinstance(base, ast.Name):
                    base_names.append(base.id)
                elif isinstance(base, ast.Attribute):
                    base_names.append(base.attr)
            out[node.name] = base_names
    return out
