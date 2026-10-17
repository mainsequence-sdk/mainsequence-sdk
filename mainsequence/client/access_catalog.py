"""Read the platform's access catalog: resource types, their actions and their policies.

Every signed-in user can read the catalog. It describes what each policy allows;
it does not say who holds a policy on which object, and reading it changes
nothing.
"""

from __future__ import annotations

from typing import Any, Literal
from urllib.parse import quote
from uuid import UUID

from pydantic import Field

from . import utils as client_utils
from .base import BasePydanticModel
from .exceptions import raise_for_response
from .utils import make_request
from .value_sets import OpenValueSet


def _api_url(path: str) -> str:
    return f"{client_utils.API_ENDPOINT.rstrip('/')}/{path.lstrip('/')}"


def _request_json(
    *,
    method: str,
    url: str,
    params: dict[str, Any] | None,
    timeout: int | float | tuple[float, float] | None,
) -> Any:
    payload = {"params": params} if params else {}
    response = make_request(
        s=client_utils.session,
        loaders=client_utils.loaders,
        r_type=method,
        url=url,
        payload=payload,
        time_out=timeout,
    )
    if response.status_code != 200:
        raise_for_response(response, payload=payload or None)
    return response.json()


def _json_object(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise TypeError("Access catalog detail endpoints must return a JSON object.")
    return data


def _json_array(data: Any) -> list[Any]:
    if not isinstance(data, list):
        raise TypeError("Access catalog list endpoints must return a JSON array.")
    return data


def _resource_type_key(value: str, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty resource type key, such as 'secrets'.")
    return value.strip()


def _resource_type_filter(resource_type: str | None) -> dict[str, str] | None:
    if resource_type is None:
        return None
    return {"resource_type": _resource_type_key(resource_type, field_name="resource_type")}


def _policy_uid(value: str | UUID) -> str:
    try:
        return str(UUID(str(value)))
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError("uid must be the policy's public UUID.") from exc


class AccessAction(BasePydanticModel):
    """One thing a principal can do on a resource type, such as `secrets.read`."""

    id: str = Field(
        ...,
        description="Action id, `<resource type>.<action>`, for example `secrets.read`.",
    )
    resource_type: str = Field(..., description="Key of the resource type the action belongs to.")
    key: str = Field(
        ...,
        description="The action's key within its resource type, for example `read`.",
    )
    label: str = Field(..., description="Short name of the action.")
    description: str = Field(..., description="What the action lets a principal do.")
    access_level: OpenValueSet[Literal["read", "use", "write", "permissions"]] = Field(
        ...,
        description=(
            "How far the action reaches: `read` sees configuration, `use` touches data or "
            "runs something, `write` changes, operates or deletes, and `permissions` sees or "
            "decides who has access."
        ),
    )
    is_deprecated: bool = Field(..., description="Whether the action is deprecated.")


class AccessPolicy(BasePydanticModel):
    """A named bundle of one resource type's actions.

    Its three texts are written for the people who grant it: what the holder can
    do (`summary`), what can happen because of it (`impact`) and what it does not
    allow (`not_included`).
    """

    uid: str = Field(
        ...,
        description="Public UID of the policy, the key `AccessCatalog.get_policy()` takes.",
    )
    id: str = Field(
        ...,
        description="Policy id, `<resource type>.<policy>`, for example `secrets.reader`.",
    )
    resource_type: str = Field(..., description="Key of the resource type the policy applies to.")
    name: str = Field(
        ...,
        description="The policy's name within its resource type, for example `reader`.",
    )
    label: str = Field(..., description="Short name of the policy.")
    summary: str = Field(..., description="What a holder of the policy can do.")
    impact: str = Field(..., description="What can happen because someone holds the policy.")
    not_included: str = Field(..., description="What the policy does not allow.")
    actions: list[str] = Field(..., description="Ids of the actions the policy contains.")
    attachable: bool = Field(
        ...,
        description=(
            "Whether the policy can be given to someone. A policy that cannot is computed "
            "by the platform, for example from Team membership."
        ),
    )
    principal_kinds: list[OpenValueSet[Literal["people", "workloads", "teams"]]] = Field(
        ...,
        description=(
            "Who the policy can be given to: `people`, `workloads` and `teams`. "
            "Empty for a computed policy."
        ),
    )
    stage: OpenValueSet[Literal["active", "deprecated"]] = Field(
        ...,
        description="`active`, or `deprecated` for a policy that is being retired.",
    )


class AccessInheritanceRule(BasePydanticModel):
    """How holding a policy on a parent object gives a policy on this resource type."""

    relation: str = Field(
        ...,
        description="Name of the relation between the parent object and this one.",
    )
    parent_type: str = Field(..., description="Key of the parent object's resource type.")
    parent_policy: str = Field(..., description="Id of the policy held on the parent object.")
    child_policy: str = Field(..., description="Id of the policy it gives on this resource type.")


class AccessGuard(BasePydanticModel):
    """A platform rule that blocks some actions on some objects for everyone, admins included."""

    name: str = Field(..., description="Name of the rule.")
    description: str = Field(..., description="What the rule blocks, and when.")
    actions: list[str] = Field(..., description="Ids of the actions the rule applies to.")


class AccessResourceType(BasePydanticModel):
    """A kind of shareable object, with its actions, policies, inheritance rules and guards."""

    key: str = Field(..., description="Resource type key, for example `secrets`.")
    label: str = Field(..., description="Short name of the resource type.")
    description: str = Field(..., description="What objects of this type are.")
    environment_scoped: bool = Field(
        ...,
        description=(
            "Whether objects of this type live in an Organization Environment, so that "
            "access to them also requires being admitted to that Environment."
        ),
    )
    actions: list[AccessAction] = Field(
        ...,
        description="The type's actions; deprecated actions are left out.",
    )
    policies: list[AccessPolicy] = Field(
        ...,
        description="The platform's active policies for the type.",
    )
    inheritance: list[AccessInheritanceRule] = Field(
        ...,
        description="Rules by which a policy held on a parent object gives a policy on this type.",
    )
    guards: list[AccessGuard] = Field(
        ...,
        description="Platform rules that block actions on objects of this type.",
    )


class AccessResourceTypeListing(BasePydanticModel):
    """Every resource type in the catalog, with the hash of the catalog's content."""

    catalog_hash: str = Field(
        ...,
        description=(
            "Identifies the catalog's content and changes whenever the catalog changes, so a "
            "kept copy can be compared with it."
        ),
    )
    resource_types: list[AccessResourceType] = Field(
        ...,
        description="Every resource type, with its actions, policies, inheritance rules and guards.",
    )


class AccessCatalog:
    """Read-only client for the platform's access catalog.

    Every signed-in user can read the catalog: the resource types, the actions
    each type offers, and the policies that bundle those actions. The catalog
    describes what a policy allows; it does not say who holds one.

    Each method sends one authenticated request. A failed request raises the
    SDK's API errors, for example `NotFoundError` for an unknown resource type
    key or policy UID; a malformed argument raises `ValueError` before any
    request is sent.

    Example:

        from mainsequence.client import AccessCatalog

        catalog = AccessCatalog()
        for policy in catalog.list_policies(resource_type="secrets"):
            print(policy.id, policy.summary)
    """

    def list_resource_types(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> AccessResourceTypeListing:
        """Return every resource type with its actions, policies, inheritance and guards."""

        data = _request_json(
            method="GET",
            url=_api_url("access/resource-types/"),
            params=None,
            timeout=timeout,
        )
        return AccessResourceTypeListing.model_validate(_json_object(data))

    def get_resource_type(
        self,
        key: str,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> AccessResourceType:
        """Return one resource type, addressed by its key, for example `secrets`."""

        resource_type = _resource_type_key(key, field_name="key")
        data = _request_json(
            method="GET",
            url=_api_url(f"access/resource-types/{quote(resource_type, safe='')}/"),
            params=None,
            timeout=timeout,
        )
        return AccessResourceType.model_validate(_json_object(data))

    def list_policies(
        self,
        *,
        resource_type: str | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> list[AccessPolicy]:
        """Return the platform's policies, of one resource type when `resource_type` is given.

        A key the catalog does not have returns an empty list.
        """

        data = _request_json(
            method="GET",
            url=_api_url("access/policies/"),
            params=_resource_type_filter(resource_type),
            timeout=timeout,
        )
        return [AccessPolicy.model_validate(item) for item in _json_array(data)]

    def get_policy(
        self,
        uid: str | UUID,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> AccessPolicy:
        """Return one policy, addressed by its public UID."""

        policy_uid = _policy_uid(uid)
        data = _request_json(
            method="GET",
            url=_api_url(f"access/policies/{policy_uid}/"),
            params=None,
            timeout=timeout,
        )
        return AccessPolicy.model_validate(_json_object(data))

    def list_actions(
        self,
        *,
        resource_type: str | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> list[AccessAction]:
        """Return the actions policies can contain, of one resource type when given.

        Deprecated actions are left out. A key the catalog does not have returns
        an empty list.
        """

        data = _request_json(
            method="GET",
            url=_api_url("access/actions/"),
            params=_resource_type_filter(resource_type),
            timeout=timeout,
        )
        return [AccessAction.model_validate(item) for item in _json_array(data)]


__all__ = [
    "AccessAction",
    "AccessCatalog",
    "AccessGuard",
    "AccessInheritanceRule",
    "AccessPolicy",
    "AccessResourceType",
    "AccessResourceTypeListing",
]
