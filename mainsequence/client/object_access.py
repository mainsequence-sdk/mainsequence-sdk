"""See and change who has access to one object, through the object's access routes.

An object whose access the platform decides with policies serves these routes
under its detail URL, for example `/api/v1/teams/{uid}/access/`:

- `access/`: the policies attached to the object, and the access the platform
  computes, such as an Organization admin's;
- `access/me/`: what the caller may do on the object;
- `access/candidates/`: who the caller may give a policy to;
- `access/bindings/`: attach a policy to a person, a workload identity or a
  Team; `access/bindings/{uid}/` detaches it;
- `access/changes/`: several attachments and detachments at once;
- `access/explain/`: why someone may or may not perform an action.

`AccessManagedObjectMixin` gives a model one method per route. `Team` uses it.
What each policy allows is in the access catalog, which `AccessCatalog` reads.
"""

from __future__ import annotations

import datetime
from collections.abc import Iterable, Mapping
from typing import Any, Literal
from uuid import UUID

from pydantic import Field

from .base import BasePydanticModel, DetailActionObjectMixin
from .exceptions import ApiError
from .value_sets import OpenValueSet

_PRINCIPAL_TYPES = ("user", "team")
_IDENTITY_TYPES = ("human", "workload")


def _canonical_uid(value: Any, *, field_name: str) -> str:
    """Return a public UID as a canonical UUID string, or raise before any request."""

    if isinstance(value, UUID):
        return str(value)
    candidate = value
    if not isinstance(candidate, str):
        if isinstance(candidate, Mapping):
            candidate = candidate.get("uid")
        else:
            candidate = getattr(candidate, "uid", None)
    try:
        return str(UUID(str(candidate).strip()))
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError(
            f"{field_name} must be a UUID, an object with a UUID .uid, or a dict with a UUID 'uid'."
        ) from exc


def _non_empty_text(value: Any, *, field_name: str, example: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string, such as {example!r}.")
    return value.strip()


def _closed_choice(value: Any, *, field_name: str, choices: tuple[str, ...]) -> str:
    if value not in choices:
        allowed = " or ".join(repr(choice) for choice in choices)
        raise ValueError(f"{field_name} must be {allowed}. Got: {value!r}")
    return value


def _attach_body(policy: Any, principal_type: Any, principal_uid: Any) -> dict[str, str]:
    return {
        "policy": _non_empty_text(policy, field_name="policy", example="viewer"),
        "principal_type": _closed_choice(
            principal_type, field_name="principal_type", choices=_PRINCIPAL_TYPES
        ),
        "principal_uid": _canonical_uid(principal_uid, field_name="principal_uid"),
    }


class AccessPrincipal(BasePydanticModel):
    """A person, a workload identity or a Team that holds a policy, or can be given one."""

    type: OpenValueSet[Literal["user", "team"]] = Field(
        ...,
        description="`user` for a person or a workload identity, `team` for a Team.",
    )
    uid: str = Field(..., description="Public UID of the User or of the Team.")
    name: str = Field(
        "",
        description=(
            "The person's name, or their email when they have none; the Team's name; "
            "empty for a workload identity."
        ),
    )
    email: str | None = Field(
        None,
        description="The person's email; None for a workload identity and for a Team.",
    )
    identity_type: (
        OpenValueSet[Literal["human", "service_account", "deleted_user", "workload"]] | None
    ) = Field(
        None,
        description="The User's identity type, for example `human` or `workload`; None for a Team.",
    )


class AccessPolicyReference(BasePydanticModel):
    """The policy a binding attaches."""

    id: str = Field(
        ...,
        description="Policy id, `<resource type>.<policy>`, for example `organization-teams.viewer`.",
    )
    label: str = Field("", description="Short name of the policy.")


class AccessGrantor(BasePydanticModel):
    """The User who attached a binding."""

    uid: str = Field(..., description="Public UID of the User.")
    email: str | None = Field(None, description="The User's email, when it has one.")


class AccessWorkflowDeclaration(BasePydanticModel):
    """The entry of a repository workflow file that declares a binding."""

    path: str = Field(..., description="Path of the workflow file in its repository.")
    key: str | None = Field(None, description="Key of the declaration inside the file.")


class AccessBinding(BasePydanticModel):
    """One policy attached to one principal on one object."""

    uid: str = Field(
        ...,
        description="Public UID of the binding, the key `detach_policy()` takes.",
    )
    policy: AccessPolicyReference = Field(..., description="The attached policy.")
    principal: AccessPrincipal = Field(..., description="Who holds the policy.")
    source: OpenValueSet[Literal["explicit", "creator", "workflow", "migrated"]] = Field(
        ...,
        description=(
            "Why the binding exists: `explicit` (someone attached it), `creator` (given to "
            "the object's creator), `workflow` (declared in a repository workflow file) or "
            "`migrated` (converted from an earlier view or edit grant). The source grants "
            "nothing by itself."
        ),
    )
    granted_by: AccessGrantor | None = Field(
        None,
        description="The User who attached the binding, when known.",
    )
    workflow_declaration: AccessWorkflowDeclaration | None = Field(
        None,
        description="The workflow file entry that declares the binding; None for other sources.",
    )
    created_at: datetime.datetime | None = Field(None, description="When the binding was created.")


class AccessComputedRow(BasePydanticModel):
    """Access the platform computes for an object instead of storing a binding."""

    source: OpenValueSet[Literal["role", "membership"]] = Field(
        ...,
        description=(
            "`role` for the Organization admin role, `membership` for the access Team "
            "members hold on their Team."
        ),
    )
    detail: str = Field("", description="Who receives this access, in words.")
    policy: str | None = Field(
        None,
        description="The policy id the computed access amounts to; None when it is every action.",
    )


class AccessObjectReference(BasePydanticModel):
    """The object an access listing describes."""

    resource_type: str = Field(
        ...,
        description="Resource type key of the object, for example `organization-teams`.",
    )
    uid: str = Field(..., description="Public UID of the object.")


class AccessListing(BasePydanticModel):
    """Who has access to one object: its bindings and the access the platform computes."""

    object: AccessObjectReference = Field(..., description="The object described.")
    bindings: list[AccessBinding] = Field(
        default_factory=list,
        description="The policies attached to the object.",
    )
    computed: list[AccessComputedRow] = Field(
        default_factory=list,
        description="Access no binding stores, such as the Organization admin role.",
    )


class CallerAccess(BasePydanticModel):
    """What the caller may do on one object."""

    allowed_actions: list[str] = Field(
        default_factory=list,
        description="Ids of the actions the caller may perform, for example `organization-teams.update`.",
    )
    policies: list[str] = Field(
        default_factory=list,
        description="Ids of the policies the caller holds on the object, from every source.",
    )


class AccessExplanationPath(BasePydanticModel):
    """One way an action is given to the principal an explanation is about."""

    source: OpenValueSet[
        Literal["role", "direct", "team", "membership", "manager", "inherited"]
    ] = Field(
        ...,
        description=(
            "`role` (Organization admin), `direct` (a binding to the principal), `team` "
            "(a binding to a Team the principal belongs to), `membership` (membership of "
            "the Team itself), `manager` (managing the workload that created the object) "
            "or `inherited` (a policy held on a related parent object)."
        ),
    )
    policy: str | None = Field(None, description="The policy id that gives the action.")
    binding_uid: str | None = Field(None, description="The binding that gives it, if any.")
    team_uid: str | None = Field(
        None,
        description="The Team the binding names, for a `team` path.",
    )
    relation: str | None = Field(
        None,
        description="The relation to the parent object, for an `inherited` path.",
    )
    parent_policy: str | None = Field(
        None,
        description="The policy held on the parent object, for an `inherited` path.",
    )
    detail: str | None = Field(None, description="The path in words, when it has no binding.")


class AccessExplanation(BasePydanticModel):
    """Why a principal may or may not perform one action on one object."""

    action: str = Field(..., description="The action id explained.")
    allowed: bool = Field(..., description="Whether the principal may perform the action.")
    in_scope: bool = Field(
        ...,
        description=(
            "Whether the object is within the principal's reach at all: their Organization "
            "and, for objects that live in an Environment, an Environment they are admitted to."
        ),
    )
    paths: list[AccessExplanationPath] = Field(
        default_factory=list,
        description="Every way the action is given; empty when nothing gives it.",
    )
    blocked_by_rule: bool = Field(
        False,
        description="Whether a platform rule blocks the action on this object for everyone.",
    )
    granting_policies: list[str] = Field(
        default_factory=list,
        description="Ids of the policies that contain the action: what to ask for.",
    )


class AccessManagedObjectMixin(DetailActionObjectMixin):
    """Read and change who has access to an object whose access is decided by policies.

    A policy is attached to a principal, a person, a workload identity or a
    Team, as a binding. Reading the bindings and explaining someone else's
    access needs the object's `read_access` action; attaching and detaching
    need its `manage_access` action and, to attach, every action of the
    policy. Removing your own binding is always allowed.

    A refused request raises `PermissionDeniedError`. Its `payload` is the
    platform's answer, with `code` `missing_action`, `required_action` and
    `granting_policies`, the policies that would allow it. A change the
    platform does not accept raises `BadRequestError`, whose `payload` names
    the reason in `code`, for example `invalid_principal_uids` for someone
    outside the people you can give access to. An object you cannot read
    raises `NotFoundError`. A malformed argument raises `ValueError` before
    anything is sent.
    """

    def _access_object(self, data: Any, *, route: str) -> dict[str, Any]:
        if not isinstance(data, dict):
            raise ApiError(
                f"Unexpected {type(self).__name__} response for {route!r}: {type(data)!r}"
            )
        return data

    def _access_list(self, data: Any, *, route: str) -> list[Any]:
        if not isinstance(data, list):
            raise ApiError(
                f"Unexpected {type(self).__name__} response for {route!r}: {type(data)!r}"
            )
        return data

    def get_access(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> AccessListing:
        """Return the policies attached to this object and the access the platform computes."""

        data = self._request_detail_action(
            r_type="GET",
            action_name="access",
            payload={},
            timeout=timeout,
            expected_statuses=(200,),
        )
        return AccessListing.model_validate(self._access_object(data, route="access"))

    def get_my_access(
        self,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> CallerAccess:
        """Return the actions the caller may perform on this object and the policies they hold."""

        data = self._request_detail_action(
            r_type="GET",
            action_name="access/me",
            payload={},
            timeout=timeout,
            expected_statuses=(200,),
        )
        return CallerAccess.model_validate(self._access_object(data, route="access/me"))

    def list_access_candidates(
        self,
        policy: str,
        *,
        principal_type: Literal["user", "team"] = "user",
        identity_type: Literal["human", "workload"] | None = None,
        search: str | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> list[AccessPrincipal]:
        """Return who the caller may give `policy` to on this object, at most 200 principals.

        `policy` is a policy name of the object's type, such as `viewer`, or its
        full id. `principal_type` is `user` (the default) or `team`. For users,
        `identity_type` is `human` (the platform's default) or `workload`.
        `search` narrows people by email and Teams by name. A policy that cannot
        be given to that kind of principal returns an empty list. It needs the
        object's `manage_access` action, unless you manage a workload identity.
        """

        params: dict[str, str] = {
            "policy": _non_empty_text(policy, field_name="policy", example="viewer"),
            "principal_type": _closed_choice(
                principal_type, field_name="principal_type", choices=_PRINCIPAL_TYPES
            ),
        }
        if identity_type is not None:
            params["identity_type"] = _closed_choice(
                identity_type, field_name="identity_type", choices=_IDENTITY_TYPES
            )
        if search is not None and str(search).strip():
            params["search"] = str(search).strip()
        data = self._request_detail_action(
            r_type="GET",
            action_name="access/candidates",
            payload={"params": params},
            timeout=timeout,
            expected_statuses=(200,),
        )
        return [
            AccessPrincipal.model_validate(item)
            for item in self._access_list(data, route="access/candidates")
        ]

    def attach_policy(
        self,
        policy: str,
        *,
        principal_type: Literal["user", "team"],
        principal_uid: Any,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> AccessBinding:
        """Attach `policy` to a person, a workload identity or a Team, and return the binding.

        `policy` is a policy name of the object's type, such as `viewer`, or its
        full id. `principal_type` is `user` for a person or a workload identity
        and `team` for a Team; `principal_uid` is its UID, or an object or dict
        that carries it as `uid`.
        """

        data = self._request_detail_action(
            r_type="POST",
            action_name="access/bindings",
            payload={"json": _attach_body(policy, principal_type, principal_uid)},
            timeout=timeout,
            expected_statuses=(201,),
        )
        return AccessBinding.model_validate(self._access_object(data, route="access/bindings"))

    def detach_policy(
        self,
        binding_uid: Any,
        *,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> None:
        """Detach the binding `binding_uid` from this object.

        `binding_uid` is the binding's UID, or an `AccessBinding`.
        """

        uid = _canonical_uid(binding_uid, field_name="binding_uid")
        self._request_detail_action(
            r_type="DELETE",
            action_name=f"access/bindings/{uid}",
            payload={},
            timeout=timeout,
            expected_statuses=(204,),
        )

    def apply_access_changes(
        self,
        *,
        attach: Iterable[Mapping[str, Any]] = (),
        detach: Iterable[Any] = (),
        timeout: int | float | tuple[float, float] | None = None,
    ) -> list[AccessBinding]:
        """Attach and detach several policies at once, and return the object's bindings after.

        Each `attach` entry is a mapping with `policy`, `principal_type` and
        `principal_uid`, as `attach_policy()` takes them; each `detach` entry is a
        binding UID or an `AccessBinding`. The platform applies every change or
        none of them.
        """

        attach_bodies = []
        for entry in attach:
            if not isinstance(entry, Mapping):
                raise ValueError(
                    "Each attach entry must be a mapping with policy, principal_type and "
                    "principal_uid."
                )
            attach_bodies.append(
                _attach_body(
                    entry.get("policy"),
                    entry.get("principal_type"),
                    entry.get("principal_uid"),
                )
            )
        detach_uids = [_canonical_uid(item, field_name="detach") for item in detach]
        if not attach_bodies and not detach_uids:
            raise ValueError("Pass at least one policy to attach or binding to detach.")

        data = self._request_detail_action(
            r_type="POST",
            action_name="access/changes",
            payload={"json": {"attach": attach_bodies, "detach": detach_uids}},
            timeout=timeout,
            expected_statuses=(200,),
        )
        bindings = self._access_object(data, route="access/changes").get("bindings")
        return [
            AccessBinding.model_validate(item)
            for item in self._access_list(bindings, route="access/changes")
        ]

    def explain_access(
        self,
        action: str,
        *,
        principal_uid: Any | None = None,
        timeout: int | float | tuple[float, float] | None = None,
    ) -> AccessExplanation:
        """Explain why the caller, or the User `principal_uid`, may or may not perform `action`.

        `action` is an action id of the object's type, such as
        `organization-teams.update`. Asking about yourself needs only read
        access to the object; asking about someone else needs `read_access`.
        """

        params = {
            "action": _non_empty_text(
                action, field_name="action", example="organization-teams.update"
            )
        }
        if principal_uid is not None:
            params["principal_uid"] = _canonical_uid(principal_uid, field_name="principal_uid")
        data = self._request_detail_action(
            r_type="GET",
            action_name="access/explain",
            payload={"params": params},
            timeout=timeout,
            expected_statuses=(200,),
        )
        return AccessExplanation.model_validate(self._access_object(data, route="access/explain"))


__all__ = [
    "AccessBinding",
    "AccessComputedRow",
    "AccessExplanation",
    "AccessExplanationPath",
    "AccessGrantor",
    "AccessListing",
    "AccessManagedObjectMixin",
    "AccessObjectReference",
    "AccessPolicyReference",
    "AccessPrincipal",
    "AccessWorkflowDeclaration",
    "CallerAccess",
]
