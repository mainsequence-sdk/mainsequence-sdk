"""ADR 0033: the SDK reads closed value sets tolerantly.

A response value this SDK release does not declare — an enum member, a literal
member — is kept as the backend wrote it instead of failing the row that carries
it and the listing that row is in. Direct release creation is retired; value-set tolerance applies to reads.

`ResourceRelease.filter(...)` returning a `harness_agent` release is the
reproduction from #119; the rest holds the rule for the models around it.
"""

from __future__ import annotations

import pytest

import mainsequence.client.base as base_mod
import mainsequence.client.models_helpers as models_helpers_mod
import mainsequence.code_repository_context as context_mod
from mainsequence.client import value_sets
from mainsequence.client.agent_runtime_models import AgentHarnessKind, AgentSessionStatus
from mainsequence.client.value_sets import OpenStrEnum, declared_values

RELEASE_UID = "2f4c4c3d-5669-4da5-9d86-b84633c1e6ed"
OTHER_RELEASE_UID = "6b1f0d2e-6a1a-4c44-9f2e-7b8c9d0e1f23"
BRANCH_UID = "1e7252f5-1d55-4a63-8be6-9cc5b42ffe7e"


class FakeResponse:
    status_code = 200
    content = b"{}"

    def __init__(self, body):
        self._body = body

    def json(self):
        return self._body


def release_payload(uid=RELEASE_UID, release_kind="harness_agent"):
    return {
        "uid": uid,
        "code_repository_branch_uid": BRANCH_UID,
        "name": "Harness Agent",
        "release_kind": release_kind,
        "automatic_deployment": True,
    }


@pytest.fixture(autouse=True)
def _forget_reported_values():
    value_sets._reset_reported_unknown_values()
    yield
    value_sets._reset_reported_unknown_values()


@pytest.fixture
def offline_listing(monkeypatch):
    """Serve a listing without the process branch context or a real session."""

    def serve(rows):
        monkeypatch.setattr(base_mod, "make_request", lambda **kwargs: FakeResponse(rows))
        monkeypatch.setattr(
            models_helpers_mod.ResourceRelease,
            "build_session",
            classmethod(lambda cls: object()),
        )
        monkeypatch.setattr(
            context_mod,
            "scope_current_code_repository_branch_filters",
            lambda caller, filters, field_name: dict(filters),
        )

    return serve


@pytest.fixture
def reported_lines(monkeypatch):
    """What value_sets logged. structlog does not reach caplog, so record it here."""
    lines: list[str] = []

    class Recorder:
        @staticmethod
        def warning(message, *args, **kwargs):
            lines.append(message)

    monkeypatch.setattr(value_sets, "logger", Recorder)
    return lines


# --- #119: the canonical vocabulary ------------------------------------------


def test_release_kind_declares_harness_agent():
    assert models_helpers_mod.ResourceReleaseKind.HARNESS_AGENT.value == "harness_agent"
    assert [kind.value for kind in models_helpers_mod.ResourceReleaseKind] == [
        "agent",
        "fastapi",
        "harness_agent",
        "static_site",
    ]


def test_harness_agent_release_parses_as_a_declared_kind():
    release = models_helpers_mod.ResourceRelease.model_validate(release_payload())

    assert release.release_kind is models_helpers_mod.ResourceReleaseKind.HARNESS_AGENT
    assert release.release_kind == "harness_agent"


def test_runtime_access_reads_the_same_release_kind_vocabulary():
    """#119: the release and its runtime-access payload name a kind the same way."""
    field = models_helpers_mod.ResourceReleaseRuntimeAccess.model_fields["release_kind"]

    assert field.annotation is models_helpers_mod.ResourceReleaseKind


def test_filter_returns_a_harness_agent_listing(offline_listing):
    """The #119 reproduction: the listing returns, with the kind readable."""
    offline_listing([release_payload(), release_payload(OTHER_RELEASE_UID)])

    releases = models_helpers_mod.ResourceRelease.filter(code_repository_branch_uid=BRANCH_UID)

    assert [release.uid for release in releases] == [RELEASE_UID, OTHER_RELEASE_UID]
    assert all(release.release_kind == "harness_agent" for release in releases)


# --- unknown enum values ------------------------------------------------------


def test_undeclared_enum_value_is_kept_and_readable():
    release = models_helpers_mod.ResourceRelease.model_validate(
        release_payload(release_kind="quantum_agent")
    )

    assert release.release_kind == "quantum_agent"
    assert release.release_kind.value == "quantum_agent"
    assert release.model_dump(mode="json")["release_kind"] == "quantum_agent"


def test_undeclared_enum_value_is_not_a_declared_member():
    release = models_helpers_mod.ResourceRelease.model_validate(
        release_payload(release_kind="quantum_agent")
    )

    assert release.release_kind not in set(models_helpers_mod.ResourceReleaseKind)
    assert release.release_kind != models_helpers_mod.ResourceReleaseKind.AGENT
    # Reading an undeclared value must not add it to the class.
    assert not hasattr(models_helpers_mod.ResourceReleaseKind, "QUANTUM_AGENT")
    assert [kind.value for kind in models_helpers_mod.ResourceReleaseKind] == [
        "agent",
        "fastapi",
        "harness_agent",
        "static_site",
    ]


def test_one_undeclared_enum_value_does_not_fail_a_listing(offline_listing):
    offline_listing(
        [release_payload(), release_payload(OTHER_RELEASE_UID, release_kind="quantum_agent")]
    )

    releases = models_helpers_mod.ResourceRelease.filter(code_repository_branch_uid=BRANCH_UID)

    assert [release.release_kind for release in releases] == ["harness_agent", "quantum_agent"]


def test_declared_enum_values_still_validate_to_their_member():
    assert AgentSessionStatus("running") is AgentSessionStatus.RUNNING
    assert AgentHarnessKind("pi") is AgentHarnessKind.PI


def test_enum_still_rejects_a_value_that_is_not_a_string():
    with pytest.raises(ValueError):
        models_helpers_mod.ResourceReleaseKind(17)


# --- unknown literal values ---------------------------------------------------


def test_undeclared_literal_value_is_kept():
    presence = models_helpers_mod.ResourceReleaseRuntimePresence.model_validate(
        {
            "phase": "draining",
            "replicas": {"desired": 1, "actual": 0},
            "detail": "Draining before shutdown.",
        }
    )

    assert presence.phase == "draining"


def test_declared_literal_values_are_still_the_release_vocabulary():
    known = declared_values(
        models_helpers_mod.ResourceReleaseRuntimeRouting.model_fields["state"]
    )

    assert known == ("routable", "unavailable")


def test_undeclared_literal_value_does_not_fail_the_row_around_it():
    notice = models_helpers_mod.ResourceReleaseRuntimeNotice.model_validate(
        {
            "code": "runtime.draining",
            "severity": "fatal",
            "title": "Draining",
            "message": "The runtime is draining.",
        }
    )

    assert notice.severity == "fatal"
    assert notice.code == "runtime.draining"


# --- what the SDK sends stays closed ------------------------------------------


# --- the report ---------------------------------------------------------------


def test_an_undeclared_value_is_reported_once_naming_model_field_and_value(reported_lines):
    for _ in range(3):
        models_helpers_mod.ResourceReleaseRuntimeRouting.model_validate({"state": "draining"})

    assert len(reported_lines) == 1
    assert "ResourceReleaseRuntimeRouting" in reported_lines[0]
    assert "state" in reported_lines[0]
    assert "draining" in reported_lines[0]
    assert "routable" in reported_lines[0]


# --- the mechanism ------------------------------------------------------------


def test_open_str_enum_reports_the_enum_it_could_not_place(reported_lines):
    class Vocabulary(OpenStrEnum):
        KNOWN = "known"

    kept = Vocabulary("surprising")

    assert kept == "surprising"
    assert [member.value for member in Vocabulary] == ["known"]
    assert len(reported_lines) == 1
    assert "Vocabulary" in reported_lines[0]
    assert "surprising" in reported_lines[0]


def test_open_value_set_requires_a_literal():
    with pytest.raises(TypeError, match="wraps a Literal"):
        value_sets.OpenValueSet[str]
