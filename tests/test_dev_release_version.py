"""Contract for the development-release version computed by the publish workflow.

The standard these tests pin down is documented in
`docs/knowledge/release_process.md`.
"""

from __future__ import annotations

import pytest
from packaging.version import Version

from scripts import dev_release_version


def _files(*, yanked: bool = False) -> list[dict[str, object]]:
    return [{"filename": "mainsequence-x-py3-none-any.whl", "yanked": yanked}]


def test_latest_final_ignores_development_and_pre_releases():
    releases = {
        "8.1.19": _files(),
        "8.1.20.dev41": _files(),
        "8.2.0rc1": _files(),
        "8.1.19.post1": _files(),
    }

    assert dev_release_version.latest_final_version(releases) == Version("8.1.19")


def test_latest_final_ignores_releases_with_no_installable_file():
    releases = {
        "8.1.19": _files(),
        "9.0.0": [],
        "9.1.0": _files(yanked=True),
    }

    assert dev_release_version.latest_final_version(releases) == Version("8.1.19")


def test_latest_final_keeps_a_release_with_one_file_left():
    releases = {
        "8.1.19": _files(),
        "8.2.0": [
            {"filename": "mainsequence-8.2.0.tar.gz", "yanked": True},
            {"filename": "mainsequence-8.2.0-py3-none-any.whl", "yanked": False},
        ],
    }

    assert dev_release_version.latest_final_version(releases) == Version("8.2.0")


def test_latest_final_orders_numerically_not_lexically():
    releases = {"8.1.9": _files(), "8.1.10": _files(), "8.1.2": _files()}

    assert dev_release_version.latest_final_version(releases) == Version("8.1.10")


def test_latest_final_skips_unparseable_versions():
    releases = {"8.1.19": _files(), "not-a-version": _files()}

    assert dev_release_version.latest_final_version(releases) == Version("8.1.19")


def test_latest_final_requires_at_least_one_installable_release():
    with pytest.raises(dev_release_version.DevVersionError):
        dev_release_version.latest_final_version({"9.0.0": []})


def test_dev_version_is_the_declared_version_with_a_dev_serial():
    assert (
        dev_release_version.dev_version(Version("8.1.20"), Version("8.1.19"), 41) == "8.1.20.dev41"
    )


def test_a_planned_minor_release_keeps_its_own_number():
    assert dev_release_version.dev_version(Version("8.2.0"), Version("8.1.19"), 7) == "8.2.0.dev7"


@pytest.mark.parametrize("declared", ["8.1.19", "8.1.18"])
def test_a_declared_version_that_is_already_released_is_refused(declared):
    # The repository once declared 8.1.19 while it published 8.1.20.devN, so no
    # final 8.1.20 could be built from it. A missing bump now stops the build.
    with pytest.raises(dev_release_version.DevVersionError, match="already released"):
        dev_release_version.dev_version(Version(declared), Version("8.1.19"), 41)


def test_dev_version_sorts_between_the_current_and_the_next_release():
    computed = Version(dev_release_version.dev_version(Version("8.1.20"), Version("8.1.19"), 41))

    assert Version("8.1.19") < computed < Version("8.1.20")


def test_dev_version_is_a_development_release_so_pip_skips_it_by_default():
    computed = Version(dev_release_version.dev_version(Version("8.1.20"), Version("8.1.19"), 41))

    assert computed.is_devrelease


def test_dev_version_rejects_a_negative_run_number():
    with pytest.raises(dev_release_version.DevVersionError):
        dev_release_version.dev_version(Version("8.1.20"), Version("8.1.19"), -1)


def test_the_version_after_a_release_is_the_next_patch():
    assert dev_release_version.next_patch(Version("8.1.20")) == "8.1.21"


def test_a_declared_development_version_is_refused():
    with pytest.raises(dev_release_version.DevVersionError):
        dev_release_version.declared_release("8.1.20.dev41")


def test_apply_version_rewrites_only_the_project_version():
    pyproject = '[project]\nname = "mainsequence"\nversion = "8.1.19"\n\n[tool.other]\nversion = "1.0.0"\n'

    rewritten = dev_release_version.apply_version(pyproject, "8.1.20.dev41")

    assert 'version = "8.1.20.dev41"' in rewritten
    assert rewritten.count('version = "1.0.0"') == 1


def test_apply_version_refuses_a_pyproject_without_a_project_version():
    with pytest.raises(dev_release_version.DevVersionError):
        dev_release_version.apply_version('[project]\nname = "mainsequence"\n', "8.1.20.dev41")


def test_read_declared_version_reports_the_repository_version():
    text = dev_release_version.PYPROJECT.read_text(encoding="utf-8")

    declared = dev_release_version.read_declared_version(text)

    assert declared is not None
    Version(declared)


def test_the_repository_pyproject_accepts_a_rewritten_version():
    text = dev_release_version.PYPROJECT.read_text(encoding="utf-8")

    rewritten = dev_release_version.apply_version(text, "8.1.20.dev41")

    assert dev_release_version.read_declared_version(rewritten) == "8.1.20.dev41"
