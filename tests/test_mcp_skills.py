"""Application skills served through the MCP Skills extension (ADR 0039)."""

import base64
import hashlib
import importlib.util
import os
from pathlib import Path
from uuid import uuid4

import pytest
import yaml
from fastapi import FastAPI
from fastapi.testclient import TestClient
from mcp.server.mcpserver import MCPServer
from mcp.server.transport_security import TransportSecuritySettings

import tests.test_fastapi_request_identity as identity_tests
from mainsequence.server import _mcp_skills_upstream, mcp_skills
from mainsequence.server.caller_assertions import ASSERTION_HEADER
from mainsequence.server.fastapi import install_mcp, install_request_identity
from mainsequence.server.mcp_skills import skills_extension

clean_environment = identity_tests.clean_environment
signed = identity_tests.signed

MODERN, LEGACY = "2026-07-28", "2025-11-25"
EXTENSION = "io.modelcontextprotocol/skills"
INVALID_PARAMS, METHOD_NOT_FOUND = -32602, -32601

REFUND_REVIEW = """\
---
name: refund-review
description: Review a refund request against the refund policy.
license: Proprietary
metadata:
  owner: payments
  version: "1.2"
x-reviewed: true
x-steps: [read the policy, decide]
---

# Refund review

Read `references/policy.md` before deciding.
"""

SERVED = {
    "refund-review/SKILL.md": REFUND_REVIEW.encode(),
    # A byte order mark and CRLF line endings are served exactly as written.
    "refund-review/references/policy.md": "﻿Within 30 days.\r\nNo cash.\r\n".encode(),
    "refund-review/assets/stamp.bin": bytes(range(256)),
    "refund-review/assets/empty.txt": b"",
    "daily-close/SKILL.md": b"---\nname: daily-close\ndescription: Close the books.\n---\nSteps.\n",
}
# Hidden names, and files outside a skill directory, are never served.
NOT_SERVED = {
    "refund-review/.notes.md": b"draft",
    "refund-review/.cache/index": b"cache",
    ".git/config": b"[core]",
    "README.md": b"# Skills\n",
}


@pytest.fixture(autouse=True)
def no_cors_origins(monkeypatch):
    monkeypatch.delenv("FASTAPI_CORS_ALLOW_ORIGINS", raising=False)


def _write(root, files):
    for relative, data in files.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
    return root


@pytest.fixture
def catalog(tmp_path):
    return _write(tmp_path / "skills", {**SERVED, **NOT_SERVED})


def _app(*extensions):
    server = MCPServer("refunds", extensions=list(extensions))
    app = FastAPI()
    install_request_identity(app)
    http_app = server.streamable_http_app(
        stateless_http=True,
        json_response=True,
        transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
    )
    install_mcp(app, http_app, lifespan=server.session_manager.run)
    return app


def _rpc(client, method, params=None, *, proof, version=MODERN):
    """One JSON-RPC request, as an extension-aware client of ``version`` sends it."""
    params = dict(params or {})
    headers = {
        "accept": "application/json, text/event-stream",
        "content-type": "application/json",
        "mcp-protocol-version": version,
        "mcp-method": method,
    }
    if proof is not None:
        headers[ASSERTION_HEADER] = proof
    if version == MODERN:
        params["_meta"] = {
            "io.modelcontextprotocol/protocolVersion": MODERN,
            "io.modelcontextprotocol/clientCapabilities": {"extensions": {EXTENSION: {}}},
        }
        if method == "resources/read":
            headers["mcp-name"] = params["uri"]
    body = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    return client.post("/mcp", json=body, headers=headers)


def _result(response):
    assert response.status_code == 200, response.text
    return response.json()["result"]


def _error(response):
    return response.json()["error"]["code"]


def _digest(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _served_bytes(content):
    if "text" in content:
        return content["text"].encode("utf-8")
    return base64.b64decode(content["blob"], validate=True)


def _manifest_on_disk(catalog, name):
    """A skill's files as found on disk, without hidden names."""
    manifest = []
    for directory, subdirectories, files in os.walk(catalog / name):
        subdirectories[:] = [entry for entry in subdirectories if not entry.startswith(".")]
        for file in files:
            if not file.startswith("."):
                path = Path(directory, file)
                data = path.read_bytes()
                relative = path.relative_to(catalog / name).as_posix()
                uri = f"skill://{name}/{relative}"
                manifest.append({"uri": uri, "digest": _digest(data), "size": len(data)})
    return sorted(manifest, key=lambda entry: entry["uri"])


def test_an_extension_aware_client_lists_gets_and_reads_every_skill(signed, catalog):
    token, _, _ = signed
    with TestClient(_app(skills_extension(catalog))) as client:
        discovered = _result(_rpc(client, "server/discover", proof=token()))
        listed = _result(_rpc(client, "skills/list", proof=token()))
        fetched = {
            skill["uri"]: _result(_rpc(client, "skills/get", {"uri": skill["uri"]}, proof=token()))
            for skill in listed["skills"]
        }
        served = {
            resource["uri"]: _result(
                _rpc(client, "resources/read", {"uri": resource["uri"]}, proof=token())
            )["contents"]
            for skill in listed["skills"]
            for resource in skill["resources"]
        }
        hidden = _rpc(
            client, "resources/read", {"uri": "skill://refund-review/.notes.md"}, proof=token()
        )

    assert discovered["capabilities"]["extensions"] == {EXTENSION: {}}
    assert "resources" in discovered["capabilities"]
    assert "nextCursor" not in listed
    for result in (listed, *fetched.values()):
        assert (result["resultType"], result["ttlMs"], result["cacheScope"]) == (
            "complete",
            300_000,
            "private",
        )
    assert [skill["uri"] for skill in listed["skills"]] == [
        "skill://daily-close/SKILL.md",
        "skill://refund-review/SKILL.md",
    ]
    for skill in listed["skills"]:
        name = skill["uri"].split("/")[2]
        source = SERVED[f"{name}/SKILL.md"].decode()
        # Frontmatter as an independent YAML parser reads the file, every field kept.
        assert skill["frontmatter"] == yaml.safe_load(source.split("---\n")[1])
        assert sorted(skill["resources"], key=lambda entry: entry["uri"]) == _manifest_on_disk(
            catalog, name
        )
        assert fetched[skill["uri"]]["skill"] == skill
        for resource in skill["resources"]:
            (content,) = served[resource["uri"]]
            data = _served_bytes(content)
            assert content["uri"] == resource["uri"]
            assert (_digest(data), len(data)) == (resource["digest"], resource["size"])
            assert data == SERVED[resource["uri"].removeprefix("skill://")]

    assert set(served) == {f"skill://{path}" for path in SERVED}
    assert served["skill://refund-review/SKILL.md"][0]["mimeType"] == "text/markdown"
    assert served["skill://refund-review/assets/stamp.bin"][0]["mimeType"] == (
        "application/octet-stream"
    )
    assert "blob" in served["skill://refund-review/assets/stamp.bin"][0]
    assert served["skill://refund-review/references/policy.md"][0]["text"].startswith("﻿")
    assert _error(hidden) == INVALID_PARAMS


GROUPED = {
    "billing/refund-review/SKILL.md": REFUND_REVIEW.encode(),
    "billing/refund-review/references/policy.md": SERVED["refund-review/references/policy.md"],
    "billing/chargebacks/SKILL.md": b"---\nname: chargebacks\ndescription: Dispute one.\n---\n",
    # Beside skills, a file belongs to none.
    "billing/README.md": b"# Billing skills\n",
    "daily-close/SKILL.md": SERVED["daily-close/SKILL.md"],
}


def test_group_folders_prefix_the_skills_they_hold(signed, tmp_path):
    token, _, _ = signed
    catalog = _write(tmp_path / "skills", GROUPED)
    with TestClient(_app(skills_extension(catalog))) as client:
        listed = _result(_rpc(client, "skills/list", proof=token()))
        fetched = [
            _result(_rpc(client, "skills/get", {"uri": skill["uri"]}, proof=token()))["skill"]
            for skill in listed["skills"]
        ]
        served = {
            resource["uri"]: _served_bytes(
                _result(_rpc(client, "resources/read", {"uri": resource["uri"]}, proof=token()))[
                    "contents"
                ][0]
            )
            for skill in listed["skills"]
            for resource in skill["resources"]
        }
        unprefixed = _rpc(
            client, "skills/get", {"uri": "skill://refund-review/SKILL.md"}, proof=token()
        )

    assert [skill["uri"] for skill in listed["skills"]] == [
        "skill://billing/chargebacks/SKILL.md",
        "skill://billing/refund-review/SKILL.md",
        "skill://daily-close/SKILL.md",
    ]
    assert [skill["frontmatter"]["name"] for skill in listed["skills"]] == [
        "chargebacks",
        "refund-review",
        "daily-close",
    ]
    assert fetched == listed["skills"]
    for skill in listed["skills"]:
        for resource in skill["resources"]:
            data = served[resource["uri"]]
            assert (_digest(data), len(data)) == (resource["digest"], resource["size"])
    assert served == {
        f"skill://{path}": data for path, data in GROUPED.items() if path != "billing/README.md"
    }
    assert _error(unprefixed) == INVALID_PARAMS


def test_files_are_served_as_read_at_startup(signed, catalog):
    token, _, _ = signed
    app = _app(skills_extension(catalog))
    (catalog / "daily-close" / "SKILL.md").write_bytes(b"changed on disk")
    (catalog / "daily-close" / "later.md").write_bytes(b"added on disk")
    with TestClient(app) as client:
        listed = _result(_rpc(client, "skills/list", proof=token()))
        read = _result(
            _rpc(client, "resources/read", {"uri": "skill://daily-close/SKILL.md"}, proof=token())
        )
        later = _rpc(
            client, "resources/read", {"uri": "skill://daily-close/later.md"}, proof=token()
        )

    assert _served_bytes(read["contents"][0]) == SERVED["daily-close/SKILL.md"]
    assert [entry["uri"] for entry in listed["skills"][0]["resources"]] == [
        "skill://daily-close/SKILL.md"
    ]
    assert _error(later) == INVALID_PARAMS


def test_unknown_skills_malformed_uris_and_cursors_are_invalid_params(signed, catalog):
    token, _, _ = signed
    with TestClient(_app(skills_extension(catalog))) as client:
        unknown = [
            _error(_rpc(client, "skills/get", {"uri": uri}, proof=token()))
            for uri in (
                "skill://chargebacks/SKILL.md",
                "skill://refund-review/references/policy.md",
                "skill://refund-review",
                "skill://acme/refund-review/SKILL.md",
            )
        ]
        cursor = _error(_rpc(client, "skills/list", {"cursor": "page-2"}, proof=token()))

    assert unknown == [INVALID_PARAMS] * 4
    assert cursor == INVALID_PARAMS


def test_a_2025_11_25_client_reads_skills_without_a_declaration(signed, catalog):
    """That revision has no extensions capability; the official SDK still answers the methods."""
    token, _, _ = signed
    hello = {
        "protocolVersion": LEGACY,
        "capabilities": {},
        "clientInfo": {"name": "t", "version": "1"},
    }
    with TestClient(_app(skills_extension(catalog))) as client:
        initialized = _result(_rpc(client, "initialize", hello, proof=token(), version=LEGACY))
        listed = _result(_rpc(client, "skills/list", proof=token(), version=LEGACY))
        read = _result(
            _rpc(
                client,
                "resources/read",
                {"uri": "skill://refund-review/assets/stamp.bin"},
                proof=token(),
                version=LEGACY,
            )
        )

    assert "resources" in initialized["capabilities"]
    assert "extensions" not in initialized["capabilities"]
    assert not {"ttlMs", "cacheScope"} & set(listed)
    assert [skill["uri"] for skill in listed["skills"]] == [
        "skill://daily-close/SKILL.md",
        "skill://refund-review/SKILL.md",
    ]
    assert _served_bytes(read["contents"][0]) == SERVED["refund-review/assets/stamp.bin"]


def test_skills_and_their_files_need_an_authenticated_caller(signed, catalog):
    token, _, _ = signed
    forged = token(aud="urn:other:" + str(uuid4()))
    requests = [
        ("server/discover", {}),
        ("skills/list", {}),
        ("skills/get", {"uri": "skill://daily-close/SKILL.md"}),
        ("resources/read", {"uri": "skill://daily-close/SKILL.md"}),
    ]
    with TestClient(_app(skills_extension(catalog))) as client:
        statuses = [
            _rpc(client, method, params, proof=proof).status_code
            for method, params in requests
            for proof in (None, forged)
        ]

    assert statuses == [401] * 8


def _repository(tmp_path):
    """A repository whose MCP skills folder sits beside the coding agents' skills."""
    root = tmp_path / "repository"
    _write(root / ".agents" / "skills" / "mcp_only", SERVED)
    coding_agent_skill = _skill_md("name: api-surfaces\ndescription: For coding agents.")
    _write(
        root / ".agents" / "skills" / "mainsequence",
        {
            "PINNED_FROM.txt": b"9.0.23\n",
            "application_surfaces/api_surfaces/SKILL.md": coding_agent_skill,
        },
    )
    module = root / "api" / "provider" / "main.py"
    module.parent.mkdir(parents=True)
    module.write_text(
        "from mainsequence.server.mcp_skills import skills_extension\n\n"
        'extension = skills_extension(".agents/skills/mcp_only")\n'
    )
    return root


def _load(path):
    spec = importlib.util.spec_from_file_location(f"application_{uuid4().hex}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_a_repository_relative_folder_is_found_from_the_calling_module(tmp_path, monkeypatch):
    """Whatever the working directory; only the folder passed is published."""
    root = _repository(tmp_path)
    monkeypatch.chdir(tmp_path)

    extension = _load(root / "api" / "provider" / "main.py").extension

    assert {binding.resource.uri for binding in extension.resources()} == {
        f"skill://{path}" for path in SERVED
    }


def test_the_coding_agents_skills_folder_itself_is_not_a_folder_of_skills(tmp_path):
    with pytest.raises(ValueError, match="application_surfaces: a skill or group folder's name"):
        skills_extension(_repository(tmp_path) / ".agents" / "skills")


def test_without_the_folder_the_server_exposes_no_skill(signed, tmp_path, monkeypatch):
    """Nothing is discovered: not from the working directory, not from .agents/skills."""
    token, _, _ = signed
    monkeypatch.chdir(_repository(tmp_path))
    with TestClient(_app()) as client:
        discovered = _result(_rpc(client, "server/discover", proof=token()))
        listed = _rpc(client, "skills/list", proof=token())
        uri = {"uri": "skill://refund-review/SKILL.md"}
        read = _rpc(client, "resources/read", uri, proof=token())

    assert EXTENSION not in discovered["capabilities"].get("extensions", {})
    assert _error(listed) == METHOD_NOT_FOUND
    assert _error(read) == INVALID_PARAMS


def test_a_skill_may_hold_up_to_512_files(catalog):
    _write(catalog, {f"daily-close/steps/{index:03}.md": b"step" for index in range(511)})

    extension = skills_extension(catalog)

    assert len(extension.resources()) == 512 + 4


def _skill_md(frontmatter):
    return f"---\n{frontmatter}\n---\nBody\n".encode()


GOOD = "name: refund-review\ndescription: Review refunds."
SKILL_MD = "refund-review/SKILL.md"

INVALID_CATALOGS = {
    "no skill": ({"README.md": b"# Skills\n"}, "holds no skill"),
    "a skill instead of its parent": ({"SKILL.md": _skill_md(GOOD)}, "not a skill"),
    "no SKILL.md": ({"refund-review/notes.md": b"notes"}, "holds no SKILL.md and no skill"),
    "directory name": (
        {"Refund_Review/SKILL.md": _skill_md("name: Refund_Review\ndescription: d")},
        "folder's name must be",
    ),
    "name rule": (
        {SKILL_MD: _skill_md("name: refund--review\ndescription: d")},
        "name must be",
    ),
    "name differs": ({SKILL_MD: _skill_md("name: refunds\ndescription: d")}, "must equal"),
    "no description": ({SKILL_MD: _skill_md("name: refund-review")}, "has no description"),
    "empty description": (
        {SKILL_MD: _skill_md("name: refund-review\ndescription: ''")},
        "description must be 1 to 1024",
    ),
    "long description": (
        {SKILL_MD: _skill_md(f"name: refund-review\ndescription: {'d' * 1025}")},
        "description must be 1 to 1024",
    ),
    "long compatibility": (
        {SKILL_MD: _skill_md(f"{GOOD}\ncompatibility: {'c' * 501}")},
        "compatibility must be 1 to 500",
    ),
    "license": ({SKILL_MD: _skill_md(f"{GOOD}\nlicense: 2")}, "license must be"),
    "allowed tools": ({SKILL_MD: _skill_md(f"{GOOD}\nallowed-tools: [Bash]")}, "allowed-tools"),
    "metadata values": (
        {SKILL_MD: _skill_md(f"{GOOD}\nmetadata:\n  version: 1.2")},
        "metadata must map strings to strings",
    ),
    "no frontmatter": ({SKILL_MD: b"# Refund review\n"}, "must begin with a '---' line"),
    "unclosed frontmatter": ({SKILL_MD: f"---\n{GOOD}\n".encode()}, "no closing '---'"),
    "not YAML": ({SKILL_MD: _skill_md(f"{GOOD}\nx-list: [a")}, "not valid YAML"),
    "not a mapping": ({SKILL_MD: _skill_md("- refund-review")}, "must be a mapping"),
    "repeated key": ({SKILL_MD: _skill_md(f"{GOOD}\nname: refund-review")}, "duplicate key"),
    "YAML 1.1 boolean": (
        {SKILL_MD: _skill_md(f"{GOOD}\nx-reviewed: yes")},
        "frontmatter.x-reviewed reads differently under YAML 1.1 and YAML 1.2",
    ),
    "YAML 1.1 date": ({SKILL_MD: _skill_md(f"{GOOD}\nx-on: 2026-10-12")}, "x-on reads differently"),
    "YAML 1.1 octal": ({SKILL_MD: _skill_md(f"{GOOD}\nx-mode: 0755")}, "x-mode reads differently"),
    "YAML 1.2 exponent": (
        {SKILL_MD: _skill_md(f"{GOOD}\nx-rate: 1e3")},
        "x-rate reads differently",
    ),
    "not finite": ({SKILL_MD: _skill_md(f"{GOOD}\nx-limit: .inf")}, "not a finite number"),
    "key not a string": ({SKILL_MD: _skill_md(f"{GOOD}\n1: one")}, "which is not a string"),
    "binary value": ({SKILL_MD: _skill_md(f"{GOOD}\nx-blob: !!binary aGk=")}, "not a JSON value"),
    "not UTF-8": (
        {SKILL_MD: b"---\nname: refund-review\ndescription: \xff\n---\n"},
        "SKILL.md must be UTF-8",
    ),
    "nested skill": (
        {SKILL_MD: _skill_md(GOOD), "refund-review/inner/SKILL.md": _skill_md(GOOD)},
        "a skill inside another skill is not served",
    ),
    "group without skills": (
        {"billing/README.md": b"notes", "daily-close/SKILL.md": _skill_md(GOOD)},
        "billing: holds no SKILL.md and no skill",
    ),
    "group name": ({"Billing/refund-review/SKILL.md": _skill_md(GOOD)}, "folder's name must be"),
    "path characters": (
        {SKILL_MD: _skill_md(GOOD), "refund-review/my notes.md": b"notes"},
        "may use only",
    ),
    "too many files": (
        {SKILL_MD: _skill_md(GOOD), **{f"refund-review/{i:03}.md": b"" for i in range(512)}},
        "at most 512 files and 16 MiB",
    ),
}


@pytest.mark.parametrize("case", list(INVALID_CATALOGS))
def test_an_invalid_skill_fails_before_anything_is_served(tmp_path, case):
    files, message = INVALID_CATALOGS[case]

    with pytest.raises(ValueError, match=message):
        skills_extension(_write(tmp_path / "skills", files))


def test_a_missing_folder_fails(tmp_path):
    with pytest.raises(ValueError, match="No directory no-such-skills-folder was found in"):
        skills_extension("no-such-skills-folder")
    with pytest.raises(ValueError, match="does not exist"):
        skills_extension(tmp_path / "missing")


def _sixteen_mib(path):
    with open(path, "wb") as file:
        file.truncate(16 * 1024 * 1024)


@pytest.mark.parametrize(
    "cause, make, message",
    [
        (
            "refund-review/policy.md",
            lambda path: os.symlink(path.parent / "references" / "policy.md", path),
            "is a symbolic link; a skill holds only regular files",
        ),
        (
            "linked-skill",
            lambda path: os.symlink(path.parent / "daily-close", path),
            "is a symbolic link; skills and group folders are directories",
        ),
        ("refund-review/pipe", os.mkfifo, "is not a regular file or directory"),
        # With SKILL.md and the other files, more than 16 MiB in total.
        ("refund-review/large.bin", _sixteen_mib, "at most 512 files and 16 MiB"),
    ],
)
def test_links_special_files_and_oversized_skills_fail(catalog, cause, make, message):
    skills_extension(catalog)
    make(catalog / cause)

    with pytest.raises(ValueError, match=message):
        skills_extension(catalog)


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads files whatever their mode")
def test_an_unreadable_file_fails(catalog):
    policy = catalog / "refund-review" / "references" / "policy.md"
    policy.chmod(0)
    try:
        with pytest.raises(ValueError, match="cannot be read"):
            skills_extension(catalog)
    finally:
        policy.chmod(0o644)


def test_the_copied_extension_is_upstream_at_its_commit():
    """Only the recorded import differs from the files upstream published."""
    package = Path(_mcp_skills_upstream.__file__).parent
    upstream_import, copied_import = (line.encode() for line in _mcp_skills_upstream.CHANGED_IMPORT)
    for name, (_, digest) in _mcp_skills_upstream.UPSTREAM.items():
        source = (package / name).read_bytes()
        if name == "server.py":
            assert source.count(copied_import) == 1
            source = source.replace(copied_import, upstream_import)
        assert hashlib.sha256(source).hexdigest() == digest, name


def test_the_official_extension_replaces_the_copy_once_mcp_ships_it():
    official = importlib.util.find_spec("mcp.server.skills") is not None
    copy = "mainsequence.server._mcp_skills_upstream.server"
    assert mcp_skills.Skills.__module__ == ("mcp.server.skills" if official else copy)
