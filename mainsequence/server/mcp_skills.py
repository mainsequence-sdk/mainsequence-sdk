"""An application's Agent Skills served through the MCP Skills extension (ADR 0039).

``skills_extension(directory)`` reads a directory of Agent Skills once and returns
the official ``io.modelcontextprotocol/skills`` extension for the application's
``MCPServer``. The extension answers ``skills/list`` and ``skills/get``, and every
skill file is an ordinary resource served with the exact bytes its manifest
digests. Needs the ``mcp`` extra.
"""

from __future__ import annotations

import hashlib
import math
import mimetypes
import os
import re
import sys
from pathlib import Path

import yaml

try:
    from mcp.server.extension import ResourceBinding
    from mcp.server.mcpserver.resources import BinaryResource, TextResource
    from mcp.shared.exceptions import MCPError
    from mcp_types.jsonrpc import INVALID_PARAMS
    from pydantic import ValidationError
except ImportError as exc:
    raise ImportError(
        "Serving skills needs the official MCP Python SDK 2.x: install mainsequence[mcp]."
    ) from exc

try:
    from mcp.server.skills import Skills
    from mcp.shared.skills import GetSkillResult, ListSkillsResult, Skill, SkillResource
except ImportError:
    # Until an mcp release contains the extension, its verbatim copy.
    from ._mcp_skills_upstream.server import Skills
    from ._mcp_skills_upstream.shared import (
        GetSkillResult,
        ListSkillsResult,
        Skill,
        SkillResource,
    )

# Content changes only with a new revision, and digests catch a stale entry.
_TTL_MS = 300_000
_MAX_FILES = 512
_MAX_BYTES = 16 * 1024 * 1024
_SKILL_MD = "SKILL.md"
_NAME = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*", flags=re.ASCII)
_NAME_RULE = "1 to 64 characters of a-z, 0-9 and single hyphens, not at either end"
# Unreserved URI characters only, so a file's URI never needs percent-encoding.
_SEGMENT = re.compile(r"[A-Za-z0-9._~-]+", flags=re.ASCII)
# Python's built-in table only, not the host's mime.types files.
_MIME_TYPES = mimetypes.MimeTypes()
_MIME_TYPES.add_type("text/markdown", ".md")


class _Yaml11Loader(yaml.SafeLoader):
    """PyYAML's safe loader: YAML 1.1, as hosts using PyYAML read frontmatter."""


class _Yaml12Loader(yaml.SafeLoader):
    """The safe loader with YAML 1.2 core schema scalars, as most other hosts read them."""


def _mapping(loader, node):
    """A mapping without repeated keys, which parsers resolve differently."""
    seen = set()
    for key, _ in node.value:
        if isinstance(key, yaml.ScalarNode):
            if key.value in seen:
                raise yaml.constructor.ConstructorError(
                    None, None, f"found duplicate key {key.value!r}", key.start_mark
                )
            seen.add(key.value)
    return loader.construct_mapping(node, deep=True)


def _int_12(loader, node):
    value = loader.construct_scalar(node)
    if value.startswith("0o"):
        return int(value[2:], 8)
    if value.startswith("0x"):
        return int(value[2:], 16)
    return int(value)


_Yaml12Loader.yaml_implicit_resolvers = {}
for _tag, _pattern, _first in (
    ("null", r"^(?:~|null|Null|NULL|)$", ["~", "n", "N", ""]),
    ("bool", r"^(?:true|True|TRUE|false|False|FALSE)$", list("tTfF")),
    ("int", r"^(?:[-+]?[0-9]+|0o[0-7]+|0x[0-9a-fA-F]+)$", list("-+0123456789")),
    (
        "float",
        r"^(?:[-+]?(?:\.[0-9]+|[0-9]+(?:\.[0-9]*)?)(?:[eE][-+]?[0-9]+)?"
        r"|[-+]?\.(?:inf|Inf|INF)|\.(?:nan|NaN|NAN))$",
        list("-+.0123456789"),
    ),
):
    _Yaml12Loader.add_implicit_resolver(f"tag:yaml.org,2002:{_tag}", re.compile(_pattern), _first)
_Yaml12Loader.add_constructor("tag:yaml.org,2002:int", _int_12)
for _loader in (_Yaml11Loader, _Yaml12Loader):
    _loader.add_constructor("tag:yaml.org,2002:map", _mapping)


def _invalid(path, rule):
    return ValueError(f"{path}: {rule}.")


def _listing(path):
    """A directory's entries in name order, without hidden ones."""
    try:
        with os.scandir(path) as entries:
            return sorted(
                (entry for entry in entries if not entry.name.startswith(".")),
                key=lambda entry: entry.name,
            )
    except OSError as exc:
        raise _invalid(path, f"cannot be read ({exc.strerror})") from None


def _files(path, parts=()):
    """Every regular file under a skill directory: its path parts and path, in name order."""
    for entry in _listing(path):
        entry_path = Path(entry.path)
        if entry.is_symlink():
            raise _invalid(entry_path, "is a symbolic link; a skill holds only regular files")
        if not _SEGMENT.fullmatch(entry.name):
            raise _invalid(entry_path, "a skill path may use only A-Z a-z 0-9 . _ ~ -")
        if entry.is_dir(follow_symlinks=False):
            yield from _files(entry_path, (*parts, entry.name))
        elif entry.is_file(follow_symlinks=False):
            yield (*parts, entry.name), entry_path
        else:
            raise _invalid(entry_path, "is not a regular file or directory")


def _json_value(path, value, where):
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise _invalid(path, f"{where} has the key {key!r}, which is not a string")
            _json_value(path, item, f"{where}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _json_value(path, item, f"{where}[{index}]")
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise _invalid(path, f"{where} is not a finite number")
    elif value is not None and not isinstance(value, bool | int | str):
        raise _invalid(path, f"{where} is a {type(value).__name__}, not a JSON value")


def _difference(first, second, where):
    """Where two parses of the same YAML disagree, or None."""
    if type(first) is not type(second):
        return where
    if isinstance(first, dict):
        if list(first) != list(second):
            return where
        pairs = [(first[key], second[key], f"{where}.{key}") for key in first]
    elif isinstance(first, list):
        if len(first) != len(second):
            return where
        pairs = [
            (a, b, f"{where}[{index}]")
            for index, (a, b) in enumerate(zip(first, second, strict=True))
        ]
    else:
        return None if first == second else where
    for a, b, inner in pairs:
        found = _difference(a, b, inner)
        if found:
            return found
    return None


def _text(path, frontmatter, key, *, longest=None, required=False):
    if key not in frontmatter:
        if required:
            raise _invalid(path, f"the frontmatter has no {key}")
        return
    value = frontmatter[key]
    if not isinstance(value, str) or not value or (longest and len(value) > longest):
        size = f"1 to {longest} characters" if longest else "a non-empty string"
        raise _invalid(path, f"{key} must be {size}")


def _frontmatter(path, data, name):
    """The SKILL.md frontmatter as a JSON object that every YAML parser reads alike."""
    try:
        lines = data.decode("utf-8").split("\n")
    except UnicodeDecodeError:
        raise _invalid(path, "SKILL.md must be UTF-8") from None
    if lines[0].removesuffix("\r") != "---":
        raise _invalid(path, "SKILL.md must begin with a '---' line that opens its frontmatter")
    end = next(
        (index for index, line in enumerate(lines) if index and line.removesuffix("\r") == "---"),
        None,
    )
    if end is None:
        raise _invalid(path, "the frontmatter has no closing '---' line")
    source = "\n".join(lines[1:end])
    try:
        as_yaml_11 = yaml.load(source, Loader=_Yaml11Loader)
        frontmatter = yaml.load(source, Loader=_Yaml12Loader)
    except yaml.YAMLError as exc:
        raise _invalid(path, f"the frontmatter is not valid YAML ({exc})") from None
    if not isinstance(frontmatter, dict):
        raise _invalid(path, "the frontmatter must be a mapping")
    _json_value(path, frontmatter, "frontmatter")
    differs = _difference(as_yaml_11, frontmatter, "frontmatter")
    if differs:
        raise _invalid(
            path,
            f"{differs} reads differently under YAML 1.1 and YAML 1.2; quote it "
            "(for example yes, no, on, off, dates and numbers with leading zeros)",
        )
    value = frontmatter.get("name")
    if not (isinstance(value, str) and len(value) <= 64 and _NAME.fullmatch(value)):
        raise _invalid(path, f"name must be {_NAME_RULE}")
    if value != name:
        raise _invalid(path, f"name {value!r} must equal its directory's name {name!r}")
    _text(path, frontmatter, "description", longest=1024, required=True)
    _text(path, frontmatter, "compatibility", longest=500)
    _text(path, frontmatter, "license")
    _text(path, frontmatter, "allowed-tools")
    metadata = frontmatter.get("metadata", {})
    if not isinstance(metadata, dict) or not all(isinstance(v, str) for v in metadata.values()):
        raise _invalid(path, "metadata must map strings to strings")
    return frontmatter


def _resource(uri, data, *, name, description=None):
    """A resource serving exactly ``data``: as text when it is UTF-8, else as a blob."""
    guessed, encoding = _MIME_TYPES.guess_type(uri)
    guessed = guessed if encoding is None else None
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        mime_type = guessed or "application/octet-stream"
        return BinaryResource(
            uri=uri, name=name, description=description, mime_type=mime_type, data=data
        )
    mime_type = guessed or "text/plain"
    return TextResource(uri=uri, name=name, description=description, mime_type=mime_type, text=text)


def _skill(root, skill_path):
    """One skill folder, read once: its entry and the resources serving its files."""
    name = root.name
    files, total = {}, 0
    for parts, path in _files(root):
        if parts[-1] == _SKILL_MD and len(parts) > 1:
            raise _invalid(
                path,
                "a skill inside another skill is not served; "
                "give it its own folder beside that skill or in a group folder",
            )
        try:
            if len(files) == _MAX_FILES or total + path.stat().st_size > _MAX_BYTES:
                raise _invalid(root, f"a skill may hold at most {_MAX_FILES} files and 16 MiB")
            data = path.read_bytes()
        except OSError as exc:
            raise _invalid(path, f"cannot be read ({exc.strerror})") from None
        total += len(data)
        if total > _MAX_BYTES:
            raise _invalid(root, f"a skill may hold at most {_MAX_FILES} files and 16 MiB")
        files["/".join(parts)] = data
    if _SKILL_MD not in files:
        raise _invalid(root, "a skill folder must hold a SKILL.md")
    frontmatter = _frontmatter(root / _SKILL_MD, files[_SKILL_MD], name)
    order = [_SKILL_MD, *sorted(path for path in files if path != _SKILL_MD)]
    uris = {path: f"skill://{skill_path}/{path}" for path in order}
    try:
        entry = Skill(
            uri=uris[_SKILL_MD],
            frontmatter=frontmatter,
            resources=[
                SkillResource(
                    uri=uris[path],
                    digest="sha256:" + hashlib.sha256(files[path]).hexdigest(),
                    size=len(files[path]),
                )
                for path in order
            ],
        )
    except ValidationError as exc:
        raise _invalid(root, f"is not a valid Skills extension entry ({exc})") from None
    resources = [
        _resource(
            uris[_SKILL_MD], files[_SKILL_MD], name=name, description=frontmatter["description"]
        )
    ]
    resources += [
        _resource(uris[path], files[path], name=path.rsplit("/", 1)[-1]) for path in order[1:]
    ]
    return entry, resources


class _DirectorySkills(Skills):
    """The official Skills extension, serving one skills directory read at startup."""

    def __init__(self, catalog):
        super().__init__(list_skills=self._list_skills, get_skill=self._get_skill)
        self._skills = [entry for entry, _ in catalog]
        self._by_uri = {entry.uri: entry for entry in self._skills}
        self._resources = [
            ResourceBinding(resource) for _, resources in catalog for resource in resources
        ]

    def resources(self):
        return self._resources

    async def _list_skills(self, ctx, params):
        if params.cursor is not None:
            raise MCPError(code=INVALID_PARAMS, message="Invalid cursor: skills/list has one page.")
        return ListSkillsResult(skills=self._skills, ttl_ms=_TTL_MS, cache_scope="private")

    async def _get_skill(self, ctx, params):
        skill = self._by_uri.get(params.uri)
        if skill is None:
            raise MCPError(code=INVALID_PARAMS, message=f"No skill is served at {params.uri}")
        return GetSkillResult(skill=skill, ttl_ms=_TTL_MS, cache_scope="private")


def _skills_directory(directory, caller):
    """``directory`` as an absolute path; a relative one is found above the calling module."""
    path = Path(directory)
    if path.is_absolute():
        return path
    start = Path(caller).resolve().parent if caller else Path.cwd()
    for base in (start, *start.parents):
        if (base / path).is_dir():
            return base / path
    raise ValueError(
        f"No directory {path} was found in {start} or above it: a relative skills directory "
        "is resolved from the module that calls skills_extension, up to the repository root."
    )


def _holds_skill_md(folder):
    return any(
        entry.name == _SKILL_MD and entry.is_file(follow_symlinks=False)
        for entry in _listing(folder)
    )


def _collect(folder, prefix, catalog):
    """The skills under ``folder``: a folder with a SKILL.md is a skill, one without groups."""
    for entry in _listing(folder):
        path = Path(entry.path)
        if entry.is_symlink():
            raise _invalid(path, "is a symbolic link; skills and group folders are directories")
        if not entry.is_dir(follow_symlinks=False):
            continue  # A file beside skills, such as a README, belongs to no skill.
        # The first folder becomes the skill URI's host, so every name keeps the name rule.
        if not (len(entry.name) <= 64 and _NAME.fullmatch(entry.name)):
            raise _invalid(path, f"a skill or group folder's name must be {_NAME_RULE}")
        segments = (*prefix, entry.name)
        if _holds_skill_md(path):
            catalog.append(_skill(path, "/".join(segments)))
            continue
        found = len(catalog)
        _collect(path, segments, catalog)
        if len(catalog) == found:
            raise _invalid(
                path,
                "holds no SKILL.md and no skill: a skill folder needs a SKILL.md, "
                "a group folder needs skills",
            )


def skills_extension(directory):
    """The MCP Skills extension serving the Agent Skills in ``directory``.

    Pass it when constructing the application's server,
    ``MCPServer(name, extensions=[skills_extension(directory)])``. ``directory``
    is the folder the application chooses for the skills it publishes, with any
    name and place in the repository: an absolute path, or a path relative to the
    repository, found from the calling module's directory upwards. Without the
    extension the server exposes no skills, and nothing outside ``directory`` is
    published.

    A folder in ``directory`` that holds a ``SKILL.md`` is a skill; a folder
    without one groups skills, and its path prefixes their URIs:
    ``billing/refund-review/SKILL.md`` is ``skill://billing/refund-review/SKILL.md``.
    Hidden names are skipped. Every file is read now: manifests digest the bytes
    served, and a change on disk takes effect at the next start. A folder that
    breaks the Agent Skills format or the extension's limits raises
    ``ValueError`` naming the file and the rule. The server's ``skill://``
    resources belong to this extension. See ADR 0039.
    """
    directory = _skills_directory(directory, sys._getframe(1).f_globals.get("__file__"))
    if not directory.is_dir():
        raise _invalid(directory, "the skills folder does not exist")
    if _holds_skill_md(directory):
        raise _invalid(directory, "pass the folder that contains the skills, not a skill")
    catalog = []
    _collect(directory, (), catalog)
    if not catalog:
        raise _invalid(directory, "the skills folder holds no skill")
    return _DirectorySkills(sorted(catalog, key=lambda skill: skill[0].uri))


__all__ = ["skills_extension"]
