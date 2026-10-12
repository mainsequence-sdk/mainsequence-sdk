# ADR 0039: Application skills through the MCP Skills extension

Date: 2026-10-12

Status: Proposed for
[SDK issue #221](https://github.com/mainsequence-sdk/mainsequence-sdk/issues/221).
Not implemented. It ships now on the released `mcp` 2.3.0 with a verbatim
copy of the official `Skills` extension, until an `mcp` release contains it;
see [Release order](#7-release-order).

Owners: Main Sequence SDK maintainers.

Amends [ADR 0038](0038-project-mcp-on-fastapi-releases.md): its compatibility
target and the `mcp` extra's range. See [decision 5](#5-protocol-revision-and-the-mcp-extra).

## Context

[ADR 0038](0038-project-mcp-on-fastapi-releases.md) lets an application serve
its own MCP server at `/mcp` through
`mainsequence.server.fastapi.install_mcp(app, mcp_app, *, lifespan)`. The
application can offer tools. It cannot offer skills: workflows that span
several tools, checklists and reference material that should travel with the
service that owns them.

The normative source is the MCP Skills extension,
`io.modelcontextprotocol/skills`
([SEP-2640](https://modelcontextprotocol.io/seps/2640-skills-extension), Final;
[specification](https://github.com/modelcontextprotocol/ext-skills/blob/main/specification/stable/skills.mdx)).
It delegates the skill format to the
[Agent Skills specification](https://agentskills.io/specification). In short:

- The server declares the `resources` capability and
  `capabilities.extensions["io.modelcontextprotocol/skills"]`, optionally with
  `directoryRead: true`, which commits it to `resources/directory/read`.
- `skills/list` returns paginated entries. Each entry has the `SKILL.md` URI,
  the frontmatter verbatim as a JSON object, and a complete manifest of every
  file with a `sha256:` digest and byte size. A manifest is never split across
  pages. Results carry `resultType: "complete"`, `ttlMs` and `cacheScope`.
- `skills/get` returns one entry by URI and answers for every served skill. An
  unknown URI is JSON-RPC `-32602`.
- Files are ordinary resources read with `resources/read`. Digests and sizes
  cover the exact bytes served.
- A skill should not exceed 512 files or 16 MiB.
- The extension is specified against protocol revision `2026-07-28`, where
  capabilities are advertised by `server/discover`. The `2025-11-25` schema
  has no `extensions` capability field.

Observed at SDK revision `675ce90cc23ee99223d951f5d20e38d955d68886`:

- `install_mcp` forwards every JSON-RPC method of an authenticated POST
  unchanged. It does not block `skills/list`, `skills/get` or `resources/read`.
- No SDK code serves the extension.
- The `mcp` extra pins `mcp>=1.28,<2`, tested with 1.30.0. The 1.x line is in
  maintenance mode upstream and has no extension support.
- The official SDK's 2.x line (2.0.0 released 2026-07-28, latest 2.3.0) speaks
  `2026-07-28` and serves earlier revisions from the same server. `FastMCP` is
  renamed `MCPServer`, which takes `extensions=[...]`.
- python-sdk PR
  [#3485](https://github.com/modelcontextprotocol/python-sdk/pull/3485), merged
  to `main` on 2026-10-11 and not yet released, adds `mcp.server.skills.Skills`.
  It answers `skills/list`, `skills/get` and, when a handler is given,
  `resources/directory/read`; it advertises the declaration, validates entries
  and fills or strips `ttlMs`/`cacheScope` by protocol version. It does not
  read a filesystem: the server supplies the handlers and registers each file
  as an ordinary resource. A filesystem provider is an open request
  ([#3683](https://github.com/modelcontextprotocol/python-sdk/issues/3683)). An
  open bug reports that its URI checks miss percent-encoded dot segments
  ([#3685](https://github.com/modelcontextprotocol/python-sdk/issues/3685)).

- The extension's server side is two new files, `src/mcp/server/skills.py`
  and `src/mcp/shared/skills.py`. The rest of #3485 is client-side. Copied
  verbatim from the merge commit `9a754b0aabb873d9707d52dc165b569fb442ec8c`,
  with only their import paths changed, they pass the commit's own 75 server
  and shared Skills tests against the released `mcp` 2.3.0 on Python 3.13.
  The python-sdk is MIT-licensed.

Apart from that test run, these are source observations. Nothing has been run
against a client.

## Decision

### 1. The official SDK owns the wire; the SDK owns the directory

The SDK does not implement `skills/list`, `skills/get`, the declaration or the
cache fields itself. It uses the official `Skills` extension, so the wire
format is the official SDK's, and no Main Sequence variant exists. Until an
`mcp` release contains the extension, that is a verbatim copy of the official
code ([Release order](#7-release-order)).

The SDK adds what the official extension leaves to the server author: reading
a skills directory, validating it, computing manifests and registering every
file as a resource. This is the provider #3683 asks for, kept small. If the
official SDK ships a filesystem provider that meets this record, the SDK
switches to it and keeps its own interface.

### 2. Packaging and interface

Skills live in the application's repository as an Agent Skills directory:

```text
skills/
  refund-review/
    SKILL.md
    references/policy.md
  daily-close/
    SKILL.md
```

The application enables them on its own MCP server before building the ASGI
app:

```python
from pathlib import Path

from mcp.server.mcpserver import MCPServer
from mainsequence.server.fastapi import install_mcp, install_request_identity
from mainsequence.server.mcp_skills import add_skills

mcp = MCPServer("refunds")
add_skills(mcp, Path(__file__).parent / "skills")

install_request_identity(app)
install_mcp(
    app,
    mcp.streamable_http_app(stateless_http=True, json_response=True),
    lifespan=mcp.session_manager.run,
)
```

- `mainsequence.server.mcp_skills.add_skills(server, directory)` takes the
  application's `MCPServer` and an absolute directory path. A relative path is
  refused: under the launcher the working directory is not the repository's.
  There is no default location and no discovery; `skills/` is the documented
  convention, not an assumption.
- It registers the official `Skills` extension and one resource per file on
  that server, and returns nothing.
- `install_mcp` keeps its signature. It receives an ASGI app and cannot see the
  server's resources, so skills attach to the server, not to the installer.
  Without `add_skills`, nothing changes.
- The module imports `mcp` and is imported only by applications that use it.
  `mainsequence` and `mainsequence.server.fastapi` still import without the
  `mcp` extra.

### 3. Catalog, URIs and bytes

- Each direct child directory of `directory` is one skill. Its URI is
  `skill://<name>/SKILL.md`, with no organizational prefix: a skill URI is
  already scoped to the server that serves it.
- Every regular file under the skill directory is part of the skill and appears
  once in its manifest as `skill://<name>/<relative path>`. Names beginning
  with `.` (files and directories) are skipped: not served and not listed.
- Every path segment must use only `A-Z a-z 0-9 . _ ~ -`. Such URIs need no
  percent-encoding, so a served URI and its manifest entry are the same string,
  and the check in #3685 has nothing to miss.
- A `SKILL.md` below a skill's root is refused. The extension allows nested
  skills, but publishing them is a separate decision; refusing keeps every
  served `SKILL.md` a published skill.
- Files are read once, in binary, when `add_skills` runs. Digests and sizes are
  computed from those bytes, and the same in-memory bytes are served. A change
  on disk after startup has no effect until the next start. No newline or
  encoding normalization is applied.
- A file whose bytes decode as strict UTF-8 is served as a text resource; any
  other file as a blob. Either way the client recovers the identical bytes.
  `SKILL.md` is `text/markdown`; other files take their type from the file
  extension, otherwise `text/plain` for text and `application/octet-stream`
  for blobs. The `SKILL.md` resource's `name` and `description` come from the
  frontmatter, as the specification recommends.
- Frontmatter is the YAML block between the opening `---` line and the next
  `---` line, loaded with the SDK's existing `pyyaml` dependency. It must be a
  mapping with string keys whose values are JSON types (strings, numbers,
  booleans, null, lists and mappings). The entry carries it unchanged.

### 4. Startup validation

`add_skills` raises a `ValueError` naming the skill, the file and the broken
rule, and registers nothing, when:

- the directory is missing, relative, unreadable or holds no skill;
- a skill directory has no `SKILL.md`, or its frontmatter is missing,
  unparsable, not a mapping, or holds a value that is not a JSON type (for
  example an unquoted YAML date);
- `name` breaks the Agent Skills rules (1 to 64 characters of `a-z`, `0-9` and
  `-`, no leading, trailing or consecutive hyphen) or differs from the
  directory name;
- `description` is not 1 to 1024 characters, `compatibility` is present and
  not 1 to 500 characters, or `metadata` is present and not a mapping from
  strings to strings;
- a skill has more than 512 files or more than 16,777,216 bytes in total
  (`SKILL.md` included). The extension says a server should not serve such a
  skill; the SDK refuses it;
- a path segment is outside the allowed characters, a nested `SKILL.md`
  exists, or a symbolic link is found (it could leave the skill directory);
- the server already has the Skills extension, or a resource or resource
  template under `skill://`. The `skill://` scheme on that server belongs to
  `add_skills`.

`add_skills` runs while the application module is imported, so a violation
fails startup before readiness, as an invalid `install_mcp` does. An
application that registers its own `skill://` resource after `add_skills` is
unsupported; the guide says so.

### 5. Protocol revision and the `mcp` extra

The SDK invents no declaration for `2025-11-25`, such as an entry under
`experimental`. Version negotiation and the declaration are the official
SDK's: on a `2026-07-28` connection the extension appears in `server/discover`;
on an earlier connection the official SDK decides what it advertises and
strips `ttlMs` and `cacheScope`. The SDK records the tested behavior for both
eras and claims nothing beyond it.

This amends ADR 0038:

- The compatibility target becomes protocol `2026-07-28`, with `2025-11-25`
  still served by the official SDK. Stateless requests and JSON responses stay
  the profile. `subscriptions/listen` and other long-lived streams stay outside
  it, as subscriptions were.
- The `mcp` extra moves to the 2.x line, `mcp>=2.3,<3` (see
  [Release order](#7-release-order)). 1.x receives only security fixes
  upstream and cannot serve the extension.
- `install_mcp` is not tied to a library and still accepts any compliant ASGI
  app. An application that must stay on 1.x installs `mainsequence` without the
  extra and pins `mcp<2` itself; it cannot use `add_skills`, which fails with a
  message naming the required range.
- The guide, the example and the API-surfaces scaffold skill move from
  `FastMCP` to `MCPServer`. The CHANGELOG lists the extra's new range as a
  breaking change.
- ADR 0038's tests are rerun on the new line: exact `/mcp` routing,
  `server/discover` and tool calls on `2026-07-28`, `initialize` on
  `2025-11-25`, the `MCP-Protocol-Version` header, 405 and Origin handling,
  lifespan composition, and identity propagation to asynchronous and threaded
  tools.

### 6. Directory reads, caching and access

- `directoryRead` is not declared. Every manifest here is complete and static,
  and the specification says a host may answer a directory question from the
  entry alone. Declaring it would commit the server to every directory,
  including empty and hidden ones. It can be added later without a wire break.
- `skills/list` returns every skill on one page; pagination is not needed for
  a repository catalog. `ttlMs` is 300000 (five minutes): content changes only
  with a new revision, and digests catch a stale entry regardless.
  `cacheScope` is `"private"`, the official default. The catalog is the same
  for every caller, but it belongs to a release that admits only authenticated
  callers, and a shared cache must not serve it across authorization contexts.
- Skills are served only through `/mcp`, behind the same request identity as
  tools (ADR 0038, section 3). Every admitted caller sees every skill. Skills
  are instructions, not authority: the tools a skill names enforce their own
  permissions. Per-caller filtering is out of scope; adding it later would
  have to keep `skills/get` and `resources/read` consistent with the listing.
- Loading, approval, digest verification and the treatment of `allowed-tools`
  are the client's job under the specification. The SDK passes `allowed-tools`
  through like any other field and grants nothing.

### 7. Release order

The capability ships now, installable from PyPI with the `mcp` extra
(`mcp>=2.3,<3`), without git pins. No `mcp` release contains #3485 yet;
v2.3.0 (2026-10-02) predates it, and upstream has announced no date.

- The SDK carries the extension's two server-side files from the merge commit
  `9a754b0aabb873d9707d52dc165b569fb442ec8c` in a private module,
  `mainsequence/server/_mcp_skills_upstream/`. They are copied verbatim:
  only import paths change, and the upstream MIT notice and the commit are
  recorded in the module. Nothing in them is edited, fixed or extended; an
  upstream fix is taken by copying again from a later commit.
- `add_skills` uses `mcp.server.skills` when it imports, and the copy
  otherwise. Once an installed `mcp` has the extension, the copy is never used.
- The SDK's tests run the copied upstream Skills tests against `mcp` 2.3.0, and
  the SDK's own `add_skills` tests against both 2.3.0 with the copy and the
  merge commit's `mcp`.
- The copy carries the open bug #3685. It does not affect this server: the
  missed check applies to percent-encoded URIs, and decision 3 never produces
  one.
- When `mcp` publishes a release containing #3485, the extra's floor moves to
  it, the copy and its tests are deleted, and the CHANGELOG names the version.
  If that release changes the extension's interface, decisions 1 and 5 are
  revisited, not worked around.

## Scope

| Owner | Work |
| --- | --- |
| Main Sequence SDK | `mainsequence/server/mcp_skills.py`, the temporary upstream copy, the `mcp` extra's range, ADR 0038 retesting on 2.x, tests, the FastAPI guide, the API-surfaces scaffold skill, CHANGELOG. |
| Official MCP Python SDK | The wire format: declaration, `skills/list`, `skills/get`, `resources/read`, version negotiation and cache fields. |
| Application author | The skills directory and its content, and the tools' own authorization. |
| MCP client | Discovery, approval, digest and frontmatter verification, caching and origin tagging. |

The platform's release, URL and sign-in (`ResourceRelease.mcp_connection`) do
not change. ADR 0038's installation declaration does not change: skills are
content of the application's server, not wiring the launcher checks.

## Acceptance criteria before release

1. With `add_skills`, the server declares `resources` and
   `io.modelcontextprotocol/skills` as the specification defines, without
   `directoryRead`. The tests read the declaration from `server/discover` on
   `2026-07-28` and record what `initialize` returns on `2025-11-25`.
2. `skills/list` returns, for every skill, its URI, frontmatter equal to the
   parsed `SKILL.md`, and a manifest of every served file, with `resultType`,
   `ttlMs` and `cacheScope` on `2026-07-28`.
3. `skills/get` answers for every served skill and returns `-32602` for an
   unknown or malformed URI.
4. `resources/read` returns, for every manifest entry, bytes whose SHA-256 and
   length the tests compute independently, for text and binary files,
   including CRLF line endings, a UTF-8 BOM and a non-UTF-8 file.
5. Each rule in decision 4 has a test that fails `add_skills` and registers
   nothing.
6. Without `add_skills`, `install_mcp` behaves as before, and ADR 0038's tests
   pass on the new `mcp` range. A plain `mainsequence` import needs no `mcp`.
7. All skills traffic is admitted only with request identity: 401 without an
   assertion, as for tools.
8. The MCP Inspector, or another client that supports the extension, lists and
   reads a skill from a deployed release end to end. Record the client,
   library, protocol and launcher versions.
9. The FastAPI guide and the API-surfaces scaffold skill document publishing
   skills.

## Consequences

Applications can ship skills with the service that owns them, in a form any
extension-aware client understands. The SDK keeps no copy of the wire format,
so conformance follows the official SDK's fixes.

The cost is a move to the official SDK's 2.x line, which changes the
application-facing class name and the tested protocol for every MCP
application, and a temporary verbatim copy of upstream code that must be
deleted when `mcp` releases the extension. Skill
content is fixed per process: updating a skill means a new revision. A client
without the extension sees the skill files as ordinary resources, which the
specification intends.

Change classification: a new ADR for a feature, amending ADR 0038's
compatibility target and dependency range. On acceptance, ADR 0038 gets an
amendment note pointing here.
