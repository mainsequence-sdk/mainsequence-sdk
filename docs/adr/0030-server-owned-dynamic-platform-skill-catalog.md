# ADR 0030: Server-Owned Dynamic Platform Skill Catalog

Amended 2026-10-01 for SDK issue #181: catalog validation remains current, but
SDK and platform skills no longer share an installation namespace.

Amended 2026-10-11 for [SDK issue #217](https://github.com/mainsequence-sdk/mainsequence-sdk/issues/217):
every MCP request of the discovery has the SDK's default request budget; before,
the CLI sent them with no timeout. See [Discovery](#discovery).

Amended 2026-10-11: an explicit SDK update now also refreshes SDK-owned skills
from the newly installed version, as described under Installation Ownership.

Amended 2026-10-14: SDK instructions are distributed only through SDK-owned
skills. Repository-owned `AGENTS.md` is outside the SDK's installation ownership.

## Status

Accepted

## Context

`mainsequence code-repository update-agent-skills` assembles SDK-owned execution skills
and platform-owned skills retrieved from the authenticated MCP gateway. The
initial SDK implementation also hardcoded the complete platform resource URI
set, its order, each skill name and path, and the expected number of skills.

That duplicates platform catalog ownership in the SDK. An additive backend
skill then changes `resources/list` and causes every older strict client to
reject the complete catalog, even though the backend manifest, ontology, and
resource contents are internally valid.

The backend already publishes the catalog information needed by a generic
client:

- the fixed `mainsequence://platform/ontology` resource;
- `ontology.skill_resources`, containing every project skill name and URI;
- list and read metadata containing the owner application, manifest version,
  manifest SHA-256, resource path, content SHA-256, MIME type, and byte size;
- skill resources under `mainsequence://platform/skills/`.

The SDK must keep filesystem and content validation strict without deciding
which concrete platform skills are allowed to exist.

## Decision

### Installation Ownership

`update-agent-skills` is a local mirror of the installed SDK bundle and owns
all of `.agents/skills/mainsequence/`. It deletes anything absent from that
bundle, requires no authenticated platform catalog, and never retains platform
content there. Its sentinel records the installed library version. Source
checkout protections, staging, and rollback remain required.

The SDK's execution, contract-authority, routing, and maintenance instructions
ship inside those skills and share their version and update lifecycle. The
SDK does not ship an `AGENTS.md` template or create, copy, edit, or remove a
repository-owned `AGENTS.md`. The former managed-block writer and its command
are removed. Existing files, including previously scaffolded blocks, remain
as written; any cleanup is a repository edit explicitly requested by its owner.
Skills respect repository instructions when present without requiring that file
or imposing a general repository layout.

`update-platform-skills` fetches the authenticated catalog and owns only
`.agents/skills/mainsequence_platform/`. SDK copying cannot be blocked by
platform authentication or availability. Existing mixed trees are replaced,
not grandfathered in. Platform skills can then be explicitly installed in
their separate namespace; their content and membership remain backend-owned.

An explicit `update-sdk` operation upgrades the dependency and then runs the
newly installed SDK's `update-agent-skills` in a fresh process against the same
repository. It succeeds only when the SDK-owned skill namespace and sentinel
have been refreshed. If copying fails, it exits with an error explaining that
the package remains upgraded and how to retry the refresh. `--dry-run` previews
both phases without executing them. Platform skill refresh remains a separately
requested operation. Repository-owned `AGENTS.md` is untouched. Direct package
manager upgrades do not trigger the SDK's skill refresh.

The backend owns platform skill membership and content. The SDK uses
`ontology.skill_resources` as the authoritative project-skill index.

The SDK keeps only protocol-level constants:

- `mainsequence://platform/ontology`;
- `mainsequence://platform/skills/`;
- supported catalog schema versions; and
- generic naming, URI, path, MIME, front-matter, hashing, and ownership rules.

The SDK does not contain a tuple of concrete platform skill URIs, a per-skill
contract map, or a fixed skill count.

### Discovery

The SDK:

1. initializes the MCP connection with its installed package version;
2. follows `resources/list` pagination and indexes unique resources by URI;
3. ignores resources outside the exact ontology URI and platform-skill URI
   prefix used by project skill assembly;
4. requires and reads the platform ontology first;
5. validates `ontology.skill_resources` and treats it as the desired skill set;
6. requires the listed platform skill set to match the ontology declaration;
7. reads only the ontology-declared platform skills; and
8. validates the complete selected catalog before installing anything.

Each MCP request, with a token renewal and resend after `401`, has the SDK's
default request budget: 5 seconds to connect, 120 seconds to read, 125 seconds
in total. A request that does not finish in time fails the discovery with an
error before anything is installed.

An unrelated MCP resource does not participate in project skill assembly. A
resource under `mainsequence://platform/skills/` that is not declared by the
ontology is an invalid platform manifest and is rejected.

### Generic Validation

Every selected resource must:

- be owned by `mcp_gateway`;
- identify the same supported manifest schema version and manifest SHA-256;
- return matching list/read URI, MIME type, content hash, and content size;
- contain UTF-8 text whose actual hash and byte size match the metadata; and
- use a canonical safe bundle-relative path.

The ontology must be a JSON object with a `skill_resources` array. Skill names
and URIs must be unique and use safe canonical forms.

Every skill must:

- use the `mainsequence://platform/skills/<kebab-name>` URI namespace;
- use the corresponding safe snake-case resource name;
- use `text/markdown`;
- map below `skills/` through one or more safe snake-case directories and end
  with `SKILL.md` or `SKILL.markdown`;
- contain YAML front matter whose name matches the URI identity; and
- resolve to a unique project destination.

### Ordering And Installation

MCP list order and ontology array order have no semantic meaning. After full
validation, the SDK sorts skills by name and URI. That order drives the catalog,
installation result, and sentinel rendering.

Staging, atomic replacement, and rollback remain mandatory for each owned
namespace. Validation failure leaves that namespace and its sentinel unchanged.
Equal relative names in separate owner namespaces are not collisions.

### Versioning

`manifest_version` is the catalog schema version. Adding, removing, or changing
skill content without changing the generic wire schema does not require a new
SDK release. `manifest_sha256` identifies the concrete backend catalog revision.

A breaking metadata or resource-shape change requires a new manifest version.
The SDK rejects unsupported versions with an explicit compatibility error.

## Consequences

- Adding a valid platform skill requires backend manifest, ontology, and skill
  content changes only.
- Backend catalog order changes do not affect SDK behavior.
- The backend may organize platform skills into safe hierarchical directories
  without requiring an SDK release.
- The SDK remains closed against path traversal, destination collisions,
  inconsistent manifests, malformed front matter, and content drift.
- Future non-platform MCP resources do not break project skill updates.
- Platform membership has one owner instead of being duplicated across
  repositories.
- Compatibility tests must prove that arbitrary additive platform skills are
  accepted without changing SDK production constants.
