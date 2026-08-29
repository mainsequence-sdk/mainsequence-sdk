# ADR 0030: Server-Owned Dynamic Platform Skill Catalog

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

The existing ownership collision check, staging directory, atomic managed-tree
replacement, and rollback behavior remain mandatory. Validation failure must
leave the previous managed tree and sentinel unchanged.

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
