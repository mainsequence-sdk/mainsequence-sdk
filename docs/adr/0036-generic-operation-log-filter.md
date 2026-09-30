# ADR 0036: Generic operation correlation in owner logs

Date: 2026-09-29

Status: Accepted and implemented; live collector verification remains separate.

## Decision

Existing owner `get_logs()` methods accept an optional `operation_uid` exact
filter and forward it through the existing log transport. The platform applies
this filter before pagination and binds it to continuation cursors. Omission
preserves existing behavior. Structured application extensions stay under `data`.
An operation UID is application correlation, not an authorization credential.

The SDK introduces no application-specific endpoint, storage, local file layout,
identity lookup, branch requirement or authentication mode. Application libraries
own their run identities, local handlers and log UI. Existing owner authorization,
time bounds and provider retention remain unchanged.

## Verification

The observability transport test covers forwarding `operation_uid` together with
time bounds and level, and preserving opaque structured enrichment in the result.
