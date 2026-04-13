# Changelog

All notable changes to this project should be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows versioned releases.

## [Unreleased]

### Changed

- Added first-class storage `namespace` support for DataNode and SimpleTable storage models while keeping `hash_namespace` inside build configuration for identity construction.
- Added storage registration support to send top-level `namespace` metadata to the backend.
- Added storage and updater namespace filter support in the SDK:
  `namespace`, `namespace__contains`, `namespace__in`, `namespace__isnull`,
  `related_table__namespace__contains`, `related_table__namespace__in`, and
  `related_table__namespace__isnull`.
- Added CLI support for `namespace=...` on `mainsequence simple_table list` and `mainsequence data-node list`.
- Updated CLI output and documentation to surface storage namespace information and examples.

## [3.17.6] - 2026-03-27

### Changed

- Documentation and repository maintenance updates for the `3.17.6` release line.
