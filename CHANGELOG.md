# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.0.2] - 2026-05-28

### Changed

- Stage 6 sustained-load revalidation documented two overload scenarios at `921.538969/s` and `831.652577/s`; each preserved contract health (`write_http_5xx_rate=0.00%`, `write_http_unexpected_4xx_rate=0.00%`) while saturation remained visible (`85.04%` and `98.21%` write-failure rates).
- Rolling-restart HA behavior improved to a steady-state `0.88%` per-node spread while maintaining availability, narrowing the prior convergence boundary.
- The release narrows HA and sustained-write boundaries, but restart-window write loss remains a known topology limit and sustained-write saturation remains open under overload.

## [1.0.1] - 2026-05-23

### Changed

- Release publishing now builds and publishes Linux amd64 and arm64 Docker candidate images on separate per-architecture paths before stable tag promotion.
- Stable `ghcr.io/griddlehq/flapjack:<version>` and `:latest` Docker tags are now promoted only from a candidate manifest that passed required architecture checks.

## [1.0.0] - 2026-03-28

### Added

- Full-text search with typo tolerance.
- Faceting and filtered search support.
- Geo search capabilities.
- Vector search support.
- Multi-index federated search support.
- Click analytics collection and query support.
- Query suggestions generation support.
- Synonyms and query rules support.
- Personalization API and profile-aware search support.
- Recommendations API support.
- A/B testing (experiments) support.
- AI search and chat-style RAG endpoint support.
- API keys with ACLs, restrict-sources enforcement, and per-key rate limiting.
- Per-tenant dictionaries (stop words, plurals, compounds).
- S3 snapshot backup and restore support.
- Algolia API-compatible HTTP endpoints.
- OpenAPI specification export for API contract verification.
- Feature-gated OpenTelemetry tracing export support.
- Dashboard UI for operations and search workflows.
- Replication support for peer-to-peer index synchronization.
- TLS and ACME support for secure deployments.
- Docker deployment plus install-script and systemd bare-metal paths.

### Changed

- API behavior and payloads were aligned with Algolia-compatible client expectations across key search and index routes.
- Deployment and operations guidance were expanded to support consistent setup across local, container, and hosted environments.

### Fixed

- Stabilized core indexing and query execution paths for production usage.
- Hardened transport and replication flows to reduce operational failure modes during distributed operation.

[Unreleased]: https://github.com/griddlehq/flapjack/commits/main
[1.0.2]: https://github.com/flapjackhq/flapjack/releases/tag/v1.0.2
[1.0.1]: https://github.com/griddlehq/flapjack/releases/tag/v1.0.1
[1.0.0]: https://github.com/griddlehq/flapjack/releases
