# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [1.0.0] - 2026-03-28

### Added

- Full-text search with typo tolerance.
- Faceting and filtered search support.
- Geo search capabilities.
- Vector search support.
- Click analytics collection and query support.
- Algolia API-compatible HTTP endpoints.
- Dashboard UI for operations and search workflows.
- Replication support for peer-to-peer index synchronization.
- TLS and ACME support for secure deployments.
- Docker-based and bare-metal installation paths.

### Changed

- API behavior and payloads were aligned with Algolia-compatible client expectations across key search and index routes.
- Deployment and operations guidance were expanded to support consistent setup across local, container, and hosted environments.

### Fixed

- Stabilized core indexing and query execution paths for production usage.
- Hardened transport and replication flows to reduce operational failure modes during distributed operation.
