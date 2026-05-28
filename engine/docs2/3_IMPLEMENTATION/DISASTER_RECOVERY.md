# Disaster Recovery (Measured Contract)

This page is the canonical measured contract for Flapjack file-based snapshot
backup/restore behavior proved by:

- `engine/tests/test_snapshot_restore_under_load.rs`
- `engine/target/dr_proof/latest/measurements.txt`

## What this covers

This contract proves exactly one DR path:

- file snapshot export/import over the real HTTP API surface:
  - `GET /1/indexes/{indexName}/export`
  - `POST /1/indexes/{indexName}/import`
- snapshot capture while deterministic batch writes are actively in flight
- restore of captured bytes into a fresh data directory on a second server
- exact restored-count parity (`DOC_COUNT_AT_SNAPSHOT=550`,
  `DOC_COUNT_AT_RESTORE=550`)
- representative record parity (`GET /1/indexes/{indexName}/{objectID}` checks)
- representative search parity (`POST /1/indexes/{indexName}/query` checks)
- restart durability after import (restored data remains queryable after restart)

This page does not claim proof for:

- S3 snapshot routes
- cross-region DR
- oplog replay
- any stronger DR guarantee than this measured file-based snapshot test

## Recovery time objective

Measured restore time for this proved file-snapshot path:

- `RTO_MEASURED_MS=221`

## Recovery point objective

Measured data-loss window for this proved file-snapshot path:

- `RPO_MEASURED_MS=10`
