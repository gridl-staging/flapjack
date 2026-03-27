# S3 Snapshot Verification (MinIO)

Single-node S3 snapshot round-trip verification using MinIO as the S3-compatible
object store.

## What this proves

- `POST /1/indexes/:indexName/snapshot` uploads a snapshot to S3
- `GET /1/indexes/:indexName/snapshots` lists available snapshots
- `POST /1/indexes/:indexName/restore` restores from the latest snapshot
- `POST /1/indexes/:indexName/restore` with `{"key":"..."}` restores a specific snapshot
- Scheduled backups run automatically at `FLAPJACK_SNAPSHOT_INTERVAL` seconds
- Auto-restore from S3 fires on startup when the data directory is empty

## What this does NOT prove

- Failover or multi-node replication
- Oplog replay or point-in-time recovery
- DR scenarios (see `_dev/s/manual-tests/test_s3_backup_restore.sh` for those)
- Production S3 (AWS) compatibility (this uses MinIO only)

## Prerequisites

- Docker and Docker Compose v2
- Python 3 (for JSON parsing in test script)
- `curl`

## Environment variables

> Values below are example-specific (set in docker-compose.yml for MinIO testing). For server defaults, see [OPS_CONFIGURATION.md](../../docs2/3_IMPLEMENTATION/OPS_CONFIGURATION.md).

| Variable | Example Value | Description |
|----------|---------|-------------|
| `FLAPJACK_S3_BUCKET` | `flapjack-snapshots` | S3 bucket name |
| `FLAPJACK_S3_REGION` | `us-east-1` | S3 region |
| `FLAPJACK_S3_ENDPOINT` | `http://minio:9000` | Custom S3 endpoint (MinIO) |
| `FLAPJACK_SNAPSHOT_INTERVAL` | `30` | Seconds between scheduled backups |
| `FLAPJACK_SNAPSHOT_RETENTION` | `10` | Max snapshots to keep per index |
| `AWS_ACCESS_KEY_ID` | `minioadmin` | MinIO access key |
| `AWS_SECRET_ACCESS_KEY` | `minioadmin` | MinIO secret key |

## Usage

```bash
# Start MinIO + Flapjack
docker compose up -d --build

# Run the verification suite
./test_snapshots.sh

# Clean up
docker compose down -v
```

The test takes approximately 2 minutes (includes a ~45s wait for scheduled backup
verification).

## Limitations

The Flapjack Docker image builds the full Rust workspace including vector-search
features, which requires significant CPU and memory. On resource-constrained
Docker hosts (e.g. Docker Desktop with default 2 CPU / 2 GiB limits), the build
may time out or the daemon may become unresponsive.

If the Docker build fails or MinIO cannot start on your platform, the test cannot
run. This compose path has been verified on a working Docker host, but a blocked
local run is still not a passing test for your environment.
