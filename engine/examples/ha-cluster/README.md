# 3-Node HA Cluster with nginx Load Balancer

`test_ha.sh` is the authoritative proof surface for this example topology.
It verifies nginx rerouting, replication visibility across nodes, startup catch-up,
and analytics fan-out for this compose stack.

Scope:

- Proves nginx-routed availability for a single-node outage in this compose deployment.
- Proves peer oplog replication and restarted-node pre-serve catch-up.
- Proves analytics query-time fan-out/merge with `cluster` metadata.
- Does not prove leader election, automatic promotion, or load-balancer HA.

## Architecture

```
Client
  │
  ▼
nginx (port 7800)
  ├── node-a (mesh peer)
  ├── node-b (mesh peer)
  └── node-c (mesh peer)
       │
       ▼
  Replication mesh (each node → both peers)
```

If a node fails in this topology:
- nginx detects the failure via `proxy_next_upstream` and routes around it
- The remaining nodes continue accepting writes and serving reads
- When the failed node restarts, it runs pre-serve catch-up (`run_pre_serve_catchup`) to fetch missed ops from peers before accepting traffic

Note: nginx itself is a single point of failure in this example. For production, run nginx (or another load balancer) with its own redundancy.

## Quick start

```bash
docker compose up -d --build

# Wait for all nodes to be healthy (~30s on first build), then run tests:
./test_ha.sh

docker compose down -v
```

## Ports

| Service       | Port |
|---------------|------|
| nginx (LB)    | 7800 |
| nginx status  | 7801 |

The individual Flapjack nodes are not exposed externally — all traffic goes through nginx.

## Testing

```bash
# Automated integration test (nginx reroute + replication + catch-up + fan-out):
./test_ha.sh

# Or build + test + teardown in one shot:
./test_ha.sh --with-docker

# Manual verification via LB (host-exposed on 7800):
curl http://localhost:7800/health
curl http://localhost:7800/internal/cluster/status | jq .

# Direct node check (nodes are only reachable inside the compose network):
docker compose exec -T node-c curl -sf http://localhost:7700/health
```

## Simulating node failure

```bash
# Take down node-b (nginx will route around it within 1-2 requests)
docker compose stop node-b

# Cluster still serves from node-a and node-c:
curl http://localhost:7800/health

# Restart node-b (it catches up missed ops via pre-serve catch-up before serving)
docker compose start node-b

# Verify node-b is healthy inside the compose network:
docker compose exec -T node-b curl -sf http://localhost:7700/health
```

## Configuration

> For authoritative env var defaults, see [OPS_CONFIGURATION.md](../../docs2/3_IMPLEMENTATION/OPS_CONFIGURATION.md). Values below are example-specific.

Peers are configured via the `FLAPJACK_PEERS` environment variable:

```yaml
FLAPJACK_NODE_ID: node-a
FLAPJACK_PEERS: "node-b=http://node-b:7700,node-c=http://node-c:7700"
```

For bare-metal / EC2 deployments, write `$FLAPJACK_DATA_DIR/node.json` instead:

```json
{
  "node_id": "node-a",
  "bind_addr": "0.0.0.0:7700",
  "peers": [
    {"node_id": "node-b", "addr": "http://10.0.1.2:7700"},
    {"node_id": "node-c", "addr": "http://10.0.1.3:7700"}
  ]
}
```

The bare-metal `node.json` path is configuration guidance only; Stage 2 verification
was executed on the compose topology in this directory.

## Production notes

- Run behind a proper TLS terminator (AWS ALB, Cloudflare, or nginx with Let's Encrypt)
- Flapjack internal endpoints (`/internal/*`) are trusted — restrict via VPC security groups
- Health check URL: `/health` — returns 200 when the node is ready to serve traffic
- Pre-serve catch-up timeout is configurable via `FLAPJACK_STARTUP_CATCHUP_TIMEOUT_SECS` (see [OPS_CONFIGURATION.md](../../docs2/3_IMPLEMENTATION/OPS_CONFIGURATION.md) for default)
