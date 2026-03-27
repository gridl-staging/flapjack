# 2-node replication + analytics fan-out example

`test_replication.sh` is the single executable proof surface for this example.
`demo.sh` and this README mirror that verified behavior through public routes on
`http://localhost:7700` and `http://localhost:7701`.

Scope:
- Proves `/health`, `/1/...` replication routes, and `/2/...` analytics fan-out routes.
- Does not prove failover, promotion, or leadership semantics.

## Quick start

```bash
# Build and start both nodes (localhost:7700 and localhost:7701)
docker compose up -d --build

# Run the authoritative assertions
./test_replication.sh

# Run a non-asserting walkthrough of the same routes
./demo.sh

# Tear down
docker compose down -v
```

## Public routes proven by test_replication.sh

1. **Readiness**

   ```bash
   curl "http://localhost:7700/health"
   curl "http://localhost:7701/health"
   ```

2. **Replication via public `/1/...` routes in both directions**

   ```bash
   # node-a write, node-b read
   curl -X POST "http://localhost:7700/1/indexes/<index>/batch" ...
   curl -X POST "http://localhost:7701/1/indexes/<index>/query" \
     -H "Content-Type: application/json" \
     -d '{"query":"pancakes"}'

   # node-b write, node-a read
   curl -X POST "http://localhost:7701/1/indexes/<index2>/batch" ...
   curl -X POST "http://localhost:7700/1/indexes/<index2>/query" \
     -H "Content-Type: application/json" \
     -d '{"query":"cardamom"}'
   ```

3. **Analytics fan-out via public `/2/...` routes**

   Local-only counts per node:

   ```bash
   curl -H "X-Flapjack-Local-Only: true" \
     "http://localhost:7700/2/searches/count?index=<index>"
   curl -H "X-Flapjack-Local-Only: true" \
     "http://localhost:7701/2/searches/count?index=<index>"
   ```

   Merged counts from either node:

   ```bash
   curl "http://localhost:7700/2/searches/count?index=<index>"
   curl "http://localhost:7701/2/searches/count?index=<index>"
   ```

   Expected shape:

   ```json
   {
     "count": 1234,
     "cluster": {
       "nodes_total": 2,
       "nodes_responding": 2,
       "partial": false,
       "node_details": [
         {"node_id": "node-a", "status": "Ok", "latency_ms": 1},
         {"node_id": "node-b", "status": "Ok", "latency_ms": 1}
       ]
     }
   }
   ```

4. **Optional `cluster.partial` demonstration**

   If `node-b` is stopped, querying analytics from `node-a` returns partial
   cluster metadata. This demonstrates analytics fan-out degradation behavior,
   not failover semantics.

   ```json
   {
     "count": 617,
     "cluster": {"nodes_total": 2, "nodes_responding": 1, "partial": true}
   }
   ```

## Configuration

> For authoritative env var defaults, see [OPS_CONFIGURATION.md](../../docs2/3_IMPLEMENTATION/OPS_CONFIGURATION.md). Values below are example-specific.

Compose wires a 2-node topology with explicit peer addresses:

```yaml
environment:
  FLAPJACK_NODE_ID: node-a
  FLAPJACK_PEERS: "node-b=http://node-b:7700"
```

For bare-metal / VM deployments, write `$FLAPJACK_DATA_DIR/node.json` instead:

```json
{
  "node_id": "node-a",
  "bind_addr": "0.0.0.0:7700",
  "peers": [
    {"node_id": "node-b", "addr": "http://10.0.1.2:7700"}
  ]
}
```

The bare-metal `node.json` path is illustrative configuration guidance.
Stage 3 verification in this repo is based on the compose topology and
`test_replication.sh`.

## Script roles

- `test_replication.sh`: authoritative assertions and pass/fail output.
- `demo.sh`: readable walkthrough of the same public routes and output shapes.

Run `test_replication.sh` first whenever behavior claims in this example change.
