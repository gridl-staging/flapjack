#!/usr/bin/env bash
# Docker integration test: 2-node replication + analytics fan-out.
#
# This is the single authoritative proof surface for the replication example.
# It proves:
#   - Both nodes are healthy via /health
#   - node-a → node-b document replication (create + delete propagation)
#   - node-b → node-a replication on a separate index
#   - Analytics fan-out: merged count == local_a + local_b from both nodes
#   - Cluster metadata: nodes_total, nodes_responding, partial, node_details
#
# This example does NOT prove failover, promotion, or leadership semantics.
#
# Usage:
#   docker compose up -d --build
#   ./test_replication.sh
#   docker compose down -v
#
# Or run everything in one shot:
#   ./test_replication.sh --with-docker

set -euo pipefail

NODE_A="http://localhost:7700"
NODE_B="http://localhost:7701"
PASS=0
FAIL=0

# ── helpers ─────────────────────────────────────────────────────────────────

green() { printf "\033[32m✓\033[0m %s\n" "$*"; }
red()   { printf "\033[31m✗\033[0m %s\n" "$*"; }

assert_eq() {
  local label="$1" expected="$2" actual="$3"
  if [ "$actual" = "$expected" ]; then
    green "$label"
    PASS=$((PASS + 1))
  else
    red "$label (expected '$expected', got '$actual')"
    FAIL=$((FAIL + 1))
  fi
}

assert_ge() {
  local label="$1" expected="$2" actual="$3"
  if [ "$actual" -ge "$expected" ] 2>/dev/null; then
    green "$label"
    PASS=$((PASS + 1))
  else
    red "$label (expected >= $expected, got '$actual')"
    FAIL=$((FAIL + 1))
  fi
}

# Parse a JSON field by dot-separated path: json_val "$json" "cluster.nodes_total" "0"
json_val() {
  local json="$1" path="$2" default="${3:-}"
  echo "$json" | python3 -c "
import sys, json
d = json.load(sys.stdin)
for key in sys.argv[1].split('.'):
    if isinstance(d, dict):
        d = d.get(key, None)
    else:
        d = None
        break
print(d if d is not None else sys.argv[2])
" "$path" "$default" 2>/dev/null || echo "$default"
}

# Extract sorted comma-separated node_ids from cluster.node_details
json_node_ids() {
  local json="$1"
  echo "$json" | python3 -c "
import sys, json
d = json.load(sys.stdin)
details = d.get('cluster', {}).get('node_details', [])
print(','.join(sorted(n.get('node_id', '') for n in details)))
" 2>/dev/null || echo ""
}

# Wait for a Flapjack node to be healthy (up to 60s)
wait_healthy() {
  local url="$1/health"
  local name="$2"
  printf "  Waiting for %s..." "$name"
  for i in $(seq 1 30); do
    if curl -sf "$url" >/dev/null 2>&1; then
      echo " ready"
      return 0
    fi
    sleep 2
  done
  echo " TIMEOUT"
  return 1
}

# Poll until search returns at least N hits (up to max_wait seconds)
wait_for_hits() {
  local node="$1" index="$2" query="$3" min_hits="$4" max_wait="${5:-8}"
  local elapsed=0
  while [ "$elapsed" -lt "$max_wait" ]; do
    local hits response
    response=$(curl -sf -X POST "$node/1/indexes/$index/query" \
      -H 'Content-Type: application/json' \
      -d "{\"query\":\"$query\",\"hitsPerPage\":10}" 2>/dev/null || echo "{}")
    hits=$(json_val "$response" "nbHits" "0")
    if [ "$hits" -ge "$min_hits" ] 2>/dev/null; then
      echo "$hits"
      return 0
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  echo "0"
  return 0
}

# ── optional: docker lifecycle ───────────────────────────────────────────────

WITH_DOCKER=false
if [ "${1:-}" = "--with-docker" ]; then
  WITH_DOCKER=true
fi

if $WITH_DOCKER; then
  echo "=== Building and starting cluster ==="
  docker compose up -d --build
fi

# ── 1. Health checks ─────────────────────────────────────────────────────────

echo ""
echo "=== 1. Health checks ==="
wait_healthy "$NODE_A" "node-a"
wait_healthy "$NODE_B" "node-b"

HEALTH_A_RAW=$(curl -sf "$NODE_A/health" 2>/dev/null || echo "{}")
HEALTH_B_RAW=$(curl -sf "$NODE_B/health" 2>/dev/null || echo "{}")
HEALTH_A=$(json_val "$HEALTH_A_RAW" "status" "")
HEALTH_B=$(json_val "$HEALTH_B_RAW" "status" "")
assert_eq "node-a health=ok" "ok" "$HEALTH_A"
assert_eq "node-b health=ok" "ok" "$HEALTH_B"

# ── 2. Cluster topology sanity (internal route, not public API) ──────────────
# This section uses /internal/cluster/status for topology sanity only.
# It does NOT prove failover, promotion, or leadership semantics.

echo ""
echo "=== 2. Cluster topology sanity ==="
STATUS_A=$(curl -sf "$NODE_A/internal/cluster/status" 2>/dev/null || echo "{}")
REPL_A=$(json_val "$STATUS_A" "replication_enabled" "")
PEERS_A=$(json_val "$STATUS_A" "peers_total" "0")
assert_eq "node-a replication_enabled=true" "True" "$REPL_A"
assert_ge "node-a sees >= 1 peer" 1 "$PEERS_A"

# ── 3. Search replication: write on node-a, read on node-b ──────────────────

echo ""
echo "=== 3. Search replication (node-a → node-b) ==="

# Create index and index documents on node-a
INDEX="repl-test-$(date +%s)"
curl -sf -X POST "$NODE_A/1/indexes/$INDEX/batch" \
  -H 'Content-Type: application/json' \
  -d '{
    "requests": [
      {"action":"addObject","body":{"_id":"doc-1","title":"Saffron Pancakes","category":"breakfast"}},
      {"action":"addObject","body":{"_id":"doc-2","title":"Matcha Waffles","category":"breakfast"}},
      {"action":"addObject","body":{"_id":"doc-3","title":"Lavender Coffee","category":"drinks"}}
    ]
  }' >/dev/null
echo "  Indexed 3 docs on node-a"

# node-a should see all 3 immediately
HITS_LOCAL=$(wait_for_hits "$NODE_A" "$INDEX" "pancakes" 1 5)
assert_ge "node-a finds 'pancakes' locally" 1 "$HITS_LOCAL"

# node-b should see replicated docs within 8s
echo "  Waiting for replication to node-b (up to 8s)..."
HITS_REPLICATED=$(wait_for_hits "$NODE_B" "$INDEX" "pancakes" 1 8)
assert_ge "node-b finds 'pancakes' after replication" 1 "$HITS_REPLICATED"

# Verify total doc count on node-b
ALL_B=$(wait_for_hits "$NODE_B" "$INDEX" "" 3 5)
assert_ge "node-b has all 3 docs replicated" 3 "$ALL_B"

# ── 4. Replication: delete propagation ───────────────────────────────────────

echo ""
echo "=== 4. Delete propagation (node-a → node-b) ==="

# Delete doc-3 on node-a
curl -sf -X DELETE "$NODE_A/1/indexes/$INDEX/doc-3" >/dev/null
echo "  Deleted doc-3 on node-a"

# Wait for deletion to propagate
sleep 3
AFTER_DEL=$(wait_for_hits "$NODE_B" "$INDEX" "lavender" 0 6)
assert_eq "node-b: deleted doc gone after propagation" "0" "$AFTER_DEL"

# node-a and node-b should agree on count (2 docs)
COUNT_A=$(wait_for_hits "$NODE_A" "$INDEX" "" 2 5)
COUNT_B=$(wait_for_hits "$NODE_B" "$INDEX" "" 2 5)
assert_ge "node-a has 2 docs after delete" 2 "$COUNT_A"
assert_ge "node-b has 2 docs after delete" 2 "$COUNT_B"

# ── 5. Bidirectional replication: write on node-b, read on node-a ───────────

echo ""
echo "=== 5. Bidirectional replication (node-b → node-a) ==="

INDEX2="repl-bidir-$(date +%s)"
curl -sf -X POST "$NODE_B/1/indexes/$INDEX2/batch" \
  -H 'Content-Type: application/json' \
  -d '{
    "requests": [
      {"action":"addObject","body":{"_id":"b-1","title":"Cardamom Croissant","source":"node-b"}},
      {"action":"addObject","body":{"_id":"b-2","title":"Turmeric Toast","source":"node-b"}}
    ]
  }' >/dev/null
echo "  Indexed 2 docs on node-b"

# node-b sees immediately
HITS_B=$(wait_for_hits "$NODE_B" "$INDEX2" "cardamom" 1 5)
assert_ge "node-b finds 'cardamom' locally" 1 "$HITS_B"

# node-a should see replicated docs within 8s
echo "  Waiting for replication to node-a (up to 8s)..."
HITS_A_FROM_B=$(wait_for_hits "$NODE_A" "$INDEX2" "cardamom" 1 8)
assert_ge "node-a finds node-b's docs after replication" 1 "$HITS_A_FROM_B"

# ── 6. Analytics fan-out: complete proof ─────────────────────────────────────
# Proves merged count == local_a + local_b, cluster metadata from both nodes.
# Uses seed_analytics and flush_analytics to generate local data per node,
# then verifies fan-out via the public /2/searches/count route.

echo ""
echo "=== 6. Analytics fan-out ==="

# Use the replication test index for analytics — it already has docs on node-a
ANALYTICS_INDEX="$INDEX"

# Seed independent analytics on each node
curl -sf -X POST "$NODE_A/2/analytics/seed" \
  -H 'Content-Type: application/json' \
  -d "{\"index\":\"$ANALYTICS_INDEX\",\"days\":7}" >/dev/null
curl -sf -X POST "$NODE_B/2/analytics/seed" \
  -H 'Content-Type: application/json' \
  -d "{\"index\":\"$ANALYTICS_INDEX\",\"days\":7}" >/dev/null
echo "  Seeded analytics on both nodes"

# Flush buffered events to disk so queries can read them
curl -sf -X POST "$NODE_A/2/analytics/flush" >/dev/null
curl -sf -X POST "$NODE_B/2/analytics/flush" >/dev/null
sleep 1
echo "  Flushed analytics on both nodes"

# ── 6a. Get local-only counts from each node ────────────────────────────────
LOCAL_A_RAW=$(curl -sf -H "X-Flapjack-Local-Only: true" "$NODE_A/2/searches/count?index=$ANALYTICS_INDEX" 2>/dev/null || echo "{}")
LOCAL_B_RAW=$(curl -sf -H "X-Flapjack-Local-Only: true" "$NODE_B/2/searches/count?index=$ANALYTICS_INDEX" 2>/dev/null || echo "{}")
LOCAL_A_COUNT=$(json_val "$LOCAL_A_RAW" "count" "0")
LOCAL_B_COUNT=$(json_val "$LOCAL_B_RAW" "count" "0")
echo "  Local counts: node-a=$LOCAL_A_COUNT, node-b=$LOCAL_B_COUNT"

assert_ge "node-a local analytics count > 0" 1 "$LOCAL_A_COUNT"
assert_ge "node-b local analytics count > 0" 1 "$LOCAL_B_COUNT"

EXPECTED_MERGED=$((LOCAL_A_COUNT + LOCAL_B_COUNT))
echo "  Expected merged count: $EXPECTED_MERGED"

# ── 6b. Fan-out query from node-a ───────────────────────────────────────────
FANOUT_A=$(curl -sf "$NODE_A/2/searches/count?index=$ANALYTICS_INDEX" 2>/dev/null || echo "{}")
MERGED_A=$(json_val "$FANOUT_A" "count" "0")
NODES_TOTAL_A=$(json_val "$FANOUT_A" "cluster.nodes_total" "0")
NODES_RESP_A=$(json_val "$FANOUT_A" "cluster.nodes_responding" "0")
PARTIAL_A=$(json_val "$FANOUT_A" "cluster.partial" "True")
DETAILS_A=$(json_node_ids "$FANOUT_A")

echo "  node-a fan-out: count=$MERGED_A, nodes_total=$NODES_TOTAL_A, nodes_responding=$NODES_RESP_A, partial=$PARTIAL_A"

assert_eq "node-a fan-out: merged count == local_a + local_b" "$EXPECTED_MERGED" "$MERGED_A"
assert_eq "node-a fan-out: nodes_total == 2" "2" "$NODES_TOTAL_A"
assert_eq "node-a fan-out: nodes_responding == 2" "2" "$NODES_RESP_A"
assert_eq "node-a fan-out: partial == False" "False" "$PARTIAL_A"
assert_eq "node-a fan-out: node_details names both nodes" "node-a,node-b" "$DETAILS_A"

# ── 6c. Fan-out query from node-b (symmetry proof) ──────────────────────────
FANOUT_B=$(curl -sf "$NODE_B/2/searches/count?index=$ANALYTICS_INDEX" 2>/dev/null || echo "{}")
MERGED_B=$(json_val "$FANOUT_B" "count" "0")
NODES_TOTAL_B=$(json_val "$FANOUT_B" "cluster.nodes_total" "0")
NODES_RESP_B=$(json_val "$FANOUT_B" "cluster.nodes_responding" "0")
PARTIAL_B=$(json_val "$FANOUT_B" "cluster.partial" "True")
DETAILS_B=$(json_node_ids "$FANOUT_B")

echo "  node-b fan-out: count=$MERGED_B, nodes_total=$NODES_TOTAL_B, nodes_responding=$NODES_RESP_B, partial=$PARTIAL_B"

assert_eq "node-b fan-out: merged count == local_a + local_b" "$EXPECTED_MERGED" "$MERGED_B"
assert_eq "node-b fan-out: nodes_total == 2" "2" "$NODES_TOTAL_B"
assert_eq "node-b fan-out: nodes_responding == 2" "2" "$NODES_RESP_B"
assert_eq "node-b fan-out: partial == False" "False" "$PARTIAL_B"
assert_eq "node-b fan-out: node_details names both nodes" "node-a,node-b" "$DETAILS_B"

# ── Summary ──────────────────────────────────────────────────────────────────

echo ""
echo "════════════════════════════════════════"
TOTAL=$((PASS + FAIL))
if [ "$FAIL" -eq 0 ]; then
  printf "\033[32m✓ All %d assertions passed\033[0m\n" "$TOTAL"
else
  printf "\033[31m✗ %d/%d assertions failed\033[0m\n" "$FAIL" "$TOTAL"
fi
echo "════════════════════════════════════════"

if $WITH_DOCKER; then
  echo ""
  echo "=== Tearing down ==="
  docker compose down -v
fi

[ "$FAIL" -eq 0 ]
