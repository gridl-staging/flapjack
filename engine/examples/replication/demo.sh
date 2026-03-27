#!/usr/bin/env bash
# 2-node replication + analytics fan-out walkthrough.
#
# This script mirrors the public routes proven by test_replication.sh.
# It is a walkthrough, not an assertion harness.
#
# Prereq:
#   docker compose up -d --build
# Run:
#   ./demo.sh

set -euo pipefail

NODE_A="http://localhost:7700"
NODE_B="http://localhost:7701"

echo "=== Waiting for nodes ==="
for port in 7700 7701; do
  for i in $(seq 1 30); do
    curl -sf "http://localhost:$port/health" >/dev/null 2>&1 && echo "  :$port ready" && break
    [ "$i" = "30" ] && echo "FATAL: :$port not ready" && exit 1
    sleep 2
  done
done

echo ""
echo "=== Public replication route: node-a -> node-b ==="
INDEX_A="demo-repl-a-$(date +%s)"
curl -sf -X POST "$NODE_A/1/indexes/$INDEX_A/batch" \
  -H 'Content-Type: application/json' \
  -d '{
    "requests": [
      {"action":"addObject","body":{"_id":"a-1","title":"Saffron Pancakes"}},
      {"action":"addObject","body":{"_id":"a-2","title":"Matcha Waffles"}}
    ]
  }' >/dev/null
sleep 3
curl -sf -X POST "$NODE_B/1/indexes/$INDEX_A/query" \
  -H 'Content-Type: application/json' \
  -d '{"query":"pancakes","hitsPerPage":5}' | jq '{nbHits, hit_ids: [.hits[]._id]}'

echo ""
echo "=== Delete propagation: node-a -> node-b ==="
curl -sf -X DELETE "$NODE_A/1/indexes/$INDEX_A/a-2" >/dev/null
sleep 3
curl -sf -X POST "$NODE_B/1/indexes/$INDEX_A/query" \
  -H 'Content-Type: application/json' \
  -d '{"query":"waffles","hitsPerPage":5}' | jq '{nbHits}'

echo ""
echo "=== Public replication route: node-b -> node-a ==="
INDEX_B="demo-repl-b-$(date +%s)"
curl -sf -X POST "$NODE_B/1/indexes/$INDEX_B/batch" \
  -H 'Content-Type: application/json' \
  -d '{
    "requests": [
      {"action":"addObject","body":{"_id":"b-1","title":"Cardamom Croissant"}}
    ]
  }' >/dev/null
sleep 3
curl -sf -X POST "$NODE_A/1/indexes/$INDEX_B/query" \
  -H 'Content-Type: application/json' \
  -d '{"query":"cardamom","hitsPerPage":5}' | jq '{nbHits, hit_ids: [.hits[]._id]}'

echo ""
echo "=== Seed and flush analytics on both nodes ==="
ANALYTICS_INDEX="$INDEX_A"
curl -sf -X POST "$NODE_A/2/analytics/seed" -H 'Content-Type: application/json' \
  -d "{\"index\":\"$ANALYTICS_INDEX\",\"days\":7}" | jq '.status'
curl -sf -X POST "$NODE_B/2/analytics/seed" -H 'Content-Type: application/json' \
  -d "{\"index\":\"$ANALYTICS_INDEX\",\"days\":7}" | jq '.status'
curl -sf -X POST "$NODE_A/2/analytics/flush" >/dev/null
curl -sf -X POST "$NODE_B/2/analytics/flush" >/dev/null
sleep 1

echo ""
echo "=== Local-only analytics counts ==="
COUNT_A=$(curl -sf -H "X-Flapjack-Local-Only: true" "$NODE_A/2/searches/count?index=$ANALYTICS_INDEX" | jq '.count')
COUNT_B=$(curl -sf -H "X-Flapjack-Local-Only: true" "$NODE_B/2/searches/count?index=$ANALYTICS_INDEX" | jq '.count')
echo "  node-a local count: $COUNT_A"
echo "  node-b local count: $COUNT_B"
echo "  expected merged count: $((COUNT_A + COUNT_B))"

echo ""
echo "=== Merged analytics count from node-a ==="
curl -sf "$NODE_A/2/searches/count?index=$ANALYTICS_INDEX" | \
  jq '{count, cluster: (.cluster | {nodes_total, nodes_responding, partial, node_details})}'

echo ""
echo "=== Merged analytics count from node-b ==="
curl -sf "$NODE_B/2/searches/count?index=$ANALYTICS_INDEX" | \
  jq '{count, cluster: (.cluster | {nodes_total, nodes_responding, partial, node_details})}'

echo ""
echo "=== Optional: analytics cluster.partial after stopping node-b ==="
docker compose stop node-b 2>/dev/null || true
sleep 2
PARTIAL=$(curl -sf "$NODE_A/2/searches/count?index=$ANALYTICS_INDEX")
echo "$PARTIAL" | jq '{count, cluster: (.cluster | {nodes_total, nodes_responding, partial})}'
docker compose start node-b 2>/dev/null || true

echo ""
echo "Walkthrough complete."
