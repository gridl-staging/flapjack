# Operational Constraints

Source: Benchmarks nov4-nov7 2024

## Memory Model (Per 4GB Node)

**Per-tenant costs (50K docs, 5 FAST fields):**
- Cold: 0.395 MB (reader metadata)
- Hot: 1.171 MB (reader + working set)

**Capacity:**
- Target: 600 tenants
- Conservative max: 681 tenants (all hot)
- Safe utilization: 702 MB = 53%

**Write transient:**
- Per-tenant buffer: 31 MB
- Max concurrent writers: 40 (10% capacity)
- Total write buffer: 1.24 GB
- Headroom: 1.33 GB

**Validation:** 150 tenants × 50K docs = 116 MB working set (0.776 MB/tenant)

## File Descriptors

NOT A CONSTRAINT. 300 indexes = 11 fds (mmap closes fds after mapping).

## Index Loading

No LRU eviction. All loaded indexes stay resident.

**Rationale:** 600 × 1.171 MB = 702 MB fits in budget. Index::open() costs 0 MB until queried.

## Query Performance (Validated P99)

- Text search: 0.08ms (BM25)
- Filters: 0.06-0.22ms
- Smart sorting: 0.2ms (relevance) / 0.5-5ms (pure)
- Faceting: 10.7ms (10K docs × 100 values)
- Combined target: <50ms

## Write Performance

- Hybrid batching: 10 ops OR 100ms timeout
- Queue depth: 1000 ops/tenant
- Backpressure: Instant rejection via try_send
- Throughput: ~500 commits/sec/tenant

## Migration Constraints

**Export timing:**
- Channel close commit: <100ms
- Safety sleep: 300ms
- Directory copy: ~38ms
- Reload: ~50ms
- Total: ~400ms downtime

**Requirement:** Application must stop writes before export.

## Single-Field Ceiling Tests (1GB t4g.micro, `names` collection)

Validated 2026-02-03 (handoffs #128-129). TS collection renamed from `names` to `namesMaxTs` in #137.

| Engine | Stable ceiling | Warm query P50 (on-box localhost) | RSS warm | Disk |
|--------|---------------|----------------------------------|----------|------|
| Meilisearch | 400K | not tested | ~600MB | 96MB |
| Typesense | 8.64M | 2-22ms | 657MB | 1178MB |
| Flapjack | 11.24M | 3-19ms | 600-635MB | ~1200MB |

FJ bottleneck is Tantivy segment merge (transient OOM), not serving. Cold RSS is 11-14MB (mmap lazy-load). TS bottleneck is steady-state RAM. MS bottleneck is steady-state RAM.

## Critical Assumptions (Not Validated)

1. Zipf query distribution (80% queries → 20% tenants)
2. Max 40 simultaneous writers enforced
3. Corpus <100K docs/tenant
4. Segment merge doesn't spike latency
5. Batching accumulation <4s

See: docs2/3_IMPLEMENTATION/decisions/active/ for architectural decisions