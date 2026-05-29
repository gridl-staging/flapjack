/**
 * Seeded metrics readiness helpers shared by seed setup and readiness tests.
 */
import {
  parsePrometheusText,
  type PrometheusMetric,
} from '../../src/lib/prometheusParser';

export interface SeedMetricsSnapshot {
  documents: number | null;
  storage: number | null;
  searches: number | null;
  oplog: number | null;
}

export interface SeedMetricsThresholds {
  minimumDocuments: number;
  minimumSearchRequests: number;
}

function extractPerIndexMetricValue(
  metrics: PrometheusMetric[],
  metricName: string,
  indexName: string,
): number | null {
  const matchingMetric = metrics.find((metric) => (
    metric.name === metricName
    && metric.labels.index === indexName
  ));
  if (!matchingMetric) {
    return null;
  }

  return Number.isFinite(matchingMetric.value) ? matchingMetric.value : null;
}

export function readSeedMetricsSnapshot(metricsBody: string, indexName: string): SeedMetricsSnapshot {
  const metrics = parsePrometheusText(metricsBody);

  return {
    documents: extractPerIndexMetricValue(metrics, 'flapjack_documents_count', indexName),
    storage: extractPerIndexMetricValue(metrics, 'flapjack_storage_bytes', indexName),
    searches: extractPerIndexMetricValue(metrics, 'flapjack_search_requests_total', indexName),
    oplog: extractPerIndexMetricValue(metrics, 'flapjack_oplog_current_seq', indexName),
  };
}

export function seededIndexMetricsReady(
  snapshot: SeedMetricsSnapshot,
  thresholds: SeedMetricsThresholds,
): boolean {
  return (
    snapshot.documents !== null
    && snapshot.storage !== null
    && snapshot.searches !== null
    && snapshot.oplog !== null
    && snapshot.storage > 0
    && snapshot.documents >= thresholds.minimumDocuments
    && snapshot.searches >= thresholds.minimumSearchRequests
  );
}
