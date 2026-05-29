import { expect, test } from '@playwright/test';
import { PRODUCTS, TEST_INDEX } from '../fixtures/test-data';
import {
  readSeedMetricsSnapshot,
  seededIndexMetricsReady,
} from './seed_metrics_readiness';

const MINIMUM_DOCUMENTS = PRODUCTS.length;

function buildMetricsBody(lines: string[]): string {
  return `${lines.join('\n')}\n`;
}

function metricLine(
  metricName: string,
  value: number,
  indexName: string = TEST_INDEX,
): string {
  return `${metricName}{index="${indexName}"} ${value}`;
}

test.describe('seed metrics readiness contract', () => {
  test('accepts scientific notation values emitted by Prometheus', () => {
    const metricsBody = buildMetricsBody([
      `flapjack_documents_count{index="${TEST_INDEX}"} ${MINIMUM_DOCUMENTS.toExponential()}`,
      'flapjack_storage_bytes{index="' + TEST_INDEX + '"} 2.8382e+4',
      'flapjack_search_requests_total{index="' + TEST_INDEX + '"} 1.9e+1',
      'flapjack_oplog_current_seq{index="' + TEST_INDEX + '"} 1.5e+1',
    ]);

    const snapshot = readSeedMetricsSnapshot(metricsBody, TEST_INDEX);
    expect(snapshot).toEqual({
      documents: MINIMUM_DOCUMENTS,
      storage: 28382,
      searches: 19,
      oplog: 15,
    });
    expect(
      seededIndexMetricsReady(snapshot, {
        minimumDocuments: MINIMUM_DOCUMENTS,
        minimumSearchRequests: 1,
      }),
    ).toBe(true);
  });

  test('stale per-index docs/search values are not ready', () => {
    const metricsBody = buildMetricsBody([
      metricLine('flapjack_documents_count', MINIMUM_DOCUMENTS - 1),
      metricLine('flapjack_storage_bytes', 1200),
      metricLine('flapjack_search_requests_total', 0),
      metricLine('flapjack_oplog_current_seq', 10),
    ]);

    const snapshot = readSeedMetricsSnapshot(metricsBody, TEST_INDEX);
    expect(
      seededIndexMetricsReady(snapshot, {
        minimumDocuments: MINIMUM_DOCUMENTS,
        minimumSearchRequests: 1,
      }),
    ).toBe(false);
  });

  test('ready only when all required per-index metrics are present', () => {
    const metricsBody = buildMetricsBody([
      metricLine('flapjack_documents_count', MINIMUM_DOCUMENTS),
      metricLine('flapjack_storage_bytes', 1200),
      metricLine('flapjack_search_requests_total', 2),
      metricLine('flapjack_oplog_current_seq', 10),
    ]);

    const snapshot = readSeedMetricsSnapshot(metricsBody, TEST_INDEX);
    expect(
      seededIndexMetricsReady(snapshot, {
        minimumDocuments: MINIMUM_DOCUMENTS,
        minimumSearchRequests: 1,
      }),
    ).toBe(true);
  });
});
