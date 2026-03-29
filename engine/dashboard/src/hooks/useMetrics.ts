/**
 */
import { useQuery } from '@tanstack/react-query';
import { parsePrometheusText, type PrometheusMetric } from '@/lib/prometheusParser';
import { useAuth } from '@/hooks/useAuth';

const DEFAULT_METRICS_APP_ID = 'flapjack';
const METRICS_QUERY_KEY = 'prometheus-metrics';

function getEffectiveMetricsAppId(appId: string | null | undefined) {
  return appId || DEFAULT_METRICS_APP_ID;
}

function getApiKeyFingerprint(apiKey: string | null | undefined) {
  if (!apiKey) {
    return 'anonymous';
  }

  // Fingerprint the credential so React Query invalidates on key rotation
  // without storing the raw secret in cache metadata.
  let hash = 2166136261;
  for (let index = 0; index < apiKey.length; index += 1) {
    hash ^= apiKey.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }

  return `authenticated:${apiKey.length}:${(hash >>> 0).toString(16)}`;
}

export function usePrometheusMetrics() {
  const appId = useAuth((state) => state.appId);
  const apiKey = useAuth((state) => state.apiKey);
  const effectiveAppId = getEffectiveMetricsAppId(appId);
  const credentialScope = getApiKeyFingerprint(apiKey);

  return useQuery<PrometheusMetric[]>({
    queryKey: [METRICS_QUERY_KEY, effectiveAppId, credentialScope],
    queryFn: async () => {
      // Fetch directly from the backend — /metrics can't go through the Vite proxy
      // because the dashboard page route is also /metrics (SPA path conflict).
      const headers: Record<string, string> = {
        'x-algolia-application-id': effectiveAppId,
      };
      if (apiKey) {
        headers['x-algolia-api-key'] = apiKey;
      }
      const res = await fetch(`${__BACKEND_URL__}/metrics`, { headers });
      if (!res.ok) throw new Error(`Metrics fetch failed: ${res.status}`);
      const text = await res.text();
      return parsePrometheusText(text);
    },
    refetchInterval: 10000,
    staleTime: 5000,
  });
}

/**
 * Group metrics by index label into a map of index name → metric short names → values.
 * Strips the `flapjack_` prefix for readability.
 */
export function getPerIndexMetrics(
  metrics: PrometheusMetric[]
): Map<string, Record<string, number>> {
  const result = new Map<string, Record<string, number>>();

  for (const m of metrics) {
    const indexName = m.labels.index;
    if (!indexName) continue;

    if (!result.has(indexName)) {
      result.set(indexName, {});
    }
    const shortName = m.name.replace(/^flapjack_/, '');
    result.get(indexName)![shortName] = m.value;
  }

  return result;
}

/**
 * Get a single system-wide metric value by name.
 * Returns undefined if not found.
 */
export function getSystemMetric(
  metrics: PrometheusMetric[],
  name: string
): number | undefined {
  return metrics.find((m) => m.name === name && Object.keys(m.labels).length === 0)?.value;
}
