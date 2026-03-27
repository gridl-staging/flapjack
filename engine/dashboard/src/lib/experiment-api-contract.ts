/**
 */
export interface DashboardCreateExperimentPayload {
  name: string;
  indexName: string;
  trafficSplit: number;
  control: {
    name?: string;
  } & Record<string, unknown>;
  variant: {
    name?: string;
    queryOverrides?: Record<string, unknown>;
    indexName?: string;
  } & Record<string, unknown>;
  primaryMetric: string;
  minimumDays?: number;
}

export interface AlgoliaCreateExperimentPayload {
  name: string;
  variants: Array<{
    index: string;
    trafficPercentage: number;
    description?: string;
    customSearchParameters?: Record<string, unknown>;
  }>;
  endAt: string;
  metrics?: Array<{ name: string }>;
}

export type CreateExperimentPayload =
  | DashboardCreateExperimentPayload
  | AlgoliaCreateExperimentPayload;

export interface ConcludeExperimentPayload {
  winner: string | null;
  reason: string;
  controlMetric: number;
  variantMetric: number;
  confidence: number;
  significant: boolean;
  promoted: boolean;
}

type RawExperimentRecord = Record<string, unknown>;

function isAlgoliaCreateExperimentPayload(
  payload: CreateExperimentPayload,
): payload is AlgoliaCreateExperimentPayload {
  return 'variants' in payload;
}

export function asExperimentRecord(value: unknown): RawExperimentRecord {
  return typeof value === 'object' && value !== null ? value as RawExperimentRecord : {};
}

export function readExperimentId(record: RawExperimentRecord): string {
  const rawId = record.id ?? record.abTestID ?? record.ab_test_id;
  if (typeof rawId === 'string' && rawId.length > 0) return rawId;
  if (typeof rawId === 'number') return String(rawId);
  return '';
}

export function toAlgoliaMetricName(metric: string): string {
  switch (metric) {
    case 'ctr':
    case 'clickThroughRate':
      return 'clickThroughRate';
    case 'conversionRate':
      return 'conversionRate';
    case 'revenue':
    case 'revenuePerSearch':
      return 'revenue';
    default:
      return 'clickThroughRate';
  }
}

/**
 * TODO: Document toAlgoliaCreateExperimentPayload.
 */
export function toAlgoliaCreateExperimentPayload(
  payload: CreateExperimentPayload,
): AlgoliaCreateExperimentPayload {
  if (isAlgoliaCreateExperimentPayload(payload)) {
    return payload;
  }

  const variantTraffic = Math.max(1, Math.min(99, Math.round(payload.trafficSplit * 100)));
  const controlTraffic = 100 - variantTraffic;
  const minimumDays = payload.minimumDays ?? 14;
  const variantOverrides =
    payload.variant.queryOverrides && Object.keys(payload.variant.queryOverrides).length > 0
      ? payload.variant.queryOverrides
      : undefined;
  const variantIndex = payload.variant.indexName ?? payload.indexName;

  return {
    name: payload.name,
    variants: [
      {
        index: payload.indexName,
        trafficPercentage: controlTraffic,
        description: payload.control.name ?? 'control',
      },
      {
        index: variantIndex,
        trafficPercentage: variantTraffic,
        description: payload.variant.name ?? 'variant',
        ...(variantOverrides ? { customSearchParameters: variantOverrides } : {}),
      },
    ],
    endAt: new Date(Date.now() + minimumDays * 24 * 60 * 60 * 1000).toISOString(),
    metrics: [{ name: toAlgoliaMetricName(payload.primaryMetric) }],
  };
}
