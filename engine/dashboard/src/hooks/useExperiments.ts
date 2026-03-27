/**
 */
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import api from '@/lib/api';
import type { Experiment } from '@/lib/types';
import { useToast } from '@/hooks/use-toast';
import {
  readExperimentId,
  toAlgoliaCreateExperimentPayload,
  type DashboardCreateExperimentPayload,
  type ConcludeExperimentPayload,
} from '@/lib/experiment-api-contract';

type RawRecord = Record<string, unknown>;
type RawExperimentListResponse = {
  abtests?: unknown;
};

const DEFAULT_PRIMARY_METRIC = 'ctr';
const EXPERIMENTS_QUERY_KEY = ['experiments'] as const;
const EXPERIMENT_RESULTS_QUERY_KEY = ['experiment-results'] as const;

function experimentPath(id: string, suffix = ''): string {
  return `/2/abtests/${encodeURIComponent(id)}${suffix}`;
}

function experimentQueryKey(experimentId: string) {
  return ['experiment', experimentId] as const;
}

function experimentResultsQueryKey(experimentId: string) {
  return [...EXPERIMENT_RESULTS_QUERY_KEY, experimentId] as const;
}

function asRecord(value: unknown): RawRecord {
  return typeof value === 'object' && value !== null ? value as RawRecord : {};
}

function asNumber(value: unknown): number | undefined {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === 'string' && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }

  return undefined;
}

function asTimestamp(value: unknown): number | undefined {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === 'string' && value.trim().length > 0) {
    const parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }

  return undefined;
}

function asString(value: unknown): string | undefined {
  if (typeof value === 'string' && value.length > 0) {
    return value;
  }

  if (typeof value === 'number' && Number.isFinite(value)) {
    return String(value);
  }

  return undefined;
}

function getErrorMessage(error: unknown): string {
  return error instanceof Error && error.message
    ? error.message
    : 'Unknown error';
}

function readStartedAt(record: RawRecord): number | undefined {
  return (
    asTimestamp(record.startedAt) ??
    asTimestamp(record.startAt) ??
    asTimestamp(record.startDate)
  );
}

function readEndedAt(record: RawRecord): number | undefined {
  return asTimestamp(record.endedAt) ?? asTimestamp(record.endAt);
}

/**
 * TODO: Document normalizePrimaryMetric.
 */
function normalizePrimaryMetric(metric: unknown): string {
  const rawMetric = asString(metric);
  switch (rawMetric) {
    case 'clickThroughRate':
    case 'ctr':
      return 'ctr';
    case 'conversionRate':
      return 'conversionRate';
    case 'revenue':
    case 'revenuePerSearch':
      return 'revenuePerSearch';
    case 'zeroResultRate':
      return 'zeroResultRate';
    case 'abandonmentRate':
      return 'abandonmentRate';
    default:
      return DEFAULT_PRIMARY_METRIC;
  }
}

function normalizeTrafficSplit(value: unknown): number {
  const parsed = asNumber(value);
  if (parsed === undefined) return 0.5;
  const scaled = parsed > 1 ? parsed / 100 : parsed;
  return Math.min(1, Math.max(0, scaled));
}

/**
 * TODO: Document normalizeStatus.
 */
function normalizeStatus(record: RawRecord): Experiment['status'] {
  const status = asString(record.status);
  if (
    status === 'draft' ||
    status === 'running' ||
    status === 'stopped' ||
    status === 'concluded' ||
    status === 'expired'
  ) {
    return status;
  }

  if (status === 'active') {
    return readStartedAt(record) === undefined ? 'draft' : 'running';
  }

  return 'draft';
}

/**
 * TODO: Document normalizeExperimentRecord.
 */
function normalizeExperimentRecord(rawExperiment: unknown): Experiment {
  const record = asRecord(rawExperiment);
  const variants = Array.isArray(record.variants) ? record.variants : [];
  const controlVariant = asRecord(variants[0]);
  const variantArm = asRecord(variants[1]);
  const control = asRecord(record.control);
  const variant = asRecord(record.variant);
  const responseMetric =
    Array.isArray(record.metrics) && record.metrics.length > 0
      ? asRecord(record.metrics[0]).name
      : undefined;

  const createdAt = asTimestamp(record.createdAt) ?? Date.now();
  const startedAt = readStartedAt(record) ?? null;
  const endedAt = readEndedAt(record) ?? null;
  const minimumDaysFromDates =
    endedAt && createdAt && endedAt > createdAt
      ? Math.ceil((endedAt - createdAt) / (1000 * 60 * 60 * 24))
      : 14;
  const minimumDays = asNumber(record.minimumDays) ?? minimumDaysFromDates;

  const variantTrafficPercentage = asNumber(variantArm.trafficPercentage);

  return {
    id: asString(record.id) ?? asString(record.abTestID) ?? '',
    name: asString(record.name) ?? '',
    indexName: asString(record.indexName) ?? asString(controlVariant.index) ?? '',
    status: normalizeStatus(record),
    trafficSplit:
      asNumber(record.trafficSplit) !== undefined
        ? normalizeTrafficSplit(record.trafficSplit)
        : normalizeTrafficSplit(variantTrafficPercentage),
    control: {
      name: asString(control.name) ?? asString(controlVariant.description) ?? 'control',
      queryOverrides: control.queryOverrides as Record<string, unknown> | undefined,
      indexName: asString(control.indexName),
    },
    variant: {
      name: asString(variant.name) ?? asString(variantArm.description) ?? 'variant',
      queryOverrides:
        (variant.queryOverrides as Record<string, unknown> | undefined) ??
        (variantArm.customSearchParameters as Record<string, unknown> | undefined),
      indexName: asString(variant.indexName) ?? asString(variantArm.index),
    },
    primaryMetric: normalizePrimaryMetric(record.primaryMetric ?? responseMetric),
    createdAt,
    startedAt,
    endedAt,
    minimumDays,
    winsorizationCap: asNumber(record.winsorizationCap) ?? null,
  };
}

function normalizeExperimentListResponse(data: unknown): Experiment[] {
  if (Array.isArray(data)) {
    return data.map(normalizeExperimentRecord);
  }

  const body = asRecord(data) as RawExperimentListResponse;
  if (!Array.isArray(body.abtests)) {
    return [];
  }
  return body.abtests.map(normalizeExperimentRecord);
}

interface UseExperimentMutationOptions<TVariables, TData> {
  mutationFn: (variables: TVariables) => Promise<TData>;
  successTitle: string;
  errorTitle: string;
  invalidateExperimentResults?: boolean;
}

/**
 * TODO: Document useExperimentMutation.
 */
function useExperimentMutation<TVariables, TData>({
  mutationFn,
  successTitle,
  errorTitle,
  invalidateExperimentResults = false,
}: UseExperimentMutationOptions<TVariables, TData>) {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation({
    mutationFn,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: EXPERIMENTS_QUERY_KEY });
      if (invalidateExperimentResults) {
        queryClient.invalidateQueries({ queryKey: EXPERIMENT_RESULTS_QUERY_KEY });
      }
      toast({ title: successTitle });
    },
    onError: (error: Error) => {
      toast({
        variant: 'destructive',
        title: errorTitle,
        description: getErrorMessage(error),
      });
    },
  });
}

export function useExperiment(experimentId: string) {
  return useQuery<Experiment>({
    queryKey: experimentQueryKey(experimentId),
    queryFn: async () => {
      const { data } = await api.get(experimentPath(experimentId));
      return normalizeExperimentRecord(data);
    },
    enabled: !!experimentId,
    retry: 1,
  });
}

export function useExperiments() {
  return useQuery<Experiment[]>({
    queryKey: EXPERIMENTS_QUERY_KEY,
    queryFn: async () => {
      const { data } = await api.get('/2/abtests');
      return normalizeExperimentListResponse(data);
    },
    staleTime: 15000,
    retry: 1,
  });
}

/**
 * Mutation hook that creates a new A/B test experiment via POST `/2/abtests`.
 * 
 * Invalidates the experiments query cache and shows a toast notification on success or failure.
 * 
 * @returns A mutation whose `mutate` accepts a `CreateExperimentPayload`.
 */
export function useCreateExperiment() {
  return useExperimentMutation({
    mutationFn: async (payload: DashboardCreateExperimentPayload) => {
      const algoliaPayload = toAlgoliaCreateExperimentPayload(payload);
      const { data } = await api.post('/2/abtests', algoliaPayload);
      const experimentId = readExperimentId(asRecord(data));
      if (!experimentId) {
        throw new Error('Create experiment response missing experiment id');
      }

      try {
        await api.post(experimentPath(experimentId, '/start'));
      } catch (error) {
        throw new Error(
          `Experiment was created as a draft but launch failed: ${getErrorMessage(error)}`,
        );
      }

      return data;
    },
    successTitle: 'Experiment launched',
    errorTitle: 'Failed to launch experiment',
  });
}

/**
 * Mutation hook that stops a running experiment via POST `/2/abtests/:id/stop`.
 * 
 * Invalidates the experiments query cache and shows a toast notification on success or failure.
 * 
 * @returns A mutation whose `mutate` accepts the experiment ID string.
 */
export function useStopExperiment() {
  return useExperimentMutation({
    mutationFn: async (id: string) => {
      const { data } = await api.post(experimentPath(id, '/stop'));
      return data;
    },
    successTitle: 'Experiment stopped',
    errorTitle: 'Failed to stop experiment',
  });
}

/**
 * Mutation hook that deletes an experiment via DELETE `/2/abtests/:id`.
 * 
 * Invalidates the experiments query cache and shows a toast notification on success or failure.
 * 
 * @returns A mutation whose `mutate` accepts the experiment ID string.
 */
export function useDeleteExperiment() {
  return useExperimentMutation({
    mutationFn: async (id: string) => {
      await api.delete(experimentPath(id));
    },
    successTitle: 'Experiment deleted',
    errorTitle: 'Failed to delete experiment',
  });
}

/**
 * Full results payload for a single A/B test experiment, including arm-level metrics, statistical significance, Bayesian probability, guard-rail alerts, interleaving results, and an optional conclusion.
 */
export interface ExperimentResultsResponse {
  experimentID: string;
  name: string;
  status: string;
  indexName: string;
  startDate: string | null;
  endedAt: string | null;
  conclusion: ExperimentConclusionResponse | null;
  trafficSplit: number;
  primaryMetric: string;
  gate: {
    minimumNReached: boolean;
    minimumDaysReached: boolean;
    readyToRead: boolean;
    requiredSearchesPerArm: number;
    currentSearchesPerArm: number;
    progressPct: number;
    estimatedDaysRemaining: number | null;
  };
  control: ArmResultsResponse;
  variant: ArmResultsResponse;
  significance: SignificanceResponse | null;
  bayesian: { probVariantBetter: number } | null;
  sampleRatioMismatch: boolean;
  cupedApplied: boolean;
  guardRailAlerts: GuardRailAlertResponse[];
  outlierUsersExcluded: number;
  noStableIdQueries: number;
  recommendation: string | null;
  interleaving: InterleavingResultsResponse | null;
}

export interface InterleavingResultsResponse {
  deltaAB: number;
  winsControl: number;
  winsVariant: number;
  ties: number;
  pValue: number;
  significant: boolean;
  totalQueries: number;
  dataQualityOk: boolean;
}

export interface GuardRailAlertResponse {
  metricName: string;
  controlValue: number;
  variantValue: number;
  dropPct: number;
}

export interface ExperimentConclusionResponse {
  winner: string | null;
  reason: string;
  controlMetric: number;
  variantMetric: number;
  confidence: number;
  significant: boolean;
  promoted: boolean;
}

export interface ArmResultsResponse {
  name: string;
  searches: number;
  users: number;
  clicks: number;
  conversions: number;
  revenue: number;
  ctr: number;
  conversionRate: number;
  revenuePerSearch: number;
  zeroResultRate: number;
  abandonmentRate: number;
  meanClickRank: number;
}

export interface SignificanceResponse {
  zScore: number;
  pValue: number;
  confidence: number;
  significant: boolean;
  relativeImprovement: number;
  winner: string | null;
}

type ExperimentResultsApiResponse = ExperimentResultsResponse & {
  interleaving: (InterleavingResultsResponse & { deltaAb?: number }) | null;
};

/**
 * TODO: Document normalizeExperimentResultsResponse.
 */
function normalizeExperimentResultsResponse(
  response: ExperimentResultsApiResponse,
): ExperimentResultsResponse {
  const interleaving = response.interleaving;
  if (!interleaving) {
    return response;
  }

  return {
    ...response,
    interleaving: {
      ...interleaving,
      deltaAB:
        typeof interleaving.deltaAB === 'number'
          ? interleaving.deltaAB
          : interleaving.deltaAb ?? 0,
    },
  };
}

/**
 * Mutation hook that concludes an experiment with a final verdict via POST `/2/abtests/:id/conclude`.
 * 
 * Invalidates both the experiments and experiment-results query caches and shows a toast notification on success or failure.
 * 
 * @returns A mutation whose `mutate` accepts an object with `id` (experiment ID) and `payload` (conclusion details).
 */
export function useConcludeExperiment() {
  return useExperimentMutation({
    mutationFn: async ({ id, payload }: { id: string; payload: ConcludeExperimentPayload }) => {
      const { data } = await api.post(experimentPath(id, '/conclude'), payload);
      return data;
    },
    successTitle: 'Experiment concluded',
    errorTitle: 'Failed to conclude experiment',
    invalidateExperimentResults: true,
  });
}

export function useExperimentResults(experimentId: string) {
  return useQuery<ExperimentResultsResponse>({
    queryKey: experimentResultsQueryKey(experimentId),
    queryFn: async () => {
      const { data } = await api.get<ExperimentResultsApiResponse>(experimentPath(experimentId, '/results'));
      return normalizeExperimentResultsResponse(data);
    },
    enabled: !!experimentId,
    refetchInterval: 30000,
    retry: 1,
  });
}
