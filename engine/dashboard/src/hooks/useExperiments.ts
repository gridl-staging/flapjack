/**
 */
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import api from '@/lib/api';
import type { Experiment } from '@/lib/types';
import { useToast } from '@/hooks/use-toast';
import {
  asExperimentRecord,
  toAlgoliaCreateExperimentPayload,
  readExperimentId,
  type DashboardCreateExperimentPayload,
  type ConcludeExperimentPayload,
} from '@/lib/experiment-api-contract';
import {
  normalizeExperimentListResponse,
  normalizeExperimentRecord,
  normalizeExperimentResultsResponse,
  type ArmResultsResponse,
  type ExperimentConclusionResponse,
  type ExperimentResultsResponse,
  type GuardRailAlertResponse,
  type InterleavingResultsResponse,
  type SignificanceResponse,
} from '@/lib/experiment-normalization';

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

function getErrorMessage(error: unknown): string {
  return error instanceof Error && error.message
    ? error.message
    : 'Unknown error';
}

interface UseExperimentMutationOptions<TVariables, TData> {
  mutationFn: (variables: TVariables) => Promise<TData>;
  successTitle: string;
  errorTitle: string;
  invalidateExperimentResults?: boolean;
}

/**
 * Builds a mutation hook with the shared experiment cache invalidation and toast
 * behavior used by the dashboard's experiment lifecycle actions.
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
      const experimentId = readExperimentId(asExperimentRecord(data));
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

type ExperimentResultsApiResponse = ExperimentResultsResponse & {
  interleaving: (InterleavingResultsResponse & { deltaAb?: number }) | null;
};

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

export type {
  ArmResultsResponse,
  ExperimentConclusionResponse,
  ExperimentResultsResponse,
  GuardRailAlertResponse,
  InterleavingResultsResponse,
  SignificanceResponse,
};
