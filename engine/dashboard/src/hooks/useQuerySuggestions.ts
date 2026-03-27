/**
 * @module React Query hooks for managing Query Suggestions configs, including CRUD operations, build triggering, and status/log polling.
 */
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import api from '@/lib/api';
import type { QsConfig, QsBuildStatus, QsLogEntry } from '@/lib/types';
import { useToast } from '@/hooks/use-toast';

export function useQsConfigs() {
  return useQuery<QsConfig[]>({
    queryKey: ['qsConfigs'],
    queryFn: async () => {
      const response = await api.get<QsConfig[]>('/1/configs');
      return response.data;
    },
  });
}

export function useQsBuildStatus(indexName: string) {
  return useQuery<QsBuildStatus>({
    queryKey: ['qsStatus', indexName],
    queryFn: async () => {
      const response = await api.get<QsBuildStatus>(`/1/configs/${indexName}/status`);
      return response.data;
    },
    refetchInterval: (query) => {
      // Poll every 2s while a build is running
      const data = query.state.data;
      return data?.isRunning ? 2000 : false;
    },
  });
}

export function useQsLogs(indexName: string) {
  return useQuery<QsLogEntry[]>({
    queryKey: ['qsLogs', indexName],
    queryFn: async () => {
      const response = await api.get<QsLogEntry[]>(`/1/logs/${indexName}`);
      return response.data;
    },
  });
}

/**
 * Returns a mutation that creates a new Query Suggestions config and triggers an initial index build.
 * Invalidates the `qsConfigs` query cache on success and shows a toast notification on success or failure.
 * @returns A TanStack Query mutation whose `mutate` accepts a `QsConfig` object.
 */
export function useCreateQsConfig() {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation({
    mutationFn: async (config: QsConfig) => {
      const response = await api.post<{ status: number; message: string }>('/1/configs', config);
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['qsConfigs'] });
      toast({
        title: 'Config created',
        description: 'Query Suggestions config created. Building index now.',
      });
    },
    onError: (error: Error) => {
      toast({
        variant: 'destructive',
        title: 'Failed to create config',
        description: error.message || 'An error occurred.',
      });
    },
  });
}

/**
 * Returns a mutation that deletes a Query Suggestions config by index name.
 * The underlying suggestions index is preserved after deletion. Invalidates the `qsConfigs` query cache on success and shows a toast notification on success or failure.
 * @returns A TanStack Query mutation whose `mutate` accepts the index name to delete.
 */
export function useDeleteQsConfig() {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation({
    mutationFn: async (indexName: string) => {
      await api.delete(`/1/configs/${indexName}`);
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['qsConfigs'] });
      toast({
        title: 'Config deleted',
        description: 'Query Suggestions config deleted. The suggestions index is preserved.',
      });
    },
    onError: (error: Error) => {
      toast({
        variant: 'destructive',
        title: 'Failed to delete config',
        description: error.message || 'An error occurred.',
      });
    },
  });
}

/**
 * Returns a mutation that triggers a rebuild of the Query Suggestions index for a given config.
 * Invalidates the corresponding `qsStatus` query cache on success to refresh polling and shows a toast notification on success or failure.
 * @returns A TanStack Query mutation whose `mutate` accepts the index name to rebuild.
 */
export function useTriggerQsBuild() {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation({
    mutationFn: async (indexName: string) => {
      await api.post(`/1/configs/${indexName}/build`, {});
    },
    onSuccess: (_data, indexName) => {
      queryClient.invalidateQueries({ queryKey: ['qsStatus', indexName] });
      toast({
        title: 'Build triggered',
        description: 'Rebuilding Query Suggestions index.',
      });
    },
    onError: (error: Error) => {
      toast({
        variant: 'destructive',
        title: 'Failed to trigger build',
        description: error.message || 'An error occurred.',
      });
    },
  });
}
