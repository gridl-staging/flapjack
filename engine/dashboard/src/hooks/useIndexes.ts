/**
 * @module React Query hooks for CRUD operations on indexes, including listing, creation, deletion, and compaction, with automatic cache invalidation and toast notifications.
 */
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import api from '@/lib/api';
import { Index } from '@/lib/types';
import { useToast } from '@/hooks/use-toast';

/**
 * Fetches all indexes from the API with a 30-second cache.
 * 
 * Normalizes the response by mapping `name` to `uid` for backward compatibility across API versions.
 * 
 * @returns A React Query result containing an array of indexes.
 */
export function useIndexes() {
  return useQuery<Index[]>({
    queryKey: ['indexes'],
    queryFn: async () => {
      const { data } = await api.get('/1/indexes');
      const items = data.results || data.items || data || [];
      // Map 'name' to 'uid' for compatibility
      return items.map((item: any) => ({
        ...item,
        uid: item.uid || item.name,
      }));
    },
    staleTime: 30000, // 30s cache
    retry: 1,
  });
}

/**
 * Mutation hook that creates a new index via the API.
 * 
 * On success, invalidates the indexes query cache and shows a success toast. On failure, shows a destructive toast with the error message.
 * 
 * @returns A React Query mutation that accepts `{ uid: string }` as its parameter.
 */
export function useCreateIndex() {
  const queryClient = useQueryClient();
  const { toast } = useToast();
  return useMutation({
    mutationFn: async (params: { uid: string }) => {
      const { data } = await api.post('/1/indexes', params);
      return data;
    },
    onSuccess: (_data, variables) => {
      queryClient.invalidateQueries({ queryKey: ['indexes'] });
      toast({ title: 'Index created', description: `Index "${variables.uid}" has been created.` });
    },
    onError: (error: Error) => {
      toast({ variant: 'destructive', title: 'Failed to create index', description: error.message });
    },
  });
}

/**
 * Mutation hook that deletes an index by name via the API.
 * 
 * On success, invalidates the indexes query cache and shows a success toast. On failure, shows a destructive toast with the error message.
 * 
 * @returns A React Query mutation that accepts the index name as its parameter.
 */
export function useDeleteIndex() {
  const queryClient = useQueryClient();
  const { toast } = useToast();
  return useMutation({
    mutationFn: async (indexName: string) => {
      await api.delete(`/1/indexes/${indexName}`);
    },
    onSuccess: (_data, indexName) => {
      queryClient.invalidateQueries({ queryKey: ['indexes'] });
      toast({ title: 'Index deleted', description: `Index "${indexName}" has been deleted.` });
    },
    onError: (error: Error) => {
      toast({ variant: 'destructive', title: 'Failed to delete index', description: error.message });
    },
  });
}

/**
 * Mutation hook that triggers compaction on an index via the API.
 * 
 * Compaction is an asynchronous server-side operation; the toast confirms the request was accepted, not that compaction is complete. Invalidates the indexes query cache on success.
 * 
 * @returns A React Query mutation that accepts the index name as its parameter.
 */
export function useCompactIndex() {
  const queryClient = useQueryClient();
  const { toast } = useToast();
  return useMutation({
    mutationFn: async (indexName: string) => {
      const { data } = await api.post(`/1/indexes/${indexName}/compact`);
      return data;
    },
    onSuccess: (_data, indexName) => {
      queryClient.invalidateQueries({ queryKey: ['indexes'] });
      toast({ title: 'Compaction started', description: `Index "${indexName}" is being compacted.` });
    },
    onError: (error: Error) => {
      toast({ variant: 'destructive', title: 'Failed to compact index', description: error.message });
    },
  });
}

export function useIndexStats(indexName: string) {
  return useQuery({
    queryKey: ['index-stats', indexName],
    queryFn: async () => {
      const { data } = await api.get(`/1/indexes/${indexName}`);
      return data;
    },
    enabled: !!indexName,
  });
}
