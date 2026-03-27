/**
 * @module React Query hooks for fetching, updating, and deriving data from index settings via the Flapjack API.
 */
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useMemo } from 'react';
import api from '@/lib/api';
import type { IndexSettings } from '@/lib/types';
import { useToast } from '@/hooks/use-toast';

interface UpdateSettingsResponse {
  unsupportedParams?: string[];
}

function formatUnsupportedParams(params: string[]): string {
  return `Unsupported settings were ignored: ${params.join(', ')}.`;
}

export function useSettings(indexName: string) {
  return useQuery({
    queryKey: ['settings', indexName],
    queryFn: async () => {
      const response = await api.get<IndexSettings>(
        `/1/indexes/${indexName}/settings`
      );
      return response.data;
    },
    enabled: !!indexName,
  });
}

export function useEmbedderNames(indexName: string) {
  const { data: settings, isLoading } = useSettings(indexName);
  const embedderNames = useMemo(
    () => Object.keys(settings?.embedders || {}).sort(),
    [settings?.embedders]
  );
  return { embedderNames, isLoading };
}

/**
 * Returns a mutation that PUTs partial index settings to the API.
 * 
 * On success, invalidates both the `settings` and `search` query caches for the given index and shows a success toast. On failure, shows a destructive error toast.
 * 
 * @param indexName - Name of the index whose settings will be updated.
 * @returns A TanStack Query `UseMutationResult` whose `mutate`/`mutateAsync` accept `Partial<IndexSettings>`.
 */
export function useUpdateSettings(indexName: string) {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation({
    mutationFn: async (settings: Partial<IndexSettings>) => {
      const response = await api.put<UpdateSettingsResponse>(
        `/1/indexes/${indexName}/settings`,
        settings
      );
      return response;
    },
    onSuccess: (response) => {
      // Invalidate settings cache
      queryClient.invalidateQueries({ queryKey: ['settings', indexName] });
      // Also invalidate search results as settings affect them
      queryClient.invalidateQueries({ queryKey: ['search', indexName] });

      if (response.status === 207 && response.data.unsupportedParams?.length) {
        toast({
          title: 'Settings partially saved',
          description: formatUnsupportedParams(response.data.unsupportedParams),
        });
        return;
      }

      toast({
        title: 'Settings saved',
        description: `Settings for ${indexName} have been updated successfully.`,
      });
    },
    onError: (error: Error) => {
      toast({
        variant: 'destructive',
        title: 'Failed to save settings',
        description: error.message || 'An error occurred while saving settings.',
      });
    },
  });
}
