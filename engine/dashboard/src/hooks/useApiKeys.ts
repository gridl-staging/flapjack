/**
 * @module React Query hooks for managing API keys, providing list, create, and delete operations with automatic cache invalidation and toast notifications.
 */
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import api from '@/lib/api';
import type { ApiKey, ApiKeyCreateResponse } from '@/lib/types';
import { useToast } from '@/hooks/use-toast';
import { apiKeysKeys } from '@/lib/queryKeys';

type CreateApiKeyParams = {
  description?: string;
  acl: string[];
  indexes?: string[];
  restrictSources?: string[];
  expiresAt?: number;
  maxHitsPerQuery?: number;
  maxQueriesPerIPPerHour?: number;
};

type ToastFn = ReturnType<typeof useToast>['toast'];
type QueryClient = ReturnType<typeof useQueryClient>;

async function fetchApiKeys() {
  const response = await api.get<{ keys: ApiKey[] }>('/1/keys');
  return response.data.keys;
}

function invalidateApiKeys(queryClient: QueryClient) {
  queryClient.invalidateQueries({ queryKey: apiKeysKeys.all });
}

function showMutationErrorToast(
  toast: ToastFn,
  title: string,
  fallbackDescription: string,
  error: Error,
) {
  toast({
    variant: 'destructive',
    title,
    description: error.message || fallbackDescription,
  });
}

export function useApiKeys() {
  return useQuery({
    queryKey: apiKeysKeys.all,
    queryFn: fetchApiKeys,
  });
}

/**
 * Creates a new API key with the specified permissions and constraints.
 * Invalidates the `apiKeys` query cache on success and displays a toast notification.
 * @returns A TanStack Query mutation for creating an API key via `POST /1/keys`.
 */
export function useCreateApiKey() {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation({
    mutationFn: async (params: CreateApiKeyParams) => {
      const response = await api.post<ApiKeyCreateResponse>('/1/keys', params);
      return response.data;
    },
    onSuccess: () => {
      invalidateApiKeys(queryClient);
      toast({
        title: 'API key created',
        description: 'Your new API key has been created successfully.',
      });
    },
    onError: (error: Error) => {
      showMutationErrorToast(
        toast,
        'Failed to create API key',
        'An error occurred while creating the API key.',
        error,
      );
    },
  });
}

/**
 * Deletes an existing API key by its value.
 * Invalidates the `apiKeys` query cache on success and displays a toast notification.
 * @returns A TanStack Query mutation that accepts a key value string and calls `DELETE /1/keys/:keyValue`.
 */
export function useDeleteApiKey() {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation({
    mutationFn: async (keyValue: string) => {
      await api.delete(`/1/keys/${encodeURIComponent(keyValue)}`);
    },
    onSuccess: () => {
      invalidateApiKeys(queryClient);
      toast({
        title: 'API key deleted',
        description: 'The API key has been deleted successfully.',
      });
    },
    onError: (error: Error) => {
      showMutationErrorToast(
        toast,
        'Failed to delete API key',
        'An error occurred while deleting the API key.',
        error,
      );
    },
  });
}
