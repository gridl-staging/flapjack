/**
 * @module React Query hooks for searching, saving, deleting, and clearing index rules via the Flapjack API.
 */
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import api from '@/lib/api';
import type { Rule, RuleSearchResponse } from '@/lib/types';
import { rulesKeys } from '@/lib/queryKeys';
import { useToast } from '@/hooks/use-toast';

interface UseRulesOptions {
  indexName: string;
  query?: string;
  page?: number;
  hitsPerPage?: number;
}

export function useRules({ indexName, query = '', page = 0, hitsPerPage = 50 }: UseRulesOptions) {
  return useQuery({
    queryKey: rulesKeys.list(indexName, query, page, hitsPerPage),
    queryFn: async () => {
      const response = await api.post<RuleSearchResponse>(
        `/1/indexes/${indexName}/rules/search`,
        { query, page, hitsPerPage }
      );
      return response.data;
    },
    enabled: !!indexName,
  });
}

/**
 * Creates or updates a rule by PUT-ing it to the API.
 * 
 * Invalidates the rules query cache and shows a toast on success or failure.
 * 
 * @param indexName - Name of the index the rule belongs to.
 * @returns A React Query mutation whose `mutate` accepts a `Rule` object.
 */
export function useSaveRule(indexName: string) {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation({
    mutationFn: async (rule: Rule) => {
      const response = await api.put(
        `/1/indexes/${indexName}/rules/${rule.objectID}`,
        rule
      );
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: rulesKeys.index(indexName) });
      toast({ title: 'Rule saved' });
    },
    onError: (error: any) => {
      toast({
        title: 'Failed to save rule',
        description: error.response?.data || error.message,
        variant: 'destructive',
      });
    },
  });
}

/**
 * Deletes a single rule by its object ID.
 * 
 * Invalidates the rules query cache and shows a toast on success or failure.
 * 
 * @param indexName - Name of the index the rule belongs to.
 * @returns A React Query mutation whose `mutate` accepts the rule's `objectID` string.
 */
export function useDeleteRule(indexName: string) {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation({
    mutationFn: async (objectID: string) => {
      const response = await api.delete(
        `/1/indexes/${indexName}/rules/${objectID}`
      );
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: rulesKeys.index(indexName) });
      toast({ title: 'Rule deleted' });
    },
    onError: (error: any) => {
      toast({
        title: 'Failed to delete rule',
        description: error.response?.data || error.message,
        variant: 'destructive',
      });
    },
  });
}

/**
 * Removes all rules from the given index.
 * 
 * Invalidates the rules query cache and shows a toast on success or failure.
 * 
 * @param indexName - Name of the index to clear rules from.
 * @returns A React Query mutation with a parameterless `mutate`.
 */
export function useClearRules(indexName: string) {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation({
    mutationFn: async () => {
      const response = await api.post(`/1/indexes/${indexName}/rules/clear`);
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: rulesKeys.index(indexName) });
      toast({ title: 'All rules cleared' });
    },
    onError: (error: any) => {
      toast({
        title: 'Failed to clear rules',
        description: error.response?.data || error.message,
        variant: 'destructive',
      });
    },
  });
}
