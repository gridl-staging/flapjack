/**
 * @module React Query hooks for searching, saving, deleting, and clearing synonyms on a Flapjack index.
 */
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import api from '@/lib/api';
import type { Synonym, SynonymSearchResponse } from '@/lib/types';
import { useToast } from '@/hooks/use-toast';

interface UseSynonymsOptions {
  indexName: string;
  query?: string;
  type?: string;
  page?: number;
  hitsPerPage?: number;
}

export function useSynonyms({ indexName, query = '', type, page = 0, hitsPerPage = 50 }: UseSynonymsOptions) {
  return useQuery({
    queryKey: ['synonyms', indexName, query, type, page, hitsPerPage],
    queryFn: async () => {
      const response = await api.post<SynonymSearchResponse>(
        `/1/indexes/${indexName}/synonyms/search`,
        { query, type, page, hitsPerPage }
      );
      return response.data;
    },
    enabled: !!indexName,
  });
}

/**
 * Returns a mutation that creates or updates a synonym by PUT-ing it to the index endpoint.
 * Invalidates the synonyms query cache and shows a toast on success or failure.
 * @param indexName - Target index name.
 * @returns A TanStack Query mutation whose `mutate` accepts a `Synonym` object.
 */
export function useSaveSynonym(indexName: string) {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation({
    mutationFn: async (synonym: Synonym) => {
      const response = await api.put(
        `/1/indexes/${indexName}/synonyms/${synonym.objectID}`,
        synonym
      );
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['synonyms', indexName] });
      toast({ title: 'Synonym saved' });
    },
    onError: (error: any) => {
      toast({
        title: 'Failed to save synonym',
        description: error.response?.data || error.message,
        variant: 'destructive',
      });
    },
  });
}

/**
 * Returns a mutation that deletes a single synonym by its object ID.
 * Invalidates the synonyms query cache and shows a toast on success or failure.
 * @param indexName - Target index name.
 * @returns A TanStack Query mutation whose `mutate` accepts the synonym `objectID` string.
 */
export function useDeleteSynonym(indexName: string) {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation({
    mutationFn: async (objectID: string) => {
      const response = await api.delete(
        `/1/indexes/${indexName}/synonyms/${objectID}`
      );
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['synonyms', indexName] });
      toast({ title: 'Synonym deleted' });
    },
    onError: (error: any) => {
      toast({
        title: 'Failed to delete synonym',
        description: error.response?.data || error.message,
        variant: 'destructive',
      });
    },
  });
}

/**
 * Returns a mutation that removes all synonyms from the given index.
 * Invalidates the synonyms query cache and shows a toast on success or failure.
 * @param indexName - Target index name.
 * @returns A TanStack Query mutation (no arguments needed for `mutate`).
 */
export function useClearSynonyms(indexName: string) {
  const queryClient = useQueryClient();
  const { toast } = useToast();

  return useMutation({
    mutationFn: async () => {
      const response = await api.post(`/1/indexes/${indexName}/synonyms/clear`);
      return response.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['synonyms', indexName] });
      toast({ title: 'All synonyms cleared' });
    },
    onError: (error: any) => {
      toast({
        title: 'Failed to clear synonyms',
        description: error.response?.data || error.message,
        variant: 'destructive',
      });
    },
  });
}
