/**
 * @module React Query hook that introspects an index's schema by fetching a sample document and inferring field types at runtime.
 */
import { useQuery } from '@tanstack/react-query';
import api from '@/lib/api';
import type { FieldInfo, SearchResponse } from '@/lib/types';
export type { FieldInfo } from '@/lib/types';

function inferType(value: unknown): 'text' | 'number' | 'boolean' {
  if (typeof value === 'number') return 'number';
  if (typeof value === 'boolean') return 'boolean';
  return 'text';
}

/**
 * Fetches and infers field names and types for a given index by sampling the first hit.
 * Performs a minimal search query (1 hit, empty query) and derives each field's type via runtime inspection, excluding `objectID` and underscore-prefixed internal fields.
 * @param indexName - Name of the index to introspect.
 * @param enabled - Whether the query should execute; defaults to `true`. The query is also disabled when `indexName` is falsy.
 * @returns A React Query result containing an array of discovered fields with inferred types.
 */
export function useIndexFields(indexName: string, enabled = true) {
  return useQuery<FieldInfo[]>({
    queryKey: ['index-fields', indexName],
    queryFn: async () => {
      const { data } = await api.post<SearchResponse>(
        `/1/indexes/${indexName}/query`,
        { query: '', hitsPerPage: 1 }
      );
      if (!data.hits || data.hits.length === 0) return [];

      const sample = data.hits[0];
      return Object.entries(sample)
        .filter(([key]) => key !== 'objectID' && !key.startsWith('_'))
        .map(([key, value]) => ({
          name: key,
          type: inferType(value),
        }));
    },
    enabled: enabled && !!indexName,
    staleTime: 60000,
  });
}
