/**
 * @module React Query hooks for performing full-text search and facet search against a Flapjack index via the Algolia-compatible REST API.
 */
import { useQuery, keepPreviousData } from '@tanstack/react-query';
import api from '@/lib/api';
import type { SearchParams, SearchResponse, Document } from '@/lib/types';

interface UseSearchOptions {
  indexName: string;
  params: SearchParams;
  enabled?: boolean;
  userToken?: string;
  keepPrevious?: boolean;
}

/**
 * Executes a search query against a specific index using React Query.
 * 
 * Sends a POST to `/1/indexes/{indexName}/query` with the given search params (analytics disabled by default). The query is always treated as stale and retries are disabled. When `keepPrevious` is true, the previous result set is shown as placeholder data while a new query loads.
 * 
 * @param options.indexName - Target index to search against; query is disabled when empty.
 * @param options.params - Search parameters forwarded in the request body.
 * @param options.enabled - Whether the query should execute (default `true`).
 * @param options.userToken - Optional user token sent via the `x-algolia-usertoken` header for personalization/analytics.
 * @param options.keepPrevious - When true, retains the previous result set as placeholder data during refetches.
 * @returns A React Query result containing a `SearchResponse<T>` with hits and metadata.
 */
export function useSearch<T = Document>({ indexName, params, enabled = true, userToken, keepPrevious }: UseSearchOptions) {
  return useQuery({
    queryKey: ['search', indexName, params],
    queryFn: async () => {
      const payload = { analytics: false, ...params };
      const headers: Record<string, string> = {};
      if (userToken) {
        headers['x-algolia-usertoken'] = userToken;
      }
      const response = await api.post<SearchResponse<T>>(
        `/1/indexes/${indexName}/query`,
        payload,
        { headers }
      );
      return response.data;
    },
    enabled: enabled && !!indexName,
    staleTime: 0, // Always refetch for fresh results
    retry: false,
    placeholderData: keepPrevious ? keepPreviousData : undefined,
  });
}

export function useFacetSearch(indexName: string, facetName: string, facetQuery?: string) {
  return useQuery({
    queryKey: ['facetSearch', indexName, facetName, facetQuery],
    queryFn: async () => {
      const response = await api.post(
        `/1/indexes/${indexName}/facets/${facetName}/query`,
        { facetQuery }
      );
      return response.data;
    },
    enabled: !!indexName && !!facetName,
  });
}
