/**
 * @module React Query hook that polls the debug events API with optional filters for index, event type, status, and time range.
 */
import { useQuery, keepPreviousData } from '@tanstack/react-query';
import api from '@/lib/api';

export interface DebugEvent {
  timestampMs: number;
  index: string;
  eventType: string;
  eventSubtype: string | null;
  eventName: string;
  userToken: string;
  objectIds: string[];
  httpCode: number;
  validationErrors: string[];
}

export interface DebugEventsResponse {
  events: DebugEvent[];
  count: number;
}

export interface DebugEventsFilters {
  index?: string;
  eventType?: string;
  status?: string;
  limit?: number;
  from?: number;
  until?: number;
}

/**
 * Polls the debug events endpoint at a configurable interval, returning timestamped engine events filtered by index, type, status, and time range.
 * @param filters - Query filters forwarded as URL search params to `/1/events/debug`.
 * @param pollInterval - Refetch interval in milliseconds (default 5 000) or `false` to disable interval polling.
 * @returns A React Query result containing the matching events and their count.
 */
export function useDebugEvents(filters: DebugEventsFilters, pollInterval: number | false = 5000) {
  return useQuery<DebugEventsResponse>({
    queryKey: ['debugEvents', filters],
    queryFn: async () => {
      const params = new URLSearchParams();
      if (filters.index) params.set('index', filters.index);
      if (filters.eventType) params.set('eventType', filters.eventType);
      if (filters.status) params.set('status', filters.status);
      if (filters.limit) params.set('limit', String(filters.limit));
      if (filters.from !== undefined) params.set('from', String(filters.from));
      if (filters.until !== undefined) params.set('until', String(filters.until));
      const qs = params.toString();
      const url = `/1/events/debug${qs ? `?${qs}` : ''}`;
      const { data } = await api.get(url);
      return data;
    },
    refetchInterval: pollInterval,
    staleTime: 2000,
    retry: 1,
    placeholderData: keepPreviousData,
  });
}
