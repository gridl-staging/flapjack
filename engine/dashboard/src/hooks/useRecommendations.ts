import { useMutation } from '@tanstack/react-query';
import api from '@/lib/api';
import { recommendationKeys } from '@/lib/queryKeys';
import {
  buildRecommendationBatchPayload,
  RECOMMENDATIONS_PREVIEW_PATH,
} from '@/lib/recommendation-contract';
import type {
  RecommendationBatchResponse,
  RecommendationRequest,
  RecommendationResult,
} from '@/lib/types';

export type RecommendationsPreviewInput = RecommendationRequest | RecommendationRequest[];

export function useRecommendations() {
  return useMutation<RecommendationResult[], Error, RecommendationsPreviewInput>({
    mutationKey: recommendationKeys.all,
    mutationFn: async (input) => {
      const response = await api.post<RecommendationBatchResponse>(
        RECOMMENDATIONS_PREVIEW_PATH,
        buildRecommendationBatchPayload(input),
      );
      return response.data.results;
    },
  });
}
