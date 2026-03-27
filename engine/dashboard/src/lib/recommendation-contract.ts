export const DEFAULT_RECOMMENDATION_THRESHOLD = 0;
export const DEFAULT_RECOMMENDATION_MAX_RECOMMENDATIONS = 30;
export const RECOMMENDATIONS_PREVIEW_PATH = '/1/indexes/*/recommendations';

export const RECOMMENDATION_MODEL_METADATA = [
  {
    id: 'related-products',
    label: 'Related Products',
    requiresObjectID: true,
    requiresFacetName: false,
  },
  {
    id: 'bought-together',
    label: 'Bought Together',
    requiresObjectID: true,
    requiresFacetName: false,
  },
  {
    id: 'trending-items',
    label: 'Trending Items',
    requiresObjectID: false,
    requiresFacetName: false,
  },
  {
    id: 'trending-facets',
    label: 'Trending Facets',
    requiresObjectID: false,
    requiresFacetName: true,
  },
  {
    id: 'looking-similar',
    label: 'Looking Similar',
    requiresObjectID: true,
    requiresFacetName: false,
  },
] as const;

export type RecommendationModelId = (typeof RECOMMENDATION_MODEL_METADATA)[number]['id'];
export type RecommendationModelMetadata = (typeof RECOMMENDATION_MODEL_METADATA)[number];

type RecommendationPreviewDefaults = {
  threshold?: number;
  maxRecommendations?: number;
};

export function withRecommendationPreviewDefaults<T extends RecommendationPreviewDefaults>(
  request: T,
): T & { threshold: number; maxRecommendations: number } {
  return {
    ...request,
    threshold: request.threshold ?? DEFAULT_RECOMMENDATION_THRESHOLD,
    maxRecommendations:
      request.maxRecommendations ?? DEFAULT_RECOMMENDATION_MAX_RECOMMENDATIONS,
  };
}

export function buildRecommendationBatchPayload<T extends RecommendationPreviewDefaults>(
  input: T | T[],
) {
  const requests = Array.isArray(input) ? input : [input];
  return { requests: requests.map(withRecommendationPreviewDefaults) };
}
