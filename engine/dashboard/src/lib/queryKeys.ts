const RULES_ROOT = ['rules'] as const
const ANALYTICS_ROOT = ['analytics'] as const
const PERSONALIZATION_ROOT = ['personalization'] as const
const RECOMMENDATIONS_ROOT = ['recommendations'] as const
const API_KEYS_ROOT = ['apiKeys'] as const
const DICTIONARIES_ROOT = ['dictionaries'] as const
const SECURITY_SOURCES_ROOT = ['securitySources'] as const

export const rulesKeys = {
  all: RULES_ROOT,
  index: (indexName: string) => [...RULES_ROOT, indexName] as const,
  list: (indexName: string, query = '', page = 0, hitsPerPage = 50) =>
    [...RULES_ROOT, indexName, { query, page, hitsPerPage }] as const,
}

export const analyticsKeys = {
  all: ANALYTICS_ROOT,
}

export const personalizationKeys = {
  all: PERSONALIZATION_ROOT,
  strategy: () => [...PERSONALIZATION_ROOT, 'strategy'] as const,
  profile: (userToken: string) => [...PERSONALIZATION_ROOT, 'profile', userToken] as const,
}

export const recommendationKeys = {
  all: RECOMMENDATIONS_ROOT,
  index: (indexName: string) => [...RECOMMENDATIONS_ROOT, indexName] as const,
  preview: (indexName: string) => [...RECOMMENDATIONS_ROOT, indexName, 'preview'] as const,
}

export const apiKeysKeys = {
  all: API_KEYS_ROOT,
}

export const dictionariesKeys = {
  all: DICTIONARIES_ROOT,
  dictionary: (dictName: string) => [...DICTIONARIES_ROOT, dictName] as const,
  list: (dictName: string, query = '', page = 0, hitsPerPage = 50) =>
    [...DICTIONARIES_ROOT, dictName, { query, page, hitsPerPage }] as const,
}

export const securitySourcesKeys = {
  all: SECURITY_SOURCES_ROOT,
  list: () => [...SECURITY_SOURCES_ROOT, 'list'] as const,
}
