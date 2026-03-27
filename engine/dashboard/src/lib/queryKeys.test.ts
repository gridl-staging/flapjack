import { describe, expect, it } from 'vitest'
import { analyticsKeys, personalizationKeys, rulesKeys } from './queryKeys'

describe('queryKeys', () => {
  it('builds stable rule query keys for list and index invalidation', () => {
    expect(rulesKeys.all).toEqual(['rules'])
    expect(rulesKeys.index('products')).toEqual(['rules', 'products'])
    expect(rulesKeys.list('products', 'apple', 2, 25)).toEqual([
      'rules',
      'products',
      { query: 'apple', page: 2, hitsPerPage: 25 },
    ])
  })

  it('exposes shared analytics key root', () => {
    expect(analyticsKeys.all).toEqual(['analytics'])
  })

  it('builds stable personalization query keys', () => {
    expect(personalizationKeys.all).toEqual(['personalization'])
    expect(personalizationKeys.strategy()).toEqual(['personalization', 'strategy'])
    expect(personalizationKeys.profile('user-1')).toEqual(['personalization', 'profile', 'user-1'])
  })
})
