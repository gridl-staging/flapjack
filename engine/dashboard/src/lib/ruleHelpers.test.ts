import { describe, expect, it } from 'vitest'
import {
  buildRuleDescription,
  createEmptyRule,
  createMerchandisingRule,
  normalizeRule,
  parseRuleEditorJson,
} from './ruleHelpers'

describe('ruleHelpers', () => {
  it('creates an empty rule with predictable defaults', () => {
    const rule = createEmptyRule(12345)

    expect(rule).toEqual({
      objectID: 'rule-12345',
      conditions: [{ pattern: '' }],
      consequence: {},
      description: '',
      enabled: true,
    })
  })

  it('describes context-only and filters-only conditions without requiring pattern', () => {
    const contextRule = normalizeRule({
      objectID: 'ctx-rule',
      conditions: [{ context: 'mobile' }],
      consequence: {},
    } as any)

    const filterRule = normalizeRule({
      objectID: 'filter-rule',
      conditions: [{ filters: 'brand:Apple' }],
      consequence: {},
    } as any)

    expect(buildRuleDescription(contextRule)).toContain('context "mobile"')
    expect(buildRuleDescription(filterRule)).toContain('filters "brand:Apple"')
  })

  it('normalizes missing conditions to an empty array', () => {
    const normalized = normalizeRule({
      objectID: 'no-conditions',
      consequence: {},
    } as any)

    expect(normalized.conditions).toEqual([])
    expect(normalized.consequence).toEqual({})
  })

  it('parses editor JSON and reports validation errors', () => {
    const missingConsequence = parseRuleEditorJson('{"objectID":"rule-a"}')
    const invalidJson = parseRuleEditorJson('{"objectID":')
    const invalidObjectIdType = parseRuleEditorJson('{"objectID":123,"consequence":{}}')
    const invalidConsequenceType = parseRuleEditorJson('{"objectID":"rule-a","consequence":[]}')

    expect(missingConsequence.error).toBe('consequence is required')
    expect(missingConsequence.rule).toBeUndefined()
    expect(invalidJson.error).toMatch(/Unexpected/)
    expect(invalidJson.rule).toBeUndefined()
    expect(invalidObjectIdType.error).toBe('objectID must be a non-empty string')
    expect(invalidObjectIdType.rule).toBeUndefined()
    expect(invalidConsequenceType.error).toBe('consequence must be an object')
    expect(invalidConsequenceType.rule).toBeUndefined()
  })

  it('builds merchandising rules with promote and hide arrays', () => {
    const merchRule = createMerchandisingRule({
      query: 'iphone case',
      description: 'Pin best-selling case',
      pins: [{ objectID: 'sku-1', position: 0 }],
      hides: [{ objectID: 'sku-2' }],
      timestamp: 999,
    })

    expect(merchRule.objectID).toBe('merch-iphone-case-999')
    expect(merchRule.conditions).toEqual([{ pattern: 'iphone case', anchoring: 'is' }])
    expect(merchRule.consequence.promote).toEqual([{ objectID: 'sku-1', position: 0 }])
    expect(merchRule.consequence.hide).toEqual([{ objectID: 'sku-2' }])
    expect(merchRule.description).toBe('Pin best-selling case')
    expect(merchRule.enabled).toBe(true)
  })
})
