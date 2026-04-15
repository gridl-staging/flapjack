import { describe, expect, it } from 'vitest'
import {
  buildRuleDescription,
  createEmptyRule,
  createMerchandisingRule,
  normalizeRule,
  normalizeRuleForSerialization,
  parseRuleEditorJson,
  prepareRuleEditorSave,
  validateRule,
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

  it('normalizes rule payloads for serialization by trimming and removing empty fields', () => {
    const normalized = normalizeRuleForSerialization({
      objectID: 'rule-a',
      conditions: [
        { pattern: '  hello  ', anchoring: 'contains' },
        { pattern: '   ' },
      ],
      consequence: {
        promote: [],
        hide: [],
        params: {
          filters: '',
          hitsPerPage: 20,
        },
      },
      validity: [],
    } as any)

    expect(normalized.conditions).toEqual([{ pattern: 'hello', anchoring: 'contains' }])
    expect(normalized.consequence).toEqual({ params: { hitsPerPage: 20 } })
    expect(normalized.validity).toBeUndefined()
  })

  it('validates rule editor edge cases before save', () => {
    const errors = validateRule({
      objectID: 'rule-b',
      conditions: [{ pattern: 'hello' }],
      consequence: {
        promote: [
          { objectID: 'doc-1', position: 0 },
          { objectID: 'doc-1', position: 1 },
        ],
        userData: '{bad json',
        params: {
          renderingContent: '{bad json' as any,
        },
      },
    } as any)

    expect(errors).toContain('Condition 1: anchoring is required when pattern is provided.')
    expect(errors).toContain('Invalid JSON in User Data field.')
    expect(errors).toContain('Invalid JSON in Rendering Content field.')
    expect(errors).toContain('Duplicate objectID in promoted items.')
  })

  it('prepares form-mode rule saves by parsing JSON fields into objects', () => {
    const result = prepareRuleEditorSave({
      objectID: 'rule-c',
      conditions: [{ pattern: 'iphone', anchoring: 'contains' }],
      consequence: {
        userData: '{"redirect":"/sale"}',
        params: {
          renderingContent: '{"widgets":{"banner":true}}' as any,
        },
      },
    } as any)

    expect(result.error).toBeUndefined()
    expect(result.rule).toEqual({
      objectID: 'rule-c',
      conditions: [{ pattern: 'iphone', anchoring: 'contains' }],
      consequence: {
        userData: { redirect: '/sale' },
        params: {
          renderingContent: { widgets: { banner: true } },
        },
      },
    })
    expect(result.json).toContain('"redirect": "/sale"')
    expect(result.json).toContain('"banner": true')
  })
})
