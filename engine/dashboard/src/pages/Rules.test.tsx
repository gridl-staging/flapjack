import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { TEST_ROUTER_FUTURE } from '@/test/routerFuture'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { IndexLayout } from '@/components/layout/IndexLayout'
import { Rules } from './Rules'

const hooksMocks = vi.hoisted(() => ({
  saveRuleMutate: vi.fn().mockResolvedValue({}),
  deleteRuleMutate: vi.fn().mockResolvedValue({}),
  clearRulesMutate: vi.fn().mockResolvedValue({}),
}))

vi.mock('@/hooks/useRules', () => ({
  useRules: vi.fn(),
  useSaveRule: () => ({ mutateAsync: hooksMocks.saveRuleMutate, isPending: false }),
  useDeleteRule: () => ({ mutateAsync: hooksMocks.deleteRuleMutate, isPending: false }),
  useClearRules: () => ({ mutateAsync: hooksMocks.clearRulesMutate, isPending: false }),
}))

vi.mock('@/hooks/useSettings', () => ({
  useSettings: vi.fn(),
}))

// Monaco editor isn't available in jsdom — mock the lazy-loaded dialog internals
vi.mock('@monaco-editor/react', () => ({
  default: ({ value, onChange }: { value: string; onChange: (v: string) => void }) => (
    <textarea
      data-testid="monaco-editor"
      value={value}
      onChange={(e) => onChange(e.target.value)}
    />
  ),
}))

import { useRules } from '@/hooks/useRules'
import { useSettings } from '@/hooks/useSettings'

function wrapper({ children }: { children: React.ReactNode }) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return (
    <QueryClientProvider client={qc}>
      <MemoryRouter future={TEST_ROUTER_FUTURE} initialEntries={['/index/products/rules']}>
        <Routes>
          <Route path="/index/:indexName" element={<IndexLayout />}>
            <Route path="rules" element={children} />
          </Route>
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>
  )
}

function noIndexWrapper({ children }: { children: React.ReactNode }) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return (
    <QueryClientProvider client={qc}>
      <MemoryRouter future={TEST_ROUTER_FUTURE} initialEntries={['/rules']}>
        <Routes>
          <Route path="/rules" element={children} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>
  )
}

const RULE_ENABLED = {
  objectID: 'boost-apple',
  conditions: [{ pattern: 'apple', anchoring: 'contains' as const }],
  consequence: { promote: [{ objectID: 'prod-1', position: 0 }] },
  description: 'Boost Apple products',
  enabled: true,
}

const RULE_DISABLED = {
  objectID: 'hide-refurb',
  conditions: [],
  consequence: { hide: [{ objectID: 'prod-2' }, { objectID: 'prod-3' }] },
  description: '',
  enabled: false,
}

describe('Rules', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(useSettings).mockReturnValue({ data: { mode: 'keywordSearch' }, isLoading: false } as any)
    hooksMocks.saveRuleMutate.mockResolvedValue({})
    hooksMocks.deleteRuleMutate.mockResolvedValue({})
    hooksMocks.clearRulesMutate.mockResolvedValue({})
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('shows no-index state when indexName is missing from route', () => {
    vi.mocked(useRules).mockReturnValue({ data: undefined, isLoading: false } as any)
    render(<Rules />, { wrapper: noIndexWrapper })
    expect(screen.getByText('No index selected')).toBeInTheDocument()
  })

  it('shows loading state while fetching', () => {
    vi.mocked(useRules).mockReturnValue({ data: undefined, isLoading: true } as any)
    render(<Rules />, { wrapper })
    // No rules list while loading
    expect(screen.queryByTestId('rules-list')).not.toBeInTheDocument()
    // No empty-state message either
    expect(screen.queryByText('No rules')).not.toBeInTheDocument()
  })

  it('shows empty state when there are no rules', () => {
    vi.mocked(useRules).mockReturnValue({ data: { hits: [], nbHits: 0 }, isLoading: false } as any)
    render(<Rules />, { wrapper })
    expect(screen.getByText('No rules')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /create a rule/i })).toBeInTheDocument()
  })

  it('renders rule cards when rules exist', () => {
    vi.mocked(useRules).mockReturnValue({
      data: { hits: [RULE_ENABLED, RULE_DISABLED], nbHits: 2 },
      isLoading: false,
    } as any)
    render(<Rules />, { wrapper })
    expect(screen.getByTestId('rules-list')).toBeInTheDocument()
    expect(screen.getAllByTestId('rule-card')).toHaveLength(2)
    expect(screen.getByText('boost-apple')).toBeInTheDocument()
    expect(screen.getByText('Boost Apple products')).toBeInTheDocument()
  })

  it('shows promote/hide badges on rule cards', () => {
    vi.mocked(useRules).mockReturnValue({
      data: { hits: [RULE_ENABLED, RULE_DISABLED], nbHits: 2 },
      isLoading: false,
    } as any)
    render(<Rules />, { wrapper })
    expect(screen.getByText('1 pinned')).toBeInTheDocument()
    expect(screen.getByText('2 hidden')).toBeInTheDocument()
  })

  it('shows rules count badge in header', () => {
    vi.mocked(useRules).mockReturnValue({
      data: { hits: [RULE_ENABLED], nbHits: 1 },
      isLoading: false,
    } as any)
    render(<Rules />, { wrapper })
    expect(screen.getByTestId('rules-count-badge')).toHaveTextContent('1')
  })

  it('uses ConfirmDialog before deleting a rule', async () => {
    const user = userEvent.setup()
    const confirmSpy = vi.spyOn(window, 'confirm').mockImplementation(() => true)
    vi.mocked(useRules).mockReturnValue({
      data: { hits: [RULE_ENABLED], nbHits: 1 },
      isLoading: false,
    } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: 'Delete' }))

    expect(screen.getByRole('dialog')).toBeInTheDocument()
    expect(screen.getByText('Delete Rule')).toBeInTheDocument()
    expect(hooksMocks.deleteRuleMutate).not.toHaveBeenCalled()
    expect(confirmSpy).not.toHaveBeenCalled()

    await user.click(screen.getByRole('button', { name: 'Delete' }))
    expect(hooksMocks.deleteRuleMutate).toHaveBeenCalledWith('boost-apple')
  })

  it('uses ConfirmDialog before clearing all rules', async () => {
    const user = userEvent.setup()
    const confirmSpy = vi.spyOn(window, 'confirm').mockImplementation(() => true)
    vi.mocked(useRules).mockReturnValue({
      data: { hits: [RULE_ENABLED], nbHits: 1 },
      isLoading: false,
    } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: /clear all/i }))

    expect(screen.getByRole('dialog')).toBeInTheDocument()
    expect(screen.getByText('Delete All Rules')).toBeInTheDocument()
    expect(hooksMocks.clearRulesMutate).not.toHaveBeenCalled()
    expect(confirmSpy).not.toHaveBeenCalled()

    await user.click(screen.getByRole('button', { name: 'Delete All' }))
    expect(hooksMocks.clearRulesMutate).toHaveBeenCalledTimes(1)
  })

  it('opens rule editor in form mode by default and can switch to JSON mode', async () => {
    const user = userEvent.setup()
    vi.mocked(useRules).mockReturnValue({ data: { hits: [], nbHits: 0 }, isLoading: false } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: /add rule/i }))

    expect(screen.getByRole('dialog')).toBeInTheDocument()
    expect(screen.getByText('Create Rule')).toBeInTheDocument()
    expect(screen.getByRole('tab', { name: 'Form' })).toHaveAttribute('data-state', 'active')
    expect(screen.getByLabelText('Object ID')).toBeInTheDocument()
    expect(screen.queryByTestId('monaco-editor')).not.toBeInTheDocument()

    await user.click(screen.getByRole('tab', { name: 'JSON' }))
    expect(screen.getByRole('tab', { name: 'JSON' })).toHaveAttribute('data-state', 'active')
    expect(await screen.findByTestId('monaco-editor')).toBeInTheDocument()
  })

  it('saves rule edits from form mode', async () => {
    const user = userEvent.setup()
    vi.mocked(useRules).mockReturnValue({ data: { hits: [], nbHits: 0 }, isLoading: false } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: /add rule/i }))

    await user.clear(screen.getByLabelText('Object ID'))
    await user.type(screen.getByLabelText('Object ID'), 'rule-from-form')
    await user.type(screen.getByLabelText('Description'), 'Rule saved from form mode')
    await user.click(screen.getByRole('switch', { name: 'Enabled' }))
    await user.click(screen.getByRole('button', { name: 'Create' }))

    expect(hooksMocks.saveRuleMutate).toHaveBeenCalledTimes(1)
    expect(hooksMocks.saveRuleMutate).toHaveBeenCalledWith(
      expect.objectContaining({
        objectID: 'rule-from-form',
        description: 'Rule saved from form mode',
        enabled: false,
        conditions: [],
      })
    )
  })

  it('adds and removes conditions in form mode before saving', async () => {
    const user = userEvent.setup()
    vi.mocked(useRules).mockReturnValue({ data: { hits: [], nbHits: 0 }, isLoading: false } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: /add rule/i }))

    await user.click(screen.getByRole('button', { name: /add condition/i }))
    await user.type(screen.getByLabelText('Pattern 2'), 'iphone')
    await user.selectOptions(screen.getByLabelText('Anchoring 2'), 'contains')

    await user.click(screen.getByRole('button', { name: /remove condition 1/i }))
    await user.click(screen.getByRole('button', { name: 'Create' }))

    expect(hooksMocks.saveRuleMutate).toHaveBeenCalledTimes(1)
    expect(hooksMocks.saveRuleMutate).toHaveBeenCalledWith(
      expect.objectContaining({
        conditions: [{ pattern: 'iphone', anchoring: 'contains' }],
      })
    )
  })

  it('supports conditionless rules by allowing all conditions to be removed', async () => {
    const user = userEvent.setup()
    vi.mocked(useRules).mockReturnValue({ data: { hits: [], nbHits: 0 }, isLoading: false } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: /add rule/i }))
    await user.click(screen.getByRole('button', { name: /remove condition 1/i }))
    await user.click(screen.getByRole('button', { name: 'Create' }))

    expect(hooksMocks.saveRuleMutate).toHaveBeenCalledTimes(1)
    expect(hooksMocks.saveRuleMutate).toHaveBeenCalledWith(
      expect.objectContaining({
        conditions: [],
      })
    )
  })

  it('blocks saving when a condition has anchoring without a pattern', async () => {
    const user = userEvent.setup()
    vi.mocked(useRules).mockReturnValue({ data: { hits: [], nbHits: 0 }, isLoading: false } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: /add rule/i }))
    await user.selectOptions(screen.getByLabelText('Anchoring 1'), 'contains')
    await user.click(screen.getByRole('button', { name: 'Create' }))

    expect(hooksMocks.saveRuleMutate).not.toHaveBeenCalled()
    expect(
      screen.getByText('Condition 1: pattern is required when anchoring is selected.')
    ).toBeInTheDocument()
  })

  // --- Consequence editor: Promote section ---

  it('adds and removes promoted items in consequence editor', async () => {
    const user = userEvent.setup()
    vi.mocked(useRules).mockReturnValue({ data: { hits: [], nbHits: 0 }, isLoading: false } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: /add rule/i }))
    await user.click(screen.getByRole('button', { name: /remove condition 1/i }))

    // Add a promoted item
    await user.click(screen.getByRole('button', { name: /add promoted item/i }))
    await user.type(screen.getByLabelText('Promote Object ID 1'), 'prod-abc')
    await user.clear(screen.getByLabelText('Promote Position 1'))
    await user.type(screen.getByLabelText('Promote Position 1'), '0')

    // Add a second promoted item
    await user.click(screen.getByRole('button', { name: /add promoted item/i }))
    await user.type(screen.getByLabelText('Promote Object ID 2'), 'prod-xyz')
    await user.clear(screen.getByLabelText('Promote Position 2'))
    await user.type(screen.getByLabelText('Promote Position 2'), '3')

    // Remove first promoted item
    await user.click(screen.getByRole('button', { name: /remove promote 1/i }))

    await user.click(screen.getByRole('button', { name: 'Create' }))

    expect(hooksMocks.saveRuleMutate).toHaveBeenCalledTimes(1)
    const savedRule = hooksMocks.saveRuleMutate.mock.calls[0][0]
    expect(savedRule.consequence.promote).toEqual([
      { objectID: 'prod-xyz', position: 3 },
    ])
  })

  // --- Consequence editor: Hide section ---

  it('adds and removes hidden items in consequence editor', async () => {
    const user = userEvent.setup()
    vi.mocked(useRules).mockReturnValue({ data: { hits: [], nbHits: 0 }, isLoading: false } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: /add rule/i }))
    await user.click(screen.getByRole('button', { name: /remove condition 1/i }))

    // Add hidden items
    await user.click(screen.getByRole('button', { name: /add hidden item/i }))
    await user.type(screen.getByLabelText('Hide Object ID 1'), 'prod-hide-1')
    await user.click(screen.getByRole('button', { name: /add hidden item/i }))
    await user.type(screen.getByLabelText('Hide Object ID 2'), 'prod-hide-2')

    // Remove first hidden item
    await user.click(screen.getByRole('button', { name: /remove hide 1/i }))

    await user.click(screen.getByRole('button', { name: 'Create' }))

    expect(hooksMocks.saveRuleMutate).toHaveBeenCalledTimes(1)
    const savedRule = hooksMocks.saveRuleMutate.mock.calls[0][0]
    expect(savedRule.consequence.hide).toEqual([{ objectID: 'prod-hide-2' }])
  })

  // --- Consequence editor: Query modification ---

  it('saves query replacement as literal string', async () => {
    const user = userEvent.setup()
    vi.mocked(useRules).mockReturnValue({ data: { hits: [], nbHits: 0 }, isLoading: false } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: /add rule/i }))
    await user.click(screen.getByRole('button', { name: /remove condition 1/i }))

    // Select "Literal replacement" query mode
    await user.click(screen.getByLabelText('Literal replacement'))
    await user.type(screen.getByLabelText('Replacement query'), 'new query text')

    await user.click(screen.getByRole('button', { name: 'Create' }))

    expect(hooksMocks.saveRuleMutate).toHaveBeenCalledTimes(1)
    const savedRule = hooksMocks.saveRuleMutate.mock.calls[0][0]
    expect(savedRule.consequence.params?.query).toBe('new query text')
  })

  it('saves query word edits with remove and replace operations', async () => {
    const user = userEvent.setup()
    vi.mocked(useRules).mockReturnValue({ data: { hits: [], nbHits: 0 }, isLoading: false } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: /add rule/i }))
    await user.click(screen.getByRole('button', { name: /remove condition 1/i }))

    // Select "Word edits" query mode
    await user.click(screen.getByLabelText('Word edits'))
    // Add a remove edit
    await user.click(screen.getByRole('button', { name: /add edit/i }))
    await user.type(screen.getByLabelText('Edit Delete 1'), 'old-word')

    // Add a replace edit
    await user.click(screen.getByRole('button', { name: /add edit/i }))
    await user.selectOptions(screen.getByLabelText('Edit Type 2'), 'replace')
    await user.type(screen.getByLabelText('Edit Delete 2'), 'typo')
    await user.type(screen.getByLabelText('Edit Insert 2'), 'correct')

    await user.click(screen.getByRole('button', { name: 'Create' }))

    expect(hooksMocks.saveRuleMutate).toHaveBeenCalledTimes(1)
    const savedRule = hooksMocks.saveRuleMutate.mock.calls[0][0]
    expect(savedRule.consequence.params?.query).toEqual({
      edits: [
        { type: 'remove', delete: 'old-word' },
        { type: 'replace', delete: 'typo', insert: 'correct' },
      ],
    })
  })

  // --- Consequence editor: Filter params ---

  it('saves filter params (filters, hitsPerPage, filterPromotes)', async () => {
    const user = userEvent.setup()
    vi.mocked(useRules).mockReturnValue({ data: { hits: [], nbHits: 0 }, isLoading: false } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: /add rule/i }))
    await user.click(screen.getByRole('button', { name: /remove condition 1/i }))

    await user.type(screen.getByLabelText('Filters'), 'brand:Apple AND category:phone')
    await user.clear(screen.getByLabelText('Hits Per Page'))
    await user.type(screen.getByLabelText('Hits Per Page'), '20')
    await user.click(screen.getByLabelText('Filter Promotes'))

    await user.click(screen.getByRole('button', { name: 'Create' }))

    expect(hooksMocks.saveRuleMutate).toHaveBeenCalledTimes(1)
    const savedRule = hooksMocks.saveRuleMutate.mock.calls[0][0]
    expect(savedRule.consequence.params?.filters).toBe('brand:Apple AND category:phone')
    expect(savedRule.consequence.params?.hitsPerPage).toBe(20)
    expect(savedRule.consequence.filterPromotes).toBe(true)
  })

  // --- Consequence editor: userData ---

  it('saves userData as parsed JSON', async () => {
    const user = userEvent.setup()
    vi.mocked(useRules).mockReturnValue({ data: { hits: [], nbHits: 0 }, isLoading: false } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: /add rule/i }))
    await user.click(screen.getByRole('button', { name: /remove condition 1/i }))

    fireEvent.change(screen.getByLabelText('User Data (JSON)'), {
      target: { value: '{"redirect": "/sale"}' },
    })

    await user.click(screen.getByRole('button', { name: 'Create' }))

    expect(hooksMocks.saveRuleMutate).toHaveBeenCalledTimes(1)
    const savedRule = hooksMocks.saveRuleMutate.mock.calls[0][0]
    expect(savedRule.consequence.userData).toEqual({ redirect: '/sale' })
  })

  it('blocks save when userData is invalid JSON', async () => {
    const user = userEvent.setup()
    vi.mocked(useRules).mockReturnValue({ data: { hits: [], nbHits: 0 }, isLoading: false } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: /add rule/i }))
    await user.click(screen.getByRole('button', { name: /remove condition 1/i }))

    fireEvent.change(screen.getByLabelText('User Data (JSON)'), {
      target: { value: '{invalid json' },
    })

    await user.click(screen.getByRole('button', { name: 'Create' }))

    expect(hooksMocks.saveRuleMutate).not.toHaveBeenCalled()
    expect(screen.getByText('Invalid JSON in User Data field.')).toBeInTheDocument()
  })

  // --- Validity picker ---

  it('adds and removes validity time ranges', async () => {
    const user = userEvent.setup()
    vi.mocked(useRules).mockReturnValue({ data: { hits: [], nbHits: 0 }, isLoading: false } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: /add rule/i }))
    await user.click(screen.getByRole('button', { name: /remove condition 1/i }))

    // Add a validity time range
    await user.click(screen.getByRole('button', { name: /add time range/i }))
    // The from/until datetime-local inputs should be present
    expect(screen.getByLabelText('Valid From 1')).toBeInTheDocument()
    expect(screen.getByLabelText('Valid Until 1')).toBeInTheDocument()

    // Set date values
    await user.type(screen.getByLabelText('Valid From 1'), '2026-03-01T00:00')
    await user.type(screen.getByLabelText('Valid Until 1'), '2026-03-31T23:59')

    // Add a second time range then remove it
    await user.click(screen.getByRole('button', { name: /add time range/i }))
    expect(screen.getByLabelText('Valid From 2')).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: /remove time range 2/i }))
    expect(screen.queryByLabelText('Valid From 2')).not.toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Create' }))

    expect(hooksMocks.saveRuleMutate).toHaveBeenCalledTimes(1)
    const savedRule = hooksMocks.saveRuleMutate.mock.calls[0][0]
    expect(savedRule.validity).toHaveLength(1)
    expect(savedRule.validity[0].from).toBeGreaterThan(0)
    expect(savedRule.validity[0].until).toBeGreaterThan(savedRule.validity[0].from)
  })

  // --- JSON Preview ---

  it('shows live JSON preview that reflects form state', async () => {
    const user = userEvent.setup()
    vi.mocked(useRules).mockReturnValue({ data: { hits: [], nbHits: 0 }, isLoading: false } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: /add rule/i }))

    const preview = screen.getByTestId('rule-json-preview')
    expect(preview).toBeInTheDocument()

    // Change object ID and verify preview updates
    await user.clear(screen.getByLabelText('Object ID'))
    await user.type(screen.getByLabelText('Object ID'), 'test-preview')

    expect(preview.textContent).toContain('test-preview')
  })

  // --- Bidirectional sync: loading existing rule into form mode ---

  it('loads existing rule with consequences into form mode fields', async () => {
    const user = userEvent.setup()
    const existingRule = {
      objectID: 'existing-rule',
      conditions: [{ pattern: 'phone', anchoring: 'is' as const }],
      consequence: {
        promote: [{ objectID: 'prod-1', position: 0 }],
        hide: [{ objectID: 'prod-2' }],
        params: { filters: 'brand:Samsung', hitsPerPage: 10 },
        filterPromotes: true,
      },
      description: 'Existing rule desc',
      enabled: true,
    }
    vi.mocked(useRules).mockReturnValue({
      data: { hits: [existingRule], nbHits: 1 },
      isLoading: false,
    } as any)
    render(<Rules />, { wrapper })

    // Click Edit on the existing rule
    await user.click(screen.getByRole('button', { name: 'Edit' }))

    // Verify form fields are populated
    expect(screen.getByLabelText('Object ID')).toHaveValue('existing-rule')
    expect(screen.getByLabelText('Description')).toHaveValue('Existing rule desc')
    expect(screen.getByLabelText('Promote Object ID 1')).toHaveValue('prod-1')
    expect(screen.getByLabelText('Promote Position 1')).toHaveValue(0)
    expect(screen.getByLabelText('Hide Object ID 1')).toHaveValue('prod-2')
    expect(screen.getByLabelText('Filters')).toHaveValue('brand:Samsung')
    expect(screen.getByLabelText('Hits Per Page')).toHaveValue(10)
    expect(screen.getByLabelText('Filter Promotes')).toBeChecked()
  })

  // --- renderingContent JSON validation ---

  it('blocks save when renderingContent is invalid JSON', async () => {
    const user = userEvent.setup()
    vi.mocked(useRules).mockReturnValue({ data: { hits: [], nbHits: 0 }, isLoading: false } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: /add rule/i }))
    await user.click(screen.getByRole('button', { name: /remove condition 1/i }))

    fireEvent.change(screen.getByLabelText('Rendering Content (JSON)'), {
      target: { value: '{not valid json' },
    })

    await user.click(screen.getByRole('button', { name: 'Create' }))

    expect(hooksMocks.saveRuleMutate).not.toHaveBeenCalled()
    expect(screen.getByText('Invalid JSON in Rendering Content field.')).toBeInTheDocument()
  })

  // --- Missing filter array fields ---

  it('saves facetFilters, numericFilters, optionalFilters, tagFilters as JSON arrays', async () => {
    const user = userEvent.setup()
    vi.mocked(useRules).mockReturnValue({ data: { hits: [], nbHits: 0 }, isLoading: false } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: /add rule/i }))
    await user.click(screen.getByRole('button', { name: /remove condition 1/i }))

    fireEvent.change(screen.getByLabelText('Facet Filters (JSON)'), {
      target: { value: '[["brand:Apple"]]' },
    })
    fireEvent.change(screen.getByLabelText('Numeric Filters (JSON)'), {
      target: { value: '["price > 10"]' },
    })
    fireEvent.change(screen.getByLabelText('Optional Filters (JSON)'), {
      target: { value: '["brand:Apple"]' },
    })
    fireEvent.change(screen.getByLabelText('Tag Filters (JSON)'), {
      target: { value: '["promo"]' },
    })

    await user.click(screen.getByRole('button', { name: 'Create' }))

    expect(hooksMocks.saveRuleMutate).toHaveBeenCalledTimes(1)
    const savedRule = hooksMocks.saveRuleMutate.mock.calls[0][0]
    expect(savedRule.consequence.params?.facetFilters).toEqual([['brand:Apple']])
    expect(savedRule.consequence.params?.numericFilters).toEqual(['price > 10'])
    expect(savedRule.consequence.params?.optionalFilters).toEqual(['brand:Apple'])
    expect(savedRule.consequence.params?.tagFilters).toEqual(['promo'])
  })

  // --- aroundRadius ---

  it('saves aroundRadius as number', async () => {
    const user = userEvent.setup()
    vi.mocked(useRules).mockReturnValue({ data: { hits: [], nbHits: 0 }, isLoading: false } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: /add rule/i }))
    await user.click(screen.getByRole('button', { name: /remove condition 1/i }))

    await user.clear(screen.getByLabelText('Around Radius'))
    await user.type(screen.getByLabelText('Around Radius'), '5000')

    await user.click(screen.getByRole('button', { name: 'Create' }))

    expect(hooksMocks.saveRuleMutate).toHaveBeenCalledTimes(1)
    const savedRule = hooksMocks.saveRuleMutate.mock.calls[0][0]
    expect(savedRule.consequence.params?.aroundRadius).toBe(5000)
  })

  // --- objectIDs[] expansion ---

  it('expands objectIDs[] format into individual promote entries when loading', async () => {
    const user = userEvent.setup()
    const existingRule = {
      objectID: 'multi-promote-rule',
      conditions: [],
      consequence: {
        promote: [{ objectIDs: ['prod-a', 'prod-b', 'prod-c'], position: 0 }],
      },
      enabled: true,
    }
    vi.mocked(useRules).mockReturnValue({
      data: { hits: [existingRule], nbHits: 1 },
      isLoading: false,
    } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: 'Edit' }))

    // All three IDs should appear as separate promote entries
    expect(screen.getByLabelText('Promote Object ID 1')).toHaveValue('prod-a')
    expect(screen.getByLabelText('Promote Object ID 2')).toHaveValue('prod-b')
    expect(screen.getByLabelText('Promote Object ID 3')).toHaveValue('prod-c')
  })

  // --- Duplicate objectID validation ---

  it('blocks save when promote has duplicate objectIDs', async () => {
    const user = userEvent.setup()
    vi.mocked(useRules).mockReturnValue({ data: { hits: [], nbHits: 0 }, isLoading: false } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: /add rule/i }))
    await user.click(screen.getByRole('button', { name: /remove condition 1/i }))

    await user.click(screen.getByRole('button', { name: /add promoted item/i }))
    await user.type(screen.getByLabelText('Promote Object ID 1'), 'prod-dup')
    await user.click(screen.getByRole('button', { name: /add promoted item/i }))
    await user.type(screen.getByLabelText('Promote Object ID 2'), 'prod-dup')

    await user.click(screen.getByRole('button', { name: 'Create' }))

    expect(hooksMocks.saveRuleMutate).not.toHaveBeenCalled()
    expect(screen.getByText(/duplicate.*objectID/i)).toBeInTheDocument()
  })

  // --- automaticFacetFilters / automaticOptionalFacetFilters editor ---

  it('adds and saves automaticFacetFilters with facet name, disjunctive, and score', async () => {
    const user = userEvent.setup()
    vi.mocked(useRules).mockReturnValue({ data: { hits: [], nbHits: 0 }, isLoading: false } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: /add rule/i }))
    await user.click(screen.getByRole('button', { name: /remove condition 1/i }))

    // Add an automatic facet filter
    await user.click(screen.getByRole('button', { name: /add automatic facet filter/i }))
    await user.type(screen.getByLabelText('Automatic Facet Filter Name 1'), 'brand')
    await user.click(screen.getByLabelText('Disjunctive 1'))
    await user.clear(screen.getByLabelText('Score 1'))
    await user.type(screen.getByLabelText('Score 1'), '5')

    // Add a second one
    await user.click(screen.getByRole('button', { name: /add automatic facet filter/i }))
    await user.type(screen.getByLabelText('Automatic Facet Filter Name 2'), 'category')

    await user.click(screen.getByRole('button', { name: 'Create' }))

    expect(hooksMocks.saveRuleMutate).toHaveBeenCalledTimes(1)
    const savedRule = hooksMocks.saveRuleMutate.mock.calls[0][0]
    expect(savedRule.consequence.params?.automaticFacetFilters).toEqual([
      { facet: 'brand', disjunctive: true, score: 5 },
      { facet: 'category' },
    ])
  })

  it('removes automaticFacetFilter rows', async () => {
    const user = userEvent.setup()
    vi.mocked(useRules).mockReturnValue({ data: { hits: [], nbHits: 0 }, isLoading: false } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: /add rule/i }))
    await user.click(screen.getByRole('button', { name: /remove condition 1/i }))

    await user.click(screen.getByRole('button', { name: /add automatic facet filter/i }))
    await user.type(screen.getByLabelText('Automatic Facet Filter Name 1'), 'brand')
    await user.click(screen.getByRole('button', { name: /add automatic facet filter/i }))
    await user.type(screen.getByLabelText('Automatic Facet Filter Name 2'), 'color')

    // Remove the first one
    await user.click(screen.getByRole('button', { name: /remove automatic facet filter 1/i }))

    await user.click(screen.getByRole('button', { name: 'Create' }))

    expect(hooksMocks.saveRuleMutate).toHaveBeenCalledTimes(1)
    const savedRule = hooksMocks.saveRuleMutate.mock.calls[0][0]
    expect(savedRule.consequence.params?.automaticFacetFilters).toEqual([
      { facet: 'color' },
    ])
  })

  it('adds and saves automaticOptionalFacetFilters', async () => {
    const user = userEvent.setup()
    vi.mocked(useRules).mockReturnValue({ data: { hits: [], nbHits: 0 }, isLoading: false } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: /add rule/i }))
    await user.click(screen.getByRole('button', { name: /remove condition 1/i }))

    await user.click(screen.getByRole('button', { name: /add automatic optional facet filter/i }))
    await user.type(screen.getByLabelText('Automatic Optional Facet Filter Name 1'), 'size')
    await user.click(screen.getByLabelText('Optional Disjunctive 1'))
    await user.clear(screen.getByLabelText('Optional Score 1'))
    await user.type(screen.getByLabelText('Optional Score 1'), '3')

    await user.click(screen.getByRole('button', { name: 'Create' }))

    expect(hooksMocks.saveRuleMutate).toHaveBeenCalledTimes(1)
    const savedRule = hooksMocks.saveRuleMutate.mock.calls[0][0]
    expect(savedRule.consequence.params?.automaticOptionalFacetFilters).toEqual([
      { facet: 'size', disjunctive: true, score: 3 },
    ])
  })

  it('loads existing rule with automaticFacetFilters into form mode', async () => {
    const user = userEvent.setup()
    const existingRule = {
      objectID: 'auto-facet-rule',
      conditions: [],
      consequence: {
        params: {
          automaticFacetFilters: [
            { facet: 'brand', disjunctive: true, score: 10 },
            { facet: 'color' },
          ],
          automaticOptionalFacetFilters: [
            { facet: 'size', score: 2 },
          ],
        },
      },
      enabled: true,
    }
    vi.mocked(useRules).mockReturnValue({
      data: { hits: [existingRule], nbHits: 1 },
      isLoading: false,
    } as any)
    render(<Rules />, { wrapper })

    await user.click(screen.getByRole('button', { name: 'Edit' }))

    expect(screen.getByLabelText('Automatic Facet Filter Name 1')).toHaveValue('brand')
    expect(screen.getByLabelText('Disjunctive 1')).toBeChecked()
    expect(screen.getByLabelText('Score 1')).toHaveValue(10)
    expect(screen.getByLabelText('Automatic Facet Filter Name 2')).toHaveValue('color')
    expect(screen.getByLabelText('Disjunctive 2')).not.toBeChecked()

    expect(screen.getByLabelText('Automatic Optional Facet Filter Name 1')).toHaveValue('size')
    expect(screen.getByLabelText('Optional Score 1')).toHaveValue(2)
  })
})
