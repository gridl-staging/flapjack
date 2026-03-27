import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { TEST_ROUTER_FUTURE } from '@/test/routerFuture'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { IndexLayout } from '@/components/layout/IndexLayout'
import { Synonyms } from './Synonyms'

const hooksMocks = vi.hoisted(() => ({
  saveSynonymMutate: vi.fn().mockResolvedValue({}),
  deleteSynonymMutate: vi.fn().mockResolvedValue({}),
  clearSynonymsMutate: vi.fn().mockResolvedValue({}),
}))

vi.mock('@/hooks/useSynonyms', () => ({
  useSynonyms: vi.fn(),
  useSaveSynonym: () => ({ mutateAsync: hooksMocks.saveSynonymMutate, isPending: false }),
  useDeleteSynonym: () => ({ mutateAsync: hooksMocks.deleteSynonymMutate, isPending: false }),
  useClearSynonyms: () => ({ mutateAsync: hooksMocks.clearSynonymsMutate, isPending: false }),
}))

vi.mock('@/hooks/useSettings', () => ({
  useSettings: vi.fn(),
}))

import { useSynonyms } from '@/hooks/useSynonyms'
import { useSettings } from '@/hooks/useSettings'

function wrapper({ children }: { children: React.ReactNode }) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return (
    <QueryClientProvider client={qc}>
      <MemoryRouter future={TEST_ROUTER_FUTURE} initialEntries={['/index/products/synonyms']}>
        <Routes>
          <Route path="/index/:indexName" element={<IndexLayout />}>
            <Route path="synonyms" element={children} />
          </Route>
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>
  )
}

const MULTI_WAY_SYN = {
  type: 'synonym' as const,
  objectID: 'syn-1',
  synonyms: ['laptop', 'notebook', 'computer'],
}

const ONE_WAY_SYN = {
  type: 'onewaysynonym' as const,
  objectID: 'syn-2',
  input: 'phone',
  synonyms: ['smartphone', 'mobile'],
}

describe('Synonyms', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(useSettings).mockReturnValue({ data: { mode: 'keywordSearch' }, isLoading: false } as any)
    hooksMocks.saveSynonymMutate.mockResolvedValue({})
    hooksMocks.deleteSynonymMutate.mockResolvedValue({})
    hooksMocks.clearSynonymsMutate.mockResolvedValue({})
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('shows loading skeleton while fetching', () => {
    vi.mocked(useSynonyms).mockReturnValue({ data: undefined, isLoading: true } as any)
    render(<Synonyms />, { wrapper })
    expect(screen.queryByTestId('synonyms-list')).not.toBeInTheDocument()
  })

  it('shows empty state when no synonyms exist', () => {
    vi.mocked(useSynonyms).mockReturnValue({
      data: { hits: [], nbHits: 0 },
      isLoading: false,
    } as any)
    render(<Synonyms />, { wrapper })
    expect(screen.getByText(/no synonyms/i)).toBeInTheDocument()
  })

  it('renders synonym rows when synonyms exist', () => {
    vi.mocked(useSynonyms).mockReturnValue({
      data: { hits: [MULTI_WAY_SYN], nbHits: 1 },
      isLoading: false,
    } as any)
    render(<Synonyms />, { wrapper })
    expect(screen.getByText(/laptop/i)).toBeInTheDocument()
  })

  it('displays multi-way synonym as equals chain', () => {
    vi.mocked(useSynonyms).mockReturnValue({
      data: { hits: [MULTI_WAY_SYN], nbHits: 1 },
      isLoading: false,
    } as any)
    render(<Synonyms />, { wrapper })
    // Multi-way renders as "laptop = notebook = computer"
    expect(screen.getByText(/laptop = notebook = computer/)).toBeInTheDocument()
  })

  it('displays one-way synonym with arrow', () => {
    vi.mocked(useSynonyms).mockReturnValue({
      data: { hits: [ONE_WAY_SYN], nbHits: 1 },
      isLoading: false,
    } as any)
    render(<Synonyms />, { wrapper })
    // One-way renders as "phone → smartphone, mobile"
    expect(screen.getByText(/phone → smartphone/)).toBeInTheDocument()
  })

  it('renders multiple synonyms', () => {
    vi.mocked(useSynonyms).mockReturnValue({
      data: { hits: [MULTI_WAY_SYN, ONE_WAY_SYN], nbHits: 2 },
      isLoading: false,
    } as any)
    render(<Synonyms />, { wrapper })
    expect(screen.getByText(/laptop = notebook = computer/)).toBeInTheDocument()
    expect(screen.getByText(/phone → smartphone/)).toBeInTheDocument()
  })

  it('shows Add Synonym button', () => {
    vi.mocked(useSynonyms).mockReturnValue({
      data: { hits: [], nbHits: 0 },
      isLoading: false,
    } as any)
    render(<Synonyms />, { wrapper })
    expect(screen.getByRole('button', { name: /add synonym/i })).toBeInTheDocument()
  })

  it('opens create dialog when Add Synonym is clicked', async () => {
    const user = userEvent.setup()
    vi.mocked(useSynonyms).mockReturnValue({
      data: { hits: [], nbHits: 0 },
      isLoading: false,
    } as any)
    render(<Synonyms />, { wrapper })
    await user.click(screen.getByRole('button', { name: /add synonym/i }))
    // Dialog should appear with synonym type selector
    expect(screen.getByRole('dialog')).toBeInTheDocument()
  })

  it('shows type label badges on synonym rows', () => {
    vi.mocked(useSynonyms).mockReturnValue({
      data: { hits: [MULTI_WAY_SYN, ONE_WAY_SYN], nbHits: 2 },
      isLoading: false,
    } as any)
    render(<Synonyms />, { wrapper })
    expect(screen.getByText('Multi-way')).toBeInTheDocument()
    expect(screen.getByText('One-way')).toBeInTheDocument()
  })

  it('uses ConfirmDialog before deleting a synonym', async () => {
    const user = userEvent.setup()
    const confirmSpy = vi.spyOn(window, 'confirm').mockImplementation(() => true)
    vi.mocked(useSynonyms).mockReturnValue({
      data: { hits: [MULTI_WAY_SYN], nbHits: 1 },
      isLoading: false,
    } as any)
    render(<Synonyms />, { wrapper })

    await user.click(screen.getByRole('button', { name: 'Delete' }))

    expect(screen.getByRole('dialog')).toBeInTheDocument()
    expect(screen.getByText('Delete Synonym')).toBeInTheDocument()
    expect(hooksMocks.deleteSynonymMutate).not.toHaveBeenCalled()
    expect(confirmSpy).not.toHaveBeenCalled()

    await user.click(screen.getByRole('button', { name: 'Delete' }))
    expect(hooksMocks.deleteSynonymMutate).toHaveBeenCalledWith('syn-1')
  })

  it('uses ConfirmDialog before clearing all synonyms', async () => {
    const user = userEvent.setup()
    const confirmSpy = vi.spyOn(window, 'confirm').mockImplementation(() => true)
    vi.mocked(useSynonyms).mockReturnValue({
      data: { hits: [MULTI_WAY_SYN], nbHits: 1 },
      isLoading: false,
    } as any)
    render(<Synonyms />, { wrapper })

    await user.click(screen.getByRole('button', { name: /clear all/i }))

    expect(screen.getByRole('dialog')).toBeInTheDocument()
    expect(screen.getByText('Delete All Synonyms')).toBeInTheDocument()
    expect(hooksMocks.clearSynonymsMutate).not.toHaveBeenCalled()
    expect(confirmSpy).not.toHaveBeenCalled()

    await user.click(screen.getByRole('button', { name: 'Delete All' }))
    expect(hooksMocks.clearSynonymsMutate).toHaveBeenCalledTimes(1)
  })
})
