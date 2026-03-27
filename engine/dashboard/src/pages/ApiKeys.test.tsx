import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { TEST_ROUTER_FUTURE } from '@/test/routerFuture'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ApiKeys } from './ApiKeys'

const deleteKeyMutateAsync = vi.fn()

vi.mock('@/hooks/useApiKeys', () => ({
  useApiKeys: vi.fn(),
  useDeleteApiKey: () => ({ mutateAsync: deleteKeyMutateAsync, isPending: false }),
}))

vi.mock('@/hooks/useIndexes', () => ({
  useIndexes: vi.fn(),
}))

vi.mock('@/components/keys/CreateKeyDialog', () => ({
  CreateKeyDialog: ({ open }: { open: boolean }) =>
    open ? <div data-testid="create-key-dialog" /> : null,
}))

import { useApiKeys } from '@/hooks/useApiKeys'
import { useIndexes } from '@/hooks/useIndexes'

function wrapper({ children }: { children: React.ReactNode }) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return (
    <QueryClientProvider client={qc}>
      <MemoryRouter future={TEST_ROUTER_FUTURE}>{children}</MemoryRouter>
    </QueryClientProvider>
  )
}

const SAMPLE_KEY = {
  value: 'abc123def456',
  description: 'Search Key',
  acl: ['search'],
  indexes: ['products'],
  createdAt: 1700000000,
  maxHitsPerQuery: null,
  maxQueriesPerIPPerHour: null,
  expiresAt: null,
}

describe('ApiKeys', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    deleteKeyMutateAsync.mockReset()
    vi.mocked(useIndexes).mockReturnValue({ data: [], isLoading: false, error: null } as any)
  })

  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('shows loading skeletons while fetching', () => {
    vi.mocked(useApiKeys).mockReturnValue({ data: undefined, isLoading: true } as any)
    render(<ApiKeys />, { wrapper })
    // Skeletons render as elements — just verify no crash and no keys list yet
    expect(screen.queryByTestId('keys-list')).not.toBeInTheDocument()
  })

  it('shows empty state when no keys exist', () => {
    vi.mocked(useApiKeys).mockReturnValue({ data: [], isLoading: false } as any)
    render(<ApiKeys />, { wrapper })
    expect(screen.getByText('No API keys')).toBeInTheDocument()
    expect(screen.getByText('Create Your First Key')).toBeInTheDocument()
  })

  it('renders key cards when keys exist', () => {
    vi.mocked(useApiKeys).mockReturnValue({ data: [SAMPLE_KEY], isLoading: false } as any)
    render(<ApiKeys />, { wrapper })
    expect(screen.getByTestId('keys-list')).toBeInTheDocument()
    expect(screen.getByText('Search Key')).toBeInTheDocument()
    expect(screen.getByText('abc123def456')).toBeInTheDocument()
    expect(screen.getByText('Review each key\'s scope, permissions, and lifecycle before sharing it.')).toBeInTheDocument()
    expect(screen.getByText('Key Value')).toBeInTheDocument()
    expect(screen.getByText('Lifecycle & Limits')).toBeInTheDocument()
  })

  it('shows index scope badge on key', () => {
    vi.mocked(useApiKeys).mockReturnValue({ data: [SAMPLE_KEY], isLoading: false } as any)
    render(<ApiKeys />, { wrapper })
    // Key has indexes: ['products'] — find the badge inside the key-scope section
    const scopeSection = screen.getByTestId('key-scope')
    expect(within(scopeSection).getByText('products')).toBeInTheDocument()
  })

  it('shows "All Indexes" badge for keys with no index restriction', () => {
    const globalKey = { ...SAMPLE_KEY, indexes: [] }
    vi.mocked(useApiKeys).mockReturnValue({ data: [globalKey], isLoading: false } as any)
    render(<ApiKeys />, { wrapper })
    expect(screen.getByText('All Indexes')).toBeInTheDocument()
  })

  it('does not render restrict sources section when key has no restrictSources', () => {
    const unrestrictedKey = { ...SAMPLE_KEY, restrictSources: undefined }
    vi.mocked(useApiKeys).mockReturnValue({ data: [unrestrictedKey], isLoading: false } as any)
    render(<ApiKeys />, { wrapper })

    expect(screen.queryByText('Restrict Sources')).not.toBeInTheDocument()
  })

  it('shows filter bar when keys and indexes exist', () => {
    vi.mocked(useApiKeys).mockReturnValue({ data: [SAMPLE_KEY], isLoading: false } as any)
    vi.mocked(useIndexes).mockReturnValue({
      data: [{ uid: 'products', entries: 10, dataSize: 1024 }],
      isLoading: false,
      error: null,
    } as any)
    render(<ApiKeys />, { wrapper })
    expect(screen.getByTestId('index-filter-bar')).toBeInTheDocument()
    expect(screen.getByText('Filter by Index Access')).toBeInTheDocument()
    expect(screen.getByText('Viewing keys across all indexes')).toBeInTheDocument()
    expect(screen.getByTestId('filter-all')).toHaveAttribute('aria-pressed', 'true')
    expect(screen.getByTestId('filter-index-products')).toBeInTheDocument()
    expect(screen.getByTestId('filter-index-products')).toHaveAttribute('aria-pressed', 'false')
  })

  it('updates filter summary when selecting an index', async () => {
    const user = userEvent.setup()
    const keyA = { ...SAMPLE_KEY, value: 'key-a', description: 'Key A', indexes: ['products'] }
    const keyB = { ...SAMPLE_KEY, value: 'key-b', description: 'Key B', indexes: ['articles'] }
    vi.mocked(useApiKeys).mockReturnValue({ data: [keyA, keyB], isLoading: false } as any)
    vi.mocked(useIndexes).mockReturnValue({
      data: [
        { uid: 'products', entries: 10, dataSize: 0 },
        { uid: 'articles', entries: 5, dataSize: 0 },
      ],
      isLoading: false,
      error: null,
    } as any)

    render(<ApiKeys />, { wrapper })
    expect(screen.getByText('Viewing keys across all indexes')).toBeInTheDocument()
    expect(screen.getByTestId('filter-all')).toHaveAttribute('aria-pressed', 'true')
    expect(screen.getByTestId('filter-index-products')).toHaveAttribute('aria-pressed', 'false')

    await user.click(screen.getByTestId('filter-index-products'))
    expect(screen.getByText('Viewing keys that can access products')).toBeInTheDocument()
    expect(screen.getByTestId('filter-all')).toHaveAttribute('aria-pressed', 'false')
    expect(screen.getByTestId('filter-index-products')).toHaveAttribute('aria-pressed', 'true')
  })

  it('does not show filter bar when there are no keys', () => {
    vi.mocked(useApiKeys).mockReturnValue({ data: [], isLoading: false } as any)
    vi.mocked(useIndexes).mockReturnValue({
      data: [{ uid: 'products', entries: 10, dataSize: 1024 }],
      isLoading: false,
      error: null,
    } as any)
    render(<ApiKeys />, { wrapper })
    expect(screen.queryByTestId('index-filter-bar')).not.toBeInTheDocument()
  })

  it('opens create dialog when Create Key button is clicked', async () => {
    const user = userEvent.setup()
    vi.mocked(useApiKeys).mockReturnValue({ data: [], isLoading: false } as any)
    render(<ApiKeys />, { wrapper })

    expect(screen.queryByTestId('create-key-dialog')).not.toBeInTheDocument()
    // Use exact name to target the header button, not the empty-state "Create Your First Key"
    await user.click(screen.getByRole('button', { name: 'Create Key' }))
    expect(screen.getByTestId('create-key-dialog')).toBeInTheDocument()
  })

  it('shows permissions badges', () => {
    const keyWithMultipleAcl = { ...SAMPLE_KEY, acl: ['search', 'listIndexes'] }
    vi.mocked(useApiKeys).mockReturnValue({ data: [keyWithMultipleAcl], isLoading: false } as any)
    render(<ApiKeys />, { wrapper })
    expect(screen.getByText('search')).toBeInTheDocument()
    expect(screen.getByText('listIndexes')).toBeInTheDocument()
  })

  it('filters keys by selected index', async () => {
    const user = userEvent.setup()
    const keyA = { ...SAMPLE_KEY, value: 'key-a', description: 'Key A', indexes: ['products'] }
    const keyB = { ...SAMPLE_KEY, value: 'key-b', description: 'Key B', indexes: ['articles'] }
    vi.mocked(useApiKeys).mockReturnValue({ data: [keyA, keyB], isLoading: false } as any)
    vi.mocked(useIndexes).mockReturnValue({
      data: [
        { uid: 'products', entries: 10, dataSize: 0 },
        { uid: 'articles', entries: 5, dataSize: 0 },
      ],
      isLoading: false,
      error: null,
    } as any)
    render(<ApiKeys />, { wrapper })

    // Both keys visible initially
    expect(screen.getByText('Key A')).toBeInTheDocument()
    expect(screen.getByText('Key B')).toBeInTheDocument()

    // Click "products" filter
    await user.click(screen.getByTestId('filter-index-products'))

    // Only Key A visible
    expect(screen.getByText('Key A')).toBeInTheDocument()
    expect(screen.queryByText('Key B')).not.toBeInTheDocument()
  })

  it('uses ConfirmDialog for deletion instead of window.confirm', async () => {
    const user = userEvent.setup()
    const confirmSpy = vi.spyOn(window, 'confirm')

    vi.mocked(useApiKeys).mockReturnValue({ data: [SAMPLE_KEY], isLoading: false } as any)
    render(<ApiKeys />, { wrapper })

    await user.click(screen.getByTestId('delete-key-btn'))

    expect(confirmSpy).not.toHaveBeenCalled()
    expect(screen.getByText('Delete API Key')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Delete' }))
    expect(deleteKeyMutateAsync).toHaveBeenCalledWith(SAMPLE_KEY.value)
  })

  it('shows restrictSources values in key cards when provided', () => {
    const restrictedKey = {
      ...SAMPLE_KEY,
      restrictSources: ['10.0.0.0/8', '*.example.com'],
    }
    vi.mocked(useApiKeys).mockReturnValue({ data: [restrictedKey], isLoading: false } as any)

    render(<ApiKeys />, { wrapper })

    const keyCard = screen.getByTestId('key-card')
    expect(within(keyCard).getAllByText('Restrict Sources').length).toBeGreaterThan(0)
    expect(within(keyCard).getAllByText('10.0.0.0/8').length).toBeGreaterThan(0)
    expect(within(keyCard).getAllByText('*.example.com').length).toBeGreaterThan(0)
  })
})
