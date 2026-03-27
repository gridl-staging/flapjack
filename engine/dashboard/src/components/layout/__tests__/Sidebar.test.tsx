import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { TEST_ROUTER_FUTURE } from '@/test/routerFuture'
import { Sidebar } from '@/components/layout/Sidebar'
import { useIndexes } from '@/hooks/useIndexes'

vi.mock('@/hooks/useIndexes', () => ({
  useIndexes: vi.fn(),
}))

describe('Sidebar', () => {
  it('shows the full index count when the index list is collapsed', () => {
    vi.mocked(useIndexes).mockReturnValue({
      data: [
        { uid: 'products' },
        { uid: 'products-archive' },
        { uid: 'logs' },
        { uid: 'support' },
        { uid: 'catalog' },
        { uid: 'inventory' },
      ],
      isLoading: false,
    } as any)

    render(
      <MemoryRouter future={TEST_ROUTER_FUTURE} initialEntries={['/overview']}>
        <Sidebar />
      </MemoryRouter>
    )

    expect(screen.getByTestId('sidebar-show-all-indexes')).toHaveTextContent('Show all (1 more index)')
  })
})
