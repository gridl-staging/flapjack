import { describe, expect, it } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'
import { TEST_ROUTER_FUTURE } from '@/test/routerFuture'
import { Home } from 'lucide-react'
import { SidebarSection } from '@/components/layout/SidebarSection'

const items = [
  { to: '/overview', label: 'Overview', icon: Home },
]

function renderSidebarSection() {
  return render(
    <MemoryRouter future={TEST_ROUTER_FUTURE}>
      <SidebarSection
        sectionId="developer"
        heading="Developer"
        headingLabel="Developer Navigation"
        items={items}
        headingTestId="sidebar-section-heading-developer"
        sectionTestId="sidebar-section-developer"
      >
        <div data-testid="extra-content">Extra content</div>
      </SidebarSection>
    </MemoryRouter>
  )
}

describe('SidebarSection', () => {
  it('renders section label with header styling', () => {
    renderSidebarSection()

    const header = screen.getByTestId('sidebar-section-heading-developer');
    expect(header).toHaveTextContent('Developer');
    expect(header).toHaveClass('text-xs');
    expect(header).toHaveClass('uppercase');
    expect(header).toHaveClass('tracking-wider');
  });

  it('renders child navigation content in the section body', () => {
    renderSidebarSection()

    const section = screen.getByTestId('sidebar-section-developer')
    expect(within(section).getByRole('link', { name: 'Overview' })).toHaveAttribute('href', '/overview')
    expect(within(section).getByTestId('extra-content')).toHaveTextContent('Extra content')
    expect(
      within(section).getByRole('navigation', { name: 'Developer Navigation' })
    ).toBeInTheDocument()
  })
})
