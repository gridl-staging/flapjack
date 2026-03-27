import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import type { PersonalizationStrategy } from '@/lib/types'
import { Personalization } from './Personalization'

const mockUsePersonalizationStrategy = vi.hoisted(() => vi.fn())
const mockUseSaveStrategy = vi.hoisted(() => vi.fn())
const mockUsePersonalizationProfile = vi.hoisted(() => vi.fn())

vi.mock('@/hooks/usePersonalization', () => ({
  usePersonalizationStrategy: mockUsePersonalizationStrategy,
  useSaveStrategy: mockUseSaveStrategy,
  usePersonalizationProfile: mockUsePersonalizationProfile,
}))

const SAVED_STRATEGY: PersonalizationStrategy = {
  eventsScoring: [{ eventName: 'Product Viewed', eventType: 'view', score: 20 }],
  facetsScoring: [{ facetName: 'brand', score: 70 }],
  personalizationImpact: 60,
}

function createFilledStrategy(eventCount: number, facetCount: number): PersonalizationStrategy {
  return {
    eventsScoring: Array.from({ length: eventCount }, (_, index) => ({
      eventName: `Event ${index + 1}`,
      eventType: 'view' as const,
      score: 20,
    })),
    facetsScoring: Array.from({ length: facetCount }, (_, index) => ({
      facetName: `facet_${index + 1}`,
      score: 70,
    })),
    personalizationImpact: 60,
  }
}

describe('Personalization', () => {
  beforeEach(() => {
    vi.clearAllMocks()

    mockUseSaveStrategy.mockReturnValue({
      mutateAsync: vi.fn(),
      isPending: false,
    })
    mockUsePersonalizationProfile.mockReturnValue({
      data: undefined,
      isLoading: false,
      isError: false,
      refetch: vi.fn(),
    })
  })

  it('shows a strategy load error instead of the setup state when the initial fetch fails', () => {
    mockUsePersonalizationStrategy.mockReturnValue({
      data: undefined,
      isLoading: false,
      isError: true,
      refetch: vi.fn(),
    })

    render(<Personalization />)

    expect(screen.getByText('Failed to load personalization strategy.')).toBeInTheDocument()
    expect(screen.queryByText('Personalization is not configured yet.')).not.toBeInTheDocument()
  })

  it('keeps profile lookup hidden until the strategy has been saved', async () => {
    const user = userEvent.setup()

    mockUsePersonalizationStrategy.mockReturnValue({
      data: null,
      isLoading: false,
      isError: false,
      refetch: vi.fn(),
    })

    render(<Personalization />)

    await user.click(screen.getByRole('button', { name: 'Use starter strategy' }))

    expect(screen.getByTestId('save-strategy-btn')).toBeInTheDocument()
    expect(screen.getByText('Save the strategy to enable profile lookup.')).toBeInTheDocument()
    expect(screen.queryByTestId('profile-lookup-input')).not.toBeInTheDocument()
  })

  it('shows profile lookup once a saved strategy exists', () => {
    mockUsePersonalizationStrategy.mockReturnValue({
      data: SAVED_STRATEGY,
      isLoading: false,
      isError: false,
      refetch: vi.fn(),
    })

    render(<Personalization />)

    expect(screen.getByTestId('profile-lookup-input')).toBeInTheDocument()
    expect(screen.queryByText('Save the strategy to enable profile lookup.')).not.toBeInTheDocument()
  })

  it('refetches when the same profile token is submitted again', async () => {
    const user = userEvent.setup()
    const refetch = vi.fn()

    mockUsePersonalizationStrategy.mockReturnValue({
      data: SAVED_STRATEGY,
      isLoading: false,
      isError: false,
      refetch: vi.fn(),
    })
    mockUsePersonalizationProfile.mockReturnValue({
      data: null,
      isLoading: false,
      isError: false,
      refetch,
    })

    render(<Personalization />)

    await user.type(screen.getByTestId('profile-lookup-input'), 'repeat-user')
    await user.click(screen.getByTestId('profile-lookup-btn'))
    await user.click(screen.getByTestId('profile-lookup-btn'))

    expect(refetch).toHaveBeenCalledTimes(1)
  })

  it('disables add-row controls once the backend max row count is reached', () => {
    mockUsePersonalizationStrategy.mockReturnValue({
      data: createFilledStrategy(15, 15),
      isLoading: false,
      isError: false,
      refetch: vi.fn(),
    })

    render(<Personalization />)

    expect(screen.getByRole('button', { name: 'Add event' })).toBeDisabled()
    expect(screen.getByTestId('add-facet-btn')).toBeDisabled()
  })
})
