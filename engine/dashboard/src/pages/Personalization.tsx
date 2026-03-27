import { useEffect, useState } from 'react';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  usePersonalizationProfile,
  usePersonalizationStrategy,
  useSaveStrategy,
} from '@/hooks/usePersonalization';
import type {
  EventScoring,
  FacetScoring,
  PersonalizationEventType,
  PersonalizationStrategy,
} from '@/lib/types';
import { ProfileLookupCard } from '@/pages/personalization/ProfileLookupCard';

const EVENT_TYPE_OPTIONS: PersonalizationEventType[] = ['click', 'conversion', 'view'];
const EVENT_SCORE_MIN = 1;
const EVENT_SCORE_MAX = 100;
const MAX_EVENT_ROWS = 15;
const FACET_SCORE_MIN = 1;
const FACET_SCORE_MAX = 100;
const MAX_FACET_ROWS = 15;
const IMPACT_MIN = 0;
const IMPACT_MAX = 100;

function createStarterStrategy(): PersonalizationStrategy {
  return {
    eventsScoring: [{ eventName: 'Product Viewed', eventType: 'view', score: 20 }],
    facetsScoring: [{ facetName: 'brand', score: 70 }],
    personalizationImpact: 60,
  };
}

function clampNumber(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function parseBoundedNumber(rawValue: string, fallback: number, min: number, max: number): number {
  const parsed = Number.parseInt(rawValue, 10);
  if (Number.isNaN(parsed)) {
    return fallback;
  }
  return clampNumber(parsed, min, max);
}

function isNonEmpty(value: string): boolean {
  return value.trim().length > 0;
}

function isStrategyValid(strategy: PersonalizationStrategy): boolean {
  if (strategy.personalizationImpact < IMPACT_MIN || strategy.personalizationImpact > IMPACT_MAX) {
    return false;
  }

  if (
    strategy.eventsScoring.length === 0
    || strategy.eventsScoring.length > MAX_EVENT_ROWS
    || strategy.facetsScoring.length === 0
    || strategy.facetsScoring.length > MAX_FACET_ROWS
  ) {
    return false;
  }

  return strategy.eventsScoring.every(
    (event) =>
      isNonEmpty(event.eventName)
      && EVENT_TYPE_OPTIONS.includes(event.eventType)
      && event.score >= EVENT_SCORE_MIN
      && event.score <= EVENT_SCORE_MAX,
  )
    && strategy.facetsScoring.every(
      (facet) =>
        isNonEmpty(facet.facetName)
        && facet.score >= FACET_SCORE_MIN
        && facet.score <= FACET_SCORE_MAX,
    );
}

function updateListItem<T>(
  items: T[],
  index: number,
  updater: (item: T) => T,
): T[] {
  return items.map((item, itemIndex) =>
    itemIndex === index ? updater(item) : item,
  );
}

function removeListItem<T>(items: T[], index: number): T[] {
  return items.filter((_, itemIndex) => itemIndex !== index);
}

interface SetupStateCardProps {
  onUseStarterStrategy: () => void;
}

function SetupStateCard({ onUseStarterStrategy }: SetupStateCardProps) {
  return (
    <Card className="p-6 space-y-4">
      <h3 className="text-lg font-semibold">Personalization is not configured yet.</h3>
      <p className="text-sm text-muted-foreground">
        Start with a strategy, then save it to enable profile lookups and ranking impact.
      </p>
      <Button onClick={onUseStarterStrategy}>Use starter strategy</Button>
    </Card>
  );
}

interface StrategyLoadErrorStateProps {
  onRetry: () => void;
}

function StrategyLoadErrorState({ onRetry }: StrategyLoadErrorStateProps) {
  return (
    <Card className="p-6 space-y-4">
      <h3 className="text-lg font-semibold">Failed to load personalization strategy.</h3>
      <p className="text-sm text-muted-foreground">
        The saved configuration could not be loaded from the backend. Retry before editing or saving a new strategy.
      </p>
      <div>
        <Button type="button" variant="outline" onClick={onRetry}>
          Retry
        </Button>
      </div>
    </Card>
  );
}

interface EventScoringSectionProps {
  eventsScoring: EventScoring[];
  onChange: (eventsScoring: EventScoring[]) => void;
}

function EventScoringSection({ eventsScoring, onChange }: EventScoringSectionProps) {
  return (
    <section className="space-y-3">
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold">Event scoring</h3>
        <Button
          type="button"
          variant="outline"
          disabled={eventsScoring.length >= MAX_EVENT_ROWS}
          onClick={() =>
            onChange([...eventsScoring, { eventName: '', eventType: 'view', score: EVENT_SCORE_MIN }])
          }
        >
          Add event
        </Button>
      </div>

      <div className="space-y-3" data-testid="events-scoring-list">
        {eventsScoring.map((eventScoring, index) => (
          <div
            key={`event-row-${index}`}
            className="grid gap-3 md:grid-cols-[2fr_1fr_1fr_auto]"
            data-testid={`event-row-${index}`}
          >
            <div className="space-y-1">
              <Label htmlFor={`event-name-${index}`}>Event name</Label>
              <Input
                id={`event-name-${index}`}
                value={eventScoring.eventName}
                onChange={(event) =>
                  onChange(updateListItem(eventsScoring, index, (row) => ({
                    ...row,
                    eventName: event.target.value,
                  })))
                }
              />
            </div>

            <div className="space-y-1">
              <Label htmlFor={`event-type-${index}`}>Event type</Label>
              <select
                id={`event-type-${index}`}
                className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                value={eventScoring.eventType}
                onChange={(event) =>
                  onChange(updateListItem(eventsScoring, index, (row) => ({
                    ...row,
                    eventType: event.target.value as PersonalizationEventType,
                  })))
                }
              >
                {EVENT_TYPE_OPTIONS.map((eventType) => (
                  <option key={eventType} value={eventType}>
                    {eventType}
                  </option>
                ))}
              </select>
            </div>

            <div className="space-y-1">
              <Label htmlFor={`event-score-${index}`}>Event score</Label>
              <Input
                id={`event-score-${index}`}
                type="number"
                min={EVENT_SCORE_MIN}
                max={EVENT_SCORE_MAX}
                value={eventScoring.score}
                onChange={(event) =>
                  onChange(updateListItem(eventsScoring, index, (row) => ({
                    ...row,
                    score: parseBoundedNumber(event.target.value, row.score, EVENT_SCORE_MIN, EVENT_SCORE_MAX),
                  })))
                }
              />
            </div>

            <div className="flex items-end">
              <Button
                type="button"
                variant="ghost"
                onClick={() => onChange(removeListItem(eventsScoring, index))}
                disabled={eventsScoring.length === 1}
              >
                Remove
              </Button>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

interface FacetScoringSectionProps {
  facetsScoring: FacetScoring[];
  onChange: (facetsScoring: FacetScoring[]) => void;
}

function FacetScoringSection({ facetsScoring, onChange }: FacetScoringSectionProps) {
  return (
    <section className="space-y-3">
      <div className="flex items-center justify-between">
        <h3 className="text-lg font-semibold">Facet scoring</h3>
        <Button
          type="button"
          variant="outline"
          data-testid="add-facet-btn"
          disabled={facetsScoring.length >= MAX_FACET_ROWS}
          onClick={() => onChange([...facetsScoring, { facetName: '', score: FACET_SCORE_MIN }])}
        >
          Add facet
        </Button>
      </div>

      <div className="space-y-3" data-testid="facets-scoring-list">
        {facetsScoring.map((facetScoring, index) => (
          <div
            key={`facet-row-${index}`}
            className="grid gap-3 md:grid-cols-[2fr_1fr_auto]"
            data-testid={`facet-row-${index}`}
          >
            <div className="space-y-1">
              <Label htmlFor={`facet-name-${index}`}>Facet name</Label>
              <Input
                id={`facet-name-${index}`}
                value={facetScoring.facetName}
                onChange={(event) =>
                  onChange(updateListItem(facetsScoring, index, (row) => ({
                    ...row,
                    facetName: event.target.value,
                  })))
                }
              />
            </div>

            <div className="space-y-1">
              <Label htmlFor={`facet-score-${index}`}>Facet score</Label>
              <Input
                id={`facet-score-${index}`}
                type="number"
                min={FACET_SCORE_MIN}
                max={FACET_SCORE_MAX}
                value={facetScoring.score}
                onChange={(event) =>
                  onChange(updateListItem(facetsScoring, index, (row) => ({
                    ...row,
                    score: parseBoundedNumber(event.target.value, row.score, FACET_SCORE_MIN, FACET_SCORE_MAX),
                  })))
                }
              />
            </div>

            <div className="flex items-end">
              <Button
                type="button"
                variant="ghost"
                onClick={() => onChange(removeListItem(facetsScoring, index))}
                disabled={facetsScoring.length === 1}
              >
                Remove
              </Button>
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}

interface StrategyEditorCardProps {
  strategy: PersonalizationStrategy;
  onChange: (strategy: PersonalizationStrategy) => void;
  onSave: () => void;
  canSave: boolean;
  isSaving: boolean;
  hasPersistedStrategy: boolean;
  hasLoadError: boolean;
}

function StrategyEditorCard({
  strategy,
  onChange,
  onSave,
  canSave,
  isSaving,
  hasPersistedStrategy,
  hasLoadError,
}: StrategyEditorCardProps) {
  return (
    <Card className="p-6 space-y-6">
      <section className="space-y-2">
        <Label htmlFor="personalization-impact">Personalization impact (0-100)</Label>
        <Input
          id="personalization-impact"
          data-testid="personalization-impact-input"
          type="number"
          min={IMPACT_MIN}
          max={IMPACT_MAX}
          value={strategy.personalizationImpact}
          onChange={(event) =>
            onChange({
              ...strategy,
              personalizationImpact: parseBoundedNumber(
                event.target.value,
                strategy.personalizationImpact,
                IMPACT_MIN,
                IMPACT_MAX,
              ),
            })
          }
        />
      </section>

      <EventScoringSection
        eventsScoring={strategy.eventsScoring}
        onChange={(eventsScoring) => onChange({ ...strategy, eventsScoring })}
      />

      <FacetScoringSection
        facetsScoring={strategy.facetsScoring}
        onChange={(facetsScoring) => onChange({ ...strategy, facetsScoring })}
      />

      <div className="flex items-center gap-2">
        <Button data-testid="save-strategy-btn" onClick={onSave} disabled={!canSave}>
          {isSaving ? 'Saving...' : 'Save strategy'}
        </Button>
        {hasLoadError && <p className="text-sm text-destructive">Failed to load strategy.</p>}
      </div>
      {!hasPersistedStrategy && (
        <p className="text-sm text-muted-foreground">
          Save the strategy to enable profile lookup.
        </p>
      )}
    </Card>
  );
}

export function Personalization() {
  const strategyQuery = usePersonalizationStrategy();
  const saveStrategy = useSaveStrategy();

  const [draftStrategy, setDraftStrategy] = useState<PersonalizationStrategy | null>(null);
  const [hasHydratedDraft, setHasHydratedDraft] = useState(false);
  const [profileLookupInput, setProfileLookupInput] = useState('');
  const [submittedLookupToken, setSubmittedLookupToken] = useState<string | null>(null);

  const profileQuery = usePersonalizationProfile(submittedLookupToken);

  useEffect(() => {
    if (hasHydratedDraft || strategyQuery.data === undefined) {
      return;
    }
    setDraftStrategy(strategyQuery.data);
    setHasHydratedDraft(true);
  }, [hasHydratedDraft, strategyQuery.data]);

  const canSave = !!draftStrategy && isStrategyValid(draftStrategy) && !saveStrategy.isPending;

  const handleProfileLookup = () => {
    const nextLookupToken = profileLookupInput.trim();
    if (!nextLookupToken) {
      return;
    }

    if (nextLookupToken === submittedLookupToken) {
      void profileQuery.refetch();
      return;
    }

    setSubmittedLookupToken(nextLookupToken);
  };

  const handleSave = async () => {
    if (!draftStrategy || !isStrategyValid(draftStrategy)) {
      return;
    }

    try {
      await saveStrategy.mutateAsync(draftStrategy);
    } catch {
      // Error toast is handled in the hook
    }
  };

  const isLoadingInitialStrategy = strategyQuery.isLoading && draftStrategy === null;
  const hasPersistedStrategy = strategyQuery.data !== null && strategyQuery.data !== undefined;

  return (
    <div className="space-y-6">
      <div>
        <h2 className="text-2xl font-bold">Personalization</h2>
        <p className="text-sm text-muted-foreground mt-1">
          Configure event and facet scoring to influence ranking.
        </p>
      </div>

      {isLoadingInitialStrategy && (
        <p className="text-sm text-muted-foreground">Loading personalization strategy...</p>
      )}

      {!isLoadingInitialStrategy && !draftStrategy && strategyQuery.isError && (
        <StrategyLoadErrorState onRetry={() => void strategyQuery.refetch()} />
      )}

      {!isLoadingInitialStrategy && !draftStrategy && !strategyQuery.isError && (
        <SetupStateCard
          onUseStarterStrategy={() => {
            setDraftStrategy(createStarterStrategy());
            setHasHydratedDraft(true);
          }}
        />
      )}

      {!isLoadingInitialStrategy && draftStrategy && (
        <>
          <StrategyEditorCard
            strategy={draftStrategy}
            onChange={setDraftStrategy}
            onSave={handleSave}
            canSave={canSave}
            isSaving={saveStrategy.isPending}
            hasPersistedStrategy={hasPersistedStrategy}
            hasLoadError={strategyQuery.isError}
          />

          {hasPersistedStrategy && (
            <ProfileLookupCard
              lookupInput={profileLookupInput}
              onLookupInputChange={setProfileLookupInput}
              onLookup={handleProfileLookup}
              isLookupDisabled={profileLookupInput.trim().length === 0}
              hasLookupToken={Boolean(submittedLookupToken)}
              isLoading={profileQuery.isLoading}
              isError={profileQuery.isError}
              profile={profileQuery.data}
            />
          )}
        </>
      )}
    </div>
  );
}
