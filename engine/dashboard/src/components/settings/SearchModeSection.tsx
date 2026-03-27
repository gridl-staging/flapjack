import { memo } from 'react';
import { Badge } from '@/components/ui/badge';
import { SettingSection, Field } from './shared';
import type { EmbedderConfig, IndexMode, IndexSettings } from '@/lib/types';

interface SearchModeSectionProps {
  mode: IndexMode | undefined;
  vectorSearchEnabled: boolean | undefined;
  embedders: Record<string, EmbedderConfig> | undefined;
  onChange: (updates: Partial<IndexSettings>) => void;
}

export const SearchModeSection = memo(function SearchModeSection({
  mode,
  vectorSearchEnabled,
  embedders,
  onChange,
}: SearchModeSectionProps) {
  const effectiveMode = mode || 'keywordSearch';
  const capabilityKnown = vectorSearchEnabled !== undefined;
  const hasEmbedders = Object.keys(embedders ?? {}).length > 0;
  const neuralSearchDisabled = vectorSearchEnabled !== true;
  const showWarning =
    vectorSearchEnabled === true &&
    effectiveMode === 'neuralSearch' &&
    !hasEmbedders;

  return (
    <SettingSection
      title="Search Mode"
      description="Choose between keyword-based or neural (vector) search"
    >
      <Field label="Mode">
        <select
          data-testid="search-mode-select"
          value={effectiveMode}
          onChange={(e) => onChange({ mode: e.target.value as IndexMode })}
          className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
        >
          <option value="keywordSearch">Keyword Search</option>
          <option value="neuralSearch" disabled={neuralSearchDisabled}>
            Neural Search
          </option>
        </select>
      </Field>

      {!capabilityKnown && (
        <Badge
          variant="outline"
          data-testid="search-mode-capability-pending"
          className="text-xs"
        >
          Waiting for server capability data
        </Badge>
      )}

      {vectorSearchEnabled === false && (
        <Badge
          variant="outline"
          data-testid="search-mode-compiled-out-warning"
          className="text-xs"
        >
          Vector search is not compiled in for this server build. Use Docker or a macOS
          release to enable Neural Search.
        </Badge>
      )}

      {showWarning && (
        <Badge
          variant="destructive"
          data-testid="search-mode-warning"
          className="text-xs"
        >
          No embedders configured — hybrid search will fall back to keyword-only
        </Badge>
      )}
    </SettingSection>
  );
});
