import { memo, useCallback, useMemo, useState } from 'react';
import { RotateCcw, Loader2, CheckCircle2 } from 'lucide-react';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { Switch } from '@/components/ui/switch';
import { Button } from '@/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { ConfirmDialog } from '@/components/ui/confirm-dialog';
import { SettingSection, Field } from './shared';
import {
  ArrayFieldEditor,
  SUPPORTED_QUERY_LANGUAGES,
  parseCommaSeparated,
} from './fieldEditors';
import { DisplaySettingsTab, VectorAiSettingsTab } from './SettingsTabContent';
import { useIndexFields } from '@/hooks/useIndexFields';
import { useReindex } from '@/hooks/useReindex';
import type { IndexSettings } from '@/lib/types';

type SettingsTabValue =
  | 'search'
  | 'ranking'
  | 'language'
  | 'facets'
  | 'display'
  | 'vector-ai';

interface SettingsFormProps {
  settings: Partial<IndexSettings>;
  savedSettings?: Partial<IndexSettings>;
  onChange: (updates: Partial<IndexSettings>) => void;
  indexName: string;
}

export const SettingsForm = memo(function SettingsForm({
  settings,
  savedSettings,
  onChange,
  indexName,
}: SettingsFormProps) {
  const { data: fields = [], isLoading: fieldsLoading } = useIndexFields(indexName);
  const reindex = useReindex(indexName);
  const [showReindexConfirm, setShowReindexConfirm] = useState(false);
  const [activeTab, setActiveTab] = useState<SettingsTabValue>('search');
  const distinctAttributeListId = `distinct-attributes-${indexName}`;

  // Compare current facet settings against the saved (server) values to determine
  // whether a reindex is needed. If they differ, the user changed facets since last save/reindex.
  const facetsNeedReindex = useMemo(() => {
    const current = [...(settings.attributesForFaceting || [])].sort();
    const saved = [...(savedSettings?.attributesForFaceting || [])].sort();
    return JSON.stringify(current) !== JSON.stringify(saved);
  }, [settings.attributesForFaceting, savedSettings?.attributesForFaceting]);

  const handleArrayChange = useCallback(
    (key: keyof IndexSettings, value: string) => {
      const array = parseCommaSeparated(value);
      onChange({ [key]: array.length > 0 ? array : undefined });
    },
    [onChange]
  );

  const handleNumberChange = useCallback(
    (key: keyof IndexSettings, value: string) => {
      const num = parseInt(value, 10);
      onChange({ [key]: isNaN(num) ? undefined : num });
    },
    [onChange]
  );

  const handleBooleanChange = useCallback(
    (key: keyof IndexSettings, checked: boolean) => {
      onChange({ [key]: checked });
    },
    [onChange]
  );

  const handleFieldToggle = useCallback(
    (key: keyof IndexSettings, fieldName: string) => {
      const current = (settings[key] as string[] | undefined) || [];
      const updated = current.includes(fieldName)
        ? current.filter((f) => f !== fieldName)
        : [...current, fieldName];
      onChange({ [key]: updated.length > 0 ? updated : undefined });
    },
    [settings, onChange]
  );

  const handleQueryLanguagesChange = useCallback(
    (event: React.ChangeEvent<HTMLSelectElement>) => {
      const selected = Array.from(event.target.selectedOptions, (option) => option.value);
      onChange({ queryLanguages: selected.length > 0 ? selected : undefined });
    },
    [onChange]
  );

  const handleSemanticEventSourcesChange = useCallback(
    (value: string) => {
      const eventSources = parseCommaSeparated(value);
      onChange({
        semanticSearch:
          eventSources.length > 0
            ? { ...(settings.semanticSearch || {}), eventSources }
            : undefined,
      });
    },
    [onChange, settings.semanticSearch]
  );

  const distinctEnabled =
    settings.distinct === true ||
    (typeof settings.distinct === 'number' && settings.distinct > 0);
  const distinctValue =
    typeof settings.distinct === 'number' && settings.distinct > 0
      ? settings.distinct
      : 1;

  const handleDistinctEnabledChange = useCallback(
    (enabled: boolean) => {
      if (!enabled) {
        onChange({ distinct: false, attributeForDistinct: undefined });
        return;
      }
      onChange({ distinct: distinctValue });
    },
    [distinctValue, onChange]
  );

  const handleDistinctValueChange = useCallback(
    (value: string) => {
      const parsed = parseInt(value, 10);
      if (isNaN(parsed) || parsed < 1) {
        onChange({ distinct: 1 });
        return;
      }
      onChange({ distinct: parsed });
    },
    [onChange]
  );

  const queryTypeValue: NonNullable<IndexSettings['queryType']> =
    settings.queryType || 'prefixLast';

  return (
    <div className="space-y-4">
      <Tabs value={activeTab} onValueChange={(value) => setActiveTab(value as SettingsTabValue)}>
        <div className="overflow-x-auto">
          <TabsList className="h-auto w-full min-w-[720px] justify-start gap-1 bg-muted/50 p-1">
            <TabsTrigger value="search" data-testid="settings-tab-search">Search</TabsTrigger>
            <TabsTrigger value="ranking" data-testid="settings-tab-ranking">Ranking</TabsTrigger>
            <TabsTrigger value="language" data-testid="settings-tab-language-text">
              Language & Text
            </TabsTrigger>
            <TabsTrigger value="facets" data-testid="settings-tab-facets-filters">
              Facets & Filters
            </TabsTrigger>
            <TabsTrigger value="display" data-testid="settings-tab-display">Display</TabsTrigger>
            <TabsTrigger value="vector-ai" data-testid="settings-tab-vector-ai">
              Vector / AI
            </TabsTrigger>
          </TabsList>
        </div>

        <TabsContent value="search" className="mt-4">
          <SettingSection
            title="Search"
            description="Configure how search queries are processed"
          >
            <ArrayFieldEditor
              fieldKey="searchableAttributes"
              label="Searchable Attributes"
              description="Click fields to toggle, or type comma-separated values below"
              placeholder="title, description, tags"
              selectedValues={settings.searchableAttributes}
              availableFields={fields}
              isLoading={fieldsLoading}
              onFieldToggle={handleFieldToggle}
              onArrayChange={handleArrayChange}
            />

            <Field
              label="Hits Per Page"
              description="Default number of results per page"
            >
              <Input
                type="number"
                min="1"
                max="1000"
                value={settings.hitsPerPage || ''}
                onChange={(e) => handleNumberChange('hitsPerPage', e.target.value)}
                placeholder="20"
              />
            </Field>

            <Field
              label="Query Type"
              description="Controls prefix matching strategy during query processing"
            >
              <select
                value={queryTypeValue}
                onChange={(e) =>
                  onChange({ queryType: e.target.value as NonNullable<IndexSettings['queryType']> })
                }
                className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
              >
                <option value="prefixLast">Prefix Last</option>
                <option value="prefixAll">Prefix All</option>
                <option value="prefixNone">Prefix None</option>
              </select>
            </Field>
          </SettingSection>
        </TabsContent>

        <TabsContent value="ranking" className="mt-4">
          <SettingSection
            title="Ranking"
            description="Configure ranking order and duplicate handling"
          >
            <Field
              label="Ranking Criteria"
              description="Comma-separated list of ranking criteria (typo, geo, words, filters, proximity, attribute, exact)"
            >
              <Textarea
                value={settings.ranking?.join(', ') || ''}
                onChange={(e) => handleArrayChange('ranking', e.target.value)}
                placeholder="typo, geo, words, filters, proximity, attribute, exact"
                rows={3}
              />
            </Field>

            <Field
              label="Custom Ranking"
              description="Comma-separated list of custom ranking attributes (use asc() or desc())"
            >
              <Textarea
                value={settings.customRanking?.join(', ') || ''}
                onChange={(e) => handleArrayChange('customRanking', e.target.value)}
                placeholder="desc(popularity), asc(price)"
                rows={2}
              />
            </Field>

            <Field
              label="Distinct"
              description="Enable duplicate control and set how many duplicates are kept"
            >
              <div className="space-y-3">
                <Switch
                  checked={distinctEnabled}
                  onCheckedChange={handleDistinctEnabledChange}
                  data-testid="distinct-enabled-switch"
                />
                {distinctEnabled && (
                  <Input
                    type="number"
                    min="1"
                    value={distinctValue}
                    onChange={(e) => handleDistinctValueChange(e.target.value)}
                    placeholder="1"
                  />
                )}
              </div>
            </Field>

            {distinctEnabled && (
              <Field
                label="Attribute For Distinct"
                description="Choose the attribute used to identify duplicate records"
              >
                <Input
                  list={distinctAttributeListId}
                  value={settings.attributeForDistinct || ''}
                  onChange={(e) =>
                    onChange({ attributeForDistinct: e.target.value || undefined })
                  }
                  placeholder="sku"
                />
                <datalist id={distinctAttributeListId}>
                  {fields.map((field) => (
                    <option key={field.name} value={field.name} />
                  ))}
                </datalist>
              </Field>
            )}
          </SettingSection>
        </TabsContent>

        <TabsContent value="language" className="mt-4">
          <SettingSection
            title="Language & Text"
            description="Configure language-aware text processing behavior"
          >
            <Field
              label="Query Languages"
              description="Select supported language codes used for language-specific processing"
            >
              <select
                multiple
                value={settings.queryLanguages || []}
                onChange={handleQueryLanguagesChange}
                data-testid="query-languages-select"
                className="min-h-[144px] w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
              >
                {SUPPORTED_QUERY_LANGUAGES.map((languageCode) => (
                  <option key={languageCode} value={languageCode}>
                    {languageCode}
                  </option>
                ))}
              </select>
            </Field>

            <Field
              label="Remove Stop Words"
              description="Enable stop words removal for better search relevance"
            >
              <Switch
                checked={settings.removeStopWords === true}
                onCheckedChange={(checked) =>
                  handleBooleanChange('removeStopWords', checked)
                }
              />
            </Field>

            <Field
              label="Ignore Plurals"
              description="Treat singular and plural forms as equivalent"
            >
              <Switch
                checked={settings.ignorePlurals === true}
                onCheckedChange={(checked) =>
                  handleBooleanChange('ignorePlurals', checked)
                }
              />
            </Field>

            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
              <Field
                label="Min Word Size for 1 Typo"
                description="Minimum word length to allow 1 typo"
              >
                <Input
                  type="number"
                  min="1"
                  max="10"
                  value={settings.minWordSizefor1Typo || ''}
                  onChange={(e) =>
                    handleNumberChange('minWordSizefor1Typo', e.target.value)
                  }
                  placeholder="4"
                />
              </Field>

              <Field
                label="Min Word Size for 2 Typos"
                description="Minimum word length to allow 2 typos"
              >
                <Input
                  type="number"
                  min="1"
                  max="20"
                  value={settings.minWordSizefor2Typos || ''}
                  onChange={(e) =>
                    handleNumberChange('minWordSizefor2Typos', e.target.value)
                  }
                  placeholder="8"
                />
              </Field>
            </div>
          </SettingSection>
        </TabsContent>

        <TabsContent value="facets" className="mt-4">
          <SettingSection
            title="Facets & Filters"
            description="Configure faceted search and filtering"
            warning={facetsNeedReindex ? 'Reindex needed' : undefined}
            warningDetail={
              facetsNeedReindex
                ? 'Facet attributes have changed. Save your settings, then re-index so existing documents pick up the new facets.'
                : undefined
            }
            warningAction={
              facetsNeedReindex ? (
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setShowReindexConfirm(true)}
                  disabled={reindex.isPending}
                  className="h-6 text-xs"
                >
                  {reindex.isPending ? (
                    <>
                      <Loader2 className="mr-1 h-3 w-3 animate-spin" />
                      Re-indexing...
                    </>
                  ) : (
                    <>
                      <RotateCcw className="mr-1 h-3 w-3" />
                      Re-index now
                    </>
                  )}
                </Button>
              ) : (
                <span className="inline-flex items-center gap-1 text-xs text-muted-foreground">
                  <CheckCircle2 className="h-3.5 w-3.5 text-green-500" />
                  Up to date
                </span>
              )
            }
          >
            <ArrayFieldEditor
              fieldKey="attributesForFaceting"
              label="Attributes For Faceting"
              description="Click fields to toggle, or type comma-separated values below"
              placeholder="category, brand, color"
              selectedValues={settings.attributesForFaceting}
              availableFields={fields}
              isLoading={fieldsLoading}
              onFieldToggle={handleFieldToggle}
              onArrayChange={handleArrayChange}
            />
          </SettingSection>
        </TabsContent>

        <DisplaySettingsTab
          settings={settings}
          fields={fields}
          fieldsLoading={fieldsLoading}
          onChange={onChange}
          onFieldToggle={handleFieldToggle}
          onArrayChange={handleArrayChange}
        />

        <VectorAiSettingsTab
          settings={settings}
          onChange={onChange}
          onSemanticEventSourcesChange={handleSemanticEventSourcesChange}
        />
      </Tabs>

      <ConfirmDialog
        open={showReindexConfirm}
        onOpenChange={setShowReindexConfirm}
        title="Re-index All Documents"
        description={
          <>
            This will clear and re-add all documents in{' '}
            <code className="font-mono text-sm bg-muted px-1 py-0.5 rounded">
              {indexName}
            </code>{' '}
            so they are indexed with the current settings. This may take a moment
            for large indexes.
          </>
        }
        confirmLabel="Re-index"
        onConfirm={() => {
          reindex.mutate(undefined, {
            onSettled: () => setShowReindexConfirm(false),
          });
        }}
        isPending={reindex.isPending}
      />
    </div>
  );
});
