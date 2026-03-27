import { memo, useCallback } from 'react';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { TabsContent } from '@/components/ui/tabs';
import { SettingSection, Field } from './shared';
import { SearchModeSection } from './SearchModeSection';
import { EmbedderPanel } from './EmbedderPanel';
import { ArrayFieldEditor } from './fieldEditors';
import { useHealthDetail } from '@/hooks/useSystemStatus';
import type { IndexSettings } from '@/lib/types';

interface DisplaySettingsTabProps {
  settings: Partial<IndexSettings>;
  fields: Array<{ name: string; type: 'text' | 'number' | 'boolean' }>;
  fieldsLoading: boolean;
  onChange: (updates: Partial<IndexSettings>) => void;
  onFieldToggle: (key: keyof IndexSettings, fieldName: string) => void;
  onArrayChange: (key: keyof IndexSettings, value: string) => void;
}

export const DisplaySettingsTab = memo(function DisplaySettingsTab({
  settings,
  fields,
  fieldsLoading,
  onChange,
  onFieldToggle,
  onArrayChange,
}: DisplaySettingsTabProps) {
  return (
    <TabsContent value="display" className="mt-4">
      <SettingSection
        title="Display"
        description="Configure returned and highlighted fields in search responses"
      >
        <ArrayFieldEditor
          fieldKey="attributesToRetrieve"
          label="Attributes To Retrieve"
          description="Click fields to toggle, or type comma-separated values below"
          placeholder="title, description, image, price"
          selectedValues={settings.attributesToRetrieve}
          availableFields={fields}
          isLoading={fieldsLoading}
          onFieldToggle={onFieldToggle}
          onArrayChange={onArrayChange}
        />

        <ArrayFieldEditor
          fieldKey="unretrievableAttributes"
          label="Unretrievable Attributes"
          description="Stored attributes that are never returned in search responses"
          placeholder="internal_notes, supplier_cost"
          selectedValues={settings.unretrievableAttributes}
          availableFields={fields}
          isLoading={fieldsLoading}
          onFieldToggle={onFieldToggle}
          onArrayChange={onArrayChange}
        />

        <ArrayFieldEditor
          fieldKey="attributesToHighlight"
          label="Attributes To Highlight"
          description="Click fields to toggle, or type comma-separated values below"
          placeholder="title, description"
          selectedValues={settings.attributesToHighlight}
          availableFields={fields}
          isLoading={fieldsLoading}
          onFieldToggle={onFieldToggle}
          onArrayChange={onArrayChange}
        />

        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
          <Field label="Highlight Pre Tag" description="Opening tag for highlights">
            <Input
              value={settings.highlightPreTag || ''}
              onChange={(e) =>
                onChange({ highlightPreTag: e.target.value || undefined })
              }
              placeholder="<em>"
            />
          </Field>

          <Field label="Highlight Post Tag" description="Closing tag for highlights">
            <Input
              value={settings.highlightPostTag || ''}
              onChange={(e) =>
                onChange({ highlightPostTag: e.target.value || undefined })
              }
              placeholder="</em>"
            />
          </Field>
        </div>
      </SettingSection>
    </TabsContent>
  );
});

interface VectorAiSettingsTabProps {
  settings: Partial<IndexSettings>;
  onChange: (updates: Partial<IndexSettings>) => void;
  onSemanticEventSourcesChange: (value: string) => void;
}

export const VectorAiSettingsTab = memo(function VectorAiSettingsTab({
  settings,
  onChange,
  onSemanticEventSourcesChange,
}: VectorAiSettingsTabProps) {
  const { data: health } = useHealthDetail();
  const vectorSearchEnabled = health?.capabilities.vectorSearch;
  const aiProvider = settings.userData?.aiProvider;

  const handleAiProviderChange = useCallback(
    (field: string, value: string) => {
      const current = settings.userData?.aiProvider || {};
      onChange({
        userData: {
          ...settings.userData,
          aiProvider: { ...current, [field]: value || undefined },
        },
      });
    },
    [settings.userData, onChange]
  );

  return (
    <TabsContent value="vector-ai" className="mt-4 space-y-4">
      <SearchModeSection
        mode={settings.mode}
        vectorSearchEnabled={vectorSearchEnabled}
        embedders={settings.embedders}
        onChange={onChange}
      />

      <EmbedderPanel
        embedders={settings.embedders}
        vectorSearchEnabled={vectorSearchEnabled}
        onChange={onChange}
      />

      <SettingSection
        title="AI Provider"
        description="Configure the AI provider used for chat and generation features"
      >
        <Field label="AI Base URL" description="Base URL for the AI provider API (e.g. https://api.openai.com/v1)">
          <Input
            aria-label="AI Base URL"
            value={aiProvider?.baseUrl || ''}
            onChange={(e) => handleAiProviderChange('baseUrl', e.target.value)}
            placeholder="https://api.openai.com/v1"
          />
        </Field>

        <Field label="AI Model" description="Model identifier">
          <Input
            aria-label="AI Model"
            value={aiProvider?.model || ''}
            onChange={(e) => handleAiProviderChange('model', e.target.value)}
            placeholder="gpt-4"
          />
        </Field>

        <Field label="AI API Key" description="API key for the provider (stored securely, shown as redacted after save)">
          <Input
            aria-label="AI API Key"
            type="password"
            value={aiProvider?.apiKey || ''}
            onChange={(e) => handleAiProviderChange('apiKey', e.target.value)}
            placeholder="sk-..."
          />
        </Field>
      </SettingSection>

      <SettingSection
        title="Semantic Search"
        description="Optional semantic event source configuration"
      >
        <Field
          label="Semantic Event Sources"
          description="Comma-separated event source names for semantic settings"
        >
          <Textarea
            value={settings.semanticSearch?.eventSources?.join(', ') || ''}
            onChange={(e) => onSemanticEventSourcesChange(e.target.value)}
            placeholder="click, conversion"
            rows={2}
          />
        </Field>
      </SettingSection>
    </TabsContent>
  );
});
