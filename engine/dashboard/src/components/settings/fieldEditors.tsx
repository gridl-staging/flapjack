import { memo } from 'react';
import { Textarea } from '@/components/ui/textarea';
import { Field, FieldChips } from './shared';
import type { IndexSettings } from '@/lib/types';

export const SUPPORTED_QUERY_LANGUAGES = [
  'en',
  'fr',
  'de',
  'es',
  'it',
  'pt',
  'nl',
  'ja',
  'ko',
  'zh',
  'ar',
  'ru',
] as const;

export function parseCommaSeparated(value: string) {
  return value
    .split(',')
    .map((part) => part.trim())
    .filter(Boolean);
}

interface ArrayFieldEditorProps {
  fieldKey: keyof IndexSettings;
  label: string;
  description: string;
  placeholder: string;
  selectedValues: string[] | undefined;
  availableFields: Array<{ name: string; type: 'text' | 'number' | 'boolean' }>;
  isLoading: boolean;
  rows?: number;
  onFieldToggle: (key: keyof IndexSettings, fieldName: string) => void;
  onArrayChange: (key: keyof IndexSettings, value: string) => void;
}

export const ArrayFieldEditor = memo(function ArrayFieldEditor({
  fieldKey,
  label,
  description,
  placeholder,
  selectedValues,
  availableFields,
  isLoading,
  rows = 2,
  onFieldToggle,
  onArrayChange,
}: ArrayFieldEditorProps) {
  return (
    <Field label={label} description={description}>
      <FieldChips
        availableFields={availableFields}
        selectedValues={selectedValues || []}
        onToggle={(name) => onFieldToggle(fieldKey, name)}
        isLoading={isLoading}
      />
      <Textarea
        value={selectedValues?.join(', ') || ''}
        onChange={(e) => onArrayChange(fieldKey, e.target.value)}
        placeholder={placeholder}
        rows={rows}
      />
    </Field>
  );
});
