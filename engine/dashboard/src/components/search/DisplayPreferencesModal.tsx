import { useEffect, useMemo, useState } from 'react';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Label } from '@/components/ui/label';
import { FieldChips } from '@/components/settings/shared';
import { useDisplayPreferences, autoDetectPreferences } from '@/hooks/useDisplayPreferences';
import { useIndexFields } from '@/hooks/useIndexFields';
import type { DisplayPreferences, FieldInfo } from '@/lib/types';

interface DisplayPreferencesModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  indexName: string;
}

type SingleFieldPreferenceKey = 'titleAttribute' | 'subtitleAttribute' | 'imageAttribute';

interface SingleFieldControl {
  key: SingleFieldPreferenceKey;
  id: string;
  label: string;
}

interface SingleFieldSelectProps {
  control: SingleFieldControl;
  fieldOptions: string[];
  selectedValue: string;
  onValueChange: (value: string) => void;
  disabled?: boolean;
}

interface TagFieldsSectionProps {
  fields: FieldInfo[];
  selectedValues: string[];
  onToggle: (fieldName: string) => void;
  isLoading: boolean;
  errorMessage?: string | null;
}

interface ModalFooterActionsProps {
  onAutoDetect: () => void;
  onClear: () => void;
  onCancel: () => void;
  onSave: () => void;
  disableAutoDetect?: boolean;
  disableSave?: boolean;
}

const EMPTY_PREFERENCES: DisplayPreferences = {
  titleAttribute: null,
  subtitleAttribute: null,
  imageAttribute: null,
  tagAttributes: [],
};

const SINGLE_FIELD_CONTROLS: SingleFieldControl[] = [
  { key: 'titleAttribute', id: 'display-preferences-title', label: 'Title field' },
  { key: 'subtitleAttribute', id: 'display-preferences-subtitle', label: 'Subtitle field' },
  { key: 'imageAttribute', id: 'display-preferences-image', label: 'Image field' },
];

function toDraftValue(value: string | null): string {
  return value ?? '';
}

function fromDraftValue(value: string): string | null {
  return value ? value : null;
}

function toggleTagAttribute(tagAttributes: string[], fieldName: string): string[] {
  return tagAttributes.includes(fieldName)
    ? tagAttributes.filter((name) => name !== fieldName)
    : [...tagAttributes, fieldName];
}

function SingleFieldSelect({
  control,
  fieldOptions,
  selectedValue,
  onValueChange,
  disabled,
}: SingleFieldSelectProps) {
  return (
    <div className="space-y-2">
      <Label htmlFor={control.id}>{control.label}</Label>
      <select
        id={control.id}
        value={selectedValue}
        disabled={disabled}
        onChange={(event) => onValueChange(event.target.value)}
        className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
      >
        <option value="">None</option>
        {fieldOptions.map((fieldName) => (
          <option key={fieldName} value={fieldName}>
            {fieldName}
          </option>
        ))}
      </select>
    </div>
  );
}

function TagFieldsSection({
  fields,
  selectedValues,
  onToggle,
  isLoading,
  errorMessage,
}: TagFieldsSectionProps) {
  return (
    <div className="space-y-2">
      <Label>Tag fields</Label>
      {errorMessage ? (
        <p role="alert" className="text-sm text-destructive">
          {errorMessage}
        </p>
      ) : (
        <FieldChips
          availableFields={fields}
          selectedValues={selectedValues}
          onToggle={onToggle}
          isLoading={isLoading}
        />
      )}
    </div>
  );
}

function ModalFooterActions({
  onAutoDetect,
  onClear,
  onCancel,
  onSave,
  disableAutoDetect,
  disableSave,
}: ModalFooterActionsProps) {
  return (
    <DialogFooter className="flex items-center justify-between">
      <div className="flex gap-2">
        <Button type="button" variant="outline" onClick={onAutoDetect} disabled={disableAutoDetect}>
          Auto-detect
        </Button>
        <Button type="button" variant="outline" onClick={onClear}>
          Clear
        </Button>
      </div>
      <div className="flex gap-2">
        <Button type="button" variant="outline" onClick={onCancel}>
          Cancel
        </Button>
        <Button type="button" onClick={onSave} disabled={disableSave}>
          Save
        </Button>
      </div>
    </DialogFooter>
  );
}

export function DisplayPreferencesModal({
  open,
  onOpenChange,
  indexName,
}: DisplayPreferencesModalProps) {
  const {
    data: fields = [],
    isLoading: fieldsLoading,
    error: fieldsError,
  } = useIndexFields(indexName, open);
  const { preferences, setPreferences, clearPreferences } = useDisplayPreferences(indexName);
  const [draft, setDraft] = useState<DisplayPreferences>(EMPTY_PREFERENCES);
  const fieldLoadErrorMessage = fieldsError
    ? 'Unable to load index fields. Try reopening the dialog.'
    : null;
  const disableFieldControls = Boolean(fieldLoadErrorMessage);

  useEffect(() => {
    if (!open) {
      return;
    }

    setDraft(preferences ?? EMPTY_PREFERENCES);
  }, [open, indexName, preferences]);

  const fieldOptions = useMemo(() => fields.map((field) => field.name), [fields]);

  function handleSingleFieldChange(key: SingleFieldPreferenceKey, value: string) {
    setDraft((previousDraft) => ({
      ...previousDraft,
      [key]: fromDraftValue(value),
    }));
  }

  function handleTagToggle(fieldName: string) {
    setDraft((previousDraft) => ({
      ...previousDraft,
      tagAttributes: toggleTagAttribute(previousDraft.tagAttributes, fieldName),
    }));
  }

  function handleAutoDetect() {
    setDraft(autoDetectPreferences(fields) ?? EMPTY_PREFERENCES);
  }

  function handleClear() {
    clearPreferences(indexName);
    setDraft(EMPTY_PREFERENCES);
  }

  function handleSave() {
    setPreferences(indexName, draft);
    onOpenChange(false);
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[560px]">
        <DialogHeader>
          <DialogTitle>Display Preferences</DialogTitle>
          <DialogDescription>
            Configure browse card fields for this index. Changes are saved per index.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          {SINGLE_FIELD_CONTROLS.map((control) => (
            <SingleFieldSelect
              key={control.key}
              control={control}
              fieldOptions={fieldOptions}
              selectedValue={toDraftValue(draft[control.key])}
              onValueChange={(value) => handleSingleFieldChange(control.key, value)}
              disabled={disableFieldControls}
            />
          ))}
          <TagFieldsSection
            fields={fields}
            selectedValues={draft.tagAttributes}
            onToggle={handleTagToggle}
            isLoading={fieldsLoading}
            errorMessage={fieldLoadErrorMessage}
          />
        </div>
        <ModalFooterActions
          onAutoDetect={handleAutoDetect}
          onClear={handleClear}
          onCancel={() => onOpenChange(false)}
          onSave={handleSave}
          disableAutoDetect={disableFieldControls}
          disableSave={disableFieldControls}
        />
      </DialogContent>
    </Dialog>
  );
}
