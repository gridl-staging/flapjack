import { useEffect, useState } from 'react';
import { Button } from '@/components/ui/button';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import type { DictionaryEntry, DictionaryName } from '@/lib/types';
import {
  DICTIONARY_LABELS,
  LANGUAGE_OPTIONS,
  LANGUAGE_SELECT_CLASS,
  buildDialogEntry,
  createCompoundFormState,
  createPluralFormState,
  createStopwordFormState,
  type CompoundFormState,
  type PluralFormState,
  type StopwordFormState,
} from './shared';

interface LanguageSelectProps {
  id: string;
  value: string;
  onChange: (language: string) => void;
}

function LanguageSelect({ id, value, onChange }: LanguageSelectProps) {
  return (
    <div className="space-y-2">
      <Label htmlFor={id}>Language</Label>
      <select
        id={id}
        value={value}
        onChange={(event) => onChange(event.target.value)}
        className={LANGUAGE_SELECT_CLASS}
      >
        {LANGUAGE_OPTIONS.map((language) => (
          <option key={language} value={language}>{language}</option>
        ))}
      </select>
    </div>
  );
}

interface StopwordFormFieldsProps {
  form: StopwordFormState;
  onChange: (patch: Partial<StopwordFormState>) => void;
}

function StopwordFormFields({ form, onChange }: StopwordFormFieldsProps) {
  return (
    <>
      <div className="space-y-2">
        <Label htmlFor="dictionary-word">Word</Label>
        <Input
          id="dictionary-word"
          value={form.word}
          onChange={(event) => onChange({ word: event.target.value })}
          placeholder="Enter stopword"
        />
      </div>

      <LanguageSelect
        id="dictionary-language"
        value={form.language}
        onChange={(language) => onChange({ language })}
      />

      <div className="space-y-2">
        <Label htmlFor="dictionary-state">State</Label>
        <select
          id="dictionary-state"
          value={form.state}
          onChange={(event) => onChange({ state: event.target.value as 'enabled' | 'disabled' })}
          className={LANGUAGE_SELECT_CLASS}
        >
          <option value="enabled">enabled</option>
          <option value="disabled">disabled</option>
        </select>
      </div>
    </>
  );
}

interface PluralFormFieldsProps {
  form: PluralFormState;
  onChange: (patch: Partial<PluralFormState>) => void;
}

function PluralFormFields({ form, onChange }: PluralFormFieldsProps) {
  return (
    <>
      <div className="space-y-2">
        <Label htmlFor="dictionary-words">Words</Label>
        <Input
          id="dictionary-words"
          value={form.words}
          onChange={(event) => onChange({ words: event.target.value })}
          placeholder="shoe, shoes"
        />
      </div>

      <LanguageSelect
        id="dictionary-language"
        value={form.language}
        onChange={(language) => onChange({ language })}
      />
    </>
  );
}

interface CompoundFormFieldsProps {
  form: CompoundFormState;
  onChange: (patch: Partial<CompoundFormState>) => void;
}

function CompoundFormFields({ form, onChange }: CompoundFormFieldsProps) {
  return (
    <>
      <div className="space-y-2">
        <Label htmlFor="dictionary-compound-word">Word</Label>
        <Input
          id="dictionary-compound-word"
          value={form.word}
          onChange={(event) => onChange({ word: event.target.value })}
          placeholder="notebook"
        />
      </div>

      <div className="space-y-2">
        <Label htmlFor="dictionary-decomposition">Decomposition</Label>
        <Input
          id="dictionary-decomposition"
          value={form.decomposition}
          onChange={(event) => onChange({ decomposition: event.target.value })}
          placeholder="note, book"
        />
      </div>

      <LanguageSelect
        id="dictionary-language"
        value={form.language}
        onChange={(language) => onChange({ language })}
      />
    </>
  );
}

interface DictionaryEntryDialogProps {
  dictName: DictionaryName;
  open: boolean;
  isPending: boolean;
  onOpenChange: (open: boolean) => void;
  onSubmit: (entry: DictionaryEntry) => Promise<void>;
}

export function DictionaryEntryDialog({
  dictName,
  open,
  isPending,
  onOpenChange,
  onSubmit,
}: DictionaryEntryDialogProps) {
  const [stopwordForm, setStopwordForm] = useState<StopwordFormState>(createStopwordFormState());
  const [pluralForm, setPluralForm] = useState<PluralFormState>(createPluralFormState());
  const [compoundForm, setCompoundForm] = useState<CompoundFormState>(createCompoundFormState());

  useEffect(() => {
    if (!open) {
      return;
    }

    setStopwordForm(createStopwordFormState());
    setPluralForm(createPluralFormState());
    setCompoundForm(createCompoundFormState());
  }, [dictName, open]);

  const handleSubmit = async () => {
    const entry = buildDialogEntry(dictName, stopwordForm, pluralForm, compoundForm);
    if (!entry) {
      return;
    }

    try {
      await onSubmit(entry);
      onOpenChange(false);
    } catch {
      // Error toast is emitted by the mutation hook.
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Add {DICTIONARY_LABELS[dictName]} Entry</DialogTitle>
          <DialogDescription>
            Create a new {dictName} dictionary entry.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          {dictName === 'stopwords' && (
            <StopwordFormFields
              form={stopwordForm}
              onChange={(patch) => setStopwordForm((cur) => ({ ...cur, ...patch }))}
            />
          )}
          {dictName === 'plurals' && (
            <PluralFormFields
              form={pluralForm}
              onChange={(patch) => setPluralForm((cur) => ({ ...cur, ...patch }))}
            />
          )}
          {dictName === 'compounds' && (
            <CompoundFormFields
              form={compoundForm}
              onChange={(patch) => setCompoundForm((cur) => ({ ...cur, ...patch }))}
            />
          )}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>Cancel</Button>
          <Button onClick={handleSubmit} disabled={isPending}>Add Entry</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
