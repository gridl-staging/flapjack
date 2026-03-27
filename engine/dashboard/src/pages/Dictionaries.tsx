import { useMemo, useState } from 'react';
import { Plus } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { Tabs, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { useAddDictionaryEntry, useDeleteDictionaryEntry, useDictionarySearch } from '@/hooks/useDictionaries';
import type { DictionaryEntry, DictionaryName } from '@/lib/types';
import { DictionaryEntriesPanel } from './dictionaries/DictionaryEntriesPanel';
import { DictionaryEntryDialog } from './dictionaries/DictionaryEntryDialog';
import { DICTIONARY_LABELS, DICTIONARY_NAMES, isDictionaryName } from './dictionaries/shared';

export function Dictionaries() {
  const [activeDictionary, setActiveDictionary] = useState<DictionaryName>('stopwords');
  const [isDialogOpen, setIsDialogOpen] = useState(false);

  const { data, isLoading } = useDictionarySearch(activeDictionary, '');
  const addEntry = useAddDictionaryEntry(activeDictionary);
  const deleteEntry = useDeleteDictionaryEntry(activeDictionary);

  const entries = data?.hits ?? [];
  const count = useMemo(() => data?.nbHits ?? 0, [data]);

  const submitEntry = (entry: DictionaryEntry) => addEntry.mutateAsync(entry);

  const handleDelete = async (objectID: string) => {
    try {
      await deleteEntry.mutateAsync(objectID);
    } catch {
      // Error toast is emitted by the mutation hook.
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <h1 className="text-2xl font-bold">Dictionaries</h1>
          <Badge variant="secondary">{count}</Badge>
        </div>
        <Button onClick={() => setIsDialogOpen(true)} data-testid="add-dictionary-entry-btn">
          <Plus className="mr-1 h-4 w-4" />
          Add Entry
        </Button>
      </div>

      <Tabs
        value={activeDictionary}
        onValueChange={(nextValue) => {
          if (isDictionaryName(nextValue)) {
            setActiveDictionary(nextValue);
          }
        }}
      >
        <TabsList>
          {DICTIONARY_NAMES.map((dictName) => (
            <TabsTrigger key={dictName} value={dictName} data-testid={`dictionary-tab-${dictName}`}>
              {DICTIONARY_LABELS[dictName]}
            </TabsTrigger>
          ))}
        </TabsList>
      </Tabs>

      <Card className="p-6">
        <div className="mb-4 flex items-center justify-between">
          <h2 className="text-xl font-semibold">{DICTIONARY_LABELS[activeDictionary]}</h2>
          <Badge variant="outline">{count} entries</Badge>
        </div>

        <DictionaryEntriesPanel
          dictName={activeDictionary}
          entries={entries}
          count={count}
          isLoading={isLoading}
          isDeleting={deleteEntry.isPending}
          onDelete={handleDelete}
        />
      </Card>

      <DictionaryEntryDialog
        dictName={activeDictionary}
        open={isDialogOpen}
        isPending={addEntry.isPending}
        onOpenChange={setIsDialogOpen}
        onSubmit={submitEntry}
      />
    </div>
  );
}
