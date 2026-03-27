import { useState } from 'react';
import { Badge } from '@/components/ui/badge';
import { Card } from '@/components/ui/card';
import {
  useAppendSecuritySource,
  useDeleteSecuritySource,
  useSecuritySources,
} from '@/hooks/useSecuritySources';
import { SecuritySourceDialog } from './security-sources/SecuritySourceDialog';
import { SecuritySourcesHeader } from './security-sources/SecuritySourcesHeader';
import { SecuritySourcesList } from './security-sources/SecuritySourcesList';
import { SOURCE_REQUIRED_MESSAGE } from './security-sources/shared';

function getQueryErrorDetail(error: unknown): string | undefined {
  if (error instanceof Error && error.message.length > 0) {
    return error.message;
  }

  return undefined;
}

export function SecuritySources() {
  const { data, error, isError, isLoading } = useSecuritySources();
  const appendSource = useAppendSecuritySource();
  const deleteSource = useDeleteSecuritySource();

  const [isDialogOpen, setIsDialogOpen] = useState(false);
  const [sourceValue, setSourceValue] = useState('');
  const [descriptionValue, setDescriptionValue] = useState('');
  const [sourceValidationError, setSourceValidationError] = useState('');

  const entries = data ?? [];
  const errorDetail = isError ? getQueryErrorDetail(error) : undefined;

  function resetForm() {
    setSourceValue('');
    setDescriptionValue('');
    setSourceValidationError('');
  }

  function openDialog() {
    resetForm();
    setIsDialogOpen(true);
  }

  function closeDialog() {
    setIsDialogOpen(false);
    resetForm();
  }

  function handleSourceChange(value: string) {
    setSourceValue(value);
    if (sourceValidationError) {
      setSourceValidationError('');
    }
  }

  async function submitNewSource() {
    const source = sourceValue.trim();
    if (source.length === 0) {
      setSourceValidationError(SOURCE_REQUIRED_MESSAGE);
      return;
    }

    try {
      await appendSource.mutateAsync({
        source,
        description: descriptionValue.trim(),
      });
      closeDialog();
    } catch {
      // Error toast is emitted by the mutation hook.
    }
  }

  async function handleDelete(source: string) {
    try {
      await deleteSource.mutateAsync(source);
    } catch {
      // Error toast is emitted by the mutation hook.
    }
  }

  return (
    <div className="space-y-6">
      <SecuritySourcesHeader entryCount={entries.length} onAdd={openDialog} />

      <Card className="p-6">
        <div className="mb-4 flex items-center justify-between">
          <h2 className="text-xl font-semibold">Source Allowlist</h2>
          <Badge variant="outline">{entries.length} entries</Badge>
        </div>

        <SecuritySourcesList
          entries={entries}
          errorDetail={errorDetail}
          isLoading={isLoading}
          isDeleting={deleteSource.isPending}
          onDelete={handleDelete}
        />
      </Card>

      <SecuritySourceDialog
        open={isDialogOpen}
        isPending={appendSource.isPending}
        sourceValue={sourceValue}
        descriptionValue={descriptionValue}
        sourceValidationError={sourceValidationError}
        onOpenChange={(open) => (open ? openDialog() : closeDialog())}
        onSourceChange={handleSourceChange}
        onDescriptionChange={setDescriptionValue}
        onSubmit={submitNewSource}
      />
    </div>
  );
}
