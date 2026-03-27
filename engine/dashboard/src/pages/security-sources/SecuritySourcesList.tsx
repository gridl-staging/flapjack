import { Trash2 } from 'lucide-react';
import { Button } from '@/components/ui/button';
import type { SecuritySourceEntry } from '@/lib/types';
import { formatSecuritySourceDescription } from './shared';

interface SecuritySourcesListProps {
  entries: SecuritySourceEntry[];
  errorDetail?: string;
  isLoading: boolean;
  isDeleting: boolean;
  onDelete: (source: string) => Promise<void>;
}

export function SecuritySourcesList({
  entries,
  errorDetail,
  isLoading,
  isDeleting,
  onDelete,
}: SecuritySourcesListProps) {
  if (isLoading) {
    return <p className="text-sm text-muted-foreground">Loading security sources...</p>;
  }

  if (errorDetail !== undefined) {
    return (
      <div className="space-y-1" data-testid="security-sources-error-state">
        <p className="text-sm text-destructive">Unable to load security sources.</p>
        <p className="text-sm text-muted-foreground">{errorDetail}</p>
      </div>
    );
  }

  if (entries.length === 0) {
    return (
      <p className="text-sm text-muted-foreground" data-testid="security-sources-empty-state">
        No security sources configured yet.
      </p>
    );
  }

  return (
    <div className="space-y-3" data-testid="security-sources-list">
      {entries.map((entry) => (
        <div
          key={entry.source}
          className="flex items-start justify-between rounded-md border p-4"
          data-testid="security-source-row"
        >
          <div className="space-y-1">
            <p className="font-mono text-sm">{entry.source}</p>
            <p className="text-sm text-muted-foreground">
              {formatSecuritySourceDescription(entry.description)}
            </p>
          </div>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => onDelete(entry.source)}
            data-testid="delete-security-source-btn"
            disabled={isDeleting}
          >
            <Trash2 className="h-4 w-4 text-destructive" />
            <span className="sr-only">Delete</span>
          </Button>
        </div>
      ))}
    </div>
  );
}
