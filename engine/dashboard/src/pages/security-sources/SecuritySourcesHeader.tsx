import { Plus } from 'lucide-react';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';

interface SecuritySourcesHeaderProps {
  entryCount: number;
  onAdd: () => void;
}

export function SecuritySourcesHeader({ entryCount, onAdd }: SecuritySourcesHeaderProps) {
  return (
    <div className="flex items-center justify-between">
      <div className="flex items-center gap-2">
        <h1 className="text-2xl font-bold">Security Sources</h1>
        <Badge variant="secondary">{entryCount}</Badge>
      </div>
      <Button onClick={onAdd} data-testid="add-security-source-btn">
        <Plus className="mr-1 h-4 w-4" />
        Add Source
      </Button>
    </div>
  );
}
