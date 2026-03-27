import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Wrench } from 'lucide-react';

interface FeaturePlaceholderShellProps {
  title: string;
  summary: string;
  endpointSummary: string;
}

export function FeaturePlaceholderShell({
  title,
  summary,
  endpointSummary,
}: FeaturePlaceholderShellProps) {
  return (
    <div className="space-y-6" data-testid={`placeholder-page-${title.toLowerCase().replace(/\s+/g, '-')}`}>
      <div className="flex items-center gap-3">
        <Wrench className="h-6 w-6" />
        <h1 className="text-2xl font-bold">{title}</h1>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Implementation in progress</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <p className="text-sm text-muted-foreground">{summary}</p>
          <p className="text-sm">
            <span className="font-medium">Backend readiness:</span> {endpointSummary}
          </p>
          <p className="text-sm text-muted-foreground">
            This temporary page is intentionally truthful and will be replaced by the full management UI in a later stage.
          </p>
        </CardContent>
      </Card>
    </div>
  );
}
