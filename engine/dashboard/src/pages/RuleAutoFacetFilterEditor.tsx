import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import type { AutomaticFacetFilter } from '@/lib/types';

interface AutoFacetFilterEditorProps {
  label: string;
  labelPrefix: string;
  idPrefix: string;
  disjunctiveLabel: string;
  scoreLabel: string;
  filters: AutomaticFacetFilter[];
  onChange: (filters: AutomaticFacetFilter[]) => void;
}

export function AutoFacetFilterEditor({
  label,
  labelPrefix,
  idPrefix,
  disjunctiveLabel,
  scoreLabel,
  filters,
  onChange,
}: AutoFacetFilterEditorProps) {
  const addFilter = () => {
    onChange([...filters, { facet: '' }]);
  };

  const removeFilter = (index: number) => {
    onChange(filters.filter((_, i) => i !== index));
  };

  const updateFilter = (index: number, updates: Partial<AutomaticFacetFilter>) => {
    onChange(
      filters.map((filter, currentIndex) => {
        if (currentIndex !== index) return filter;
        const merged = { ...filter, ...updates };

        // Keep the serialized payload clean so the rule editor preview mirrors the saved shape.
        const result: AutomaticFacetFilter = { facet: merged.facet };
        if (merged.disjunctive) result.disjunctive = true;
        if (merged.score !== undefined && merged.score !== null) result.score = merged.score;
        return result;
      }),
    );
  };

  return (
    <div className="space-y-2 rounded-md border p-3">
      <div className="flex items-center justify-between">
        <p className="font-medium">{label}</p>
        <Button type="button" variant="outline" size="sm" onClick={addFilter}>
          {`Add ${labelPrefix}`}
        </Button>
      </div>
      {filters.length === 0 ? (
        <p className="text-sm text-muted-foreground">No {label.toLowerCase()}.</p>
      ) : (
        <div className="space-y-2">
          {filters.map((filter, index) => {
            const number = index + 1;
            return (
              <div key={`${idPrefix}-${index}`} className="flex items-end gap-2">
                <div className="flex-1 space-y-1">
                  <Label htmlFor={`${idPrefix}-name-${index}`}>{`${labelPrefix} Name ${number}`}</Label>
                  <Input
                    id={`${idPrefix}-name-${index}`}
                    value={filter.facet}
                    onChange={(event) => updateFilter(index, { facet: event.target.value })}
                  />
                </div>
                <div className="flex items-center gap-1.5 pb-2">
                  <input
                    type="checkbox"
                    id={`${idPrefix}-disj-${index}`}
                    checked={filter.disjunctive === true}
                    onChange={(event) =>
                      updateFilter(index, { disjunctive: event.target.checked })
                    }
                  />
                  <Label htmlFor={`${idPrefix}-disj-${index}`}>{`${disjunctiveLabel} ${number}`}</Label>
                </div>
                <div className="w-20 space-y-1">
                  <Label htmlFor={`${idPrefix}-score-${index}`}>{`${scoreLabel} ${number}`}</Label>
                  <Input
                    id={`${idPrefix}-score-${index}`}
                    type="number"
                    value={filter.score ?? ''}
                    onChange={(event) =>
                      updateFilter(index, {
                        score: event.target.value ? Number(event.target.value) : undefined,
                      })
                    }
                  />
                </div>
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  onClick={() => removeFilter(index)}
                >
                  {`Remove ${labelPrefix} ${number}`}
                </Button>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
