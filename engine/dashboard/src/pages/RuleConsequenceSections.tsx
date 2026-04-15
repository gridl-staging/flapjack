import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Textarea } from '@/components/ui/textarea';
import type {
  RuleConsequence,
  RulePromote,
  RuleHide,
  ConsequenceParams,
  Edit,
  AutomaticFacetFilter,
} from '@/lib/types';

type QueryMode = 'none' | 'literal' | 'edits';

interface ConsequenceSectionProps {
  consequence: RuleConsequence;
  onChange: (consequence: RuleConsequence) => void;
}

interface ParamsSectionProps extends ConsequenceSectionProps {
  params: ConsequenceParams;
  updateParams: (updates: Partial<ConsequenceParams>) => void;
}

function getQueryMode(params?: ConsequenceParams): QueryMode {
  if (params?.query === undefined || params?.query === null) return 'none';
  if (typeof params.query === 'string') return 'literal';
  return 'edits';
}

function getQueryLiteral(params?: ConsequenceParams): string {
  if (typeof params?.query === 'string') return params.query;
  return '';
}

function getQueryEdits(params?: ConsequenceParams): Edit[] {
  if (params?.query && typeof params.query === 'object' && params.query.edits) {
    return params.query.edits;
  }
  return [];
}

export function normalizeAutoFacetFilters(
  filters?: Array<AutomaticFacetFilter | string>,
): AutomaticFacetFilter[] {
  if (!filters) return [];
  return filters.map((filter) => (typeof filter === 'string' ? { facet: filter } : filter));
}

function getPromotes(consequence: RuleConsequence): Array<{ objectID: string; position: number }> {
  if (!consequence.promote) return [];
  return consequence.promote.flatMap((promote) => {
    if ('objectID' in promote) return [{ objectID: promote.objectID, position: promote.position }];
    return (promote.objectIDs || []).map((id) => ({ objectID: id, position: promote.position }));
  });
}

export function PromotedItemsSection({ consequence, onChange }: ConsequenceSectionProps) {
  const promotes = getPromotes(consequence);

  const addPromote = () => {
    const next: RulePromote[] = [...promotes, { objectID: '', position: 0 }];
    onChange({ ...consequence, promote: next });
  };

  const removePromote = (index: number) => {
    const next = promotes.filter((_, currentIndex) => currentIndex !== index);
    onChange({ ...consequence, promote: next.length > 0 ? next : undefined });
  };

  const updatePromote = (index: number, field: 'objectID' | 'position', value: string | number) => {
    const next = promotes.map((promote, currentIndex) =>
      currentIndex === index
        ? { ...promote, [field]: field === 'position' ? Number(value) : value }
        : promote,
    );
    onChange({ ...consequence, promote: next });
  };

  return (
    <div className="space-y-2 rounded-md border p-3">
      <div className="flex items-center justify-between">
        <p className="font-medium">Promoted Items</p>
        <Button type="button" variant="outline" size="sm" onClick={addPromote}>
          Add Promoted Item
        </Button>
      </div>
      {promotes.length === 0 ? (
        <p className="text-sm text-muted-foreground">No promoted items.</p>
      ) : (
        <div className="space-y-2">
          {promotes.map((promote, index) => {
            const number = index + 1;
            return (
              <div key={`promote-${index}`} className="flex items-end gap-2">
                <div className="flex-1 space-y-1">
                  <Label htmlFor={`promote-oid-${index}`}>{`Promote Object ID ${number}`}</Label>
                  <Input
                    id={`promote-oid-${index}`}
                    value={promote.objectID}
                    onChange={(event) => updatePromote(index, 'objectID', event.target.value)}
                  />
                </div>
                <div className="w-24 space-y-1">
                  <Label htmlFor={`promote-pos-${index}`}>{`Promote Position ${number}`}</Label>
                  <Input
                    id={`promote-pos-${index}`}
                    type="number"
                    min={0}
                    value={promote.position}
                    onChange={(event) => updatePromote(index, 'position', event.target.value)}
                  />
                </div>
                <Button type="button" variant="ghost" size="sm" onClick={() => removePromote(index)}>
                  {`Remove Promote ${number}`}
                </Button>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

export function HiddenItemsSection({ consequence, onChange }: ConsequenceSectionProps) {
  const hides = consequence.hide || [];

  const addHide = () => {
    const next: RuleHide[] = [...hides, { objectID: '' }];
    onChange({ ...consequence, hide: next });
  };

  const removeHide = (index: number) => {
    const next = hides.filter((_, currentIndex) => currentIndex !== index);
    onChange({ ...consequence, hide: next.length > 0 ? next : undefined });
  };

  const updateHide = (index: number, objectID: string) => {
    const next = hides.map((hide, currentIndex) =>
      currentIndex === index ? { objectID } : hide,
    );
    onChange({ ...consequence, hide: next });
  };

  return (
    <div className="space-y-2 rounded-md border p-3">
      <div className="flex items-center justify-between">
        <p className="font-medium">Hidden Items</p>
        <Button type="button" variant="outline" size="sm" onClick={addHide}>
          Add Hidden Item
        </Button>
      </div>
      {hides.length === 0 ? (
        <p className="text-sm text-muted-foreground">No hidden items.</p>
      ) : (
        <div className="space-y-2">
          {hides.map((hide, index) => {
            const number = index + 1;
            return (
              <div key={`hide-${index}`} className="flex items-end gap-2">
                <div className="flex-1 space-y-1">
                  <Label htmlFor={`hide-oid-${index}`}>{`Hide Object ID ${number}`}</Label>
                  <Input
                    id={`hide-oid-${index}`}
                    value={hide.objectID}
                    onChange={(event) => updateHide(index, event.target.value)}
                  />
                </div>
                <Button type="button" variant="ghost" size="sm" onClick={() => removeHide(index)}>
                  {`Remove Hide ${number}`}
                </Button>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

export function QueryModificationSection({
  consequence,
  onChange,
  params,
  updateParams,
}: ParamsSectionProps) {
  const queryMode = getQueryMode(params);
  const queryLiteral = getQueryLiteral(params);
  const queryEdits = getQueryEdits(params);

  const setQueryMode = (mode: QueryMode) => {
    if (mode === 'none') {
      const { query: _query, ...rest } = params;
      onChange({ ...consequence, params: Object.keys(rest).length > 0 ? rest : undefined });
    } else if (mode === 'literal') {
      updateParams({ query: '' });
    } else {
      updateParams({ query: { edits: [] } });
    }
  };

  const addEdit = () => {
    const edits = [...queryEdits, { type: 'remove' as const, delete: '' }];
    updateParams({ query: { edits } });
  };

  const removeEdit = (index: number) => {
    const edits = queryEdits.filter((_, currentIndex) => currentIndex !== index);
    updateParams({ query: { edits } });
  };

  const updateEdit = (index: number, updates: Partial<Edit>) => {
    const edits = queryEdits.map((edit, currentIndex) =>
      currentIndex === index ? { ...edit, ...updates } : edit,
    );
    updateParams({ query: { edits } });
  };

  return (
    <div className="space-y-2 rounded-md border p-3">
      <p className="font-medium">Query Modification</p>
      <div className="flex gap-4">
        <label className="flex items-center gap-1.5 text-sm">
          <input
            type="radio"
            name="query-mode"
            checked={queryMode === 'none'}
            onChange={() => setQueryMode('none')}
          />
          None
        </label>
        <label className="flex items-center gap-1.5 text-sm">
          <input
            type="radio"
            name="query-mode"
            checked={queryMode === 'literal'}
            onChange={() => setQueryMode('literal')}
          />
          Literal replacement
        </label>
        <label className="flex items-center gap-1.5 text-sm">
          <input
            type="radio"
            name="query-mode"
            checked={queryMode === 'edits'}
            onChange={() => setQueryMode('edits')}
          />
          Word edits
        </label>
      </div>

      {queryMode === 'literal' && (
        <div className="space-y-1">
          <Label htmlFor="query-literal">Replacement query</Label>
          <Input
            id="query-literal"
            value={queryLiteral}
            onChange={(event) => updateParams({ query: event.target.value })}
          />
        </div>
      )}

      {queryMode === 'edits' && (
        <div className="space-y-2">
          <Button type="button" variant="outline" size="sm" onClick={addEdit}>
            Add Edit
          </Button>
          {queryEdits.map((edit, index) => {
            const number = index + 1;
            return (
              <div key={`edit-${index}`} className="flex items-end gap-2">
                <div className="w-28 space-y-1">
                  <Label htmlFor={`edit-type-${index}`}>{`Edit Type ${number}`}</Label>
                  <select
                    id={`edit-type-${index}`}
                    value={edit.type}
                    className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                    onChange={(event) =>
                      updateEdit(index, {
                        type: event.target.value as 'remove' | 'replace',
                        ...(event.target.value === 'remove' ? { insert: undefined } : {}),
                      })
                    }
                  >
                    <option value="remove">remove</option>
                    <option value="replace">replace</option>
                  </select>
                </div>
                <div className="flex-1 space-y-1">
                  <Label htmlFor={`edit-delete-${index}`}>{`Edit Delete ${number}`}</Label>
                  <Input
                    id={`edit-delete-${index}`}
                    value={edit.delete}
                    onChange={(event) => updateEdit(index, { delete: event.target.value })}
                  />
                </div>
                {edit.type === 'replace' && (
                  <div className="flex-1 space-y-1">
                    <Label htmlFor={`edit-insert-${index}`}>{`Edit Insert ${number}`}</Label>
                    <Input
                      id={`edit-insert-${index}`}
                      value={edit.insert || ''}
                      onChange={(event) => updateEdit(index, { insert: event.target.value })}
                    />
                  </div>
                )}
                <Button type="button" variant="ghost" size="sm" onClick={() => removeEdit(index)}>
                  {`Remove Edit ${number}`}
                </Button>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

export function ParametersSection({
  consequence,
  onChange,
  params,
  updateParams,
}: ParamsSectionProps) {
  return (
    <div className="space-y-3 rounded-md border p-3">
      <p className="font-medium">Parameters</p>
      <div className="space-y-1">
        <Label htmlFor="params-filters">Filters</Label>
        <Input
          id="params-filters"
          value={params.filters || ''}
          onChange={(event) => updateParams({ filters: event.target.value || undefined })}
        />
      </div>
      <div className="grid gap-3 md:grid-cols-2">
        <div className="space-y-1">
          <Label htmlFor="params-hitsPerPage">Hits Per Page</Label>
          <Input
            id="params-hitsPerPage"
            type="number"
            min={0}
            value={params.hitsPerPage ?? ''}
            onChange={(event) =>
              updateParams({
                hitsPerPage: event.target.value ? Number(event.target.value) : undefined,
              })
            }
          />
        </div>
        <div className="space-y-1">
          <Label htmlFor="params-aroundLatLng">Around Lat/Lng</Label>
          <Input
            id="params-aroundLatLng"
            value={params.aroundLatLng || ''}
            onChange={(event) =>
              updateParams({ aroundLatLng: event.target.value || undefined })
            }
          />
        </div>
      </div>
      <div className="space-y-1">
        <Label htmlFor="params-aroundRadius">Around Radius</Label>
        <Input
          id="params-aroundRadius"
          placeholder='Number or "all"'
          value={params.aroundRadius ?? ''}
          onChange={(event) => {
            const value = event.target.value.trim();
            if (!value) {
              updateParams({ aroundRadius: undefined });
            } else if (value === 'all') {
              updateParams({ aroundRadius: 'all' });
            } else {
              const number = Number(value);
              updateParams({ aroundRadius: Number.isNaN(number) ? undefined : number });
            }
          }}
        />
      </div>
      <div className="space-y-1">
        <Label htmlFor="params-restrictSearchable">Restrict Searchable Attributes (comma-separated)</Label>
        <Input
          id="params-restrictSearchable"
          value={(params.restrictSearchableAttributes || []).join(', ')}
          onChange={(event) =>
            updateParams({
              restrictSearchableAttributes: event.target.value
                ? event.target.value.split(',').map((value) => value.trim()).filter(Boolean)
                : undefined,
            })
          }
        />
      </div>

      <JsonArrayField
        id="params-facetFilters"
        label="Facet Filters (JSON)"
        value={params.facetFilters}
        onChange={(value) => updateParams({ facetFilters: value })}
      />
      <JsonArrayField
        id="params-numericFilters"
        label="Numeric Filters (JSON)"
        value={params.numericFilters}
        onChange={(value) => updateParams({ numericFilters: value })}
      />
      <JsonArrayField
        id="params-optionalFilters"
        label="Optional Filters (JSON)"
        value={params.optionalFilters}
        onChange={(value) => updateParams({ optionalFilters: value })}
      />
      <JsonArrayField
        id="params-tagFilters"
        label="Tag Filters (JSON)"
        value={params.tagFilters}
        onChange={(value) => updateParams({ tagFilters: value })}
      />

      <label className="flex items-center gap-2 text-sm">
        <input
          type="checkbox"
          checked={consequence.filterPromotes === true}
          onChange={(event) =>
            onChange({
              ...consequence,
              filterPromotes: event.target.checked || undefined,
            })
          }
        />
        Filter Promotes
      </label>
    </div>
  );
}

interface JsonArrayFieldProps {
  id: string;
  label: string;
  value: unknown;
  onChange: (value: unknown) => void;
}

function JsonArrayField({ id, label, value, onChange }: JsonArrayFieldProps) {
  return (
    <div className="space-y-1">
      <Label htmlFor={id}>{label}</Label>
      <Textarea
        id={id}
        rows={2}
        value={value ? JSON.stringify(value) : ''}
        onChange={(event) => {
          const nextValue = event.target.value;
          if (!nextValue) {
            onChange(undefined);
          } else {
            try {
              onChange(JSON.parse(nextValue));
            } catch {
              onChange(nextValue as unknown);
            }
          }
        }}
      />
    </div>
  );
}

export function UserDataSection({ consequence, onChange }: ConsequenceSectionProps) {
  return (
    <div className="space-y-1 rounded-md border p-3">
      <Label htmlFor="consequence-userData">User Data (JSON)</Label>
      <Textarea
        id="consequence-userData"
        rows={3}
        value={
          consequence.userData !== undefined
            ? typeof consequence.userData === 'string'
              ? consequence.userData
              : JSON.stringify(consequence.userData, null, 2)
            : ''
        }
        onChange={(event) => {
          const value = event.target.value;
          if (!value) {
            onChange({ ...consequence, userData: undefined });
          } else {
            onChange({ ...consequence, userData: value });
          }
        }}
      />
    </div>
  );
}

export function RenderingContentSection({ params, updateParams }: ParamsSectionProps) {
  return (
    <div className="space-y-1 rounded-md border p-3">
      <Label htmlFor="consequence-renderingContent">Rendering Content (JSON)</Label>
      <Textarea
        id="consequence-renderingContent"
        rows={3}
        value={params.renderingContent ? JSON.stringify(params.renderingContent, null, 2) : ''}
        onChange={(event) => {
          const value = event.target.value;
          if (!value) {
            updateParams({ renderingContent: undefined });
          } else {
            try {
              updateParams({ renderingContent: JSON.parse(value) });
            } catch {
              updateParams({ renderingContent: value as unknown as Record<string, unknown> });
            }
          }
        }}
      />
    </div>
  );
}
