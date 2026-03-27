import { useCallback } from 'react';
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

interface ConsequenceEditorProps {
  consequence: RuleConsequence;
  onChange: (consequence: RuleConsequence) => void;
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

function normalizeAutoFacetFilters(
  filters?: Array<AutomaticFacetFilter | string>,
): AutomaticFacetFilter[] {
  if (!filters) return [];
  return filters.map((f) => (typeof f === 'string' ? { facet: f } : f));
}

function getPromotes(consequence: RuleConsequence): Array<{ objectID: string; position: number }> {
  if (!consequence.promote) return [];
  return consequence.promote.flatMap((p) => {
    if ('objectID' in p) return [{ objectID: p.objectID, position: p.position }];
    return (p.objectIDs || []).map((id) => ({ objectID: id, position: p.position }));
  });
}

export function ConsequenceEditor({ consequence, onChange }: ConsequenceEditorProps) {
  const promotes = getPromotes(consequence);
  const hides = consequence.hide || [];
  const params = consequence.params || {};
  const queryMode = getQueryMode(params);
  const queryLiteral = getQueryLiteral(params);
  const queryEdits = getQueryEdits(params);

  const updateParams = useCallback(
    (updates: Partial<ConsequenceParams>) => {
      onChange({
        ...consequence,
        params: { ...params, ...updates },
      });
    },
    [consequence, params, onChange],
  );

  // --- Promote ---
  const addPromote = () => {
    const next: RulePromote[] = [...promotes, { objectID: '', position: 0 }];
    onChange({ ...consequence, promote: next });
  };
  const removePromote = (index: number) => {
    const next = promotes.filter((_, i) => i !== index);
    onChange({ ...consequence, promote: next.length > 0 ? next : undefined });
  };
  const updatePromote = (index: number, field: 'objectID' | 'position', value: string | number) => {
    const next = promotes.map((p, i) =>
      i === index ? { ...p, [field]: field === 'position' ? Number(value) : value } : p,
    );
    onChange({ ...consequence, promote: next });
  };

  // --- Hide ---
  const addHide = () => {
    const next: RuleHide[] = [...hides, { objectID: '' }];
    onChange({ ...consequence, hide: next });
  };
  const removeHide = (index: number) => {
    const next = hides.filter((_, i) => i !== index);
    onChange({ ...consequence, hide: next.length > 0 ? next : undefined });
  };
  const updateHide = (index: number, objectID: string) => {
    const next = hides.map((h, i) => (i === index ? { objectID } : h));
    onChange({ ...consequence, hide: next });
  };

  // --- Query ---
  const setQueryMode = (mode: QueryMode) => {
    if (mode === 'none') {
      const { query: _, ...rest } = params;
      onChange({ ...consequence, params: Object.keys(rest).length > 0 ? rest : undefined });
    } else if (mode === 'literal') {
      updateParams({ query: '' });
    } else {
      updateParams({ query: { edits: [] } });
    }
  };

  const setQueryLiteral = (value: string) => {
    updateParams({ query: value });
  };

  const addEdit = () => {
    const edits = [...queryEdits, { type: 'remove' as const, delete: '' }];
    updateParams({ query: { edits } });
  };

  const removeEdit = (index: number) => {
    const edits = queryEdits.filter((_, i) => i !== index);
    updateParams({ query: { edits } });
  };

  const updateEdit = (index: number, updates: Partial<Edit>) => {
    const edits = queryEdits.map((e, i) => (i === index ? { ...e, ...updates } : e));
    updateParams({ query: { edits } });
  };

  return (
    <div className="space-y-4">
      {/* Promote */}
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
            {promotes.map((p, i) => {
              const n = i + 1;
              return (
                <div key={`promote-${i}`} className="flex items-end gap-2">
                  <div className="flex-1 space-y-1">
                    <Label htmlFor={`promote-oid-${i}`}>{`Promote Object ID ${n}`}</Label>
                    <Input
                      id={`promote-oid-${i}`}
                      value={p.objectID}
                      onChange={(e) => updatePromote(i, 'objectID', e.target.value)}
                    />
                  </div>
                  <div className="w-24 space-y-1">
                    <Label htmlFor={`promote-pos-${i}`}>{`Promote Position ${n}`}</Label>
                    <Input
                      id={`promote-pos-${i}`}
                      type="number"
                      min={0}
                      value={p.position}
                      onChange={(e) => updatePromote(i, 'position', e.target.value)}
                    />
                  </div>
                  <Button type="button" variant="ghost" size="sm" onClick={() => removePromote(i)}>
                    {`Remove Promote ${n}`}
                  </Button>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Hide */}
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
            {hides.map((h, i) => {
              const n = i + 1;
              return (
                <div key={`hide-${i}`} className="flex items-end gap-2">
                  <div className="flex-1 space-y-1">
                    <Label htmlFor={`hide-oid-${i}`}>{`Hide Object ID ${n}`}</Label>
                    <Input
                      id={`hide-oid-${i}`}
                      value={h.objectID}
                      onChange={(e) => updateHide(i, e.target.value)}
                    />
                  </div>
                  <Button type="button" variant="ghost" size="sm" onClick={() => removeHide(i)}>
                    {`Remove Hide ${n}`}
                  </Button>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Query Modification */}
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
              onChange={(e) => setQueryLiteral(e.target.value)}
            />
          </div>
        )}

        {queryMode === 'edits' && (
          <div className="space-y-2">
            <Button type="button" variant="outline" size="sm" onClick={addEdit}>
              Add Edit
            </Button>
            {queryEdits.map((edit, i) => {
              const n = i + 1;
              return (
                <div key={`edit-${i}`} className="flex items-end gap-2">
                  <div className="w-28 space-y-1">
                    <Label htmlFor={`edit-type-${i}`}>{`Edit Type ${n}`}</Label>
                    <select
                      id={`edit-type-${i}`}
                      value={edit.type}
                      className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm"
                      onChange={(e) =>
                        updateEdit(i, {
                          type: e.target.value as 'remove' | 'replace',
                          ...(e.target.value === 'remove' ? { insert: undefined } : {}),
                        })
                      }
                    >
                      <option value="remove">remove</option>
                      <option value="replace">replace</option>
                    </select>
                  </div>
                  <div className="flex-1 space-y-1">
                    <Label htmlFor={`edit-delete-${i}`}>{`Edit Delete ${n}`}</Label>
                    <Input
                      id={`edit-delete-${i}`}
                      value={edit.delete}
                      onChange={(e) => updateEdit(i, { delete: e.target.value })}
                    />
                  </div>
                  {edit.type === 'replace' && (
                    <div className="flex-1 space-y-1">
                      <Label htmlFor={`edit-insert-${i}`}>{`Edit Insert ${n}`}</Label>
                      <Input
                        id={`edit-insert-${i}`}
                        value={edit.insert || ''}
                        onChange={(e) => updateEdit(i, { insert: e.target.value })}
                      />
                    </div>
                  )}
                  <Button type="button" variant="ghost" size="sm" onClick={() => removeEdit(i)}>
                    {`Remove Edit ${n}`}
                  </Button>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Filter Params */}
      <div className="space-y-3 rounded-md border p-3">
        <p className="font-medium">Parameters</p>
        <div className="space-y-1">
          <Label htmlFor="params-filters">Filters</Label>
          <Input
            id="params-filters"
            value={params.filters || ''}
            onChange={(e) => updateParams({ filters: e.target.value || undefined })}
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
              onChange={(e) =>
                updateParams({
                  hitsPerPage: e.target.value ? Number(e.target.value) : undefined,
                })
              }
            />
          </div>
          <div className="space-y-1">
            <Label htmlFor="params-aroundLatLng">Around Lat/Lng</Label>
            <Input
              id="params-aroundLatLng"
              value={params.aroundLatLng || ''}
              onChange={(e) => updateParams({ aroundLatLng: e.target.value || undefined })}
            />
          </div>
        </div>
        <div className="space-y-1">
          <Label htmlFor="params-aroundRadius">Around Radius</Label>
          <Input
            id="params-aroundRadius"
            placeholder='Number or "all"'
            value={params.aroundRadius ?? ''}
            onChange={(e) => {
              const val = e.target.value.trim();
              if (!val) {
                updateParams({ aroundRadius: undefined });
              } else if (val === 'all') {
                updateParams({ aroundRadius: 'all' });
              } else {
                const num = Number(val);
                updateParams({ aroundRadius: isNaN(num) ? undefined : num });
              }
            }}
          />
        </div>
        <div className="space-y-1">
          <Label htmlFor="params-restrictSearchable">Restrict Searchable Attributes (comma-separated)</Label>
          <Input
            id="params-restrictSearchable"
            value={(params.restrictSearchableAttributes || []).join(', ')}
            onChange={(e) =>
              updateParams({
                restrictSearchableAttributes: e.target.value
                  ? e.target.value.split(',').map((s) => s.trim()).filter(Boolean)
                  : undefined,
              })
            }
          />
        </div>

        {/* JSON array filter fields */}
        <div className="space-y-1">
          <Label htmlFor="params-facetFilters">Facet Filters (JSON)</Label>
          <Textarea
            id="params-facetFilters"
            rows={2}
            value={params.facetFilters ? JSON.stringify(params.facetFilters) : ''}
            onChange={(e) => {
              const val = e.target.value;
              if (!val) {
                updateParams({ facetFilters: undefined });
              } else {
                try { updateParams({ facetFilters: JSON.parse(val) }); }
                catch { updateParams({ facetFilters: val as unknown }); }
              }
            }}
          />
        </div>
        <div className="space-y-1">
          <Label htmlFor="params-numericFilters">Numeric Filters (JSON)</Label>
          <Textarea
            id="params-numericFilters"
            rows={2}
            value={params.numericFilters ? JSON.stringify(params.numericFilters) : ''}
            onChange={(e) => {
              const val = e.target.value;
              if (!val) {
                updateParams({ numericFilters: undefined });
              } else {
                try { updateParams({ numericFilters: JSON.parse(val) }); }
                catch { updateParams({ numericFilters: val as unknown }); }
              }
            }}
          />
        </div>
        <div className="space-y-1">
          <Label htmlFor="params-optionalFilters">Optional Filters (JSON)</Label>
          <Textarea
            id="params-optionalFilters"
            rows={2}
            value={params.optionalFilters ? JSON.stringify(params.optionalFilters) : ''}
            onChange={(e) => {
              const val = e.target.value;
              if (!val) {
                updateParams({ optionalFilters: undefined });
              } else {
                try { updateParams({ optionalFilters: JSON.parse(val) }); }
                catch { updateParams({ optionalFilters: val as unknown }); }
              }
            }}
          />
        </div>
        <div className="space-y-1">
          <Label htmlFor="params-tagFilters">Tag Filters (JSON)</Label>
          <Textarea
            id="params-tagFilters"
            rows={2}
            value={params.tagFilters ? JSON.stringify(params.tagFilters) : ''}
            onChange={(e) => {
              const val = e.target.value;
              if (!val) {
                updateParams({ tagFilters: undefined });
              } else {
                try { updateParams({ tagFilters: JSON.parse(val) }); }
                catch { updateParams({ tagFilters: val as unknown }); }
              }
            }}
          />
        </div>

        <label className="flex items-center gap-2 text-sm">
          <input
            type="checkbox"
            checked={consequence.filterPromotes === true}
            onChange={(e) => onChange({ ...consequence, filterPromotes: e.target.checked || undefined })}
          />
          Filter Promotes
        </label>
      </div>

      {/* userData */}
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
          onChange={(e) => {
            const val = e.target.value;
            if (!val) {
              onChange({ ...consequence, userData: undefined });
            } else {
              // Store raw string; parsing happens at save time
              onChange({ ...consequence, userData: val });
            }
          }}
        />
      </div>

      {/* renderingContent */}
      <div className="space-y-1 rounded-md border p-3">
        <Label htmlFor="consequence-renderingContent">Rendering Content (JSON)</Label>
        <Textarea
          id="consequence-renderingContent"
          rows={3}
          value={
            params.renderingContent
              ? JSON.stringify(params.renderingContent, null, 2)
              : ''
          }
          onChange={(e) => {
            const val = e.target.value;
            if (!val) {
              updateParams({ renderingContent: undefined });
            } else {
              try {
                updateParams({ renderingContent: JSON.parse(val) });
              } catch {
                // Keep raw string in params temporarily; validated at save
                updateParams({ renderingContent: val as unknown as Record<string, unknown> });
              }
            }
          }}
        />
      </div>

      {/* automaticFacetFilters */}
      <AutoFacetFilterEditor
        label="Automatic Facet Filters"
        labelPrefix="Automatic Facet Filter"
        idPrefix="auto-facet"
        disjunctiveLabel="Disjunctive"
        scoreLabel="Score"
        filters={normalizeAutoFacetFilters(params.automaticFacetFilters)}
        onChange={(next) =>
          updateParams({ automaticFacetFilters: next.length > 0 ? next : undefined })
        }
      />

      {/* automaticOptionalFacetFilters */}
      <AutoFacetFilterEditor
        label="Automatic Optional Facet Filters"
        labelPrefix="Automatic Optional Facet Filter"
        idPrefix="auto-opt-facet"
        disjunctiveLabel="Optional Disjunctive"
        scoreLabel="Optional Score"
        filters={normalizeAutoFacetFilters(params.automaticOptionalFacetFilters)}
        onChange={(next) =>
          updateParams({ automaticOptionalFacetFilters: next.length > 0 ? next : undefined })
        }
      />
    </div>
  );
}

interface AutoFacetFilterEditorProps {
  label: string;
  labelPrefix: string;
  idPrefix: string;
  disjunctiveLabel: string;
  scoreLabel: string;
  filters: AutomaticFacetFilter[];
  onChange: (filters: AutomaticFacetFilter[]) => void;
}

function AutoFacetFilterEditor({
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
      filters.map((f, i) => {
        if (i !== index) return f;
        const merged = { ...f, ...updates };
        // Clean up: remove falsy optional fields so they don't serialize as undefined
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
          {filters.map((f, i) => {
            const n = i + 1;
            return (
              <div key={`${idPrefix}-${i}`} className="flex items-end gap-2">
                <div className="flex-1 space-y-1">
                  <Label htmlFor={`${idPrefix}-name-${i}`}>{`${labelPrefix} Name ${n}`}</Label>
                  <Input
                    id={`${idPrefix}-name-${i}`}
                    value={f.facet}
                    onChange={(e) => updateFilter(i, { facet: e.target.value })}
                  />
                </div>
                <div className="flex items-center gap-1.5 pb-2">
                  <input
                    type="checkbox"
                    id={`${idPrefix}-disj-${i}`}
                    checked={f.disjunctive === true}
                    onChange={(e) => updateFilter(i, { disjunctive: e.target.checked })}
                  />
                  <Label htmlFor={`${idPrefix}-disj-${i}`}>{`${disjunctiveLabel} ${n}`}</Label>
                </div>
                <div className="w-20 space-y-1">
                  <Label htmlFor={`${idPrefix}-score-${i}`}>{`${scoreLabel} ${n}`}</Label>
                  <Input
                    id={`${idPrefix}-score-${i}`}
                    type="number"
                    value={f.score ?? ''}
                    onChange={(e) =>
                      updateFilter(i, {
                        score: e.target.value ? Number(e.target.value) : undefined,
                      })
                    }
                  />
                </div>
                <Button
                  type="button"
                  variant="ghost"
                  size="sm"
                  onClick={() => removeFilter(i)}
                >
                  {`Remove ${labelPrefix} ${n}`}
                </Button>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
