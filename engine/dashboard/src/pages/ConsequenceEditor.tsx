import { useCallback } from 'react';
import type {
  RuleConsequence,
  ConsequenceParams,
} from '@/lib/types';
import { AutoFacetFilterEditor } from './RuleAutoFacetFilterEditor';
import {
  HiddenItemsSection,
  normalizeAutoFacetFilters,
  ParametersSection,
  PromotedItemsSection,
  QueryModificationSection,
  RenderingContentSection,
  UserDataSection,
} from './RuleConsequenceSections';

interface ConsequenceEditorProps {
  consequence: RuleConsequence;
  onChange: (consequence: RuleConsequence) => void;
}

export function ConsequenceEditor({ consequence, onChange }: ConsequenceEditorProps) {
  const params = consequence.params || {};

  const updateParams = useCallback(
    (updates: Partial<ConsequenceParams>) => {
      onChange({
        ...consequence,
        params: { ...params, ...updates },
      });
    },
    [consequence, params, onChange],
  );

  return (
    <div className="space-y-4">
      <PromotedItemsSection consequence={consequence} onChange={onChange} />
      <HiddenItemsSection consequence={consequence} onChange={onChange} />
      <QueryModificationSection
        consequence={consequence}
        onChange={onChange}
        params={params}
        updateParams={updateParams}
      />
      <ParametersSection
        consequence={consequence}
        onChange={onChange}
        params={params}
        updateParams={updateParams}
      />
      <UserDataSection consequence={consequence} onChange={onChange} />
      <RenderingContentSection
        consequence={consequence}
        onChange={onChange}
        params={params}
        updateParams={updateParams}
      />
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
