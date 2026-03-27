import { useState, useCallback, useMemo, lazy, Suspense } from 'react';
import { useParams, Link } from 'react-router-dom';
import { ChevronLeft, Plus, Trash2, Search, Power, PowerOff, Wand2, Copy, Check } from 'lucide-react';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter, DialogDescription } from '@/components/ui/dialog';
import { Label } from '@/components/ui/label';
import { Switch } from '@/components/ui/switch';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { ConfirmDialog } from '@/components/ui/confirm-dialog';
import { useRules, useSaveRule, useDeleteRule, useClearRules } from '@/hooks/useRules';
import type { Rule, RuleCondition } from '@/lib/types';
import {
  buildRuleDescription,
  createEmptyRule,
  normalizeRule,
  parseRuleEditorJson,
} from '@/lib/ruleHelpers';
import { ConsequenceEditor } from './ConsequenceEditor';
import { ValidityEditor } from './ValidityEditor';

const Editor = lazy(() =>
  import('@monaco-editor/react').then((module) => ({ default: module.default }))
);

export function Rules() {
  const { indexName } = useParams<{ indexName: string }>();
  const [searchQuery, setSearchQuery] = useState('');
  const [editingRule, setEditingRule] = useState<Rule | null>(null);
  const [isCreating, setIsCreating] = useState(false);
  const [pendingAction, setPendingAction] = useState<
    | { type: 'delete'; objectID: string }
    | { type: 'clear-all' }
    | null
  >(null);

  const { data, isLoading } = useRules({
    indexName: indexName || '',
    query: searchQuery,
  });

  const saveRule = useSaveRule(indexName || '');
  const deleteRule = useDeleteRule(indexName || '');
  const clearRules = useClearRules(indexName || '');

  const handleSave = useCallback(async (rule: Rule) => {
    await saveRule.mutateAsync(rule);
    setEditingRule(null);
    setIsCreating(false);
  }, [saveRule]);

  const handleDeleteRequest = useCallback((objectID: string) => {
    setPendingAction({ type: 'delete', objectID });
  }, []);

  const handleClearAllRequest = useCallback(() => {
    setPendingAction({ type: 'clear-all' });
  }, []);

  const handleConfirmAction = useCallback(async () => {
    if (!pendingAction) return;

    if (pendingAction.type === 'delete') {
      await deleteRule.mutateAsync(pendingAction.objectID);
    } else {
      await clearRules.mutateAsync();
    }

    setPendingAction(null);
  }, [clearRules, deleteRule, pendingAction]);

  if (!indexName) {
    return (
      <Card className="p-8 text-center">
        <h3 className="text-lg font-semibold mb-2">No index selected</h3>
        <Link to="/overview"><Button>Go to Overview</Button></Link>
      </Card>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Link to={`/index/${encodeURIComponent(indexName)}`}>
            <Button variant="ghost" size="sm">
              <ChevronLeft className="h-4 w-4 mr-1" />
              {indexName}
            </Button>
          </Link>
          <span className="text-muted-foreground">/</span>
          <h2 className="text-xl font-semibold">Rules</h2>
          {data && (
            <Badge variant="secondary" className="ml-2" data-testid="rules-count-badge">{data.nbHits}</Badge>
          )}
        </div>
        <div className="flex items-center gap-2">
          <Link to={`/index/${encodeURIComponent(indexName)}/merchandising`}>
            <Button variant="outline" size="sm">
              <Wand2 className="h-4 w-4 mr-1" />
              Merchandising Studio
            </Button>
          </Link>
          {data && data.nbHits > 0 && (
            <Button variant="outline" size="sm" onClick={handleClearAllRequest}>
              <Trash2 className="h-4 w-4 mr-1" />
              Clear All
            </Button>
          )}
          <Button onClick={() => { setEditingRule(createEmptyRule()); setIsCreating(true); }}>
            <Plus className="h-4 w-4 mr-1" />
            Add Rule
          </Button>
        </div>
      </div>

      {/* Search */}
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
        <Input
          placeholder="Search rules..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="pl-9"
        />
      </div>

      {/* Rules list */}
      {isLoading ? (
        <div className="space-y-3">
          {[1, 2, 3].map((i) => (
            <Card key={i} className="p-4">
              <Skeleton className="h-5 w-full" />
            </Card>
          ))}
        </div>
      ) : !data || data.nbHits === 0 ? (
        <Card className="p-8 text-center">
          <h3 className="text-lg font-semibold mb-2">No rules</h3>
          <p className="text-sm text-muted-foreground mb-4">
            Rules let you customize search results for specific queries.
            Pin products to the top, hide irrelevant results, or modify queries.
          </p>
          <div className="flex items-center justify-center gap-2">
            <Button onClick={() => { setEditingRule(createEmptyRule()); setIsCreating(true); }}>
              <Plus className="h-4 w-4 mr-1" /> Create a Rule
            </Button>
            <Link to={`/index/${encodeURIComponent(indexName)}/merchandising`}>
              <Button variant="outline">
                <Wand2 className="h-4 w-4 mr-1" /> Open Merchandising Studio
              </Button>
            </Link>
          </div>
        </Card>
      ) : (
        <div className="space-y-2" data-testid="rules-list">
          {data.hits.map((rule) => (
            <Card key={rule.objectID} data-testid="rule-card" className="p-4 hover:bg-accent/50 transition-colors">
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-3 min-w-0">
                  {rule.enabled !== false ? (
                    <Power className="h-4 w-4 text-green-500 shrink-0" data-testid="rule-enabled-icon" />
                  ) : (
                    <PowerOff className="h-4 w-4 text-muted-foreground shrink-0" data-testid="rule-disabled-icon" />
                  )}
                  <div className="min-w-0">
                    <div className="flex items-center gap-2">
                      <span className="font-mono text-sm font-medium truncate">
                        {rule.objectID}
                      </span>
                      {rule.consequence.promote && (
                        <Badge variant="secondary" className="text-xs">
                          {rule.consequence.promote.length} pinned
                        </Badge>
                      )}
                      {rule.consequence.hide && (
                        <Badge variant="outline" className="text-xs">
                          {rule.consequence.hide.length} hidden
                        </Badge>
                      )}
                    </div>
                    {rule.description && (
                      <p className="text-sm text-muted-foreground truncate">{rule.description}</p>
                    )}
                    <p className="text-xs text-muted-foreground mt-0.5">
                      {buildRuleDescription(rule)}
                    </p>
                  </div>
                </div>
                <div className="flex items-center gap-1 shrink-0">
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => { setEditingRule(normalizeRule(rule)); setIsCreating(false); }}
                  >
                    Edit
                  </Button>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => handleDeleteRequest(rule.objectID)}
                    disabled={deleteRule.isPending || clearRules.isPending}
                    aria-label="Delete"
                  >
                    <Trash2 className="h-4 w-4 text-destructive" />
                  </Button>
                </div>
              </div>
            </Card>
          ))}
        </div>
      )}

      {/* Edit / Create Dialog */}
      {editingRule && (
        <RuleEditor
          rule={editingRule}
          isCreating={isCreating}
          onSave={handleSave}
          onCancel={() => { setEditingRule(null); setIsCreating(false); }}
          isPending={saveRule.isPending}
        />
      )}

      <ConfirmDialog
        open={pendingAction !== null}
        onOpenChange={(open) => {
          if (!open) setPendingAction(null);
        }}
        title={pendingAction?.type === 'delete' ? 'Delete Rule' : 'Delete All Rules'}
        description={
          pendingAction?.type === 'delete'
            ? (
              <>
                Are you sure you want to delete rule{' '}
                <code className="font-mono text-sm bg-muted px-1 py-0.5 rounded">
                  {pendingAction.objectID}
                </code>
                ? This action cannot be undone.
              </>
            )
            : 'Delete ALL rules for this index? This cannot be undone.'
        }
        confirmLabel={pendingAction?.type === 'delete' ? 'Delete' : 'Delete All'}
        variant="destructive"
        onConfirm={handleConfirmAction}
        isPending={deleteRule.isPending || clearRules.isPending}
      />
    </div>
  );
}

interface RuleEditorProps {
  rule: Rule;
  isCreating: boolean;
  onSave: (rule: Rule) => void;
  onCancel: () => void;
  isPending: boolean;
}

type RuleEditorTab = 'form' | 'json';

const ANCHORING_OPTIONS: Array<NonNullable<RuleCondition['anchoring']>> = [
  'is',
  'startsWith',
  'endsWith',
  'contains',
];

function cleanParams(params?: Record<string, unknown>): Record<string, unknown> | undefined {
  if (!params) return undefined;
  const clean: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(params)) {
    if (v === undefined || v === null || v === '') continue;
    if (Array.isArray(v) && v.length === 0) continue;
    clean[k] = v;
  }
  return Object.keys(clean).length > 0 ? clean : undefined;
}

function normalizeRuleForSerialization(rule: Rule): Rule {
  const normalizedRule = normalizeRule(rule);
  const conditions = normalizedRule.conditions
    .map((condition) => {
      const pattern = condition.pattern?.trim();
      const context = condition.context?.trim();
      const filters = condition.filters?.trim();
      const hasAnchoring = condition.anchoring !== undefined;
      const hasPattern = Boolean(pattern);
      const hasContext = Boolean(context);
      const hasFilters = Boolean(filters);
      const hasAlternatives = condition.alternatives === true;

      if (!hasPattern && !hasAnchoring && !hasContext && !hasFilters && !hasAlternatives) {
        return null;
      }

      return {
        ...(hasPattern ? { pattern } : {}),
        ...(hasAnchoring ? { anchoring: condition.anchoring } : {}),
        ...(hasAlternatives ? { alternatives: true } : {}),
        ...(hasContext ? { context } : {}),
        ...(hasFilters ? { filters } : {}),
      } as RuleCondition;
    })
    .filter((condition): condition is RuleCondition => condition !== null);

  const consequence = { ...normalizedRule.consequence };
  if (consequence.params) {
    consequence.params = cleanParams(consequence.params as unknown as Record<string, unknown>) as typeof consequence.params;
  }
  if (!consequence.promote?.length) delete consequence.promote;
  if (!consequence.hide?.length) delete consequence.hide;

  const result: Rule = {
    ...normalizedRule,
    conditions,
    consequence,
  };

  // Clean validity
  if (result.validity && result.validity.length === 0) {
    delete result.validity;
  }

  return result;
}

function validateRule(rule: Rule): string[] {
  const errors: string[] = [];

  // Condition validation
  rule.conditions.forEach((condition, index) => {
    const hasPattern = Boolean(condition.pattern?.trim());
    const hasAnchoring = condition.anchoring !== undefined;

    if (hasPattern && !hasAnchoring) {
      errors.push(`Condition ${index + 1}: anchoring is required when pattern is provided.`);
    }
    if (!hasPattern && hasAnchoring) {
      errors.push(`Condition ${index + 1}: pattern is required when anchoring is selected.`);
    }
  });

  // userData JSON validation
  if (rule.consequence.userData !== undefined && rule.consequence.userData !== '') {
    if (typeof rule.consequence.userData === 'string') {
      try {
        JSON.parse(rule.consequence.userData);
      } catch {
        errors.push('Invalid JSON in User Data field.');
      }
    }
  }

  // renderingContent JSON validation
  if (rule.consequence.params?.renderingContent !== undefined) {
    if (typeof rule.consequence.params.renderingContent === 'string') {
      try {
        JSON.parse(rule.consequence.params.renderingContent as unknown as string);
      } catch {
        errors.push('Invalid JSON in Rendering Content field.');
      }
    }
  }

  // Duplicate objectID validation in promote
  if (rule.consequence.promote?.length) {
    const ids = rule.consequence.promote.map((p) =>
      'objectID' in p ? p.objectID : (p.objectIDs || []).join(','),
    );
    const seen = new Set<string>();
    for (const id of ids) {
      if (id && seen.has(id)) {
        errors.push('Duplicate objectID in promoted items.');
        break;
      }
      if (id) seen.add(id);
    }
  }

  return errors;
}

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  return (
    <Button
      type="button"
      variant="ghost"
      size="sm"
      onClick={() => {
        navigator.clipboard.writeText(text);
        setCopied(true);
        setTimeout(() => setCopied(false), 1500);
      }}
    >
      {copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
    </Button>
  );
}

function RuleEditor({ rule: initial, isCreating, onSave, onCancel, isPending }: RuleEditorProps) {
  const [activeTab, setActiveTab] = useState<RuleEditorTab>('form');
  const [draftRule, setDraftRule] = useState<Rule>(normalizeRule(initial));
  const [json, setJson] = useState(JSON.stringify(normalizeRuleForSerialization(normalizeRule(initial)), null, 2));
  const [parseError, setParseError] = useState<string | null>(null);

  const previewJson = useMemo(
    () => JSON.stringify(normalizeRuleForSerialization(draftRule), null, 2),
    [draftRule],
  );

  const updateDraftRule = useCallback((nextRule: Rule) => {
    const normalizedRule = normalizeRule(nextRule);
    setDraftRule(normalizedRule);
    setJson(JSON.stringify(normalizeRuleForSerialization(normalizedRule), null, 2));
    setParseError(null);
  }, []);

  const updateCondition = useCallback((index: number, updates: Partial<RuleCondition>) => {
    updateDraftRule({
      ...draftRule,
      conditions: draftRule.conditions.map((condition, conditionIndex) =>
        conditionIndex === index
          ? { ...condition, ...updates }
          : condition
      ),
    });
  }, [draftRule, updateDraftRule]);

  const addCondition = useCallback(() => {
    updateDraftRule({
      ...draftRule,
      conditions: [...draftRule.conditions, { pattern: '' }],
    });
  }, [draftRule, updateDraftRule]);

  const removeCondition = useCallback((index: number) => {
    updateDraftRule({
      ...draftRule,
      conditions: draftRule.conditions.filter((_, conditionIndex) => conditionIndex !== index),
    });
  }, [draftRule, updateDraftRule]);

  const handleTabChange = useCallback((nextTab: string) => {
    if (nextTab === 'form') {
      const { rule, error } = parseRuleEditorJson(json);
      if (error || !rule) {
        setParseError(error ?? 'Invalid JSON');
        return;
      }
      setDraftRule(rule);
      setParseError(null);
    }

    setActiveTab(nextTab as RuleEditorTab);
  }, [json]);

  const handleSave = () => {
    let sourceJson = json;

    if (activeTab === 'form') {
      const candidateRule = normalizeRuleForSerialization(draftRule);
      const validationErrors = validateRule(candidateRule);
      if (validationErrors.length > 0) {
        setParseError(validationErrors[0]);
        return;
      }

      // Parse userData string to object before serializing
      if (typeof candidateRule.consequence.userData === 'string' && candidateRule.consequence.userData) {
        candidateRule.consequence.userData = JSON.parse(candidateRule.consequence.userData);
      } else if (!candidateRule.consequence.userData) {
        delete candidateRule.consequence.userData;
      }

      // Parse renderingContent string to object before serializing
      if (candidateRule.consequence.params?.renderingContent &&
          typeof candidateRule.consequence.params.renderingContent === 'string') {
        candidateRule.consequence.params.renderingContent = JSON.parse(
          candidateRule.consequence.params.renderingContent as unknown as string,
        );
      }

      sourceJson = JSON.stringify(candidateRule, null, 2);
      setJson(sourceJson);
    }

    const { rule, error } = parseRuleEditorJson(sourceJson);
    if (error) {
      setParseError(error);
      setActiveTab('json');
      return;
    }
    if (rule) {
      setParseError(null);
      onSave(rule);
    }
  };

  return (
    <Dialog open onOpenChange={() => onCancel()}>
      <DialogContent className="max-w-2xl max-h-[80vh] flex flex-col">
        <DialogHeader>
          <DialogTitle>{isCreating ? 'Create Rule' : `Edit Rule: ${initial.objectID}`}</DialogTitle>
          <DialogDescription>
            Use form mode for core fields and switch to JSON for advanced consequence configuration.
          </DialogDescription>
        </DialogHeader>

        <div className="flex-1 min-h-0 flex flex-col">
          <Tabs value={activeTab} onValueChange={handleTabChange} className="flex-1 min-h-0 flex flex-col">
            <TabsList className="self-start">
              <TabsTrigger value="form">Form</TabsTrigger>
              <TabsTrigger value="json">JSON</TabsTrigger>
            </TabsList>

            <TabsContent value="form" className="mt-3 space-y-4">
              <div className="space-y-2">
                <Label htmlFor="rule-object-id">Object ID</Label>
                <Input
                  id="rule-object-id"
                  value={draftRule.objectID}
                  onChange={(event) =>
                    updateDraftRule({
                      ...draftRule,
                      objectID: event.target.value,
                    })
                  }
                />
              </div>

              <div className="space-y-2">
                <Label htmlFor="rule-description">Description</Label>
                <Input
                  id="rule-description"
                  value={draftRule.description ?? ''}
                  onChange={(event) =>
                    updateDraftRule({
                      ...draftRule,
                      description: event.target.value,
                    })
                  }
                />
              </div>

              <div className="flex items-center justify-between rounded-md border p-3">
                <Label htmlFor="rule-enabled">Enabled</Label>
                <Switch
                  id="rule-enabled"
                  aria-label="Enabled"
                  checked={draftRule.enabled !== false}
                  onCheckedChange={(checked) =>
                    updateDraftRule({
                      ...draftRule,
                      enabled: checked,
                    })
                  }
                />
              </div>

              <div className="space-y-3 rounded-md border p-3">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="font-medium">Conditions (OR)</p>
                    <p className="text-xs text-muted-foreground">
                      Leave empty for a conditionless rule that always applies.
                    </p>
                  </div>
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    onClick={addCondition}
                  >
                    Add Condition
                  </Button>
                </div>

                {draftRule.conditions.length === 0 ? (
                  <p className="text-sm text-muted-foreground">No conditions configured.</p>
                ) : (
                  <div className="space-y-3">
                    {draftRule.conditions.map((condition, index) => {
                      const rowIndex = index + 1;
                      const patternId = `rule-condition-pattern-${index}`;
                      const anchoringId = `rule-condition-anchoring-${index}`;
                      const alternativesId = `rule-condition-alternatives-${index}`;
                      const contextId = `rule-condition-context-${index}`;
                      const filtersId = `rule-condition-filters-${index}`;

                      return (
                        <div key={`condition-${index}`} className="space-y-2 rounded-md border p-3">
                          <div className="grid gap-3 md:grid-cols-2">
                            <div className="space-y-2">
                              <Label htmlFor={patternId}>{`Pattern ${rowIndex}`}</Label>
                              <Input
                                id={patternId}
                                value={condition.pattern ?? ''}
                                onChange={(event) => updateCondition(index, { pattern: event.target.value })}
                              />
                            </div>

                            <div className="space-y-2">
                              <Label htmlFor={anchoringId}>{`Anchoring ${rowIndex}`}</Label>
                              <select
                                id={anchoringId}
                                value={condition.anchoring ?? ''}
                                className="flex h-10 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background"
                                onChange={(event) => {
                                  const nextValue = event.target.value;
                                  updateCondition(index, {
                                    anchoring: nextValue === ''
                                      ? undefined
                                      : nextValue as NonNullable<RuleCondition['anchoring']>,
                                  });
                                }}
                              >
                                <option value="">Select anchoring</option>
                                {ANCHORING_OPTIONS.map((option) => (
                                  <option key={option} value={option}>{option}</option>
                                ))}
                              </select>
                            </div>
                          </div>

                          <div className="grid gap-3 md:grid-cols-2">
                            <div className="space-y-2">
                              <Label htmlFor={contextId}>{`Context ${rowIndex}`}</Label>
                              <Input
                                id={contextId}
                                value={condition.context ?? ''}
                                onChange={(event) => updateCondition(index, { context: event.target.value })}
                              />
                            </div>

                            <div className="space-y-2">
                              <Label htmlFor={filtersId}>{`Filters ${rowIndex}`}</Label>
                              <Input
                                id={filtersId}
                                value={condition.filters ?? ''}
                                onChange={(event) => updateCondition(index, { filters: event.target.value })}
                              />
                            </div>
                          </div>

                          <div className="flex items-center justify-between">
                            <label htmlFor={alternativesId} className="flex items-center gap-2 text-sm">
                              <input
                                id={alternativesId}
                                type="checkbox"
                                checked={condition.alternatives === true}
                                onChange={(event) => updateCondition(index, {
                                  alternatives: event.target.checked ? true : undefined,
                                })}
                              />
                              {`Alternatives ${rowIndex}`}
                            </label>
                            <Button
                              type="button"
                              variant="ghost"
                              size="sm"
                              onClick={() => removeCondition(index)}
                            >
                              {`Remove Condition ${rowIndex}`}
                            </Button>
                          </div>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>

              {/* Consequence Editor */}
              <ConsequenceEditor
                consequence={draftRule.consequence}
                onChange={(consequence) => updateDraftRule({ ...draftRule, consequence })}
              />

              {/* Validity Editor */}
              <ValidityEditor
                validity={draftRule.validity || []}
                onChange={(validity) => updateDraftRule({ ...draftRule, validity })}
              />

              {/* JSON Preview */}
              <div className="space-y-2 rounded-md border p-3">
                <div className="flex items-center justify-between">
                  <p className="font-medium">JSON Preview</p>
                  <CopyButton text={previewJson} />
                </div>
                <pre
                  data-testid="rule-json-preview"
                  className="text-xs bg-muted p-3 rounded-md overflow-auto max-h-48 whitespace-pre-wrap"
                >
                  {previewJson}
                </pre>
              </div>
            </TabsContent>

            <TabsContent value="json" className="mt-3 flex-1 min-h-0">
              <Suspense
                fallback={
                  <div className="h-64 flex items-center justify-center text-muted-foreground">
                    Loading editor...
                  </div>
                }
              >
                <div className="border rounded-md overflow-hidden">
                  <Editor
                    height="400px"
                    defaultLanguage="json"
                    value={json}
                    onChange={(value) => {
                      setJson(value || '');
                      setParseError(null);
                    }}
                    options={{
                      minimap: { enabled: false },
                      scrollBeyondLastLine: false,
                      lineNumbers: 'on',
                      folding: true,
                      fontSize: 13,
                      tabSize: 2,
                    }}
                    theme="vs-dark"
                  />
                </div>
              </Suspense>
            </TabsContent>
          </Tabs>

          {parseError && (
            <p className="text-sm text-destructive mt-2">{parseError}</p>
          )}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={onCancel}>Cancel</Button>
          <Button onClick={handleSave} disabled={isPending}>
            {isPending ? 'Saving...' : isCreating ? 'Create' : 'Save'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
