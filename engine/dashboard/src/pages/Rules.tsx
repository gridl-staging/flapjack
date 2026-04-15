import { useState, useCallback } from 'react';
import { useParams, Link } from 'react-router-dom';
import { ChevronLeft, Plus, Trash2, Search, Power, PowerOff, Wand2 } from 'lucide-react';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Badge } from '@/components/ui/badge';
import { Skeleton } from '@/components/ui/skeleton';
import { ConfirmDialog } from '@/components/ui/confirm-dialog';
import { useRules, useSaveRule, useDeleteRule, useClearRules } from '@/hooks/useRules';
import type { Rule } from '@/lib/types';
import { buildRuleDescription, createEmptyRule, normalizeRule } from '@/lib/ruleHelpers';
import { RuleEditorDialog } from './RuleEditorDialog';

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
        <RuleEditorDialog
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
