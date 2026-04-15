import { useState, useCallback, useMemo, lazy, Suspense } from 'react'
import { Copy, Check } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter, DialogDescription } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import type { Rule, RuleCondition } from '@/lib/types'
import {
  normalizeRule,
  normalizeRuleForSerialization,
  parseRuleEditorJson,
  prepareRuleEditorSave,
} from '@/lib/ruleHelpers'
import { ConsequenceEditor } from './ConsequenceEditor'
import { ValidityEditor } from './ValidityEditor'

const Editor = lazy(() =>
  import('@monaco-editor/react').then((module) => ({ default: module.default }))
)

type RuleEditorTab = 'form' | 'json'

const ANCHORING_OPTIONS: Array<NonNullable<RuleCondition['anchoring']>> = [
  'is',
  'startsWith',
  'endsWith',
  'contains',
]

interface RuleEditorDialogProps {
  rule: Rule
  isCreating: boolean
  onSave: (rule: Rule) => void
  onCancel: () => void
  isPending: boolean
}

function formatRuleJson(rule: Rule): string {
  return JSON.stringify(normalizeRuleForSerialization(normalizeRule(rule)), null, 2)
}

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false)

  return (
    <Button
      type="button"
      variant="ghost"
      size="sm"
      onClick={() => {
        navigator.clipboard.writeText(text)
        setCopied(true)
        setTimeout(() => setCopied(false), 1500)
      }}
    >
      {copied ? <Check className="h-3 w-3" /> : <Copy className="h-3 w-3" />}
    </Button>
  )
}

export function RuleEditorDialog({
  rule: initial,
  isCreating,
  onSave,
  onCancel,
  isPending,
}: RuleEditorDialogProps) {
  const [activeTab, setActiveTab] = useState<RuleEditorTab>('form')
  const [draftRule, setDraftRule] = useState<Rule>(normalizeRule(initial))
  const [json, setJson] = useState(formatRuleJson(initial))
  const [parseError, setParseError] = useState<string | null>(null)

  const previewJson = useMemo(() => formatRuleJson(draftRule), [draftRule])

  const updateDraftRule = useCallback((nextRule: Rule) => {
    const normalizedRule = normalizeRule(nextRule)
    setDraftRule(normalizedRule)
    setJson(formatRuleJson(normalizedRule))
    setParseError(null)
  }, [])

  const updateCondition = useCallback((index: number, updates: Partial<RuleCondition>) => {
    updateDraftRule({
      ...draftRule,
      conditions: draftRule.conditions.map((condition, conditionIndex) =>
        conditionIndex === index ? { ...condition, ...updates } : condition,
      ),
    })
  }, [draftRule, updateDraftRule])

  const addCondition = useCallback(() => {
    updateDraftRule({
      ...draftRule,
      conditions: [...draftRule.conditions, { pattern: '' }],
    })
  }, [draftRule, updateDraftRule])

  const removeCondition = useCallback((index: number) => {
    updateDraftRule({
      ...draftRule,
      conditions: draftRule.conditions.filter((_, conditionIndex) => conditionIndex !== index),
    })
  }, [draftRule, updateDraftRule])

  const handleTabChange = useCallback((nextTab: string) => {
    if (nextTab === 'form') {
      const { rule, error } = parseRuleEditorJson(json)
      if (error || !rule) {
        setParseError(error ?? 'Invalid JSON')
        return
      }
      setDraftRule(rule)
      setParseError(null)
    }

    setActiveTab(nextTab as RuleEditorTab)
  }, [json])

  const handleSave = useCallback(() => {
    if (activeTab === 'form') {
      const { rule, error, json: preparedJson } = prepareRuleEditorSave(draftRule)

      if (error || !rule || !preparedJson) {
        setParseError(error ?? 'Invalid rule')
        return
      }

      setJson(preparedJson)
      setParseError(null)
      onSave(rule)
      return
    }

    const { rule, error } = parseRuleEditorJson(json)
    if (error || !rule) {
      setParseError(error ?? 'Invalid JSON')
      return
    }

    setParseError(null)
    onSave(rule)
  }, [activeTab, draftRule, json, onSave])

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
                    })}
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
                    })}
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
                    })}
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
                  <Button type="button" variant="outline" size="sm" onClick={addCondition}>
                    Add Condition
                  </Button>
                </div>

                {draftRule.conditions.length === 0 ? (
                  <p className="text-sm text-muted-foreground">No conditions configured.</p>
                ) : (
                  <div className="space-y-3">
                    {draftRule.conditions.map((condition, index) => {
                      const rowIndex = index + 1
                      const patternId = `rule-condition-pattern-${index}`
                      const anchoringId = `rule-condition-anchoring-${index}`
                      const alternativesId = `rule-condition-alternatives-${index}`
                      const contextId = `rule-condition-context-${index}`
                      const filtersId = `rule-condition-filters-${index}`

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
                                  const nextValue = event.target.value
                                  updateCondition(index, {
                                    anchoring: nextValue === ''
                                      ? undefined
                                      : nextValue as NonNullable<RuleCondition['anchoring']>,
                                  })
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
                      )
                    })}
                  </div>
                )}
              </div>

              <ConsequenceEditor
                consequence={draftRule.consequence}
                onChange={(consequence) => updateDraftRule({ ...draftRule, consequence })}
              />

              <ValidityEditor
                validity={draftRule.validity || []}
                onChange={(validity) => updateDraftRule({ ...draftRule, validity })}
              />

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
                      setJson(value || '')
                      setParseError(null)
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

          {parseError && <p className="text-sm text-destructive mt-2">{parseError}</p>}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={onCancel}>Cancel</Button>
          <Button onClick={handleSave} disabled={isPending}>
            {isPending ? 'Saving...' : isCreating ? 'Create' : 'Save'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
