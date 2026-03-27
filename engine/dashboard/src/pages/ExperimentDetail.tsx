import { useState } from 'react';
import { useParams } from 'react-router-dom';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Skeleton } from '@/components/ui/skeleton';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from '@/components/ui/dialog';
import { useExperimentResults, useExperiment, useConcludeExperiment } from '@/hooks/useExperiments';
import type { ExperimentResultsResponse } from '@/hooks/useExperiments';
import { useUpdateSettings } from '@/hooks/useSettings';
import { formatMetricLabel } from '@/lib/constants';
import type { Experiment } from '@/lib/types';
import {
  DaysGateConfirmationCard,
  ExperimentDetailBodySections,
  ExperimentDetailHeaderSection,
} from '@/components/experiments/ExperimentDetailSections';
import { getExperimentPrimaryMetricValue } from './experiment-detail-metrics';
import { buildExperimentDetailViewModel } from './experiment-detail-view-model';

function defaultReason(results: ExperimentResultsResponse): string {
  const sig = results.significance;
  const metricLabel = formatMetricLabel(results.primaryMetric);
  if (sig?.significant && sig.winner) {
    return `Statistically significant: ${sig.winner} wins on ${metricLabel} with ${(sig.confidence * 100).toFixed(1)}% confidence.`;
  }
  if (sig && !sig.significant) {
    return `No statistically significant difference detected on ${metricLabel}.`;
  }
  return '';
}

// --- DeclareWinnerDialog ---

type WinnerChoice = 'control' | 'variant' | 'none';
const DECLARE_WINNER_ERROR_MESSAGE = 'Unable to conclude experiment. Try again.';

function SettingsDiff({ experiment }: { experiment: Experiment | undefined }) {
  if (!experiment) return null;

  const variant = experiment.variant;
  const overrides = variant.queryOverrides;
  const variantIndex = variant.indexName;
  const hasOverrides = !!overrides && Object.keys(overrides).length > 0;

  if (!hasOverrides && !variantIndex) return null;

  return (
    <div data-testid="settings-diff" className="rounded-md border p-3 bg-muted/50">
      <p className="text-sm font-medium mb-2">Variant Configuration</p>
      {variantIndex && (
        <p className="text-sm text-muted-foreground">
          Mode B: routes to index <span className="font-mono font-semibold">{variantIndex}</span>
        </p>
      )}
      {hasOverrides && (
        <ul className="text-sm space-y-1">
          {Object.entries(overrides).map(([key, value]) => (
            <li key={key} className="font-mono text-muted-foreground">
              {key}: {JSON.stringify(value)}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

type DeclareWinnerDialogProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  results: ExperimentResultsResponse;
  experimentId: string;
  experiment: Experiment | undefined;
};

type DeclareWinnerConcludePayload = {
  controlMetric: number;
  variantMetric: number;
  confidence: number;
  significant: boolean;
  promoted: boolean;
  reason: string;
  winner: 'control' | 'variant' | null;
};

type SubmitDeclarationArgs = {
  canPromote: boolean;
  experimentId: string;
  onComplete: () => void;
  onError: (message: string) => void;
  promoted: boolean;
  promoteOverrides?: Record<string, unknown>;
  reason: string;
  winner: WinnerChoice;
  conclude: ReturnType<typeof useConcludeExperiment>;
  updateSettings: ReturnType<typeof useUpdateSettings>;
  buildPayload: (winner: WinnerChoice, reason: string, promoted: boolean) => DeclareWinnerConcludePayload;
};

function submitExperimentConclusion({
  canPromote,
  conclude,
  experimentId,
  onComplete,
  onError,
  promoteOverrides,
  promoted,
  reason,
  winner,
  updateSettings,
  buildPayload,
}: SubmitDeclarationArgs): void {
  const promotedApplied = promoted && canPromote;
  const execute = async () => {
    try {
      if (promotedApplied && promoteOverrides) {
        await updateSettings.mutateAsync(promoteOverrides);
      }
      await conclude.mutateAsync({
        id: experimentId,
        payload: buildPayload(winner, reason, promotedApplied),
      });
      onComplete();
    } catch {
      onError(DECLARE_WINNER_ERROR_MESSAGE);
    }
  };

  void execute();
}

function buildDeclareWinnerPayload(
  results: ExperimentResultsResponse,
): (winner: WinnerChoice, reason: string, promoted: boolean) => DeclareWinnerConcludePayload {
  return (winner, reason, promoted) => ({
    winner: winner === 'none' ? null : winner,
    reason,
    controlMetric: getExperimentPrimaryMetricValue(results.control, results.primaryMetric),
    variantMetric: getExperimentPrimaryMetricValue(results.variant, results.primaryMetric),
    confidence: results.significance?.confidence ?? 0,
    significant: results.significance?.significant ?? false,
    promoted,
  });
}

function getInitialWinner(results: ExperimentResultsResponse): WinnerChoice {
  return results.significance?.winner === 'control'
    ? 'control'
    : results.significance?.winner === 'variant'
      ? 'variant'
      : 'none';
}

function DeclareWinnerDialog({
  open,
  onOpenChange,
  results,
  experimentId,
  experiment,
}: DeclareWinnerDialogProps) {
  const conclude = useConcludeExperiment();
  const indexName = results.indexName;
  const updateSettings = useUpdateSettings(indexName);
  const promoteOverrides = experiment?.variant.queryOverrides;
  const canPromote = !!promoteOverrides && Object.keys(promoteOverrides).length > 0;
  const initialWinner: WinnerChoice = getInitialWinner(results);
  const [winner, setWinner] = useState<WinnerChoice>(initialWinner);
  const [reason, setReason] = useState(() => defaultReason(results));
  const [promoted, setPromoted] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);

  const isPending = conclude.isPending || updateSettings.isPending;

  const handleConfirm = () => {
    setSubmitError(null);
    submitExperimentConclusion({
      canPromote,
      conclude,
      experimentId,
      onComplete: () => onOpenChange(false),
      onError: setSubmitError,
      promoteOverrides: promoteOverrides as Record<string, unknown> | undefined,
      promoted,
      reason,
      winner,
      updateSettings,
      buildPayload: buildDeclareWinnerPayload(results),
    });
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent data-testid="declare-winner-dialog">
        <DialogHeader>
          <DialogTitle>Choose a Winner</DialogTitle>
          <DialogDescription>
            Conclude this experiment by declaring a winner or marking it as inconclusive.
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4">
          <SettingsDiff experiment={experiment} />
          <WinnerSelectionSection winner={winner} onWinnerChange={setWinner} />
          <ReasonSection reason={reason} onReasonChange={setReason} />
          {canPromote && <PromoteSection promoted={promoted} onPromoteChange={setPromoted} />}
          {submitError && (
            <p className="text-sm text-destructive" data-testid="declare-winner-error">
              {submitError}
            </p>
          )}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={isPending}>
            Cancel
          </Button>
          <Button onClick={handleConfirm} disabled={isPending}>
            {isPending ? 'Concluding...' : 'Confirm'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function WinnerSelectionSection({
  onWinnerChange,
  winner,
}: {
  onWinnerChange: (winner: WinnerChoice) => void;
  winner: WinnerChoice;
}) {
  return (
    <fieldset>
      <legend className="text-sm font-medium mb-2">Winner</legend>
      <div className="space-y-2">
        <label className="flex items-center gap-2 cursor-pointer">
          <input
            type="radio"
            name="winner"
            value="control"
            checked={winner === 'control'}
            onChange={() => onWinnerChange('control')}
            aria-label="Control"
          />
          <span className="text-sm">Control</span>
        </label>
        <label className="flex items-center gap-2 cursor-pointer">
          <input
            type="radio"
            name="winner"
            value="variant"
            checked={winner === 'variant'}
            onChange={() => onWinnerChange('variant')}
            aria-label="Variant"
          />
          <span className="text-sm">Variant</span>
        </label>
        <label className="flex items-center gap-2 cursor-pointer">
          <input
            type="radio"
            name="winner"
            value="none"
            checked={winner === 'none'}
            onChange={() => onWinnerChange('none')}
            aria-label="No Winner"
          />
          <span className="text-sm">No Winner (inconclusive)</span>
        </label>
      </div>
    </fieldset>
  );
}

function ReasonSection({
  onReasonChange,
  reason,
}: {
  onReasonChange: (reason: string) => void;
  reason: string;
}) {
  return (
    <div>
      <label htmlFor="conclude-reason" className="text-sm font-medium">
        Reason
      </label>
      <textarea
        id="conclude-reason"
        aria-label="Reason"
        className="mt-1 w-full rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring min-h-[80px]"
        value={reason}
        onChange={(e) => onReasonChange(e.target.value)}
      />
    </div>
  );
}

function PromoteSection({
  onPromoteChange,
  promoted,
}: {
  onPromoteChange: (promoted: boolean) => void;
  promoted: boolean;
}) {
  return (
    <label className="flex items-center gap-2 cursor-pointer">
      <input
        type="checkbox"
        checked={promoted}
        onChange={(e) => onPromoteChange(e.target.checked)}
        aria-label="Promote winner settings"
      />
      <span className="text-sm">Promote winner settings to the base index</span>
    </label>
  );
}

function ExperimentDetailLoadingState() {
  return (
    <div className="space-y-6">
      <Skeleton className="h-8 w-48" />
      <div className="text-sm text-muted-foreground">Loading...</div>
      <Card className="p-6 space-y-3">
        <Skeleton className="h-10 w-full" />
        <Skeleton className="h-10 w-full" />
      </Card>
    </div>
  );
}

// --- Main Page ---

export function ExperimentDetail() {
  const { experimentId } = useParams<{ experimentId: string }>();
  const resolvedExperimentId = experimentId ?? '';
  const { data: results, isLoading } = useExperimentResults(resolvedExperimentId);
  const { data: experiment } = useExperiment(resolvedExperimentId);
  const [showDeclareWinner, setShowDeclareWinner] = useState(false);
  const [showDaysGateConfirmation, setShowDaysGateConfirmation] = useState(false);

  if (isLoading || !results) {
    return <ExperimentDetailLoadingState />;
  }

  const viewModel = buildExperimentDetailViewModel(results);
  const handleDeclareWinnerClick = () => {
    if (viewModel.needsDaysGateWarning) {
      setShowDaysGateConfirmation(true);
      return;
    }
    setShowDeclareWinner(true);
  };

  const handleProceedAfterDaysWarning = () => {
    setShowDaysGateConfirmation(false);
    setShowDeclareWinner(true);
  };

  return (
    <div className="space-y-6">
      <ExperimentDetailHeaderSection
        canDeclareWinner={viewModel.canDeclareWinner}
        onDeclareWinnerClick={handleDeclareWinnerClick}
        results={results}
      />
      <DaysGateConfirmationCard
        open={showDaysGateConfirmation}
        onCancel={() => setShowDaysGateConfirmation(false)}
        onProceed={handleProceedAfterDaysWarning}
      />
      {viewModel.canDeclareWinner && (
        <DeclareWinnerDialog
          open={showDeclareWinner}
          onOpenChange={setShowDeclareWinner}
          results={results}
          experimentId={resolvedExperimentId}
          experiment={experiment}
        />
      )}
      <ExperimentDetailBodySections results={results} viewModel={viewModel} />
    </div>
  );
}
