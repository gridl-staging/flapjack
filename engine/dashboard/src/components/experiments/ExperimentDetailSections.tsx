import { Link } from 'react-router-dom';
import { Badge } from '@/components/ui/badge';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import type {
  ArmResultsResponse,
  ExperimentResultsResponse,
  InterleavingResultsResponse,
  SignificanceResponse,
} from '@/hooks/useExperiments';
import { formatCurrency } from '@/lib/analytics-utils';
import { formatExperimentStatusBadgeClass, formatMetricLabel } from '@/lib/constants';
import type { ExperimentDetailViewModel } from '@/pages/experiment-detail-view-model';
import {
  formatExperimentNumber,
  formatExperimentPercentage,
  formatExperimentPrimaryMetricValue,
} from '@/pages/experiment-detail-metrics';

function formatDate(value: string | null | undefined): string {
  if (!value) return '';
  return value.slice(0, 10);
}

function interleavingDirection(deltaAB: number): string {
  if (deltaAB > 0) return 'Control preferred';
  if (deltaAB < 0) return 'Variant preferred';
  return 'No preference detected';
}

function ArmMetricsCard({ arm, label, testId }: {
  arm: ArmResultsResponse;
  label: string;
  testId: string;
}) {
  return (
    <Card className="p-4 flex-1" data-testid={testId}>
      <h4 className="text-sm font-semibold mb-3 capitalize">{label}</h4>
      <div className="space-y-2 text-sm">
        <div className="flex justify-between">
          <span className="text-muted-foreground">CTR</span>
          <span>{formatExperimentPercentage(arm.ctr)}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-muted-foreground">Conversion Rate</span>
          <span>{formatExperimentPercentage(arm.conversionRate)}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-muted-foreground">Revenue / Search</span>
          <span>{formatCurrency(arm.revenuePerSearch)}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-muted-foreground">Zero Result Rate</span>
          <span>{formatExperimentPercentage(arm.zeroResultRate)}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-muted-foreground">Abandonment Rate</span>
          <span>{formatExperimentPercentage(arm.abandonmentRate)}</span>
        </div>
        <div className="border-t pt-2 mt-2">
          <div className="flex justify-between">
            <span className="text-muted-foreground">Searches</span>
            <span>{formatExperimentNumber(arm.searches)}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">Users</span>
            <span>{formatExperimentNumber(arm.users)}</span>
          </div>
          <div className="flex justify-between">
            <span className="text-muted-foreground">Clicks</span>
            <span>{formatExperimentNumber(arm.clicks)}</span>
          </div>
        </div>
      </div>
    </Card>
  );
}

type ExperimentDetailHeaderSectionProps = {
  canDeclareWinner: boolean;
  onDeclareWinnerClick: () => void;
  results: ExperimentResultsResponse;
};

export function ExperimentDetailHeaderSection({
  canDeclareWinner,
  onDeclareWinnerClick,
  results,
}: ExperimentDetailHeaderSectionProps) {
  return (
    <div className="flex items-start justify-between gap-4">
      <div>
        <div className="flex items-center gap-3">
          <Link
            to="/experiments"
            className="text-sm text-muted-foreground hover:underline"
            data-testid="experiment-detail-back-link"
          >
            &larr; Experiments
          </Link>
        </div>
        <h2 className="text-2xl font-bold mt-2" data-testid="experiment-detail-name">{results.name}</h2>
        <div className="flex items-center gap-3 mt-1">
          <Badge
            variant="outline"
            className={formatExperimentStatusBadgeClass(results.status)}
            data-testid="experiment-detail-status"
          >
            {results.status}
          </Badge>
          <span className="text-sm text-muted-foreground">
            <span data-testid="experiment-detail-index">{results.indexName}</span>
            {' '}
            &middot;
            {' '}
            <span data-testid="experiment-detail-primary-metric">
              {formatMetricLabel(results.primaryMetric)}
            </span>
          </span>
        </div>
      </div>
      {canDeclareWinner && (
        <Button data-testid="declare-winner-button" onClick={onDeclareWinnerClick}>
          Declare Winner
        </Button>
      )}
    </div>
  );
}

type DaysGateConfirmationCardProps = {
  open: boolean;
  onCancel: () => void;
  onProceed: () => void;
};

export function DaysGateConfirmationCard({ open, onCancel, onProceed }: DaysGateConfirmationCardProps) {
  if (!open) {
    return null;
  }

  return (
    <Card className="p-4 border-amber-300 bg-amber-50" data-testid="days-gate-confirmation">
      <p className="text-sm font-medium text-amber-800 mb-3">
        The minimum duration has not been reached. Concluding early risks a novelty effect
        skewing results. Are you sure you want to proceed?
      </p>
      <div className="flex gap-2">
        <Button variant="outline" size="sm" onClick={onCancel}>
          Cancel
        </Button>
        <Button size="sm" onClick={onProceed}>
          Proceed Anyway
        </Button>
      </div>
    </Card>
  );
}

function ConclusionSummaryCard({ results, viewModel }: {
  results: ExperimentResultsResponse;
  viewModel: ExperimentDetailViewModel;
}) {
  if (!viewModel.hasConclusion || !results.conclusion) {
    return null;
  }

  return (
    <Card className="p-4 border-blue-200 bg-blue-50" data-testid="conclusion-card">
      <h3 className="text-sm font-semibold text-blue-900">Declared Winner</h3>
      <div className="mt-2 space-y-1 text-sm text-blue-900">
        <p>
          Winner: <span className="font-semibold">{viewModel.conclusionWinnerLabel}</span>
        </p>
        <p>
          Confidence: {(results.conclusion.confidence * 100).toFixed(1)}% confidence
        </p>
        <p>
          {viewModel.conclusionMetricLabel}: control {formatExperimentPrimaryMetricValue(
            results.primaryMetric,
            results.conclusion.controlMetric,
          )} vs variant {formatExperimentPrimaryMetricValue(
            results.primaryMetric,
            results.conclusion.variantMetric,
          )}
        </p>
        <p>
          Promoted to base index: {results.conclusion.promoted ? 'Yes' : 'No'}
        </p>
        {results.endedAt && (
          <p>
            Ended: {formatDate(results.endedAt)}
          </p>
        )}
        <p>{results.conclusion.reason}</p>
      </div>
    </Card>
  );
}

function MinimumDaysWarningCard() {
  return (
    <Card className="p-4 border-amber-200 bg-amber-50" data-testid="minimum-days-warning">
      <p className="text-sm font-medium text-amber-800">
        Required sample size reached, but the minimum duration has not elapsed.
        Results may be influenced by novelty effects. Consider waiting before concluding.
      </p>
    </Card>
  );
}

function SampleRatioMismatchBanner() {
  return (
    <Card className="p-4 border-amber-300 bg-amber-50" data-testid="srm-banner">
      <p className="text-sm font-medium text-amber-800">
        Traffic split mismatch detected. Possible causes: bot traffic, cookie clearing,
        variant index errors. Results may be invalid. Investigate before concluding.
      </p>
    </Card>
  );
}

function GuardRailAlertsBanner({ viewModel }: { viewModel: ExperimentDetailViewModel }) {
  if (viewModel.guardRailAlerts.length === 0) {
    return null;
  }

  return (
    <Card className="p-4 border-amber-300 bg-amber-50" data-testid="guard-rail-banner">
      <h3 className="text-sm font-semibold text-amber-900">Guard Rail Alert</h3>
      <div className="mt-2 space-y-1 text-sm text-amber-900">
        {viewModel.guardRailAlerts.map((alert, index) => (
          <p key={`${alert.metricName}-${index}`}>
            <span className="font-semibold">{formatMetricLabel(alert.metricName)}</span>
            {': '}
            {alert.dropPct.toFixed(1)}% regression
            {' '}
            (control {formatExperimentPrimaryMetricValue(alert.metricName, alert.controlValue)} vs variant {formatExperimentPrimaryMetricValue(alert.metricName, alert.variantValue)})
          </p>
        ))}
      </div>
    </Card>
  );
}

function ProgressBarCard({ gate }: { gate: ExperimentResultsResponse['gate'] }) {
  if (gate.readyToRead) {
    return null;
  }

  return (
    <Card className="p-4" data-testid="progress-bar">
      <div className="flex items-center justify-between text-sm mb-2">
        <span className="font-medium">Data collection progress</span>
        <span>
          {formatExperimentNumber(gate.currentSearchesPerArm)} / {formatExperimentNumber(gate.requiredSearchesPerArm)} searches per arm ({gate.progressPct.toFixed(1)}%)
        </span>
      </div>
      <div className="w-full bg-slate-200 rounded-full h-2">
        <div
          className="bg-blue-600 h-2 rounded-full transition-all"
          style={{ width: `${Math.min(gate.progressPct, 100)}%` }}
        />
      </div>
      {gate.estimatedDaysRemaining != null && (
        <p className="text-xs text-muted-foreground mt-2">
          ~{gate.estimatedDaysRemaining.toFixed(1)} days remaining
        </p>
      )}
    </Card>
  );
}

function BayesianCard({ bayesian }: { bayesian: ExperimentResultsResponse['bayesian'] }) {
  if (!bayesian) {
    return null;
  }

  return (
    <Card className="p-4" data-testid="bayesian-card">
      <h3 className="text-sm font-semibold mb-1">Bayesian Probability</h3>
      <p className="text-2xl font-bold">{Math.round(bayesian.probVariantBetter * 100)}% probability variant wins</p>
      <p className="text-xs text-muted-foreground mt-1">
        Valid to inspect at any time. Useful when frequentist significance may take weeks.
      </p>
    </Card>
  );
}

function confidenceBarClassName(confidence: number): string {
  if (confidence >= 0.95) return 'bg-emerald-600';
  if (confidence >= 0.90) return 'bg-emerald-400';
  if (confidence >= 0.50) return 'bg-amber-400';
  return 'bg-red-400';
}

function SignificanceSection({ cupedApplied, significance }: {
  cupedApplied: boolean;
  significance: SignificanceResponse;
}) {
  return (
    <Card className="p-4" data-testid="significance-section">
      <div className="mb-2 flex items-center gap-2">
        <h3 className="text-sm font-semibold">Statistical Significance</h3>
        {cupedApplied && (
          <Badge
            variant="outline"
            className="border-emerald-300 bg-emerald-50 text-emerald-800"
            data-testid="cuped-badge"
          >
            CUPED
          </Badge>
        )}
      </div>
      <div className="flex items-center gap-4">
        <div className="flex-1">
          <div className="w-full bg-slate-200 rounded-full h-3">
            <div
              className={`h-3 rounded-full transition-all ${confidenceBarClassName(significance.confidence)}`}
              style={{ width: `${Math.min(significance.confidence * 100, 100)}%` }}
            />
          </div>
        </div>
        <span className="text-lg font-bold whitespace-nowrap">
          {(significance.confidence * 100).toFixed(1)}% confidence
        </span>
      </div>
      {significance.significant && significance.winner && (
        <p className="text-sm mt-2">
          Winner: <span className="font-semibold capitalize">{significance.winner}</span>
          {' '}({(significance.relativeImprovement * 100).toFixed(1)}% improvement)
        </p>
      )}
    </Card>
  );
}

function InterleavingPreferenceCard({ interleaving }: { interleaving: InterleavingResultsResponse }) {
  return (
    <Card className="p-4" data-testid="interleaving-card">
      <div className="flex items-center justify-between gap-4">
        <h3 className="text-sm font-semibold">Interleaving Preference</h3>
        <Badge
          variant="outline"
          className={
            interleaving.significant
              ? 'border-emerald-300 bg-emerald-50 text-emerald-800'
              : 'border-slate-300 bg-slate-50 text-slate-700'
          }
        >
          {interleaving.significant ? 'Significant' : 'Not significant'}
        </Badge>
      </div>
      <div className="mt-3 flex items-baseline gap-2">
        <p className="text-2xl font-bold">{interleaving.deltaAB.toFixed(3)}</p>
        <p className="text-sm text-muted-foreground">ΔAB</p>
      </div>
      <p className="text-sm mt-1">
        {interleavingDirection(interleaving.deltaAB)}
        {' '}
        (p={interleaving.pValue.toFixed(3)})
      </p>
      <div className="mt-3 grid grid-cols-2 gap-3 text-sm">
        <InterleavingStatCard label="Control wins" value={interleaving.winsControl} />
        <InterleavingStatCard label="Variant wins" value={interleaving.winsVariant} />
        <InterleavingStatCard label="Ties" value={interleaving.ties} />
        <InterleavingStatCard label="Total queries" value={interleaving.totalQueries} />
      </div>
      {!interleaving.dataQualityOk && (
        <p
          className="mt-3 text-sm text-amber-800 bg-amber-50 border border-amber-200 rounded-md p-2"
          data-testid="interleaving-data-quality-warning"
        >
          First-team distribution is skewed outside the 45-55% quality band.
          Interleaving results may be invalid.
        </p>
      )}
      <p className="text-xs text-muted-foreground mt-3">
        Interleaving can be roughly 50x more sensitive than traditional A/B tests for ranking.
      </p>
    </Card>
  );
}

function InterleavingStatCard({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-md border p-2">
      <p className="text-muted-foreground">{label}</p>
      <p className="font-semibold">{formatExperimentNumber(value)}</p>
    </div>
  );
}

function MeanClickRankCard({ viewModel }: { viewModel: ExperimentDetailViewModel }) {
  return (
    <Card className="p-4" data-testid="mean-click-rank-card">
      <div className="flex items-center justify-between gap-4">
        <h3 className="text-sm font-semibold">Avg Click Position</h3>
        <span className="text-xs text-muted-foreground">&darr; Lower is better</span>
      </div>
      <div className="mt-3 grid grid-cols-2 gap-4">
        <div>
          <p className="text-xs text-muted-foreground">Control</p>
          <p className="text-lg font-semibold">{viewModel.controlMeanClickRank.toFixed(2)}</p>
        </div>
        <div>
          <p className="text-xs text-muted-foreground">Variant</p>
          <p className="text-lg font-semibold">{viewModel.variantMeanClickRank.toFixed(2)}</p>
        </div>
      </div>
    </Card>
  );
}

function ExperimentNotices({ viewModel }: { viewModel: ExperimentDetailViewModel }) {
  return (
    <>
      {viewModel.outlierUsersExcluded > 0 && (
        <p className="text-xs text-muted-foreground">
          {viewModel.outlierUsersExcluded} users excluded as outliers (bot-like traffic patterns).
        </p>
      )}
      {viewModel.unstableIdFraction > 0.05 && viewModel.noStableIdQueries > 0 && (
        <p className="text-xs text-muted-foreground">
          {formatExperimentNumber(viewModel.noStableIdQueries)} queries ({(viewModel.unstableIdFraction * 100).toFixed(1)}%) used unstable IDs and are excluded from arm statistics. Verify your userToken implementation.
        </p>
      )}
    </>
  );
}

type ExperimentDetailBodySectionsProps = {
  results: ExperimentResultsResponse;
  viewModel: ExperimentDetailViewModel;
};

export function ExperimentDetailBodySections({ results, viewModel }: ExperimentDetailBodySectionsProps) {
  return (
    <>
      <ConclusionSummaryCard results={results} viewModel={viewModel} />
      {viewModel.needsDaysGateWarning && <MinimumDaysWarningCard />}
      {results.sampleRatioMismatch && <SampleRatioMismatchBanner />}
      <GuardRailAlertsBanner viewModel={viewModel} />
      <ProgressBarCard gate={results.gate} />
      <BayesianCard bayesian={results.bayesian} />
      {results.gate.minimumNReached && results.significance && (
        <SignificanceSection cupedApplied={results.cupedApplied} significance={results.significance} />
      )}
      {results.interleaving && <InterleavingPreferenceCard interleaving={results.interleaving} />}
      {results.recommendation && (
        <Card className="p-4 bg-blue-50 border-blue-200">
          <p className="text-sm text-blue-800">{results.recommendation}</p>
        </Card>
      )}
      <div className="grid grid-cols-2 gap-4">
        <ArmMetricsCard arm={results.control} label="Control" testId="metric-card-control" />
        <ArmMetricsCard arm={results.variant} label="Variant" testId="metric-card-variant" />
      </div>
      <MeanClickRankCard viewModel={viewModel} />
      <ExperimentNotices viewModel={viewModel} />
    </>
  );
}
