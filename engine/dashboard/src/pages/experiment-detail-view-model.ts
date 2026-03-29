/**
 */
import type { ExperimentResultsResponse, GuardRailAlertResponse } from '@/hooks/useExperiments';
import { formatMetricLabel } from '@/lib/constants';

export type ExperimentDetailViewModel = {
  canDeclareWinner: boolean;
  conclusionMetricLabel: string;
  conclusionWinnerLabel: string;
  controlMeanClickRank: number;
  guardRailAlerts: GuardRailAlertResponse[];
  hasConclusion: boolean;
  needsDaysGateWarning: boolean;
  noStableIdQueries: number;
  outlierUsersExcluded: number;
  unstableIdFraction: number;
  variantMeanClickRank: number;
};

function formatConclusionWinnerLabel(winner: string | null | undefined): string {
  if (winner === 'control') return 'Control';
  if (winner === 'variant') return 'Variant';
  return 'No winner (inconclusive)';
}

export function buildExperimentDetailViewModel(
  results: ExperimentResultsResponse,
): ExperimentDetailViewModel {
  const totalSearches = results.control.searches + results.variant.searches;
  const guardRailAlerts = results.guardRailAlerts ?? [];
  const outlierUsersExcluded = results.outlierUsersExcluded ?? 0;
  const noStableIdQueries = results.noStableIdQueries ?? 0;

  return {
    canDeclareWinner: results.gate.minimumNReached && results.status !== 'concluded',
    conclusionMetricLabel: formatMetricLabel(results.primaryMetric),
    conclusionWinnerLabel: formatConclusionWinnerLabel(results.conclusion?.winner),
    controlMeanClickRank: results.control.meanClickRank ?? 0,
    guardRailAlerts,
    hasConclusion: results.status === 'concluded' && !!results.conclusion,
    needsDaysGateWarning: results.gate.minimumNReached && !results.gate.minimumDaysReached,
    noStableIdQueries,
    outlierUsersExcluded,
    unstableIdFraction: totalSearches > 0 ? noStableIdQueries / totalSearches : 0,
    variantMeanClickRank: results.variant.meanClickRank ?? 0,
  };
}
