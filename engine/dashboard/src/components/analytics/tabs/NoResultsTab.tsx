import {
  AlertCircle,
  CheckCircle2,
  Search,
} from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import type { TabProps } from '@/lib/analytics-types';
import {
  useNoResultRate,
  useNoResults,
} from '@/hooks/useAnalytics';
import { DeltaBadge } from '@/components/analytics/KpiCard';
import {
  EmptyState,
  ErrorState,
  formatAnalyticsErrorMessage,
  TableSkeleton,
} from '@/components/analytics/AnalyticsShared';

export function NoResultsTab({ index, range, prevRange }: TabProps) {
  const { data, isLoading, error } = useNoResults(index, range, 100);
  const { data: rateData, isLoading: rateLoading } = useNoResultRate(index, range);
  const { data: prevRateData } = useNoResultRate(index, prevRange!);
  const isClean = !rateLoading && rateData?.rate != null && rateData.rate === 0;
  const errorMessage = formatAnalyticsErrorMessage(error);

  return (
    <div className="space-y-6 mt-4">
      {rateData?.rate != null && (
        <Card data-testid="no-result-rate-banner">
          <CardContent className="py-5">
            <div className="flex items-center gap-4">
              {isClean ? (
                <CheckCircle2 className="h-10 w-10 text-green-500 shrink-0" />
              ) : (
                <AlertCircle className={`h-10 w-10 shrink-0 ${rateData.rate > 0.1 ? 'text-red-500' : 'text-amber-500'}`} />
              )}
              <div className="flex-1">
                <div className="flex items-baseline gap-3">
                  <span className={`text-3xl font-bold ${rateData.rate > 0.1 ? 'text-red-500' : isClean ? 'text-green-500' : ''}`} data-testid="rate-value">
                    {(rateData.rate * 100).toFixed(1)}%
                  </span>
                  <DeltaBadge current={rateData.rate} previous={prevRateData?.rate} invertColor />
                </div>
                <div className="text-sm text-muted-foreground mt-0.5">
                  {isClean
                    ? 'All searches return results. Your content coverage is excellent.'
                    : rateData.rate > 0.1
                      ? 'of searches return no results. Consider adding synonyms or content for the queries below.'
                      : 'of searches return no results'}
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      )}
      <Card data-testid="no-results-table">
        <CardHeader className="pb-2">
          <CardTitle className="text-base font-medium">Searches With No Results</CardTitle>
        </CardHeader>
        <CardContent>
        {isLoading ? (
          <TableSkeleton rows={5} />
        ) : errorMessage ? (
          <ErrorState message={errorMessage} />
        ) : data?.searches?.length ? (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-border text-left text-muted-foreground">
                    <th className="py-2.5 pr-4 font-medium w-8">#</th>
                    <th className="py-2.5 pr-4 font-medium">Query</th>
                    <th className="py-2.5 font-medium text-right">Count</th>
                  </tr>
                </thead>
                <tbody>
                  {data.searches.map((searchRow: any, index: number) => (
                    <tr key={index} className="border-b border-border/50 hover:bg-accent/30 transition-colors">
                      <td className="py-2.5 pr-4 text-muted-foreground text-xs">{index + 1}</td>
                      <td className="py-2.5 pr-4">
                        <span className="font-mono">{searchRow.search || <span className="text-muted-foreground italic">(empty query)</span>}</span>
                      </td>
                      <td className="py-2.5 text-right tabular-nums">{searchRow.count?.toLocaleString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : isClean ? (
            <EmptyState
              icon={CheckCircle2}
              title="No zero-result searches"
              description="All queries are returning results. Your content coverage is excellent."
              positive
            />
          ) : (
            <EmptyState
              icon={Search}
              title="No search data yet"
              description="Zero-result searches will appear here once users start querying your index."
            />
          )}
        </CardContent>
      </Card>
    </div>
  );
}
