import {
  AlertCircle,
  Search,
  Users,
} from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import type { TabProps } from '@/lib/analytics-types';
import { formatPercentAxisTick } from '@/lib/analytics-utils';
import {
  useNoResultRate,
  useSearchCount,
  useTopSearches,
  useUsersCount,
} from '@/hooks/useAnalytics';
import { KpiCard } from '@/components/analytics/KpiCard';
import {
  AreaTrendCard,
  EmptyState,
  ErrorState,
  formatAnalyticsErrorMessage,
  TableSkeleton,
} from '@/components/analytics/AnalyticsShared';

function SearchVolumeCard({
  loading,
  searchDates,
}: {
  loading: boolean;
  searchDates?: any[];
}) {
  return (
    <AreaTrendCard
      testId="search-volume-chart"
      title="Search Volume"
      loading={loading}
      data={searchDates}
      chartHeight={240}
      gradientId="searchGradient"
      gradientColor="hsl(var(--primary))"
      dataKey="count"
      strokeColor="hsl(var(--primary))"
      yAxisWidth={48}
      tooltipValueFormatter={(value) => value.toLocaleString()}
      seriesLabel="Searches"
      emptyState={(
        <EmptyState
          icon={Search}
          title="No search data yet"
          description="Searches will appear here once users start querying your index. Send your first search request to get started."
        />
      )}
    />
  );
}

function NoResultRateChartCard({
  loading,
  noResultDates,
}: {
  loading: boolean;
  noResultDates?: any[];
}) {
  return (
    <AreaTrendCard
      testId="no-result-rate-chart"
      title="No-Result Rate Over Time"
      loading={loading}
      data={noResultDates}
      chartHeight={180}
      gradientId="nrrGradient"
      gradientColor="#f59e0b"
      dataKey="rate"
      strokeColor="#f59e0b"
      yAxisFormatter={formatPercentAxisTick}
      tooltipValueFormatter={(value) => `${(value * 100).toFixed(1)}%`}
      seriesLabel="No-Result Rate"
      emptyState={<div className="h-44 flex items-center justify-center text-sm text-muted-foreground">No data available</div>}
    />
  );
}

function OverviewTopSearchesCard({
  loading,
  searches,
}: {
  loading: boolean;
  searches?: any[];
}) {
  return (
    <Card data-testid="top-searches-overview">
      <CardHeader className="pb-2">
        <CardTitle className="text-base font-medium">Top 10 Searches</CardTitle>
      </CardHeader>
      <CardContent>
        {loading ? (
          <TableSkeleton rows={5} />
        ) : searches?.length ? (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-left text-muted-foreground">
                  <th className="py-2 pr-3 font-medium w-6">#</th>
                  <th className="py-2 pr-3 font-medium">Query</th>
                  <th className="py-2 font-medium text-right">Count</th>
                </tr>
              </thead>
              <tbody>
                {searches.slice(0, 10).map((searchRow: any, index: number) => (
                  <tr key={index} className="border-b border-border/50">
                    <td className="py-1.5 pr-3 text-muted-foreground text-xs">{index + 1}</td>
                    <td className="py-1.5 pr-3 font-mono text-sm truncate max-w-[200px]" data-testid="search-query">
                      {searchRow.search || <span className="text-muted-foreground italic">(empty)</span>}
                    </td>
                    <td className="py-1.5 text-right tabular-nums" data-testid="search-count">
                      {searchRow.count?.toLocaleString()}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="h-44 flex items-center justify-center text-sm text-muted-foreground">No search data yet</div>
        )}
      </CardContent>
    </Card>
  );
}

export function OverviewTab({ index, range, prevRange }: TabProps) {
  const { data: searchCount, isLoading: countLoading, error: countError } = useSearchCount(index, range);
  const { data: prevSearchCount } = useSearchCount(index, prevRange!);
  const { data: usersCount, isLoading: usersLoading, error: usersError } = useUsersCount(index, range);
  const { data: prevUsersCount } = useUsersCount(index, prevRange!);
  const { data: noResultRate, isLoading: noResultLoading, error: noResultError } = useNoResultRate(index, range);
  const { data: prevNoResultRate } = useNoResultRate(index, prevRange!);
  const { data: topSearches, isLoading: topSearchesLoading, error: topSearchesError } = useTopSearches(index, range, 10, false);
  const errorMessage = formatAnalyticsErrorMessage(countError, usersError, noResultError, topSearchesError);

  if (errorMessage) {
    return (
      <Card>
        <CardContent className="py-12">
          <ErrorState message={errorMessage} />
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-6 mt-4">
      <div className="grid gap-4 grid-cols-2 lg:grid-cols-3" data-testid="kpi-cards">
        <KpiCard
          title="Total Searches"
          value={searchCount?.count}
          prevValue={prevSearchCount?.count}
          loading={countLoading}
          icon={Search}
          sparkData={searchCount?.dates}
          sparkKey="count"
          format="number"
          tooltip="Total number of search queries received during this period"
        />
        <KpiCard
          title="Unique Users"
          value={usersCount?.count}
          prevValue={prevUsersCount?.count}
          loading={usersLoading}
          icon={Users}
          format="number"
          tooltip="Number of distinct users who performed searches"
        />
        <KpiCard
          title="No-Result Rate"
          value={noResultRate?.rate}
          prevValue={prevNoResultRate?.rate}
          loading={noResultLoading}
          icon={AlertCircle}
          sparkData={noResultRate?.dates}
          sparkKey="rate"
          format="percent"
          invertDelta
          tooltip="Percentage of searches that returned zero results. Lower is better."
        />
      </div>
      <SearchVolumeCard loading={countLoading} searchDates={searchCount?.dates} />
      <div className="grid gap-4 grid-cols-1 lg:grid-cols-2">
        <NoResultRateChartCard loading={noResultLoading} noResultDates={noResultRate?.dates} />
        <OverviewTopSearchesCard loading={topSearchesLoading} searches={topSearches?.searches} />
      </div>
    </div>
  );
}
