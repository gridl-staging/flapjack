import React, { useMemo, useState } from 'react';
import {
  AlertCircle,
  ChevronDown,
  ChevronRight,
  Filter,
} from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import type { TabProps } from '@/lib/analytics-types';
import {
  useFilterValues,
  useFiltersNoResults,
  useTopFilters,
  type DateRange,
} from '@/hooks/useAnalytics';
import {
  EmptyState,
  ErrorState,
  formatAnalyticsErrorMessage,
  TableSkeleton,
} from '@/components/analytics/AnalyticsShared';

export function FiltersTab({ index, range }: TabProps) {
  const { data, isLoading, error } = useTopFilters(index, range, 100);
  const { data: noResultFilters } = useFiltersNoResults(index, range, 20);
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const maxCount = useMemo(() => {
    if (!data?.filters?.length) return 1;
    return Math.max(...data.filters.map((filterRow: any) => filterRow.count || 0));
  }, [data]);

  function toggleExpand(attribute: string) {
    setExpanded((previous) => {
      const next = new Set(previous);
      if (next.has(attribute)) next.delete(attribute);
      else next.add(attribute);
      return next;
    });
  }

  return (
    <div className="mt-4 space-y-4">
      <TopFiltersCard
        error={error}
        expanded={expanded}
        index={index}
        isLoading={isLoading}
        maxCount={maxCount}
        onToggleExpand={toggleExpand}
        range={range}
        topFilters={data?.filters}
      />
      <FiltersNoResultsCard filters={noResultFilters?.filters} />
    </div>
  );
}

function FilterValueRow({ index, attribute, range }: { index: string; attribute: string; range: DateRange }) {
  const { data, isLoading } = useFilterValues(index, attribute, range, 10);

  if (isLoading) return <tr><td colSpan={4} className="py-2 pl-12 text-muted-foreground text-xs">Loading values...</td></tr>;
  if (!data?.values?.length) return <tr><td colSpan={4} className="py-2 pl-12 text-muted-foreground text-xs">No values found</td></tr>;

  return (
    <>
      {data.values.map((valueRow: any, rowIndex: number) => (
        <tr key={rowIndex} className="bg-accent/10">
          <td className="py-1.5 pr-4" />
          <td className="py-1.5 pr-4 pl-8 text-xs text-muted-foreground font-mono">{valueRow.value}</td>
          <td className="py-1.5 pr-4 text-right tabular-nums text-xs">{valueRow.count?.toLocaleString()}</td>
          <td className="py-1.5" />
        </tr>
      ))}
    </>
  );
}

function extractFilterAttribute(filterString: string): string {
  const separatorIndex = filterString.indexOf(':');
  return separatorIndex >= 0 ? filterString.substring(0, separatorIndex) : filterString;
}

function TopFiltersCard({
  error,
  expanded,
  index,
  isLoading,
  maxCount,
  onToggleExpand,
  range,
  topFilters,
}: {
  error: unknown;
  expanded: Set<string>;
  index: string;
  isLoading: boolean;
  maxCount: number;
  onToggleExpand: (attribute: string) => void;
  range: DateRange;
  topFilters?: any[];
}) {
  const errorMessage = formatAnalyticsErrorMessage(error);

  return (
    <Card data-testid="filters-table">
      <CardHeader className="pb-2">
        <CardTitle className="text-base font-medium">Top Filter Attributes</CardTitle>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <TableSkeleton rows={5} />
        ) : errorMessage ? (
          <ErrorState message={errorMessage} />
        ) : topFilters?.length ? (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-border text-left text-muted-foreground">
                  <th className="py-2.5 pr-4 font-medium w-8" />
                  <th className="py-2.5 pr-4 font-medium">Attribute</th>
                  <th className="py-2.5 pr-4 font-medium text-right">Count</th>
                  <th className="py-2.5 font-medium text-right w-32">Usage</th>
                </tr>
              </thead>
              <tbody>
                {topFilters.map((topFilter: any, indexPosition: number) => {
                  const attribute = extractFilterAttribute(topFilter.attribute);
                  const isExpanded = expanded.has(attribute);
                  return (
                    <React.Fragment key={indexPosition}>
                      <tr
                        className="border-b border-border/50 hover:bg-accent/30 transition-colors cursor-pointer"
                        onClick={() => onToggleExpand(attribute)}
                      >
                        <td className="py-2.5 pr-4 text-muted-foreground text-xs">
                          {isExpanded ? <ChevronDown className="w-3 h-3 inline" /> : <ChevronRight className="w-3 h-3 inline" />}
                        </td>
                        <td className="py-2.5 pr-4 font-mono">{topFilter.attribute}</td>
                        <td className="py-2.5 pr-4 text-right tabular-nums">{topFilter.count?.toLocaleString()}</td>
                        <td className="py-2.5">
                          <div className="flex items-center justify-end gap-2">
                            <div className="w-24 h-2 bg-muted rounded-full overflow-hidden">
                              <div
                                className="h-full bg-primary/60 rounded-full transition-all"
                                style={{ width: `${((topFilter.count || 0) / maxCount) * 100}%` }}
                              />
                            </div>
                          </div>
                        </td>
                      </tr>
                      {isExpanded && (
                        <FilterValueRow index={index} attribute={attribute} range={range} />
                      )}
                    </React.Fragment>
                  );
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <EmptyState
            icon={Filter}
            title="No filter usage recorded"
            description="Filter analytics will appear here when users apply facet filters in their searches. Make sure you have attributesForFaceting configured."
          />
        )}
      </CardContent>
    </Card>
  );
}

function FiltersNoResultsCard({ filters }: { filters?: any[] }) {
  if (!filters?.length) return null;

  return (
    <Card data-testid="filters-no-results">
      <CardHeader className="pb-2">
        <CardTitle className="text-base font-medium flex items-center gap-2">
          <AlertCircle className="w-4 h-4 text-amber-500" />
          Filters Causing No Results
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border text-left text-muted-foreground">
                <th className="py-2.5 pr-4 font-medium">Filter</th>
                <th className="py-2.5 font-medium text-right">Times Used</th>
              </tr>
            </thead>
            <tbody>
              {filters.map((filterRow: any, index: number) => (
                <tr key={index} className="border-b border-border/50">
                  <td className="py-2.5 pr-4 font-mono text-amber-600 dark:text-amber-400">{filterRow.attribute}</td>
                  <td className="py-2.5 text-right tabular-nums">{filterRow.count?.toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  );
}
