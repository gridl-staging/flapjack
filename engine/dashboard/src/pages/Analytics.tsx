import React, { useMemo, useState } from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import {
  AlertCircle,
  CheckCircle2,
  Loader2,
  RefreshCw,
  Search,
} from 'lucide-react';
import api from '@/lib/api';
import { useIndexes } from '@/hooks/useIndexes';
import { analyticsKeys } from '@/lib/queryKeys';
import { RANGE_OPTIONS, formatDateShort } from '@/lib/analytics-utils';
import { ConfirmDialog } from '@/components/ui/confirm-dialog';
import { Badge } from '@/components/ui/badge';
import {
  defaultRange,
  previousRange,
  type DateRange,
} from '@/hooks/useAnalytics';
import { Card, CardContent } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { OverviewTab } from '@/components/analytics/tabs/OverviewTab';
import { ConversionsTab } from '@/components/analytics/tabs/ConversionsTab';
import { DevicesTab } from '@/components/analytics/tabs/DevicesTab';
import { FiltersTab } from '@/components/analytics/tabs/FiltersTab';
import { GeographyTab } from '@/components/analytics/tabs/GeographyTab';
import { NoResultsTab } from '@/components/analytics/tabs/NoResultsTab';
import { SearchesTab } from '@/components/analytics/tabs/SearchesTab';

export function Analytics() {
  const { indexName: urlIndexName } = useParams<{ indexName: string }>();
  const navigate = useNavigate();
  const { data: indexes } = useIndexes();
  const [rangeDays, setRangeDays] = useState(7);
  const queryClient = useQueryClient();

  const range: DateRange = useMemo(() => defaultRange(rangeDays), [rangeDays]);
  const prevRange: DateRange = useMemo(() => previousRange(range), [range]);

  const indexName = urlIndexName || indexes?.[0]?.uid || '';

  React.useEffect(() => {
    if (!urlIndexName && indexes?.length) {
      navigate(`/index/${encodeURIComponent(indexes[0].uid)}/analytics`, { replace: true });
    }
  }, [urlIndexName, indexes, navigate]);

  const [showClearDialog, setShowClearDialog] = useState(false);

  const clearMutation = useMutation({
    mutationFn: async (index: string) => {
      const res = await api.delete('/2/analytics/clear', { data: { index } });
      return res.data;
    },
    onSuccess: () => {
      queryClient.resetQueries({ queryKey: analyticsKeys.all });
      setShowClearDialog(false);
    },
  });

  const flushMutation = useMutation({
    mutationFn: async () => {
      const res = await api.post('/2/analytics/flush');
      return res.data;
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: analyticsKeys.all });
    },
  });

  const rangeLabel = range.startDate && range.endDate
    ? `${formatDateShort(range.startDate)} - ${formatDateShort(range.endDate)}`
    : '';

  return (
    <div className="space-y-6">
      <AnalyticsHeader
        canClear={!!indexName}
        clearMutation={clearMutation}
        clearSuccess={clearMutation.isSuccess}
        flushMutation={flushMutation}
        onClearDialogOpen={() => setShowClearDialog(true)}
        onRangeChange={setRangeDays}
        rangeDays={rangeDays}
        rangeLabel={rangeLabel}
        urlIndexName={urlIndexName}
      />
      <AnalyticsMainSection indexName={indexName} range={range} prevRange={prevRange} />

      <ConfirmDialog
        open={showClearDialog}
        onOpenChange={setShowClearDialog}
        title="Clear Analytics"
        description={
          <>
            Are you sure you want to clear all analytics data for{' '}
            <code className="font-mono text-sm bg-muted px-1 py-0.5 rounded">
              {indexName}
            </code>
            ? This action cannot be undone.
          </>
        }
        confirmLabel="Clear"
        variant="destructive"
        onConfirm={() => clearMutation.mutate(indexName)}
        isPending={clearMutation.isPending}
      />
    </div>
  );
}

type AnalyticsHeaderProps = {
  canClear: boolean;
  clearMutation: AnalyticsMutation;
  clearSuccess: boolean;
  flushMutation: AnalyticsMutation;
  onClearDialogOpen: () => void;
  onRangeChange: (days: number) => void;
  rangeDays: number;
  rangeLabel: string;
  urlIndexName: string | undefined;
};

function AnalyticsHeader({
  canClear,
  clearMutation,
  clearSuccess,
  flushMutation,
  onClearDialogOpen,
  onRangeChange,
  rangeDays,
  rangeLabel,
  urlIndexName,
}: AnalyticsHeaderProps) {
  return (
    <div className="space-y-3">
      {urlIndexName && (
        <div className="flex items-center gap-2 text-sm" data-testid="analytics-breadcrumb">
          <Link to="/overview" className="text-muted-foreground hover:text-foreground transition-colors">
            Overview
          </Link>
          <span className="text-muted-foreground">/</span>
          <Link to={`/index/${encodeURIComponent(urlIndexName)}`} className="text-muted-foreground hover:text-foreground transition-colors font-medium">
            {urlIndexName}
          </Link>
          <span className="text-muted-foreground">/</span>
          <span className="text-foreground font-medium">Analytics</span>
        </div>
      )}
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div>
          <div className="flex items-center gap-3">
            <h1 className="text-3xl font-bold" data-testid="analytics-heading">Analytics</h1>
            <Badge className="bg-orange-500 text-white hover:bg-orange-600 text-xs font-bold">BETA</Badge>
          </div>
          {rangeLabel && (
            <p className="text-sm text-muted-foreground mt-1" data-testid="analytics-date-label">{rangeLabel}</p>
          )}
        </div>
        <AnalyticsActions
          canClear={canClear}
          clearMutation={clearMutation}
          clearSuccess={clearSuccess}
          flushMutation={flushMutation}
          onClearDialogOpen={onClearDialogOpen}
          onRangeChange={onRangeChange}
          rangeDays={rangeDays}
        />
      </div>
    </div>
  );
}

type AnalyticsMutation = {
  isPending: boolean;
  isSuccess: boolean;
  mutate: (variables?: any) => any;
};

type AnalyticsActionsProps = {
  canClear: boolean;
  clearMutation: AnalyticsMutation;
  clearSuccess: boolean;
  flushMutation: AnalyticsMutation;
  onClearDialogOpen: () => void;
  onRangeChange: (days: number) => void;
  rangeDays: number;
};

function AnalyticsActions({
  canClear,
  clearMutation,
  clearSuccess,
  flushMutation,
  onClearDialogOpen,
  onRangeChange,
  rangeDays,
}: AnalyticsActionsProps) {
  return (
    <div className="flex items-center gap-3">
      <Button
        variant="outline"
        size="sm"
        onClick={() => flushMutation.mutate()}
        disabled={flushMutation.isPending}
        title="Flush buffered analytics events to disk and refresh"
        data-testid="flush-btn"
      >
        <RefreshCw className={`h-4 w-4 mr-1.5 ${flushMutation.isPending ? 'animate-spin' : ''}`} />
        {flushMutation.isPending ? 'Updating...' : 'Update'}
      </Button>
      {canClear && (
        <Button
          variant="outline"
          size="sm"
          onClick={onClearDialogOpen}
          disabled={clearMutation.isPending}
          title="Delete all analytics data for this index"
          data-testid="clear-btn"
        >
          {clearMutation.isPending ? (
            <Loader2 className="h-4 w-4 mr-1.5 animate-spin" />
          ) : (
            <AlertCircle className="h-4 w-4 mr-1.5" />
          )}
          {clearMutation.isPending ? 'Clearing...' : 'Clear Analytics'}
        </Button>
      )}
      {clearSuccess && (
        <span className="text-xs text-green-600 flex items-center gap-1">
          <CheckCircle2 className="h-3 w-3" />
          Analytics cleared
        </span>
      )}

      <div className="flex rounded-md border border-input" data-testid="analytics-date-range">
        {RANGE_OPTIONS.map((opt) => (
          <button
            key={opt.days}
            onClick={() => onRangeChange(opt.days)}
            data-testid={`range-${opt.label}`}
            className={`px-3 py-1.5 text-sm font-medium transition-colors ${
              rangeDays === opt.days
                ? 'bg-primary text-primary-foreground'
                : 'text-muted-foreground hover:bg-accent'
            } ${opt.days === 7 ? 'rounded-l-md' : ''} ${opt.days === 90 ? 'rounded-r-md' : ''}`}
          >
            {opt.label}
          </button>
        ))}
      </div>
    </div>
  );
}

function AnalyticsMainSection({
  indexName,
  range,
  prevRange,
}: {
  indexName: string;
  range: DateRange;
  prevRange: DateRange;
}) {
  if (!indexName) {
    return (
      <Card>
        <CardContent className="py-12 text-center text-muted-foreground">
          <Search className="h-12 w-12 mx-auto mb-4 opacity-30" />
          <h3 className="text-lg font-medium mb-2">No Indexes Found</h3>
          <p className="text-sm">Create a demo index (Movies or Products) to get started — analytics data is included automatically.</p>
        </CardContent>
      </Card>
    );
  }

  return (
    <Tabs defaultValue="overview" data-testid="analytics-tabs">
      <TabsList>
        <TabsTrigger value="overview" data-testid="tab-overview">Overview</TabsTrigger>
        <TabsTrigger value="searches" data-testid="tab-searches">Searches</TabsTrigger>
        <TabsTrigger value="noResults" data-testid="tab-no-results">No Results</TabsTrigger>
        <TabsTrigger value="filters" data-testid="tab-filters">Filters</TabsTrigger>
        <TabsTrigger value="conversions" data-testid="tab-conversions">Conversions</TabsTrigger>
        <TabsTrigger value="devices" data-testid="tab-devices">Devices</TabsTrigger>
        <TabsTrigger value="geography" data-testid="tab-geography">Geography</TabsTrigger>
      </TabsList>

      <TabsContent value="overview">
        <OverviewTab index={indexName} range={range} prevRange={prevRange} />
      </TabsContent>
      <TabsContent value="searches">
        <SearchesTab index={indexName} range={range} />
      </TabsContent>
      <TabsContent value="noResults">
        <NoResultsTab index={indexName} range={range} prevRange={prevRange} />
      </TabsContent>
      <TabsContent value="filters">
        <FiltersTab index={indexName} range={range} />
      </TabsContent>
      <TabsContent value="conversions">
        <ConversionsTab index={indexName} range={range} prevRange={prevRange} />
      </TabsContent>
      <TabsContent value="devices">
        <DevicesTab index={indexName} range={range} />
      </TabsContent>
      <TabsContent value="geography">
        <GeographyTab index={indexName} range={range} />
      </TabsContent>
    </Tabs>
  );
}
