import { useMemo, useState, type ReactNode } from 'react';
import {
  CheckCircle2,
  DollarSign,
  ShoppingCart,
  TrendingUp,
} from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import type { TabProps } from '@/lib/analytics-types';
import { formatCurrency, formatPercentAxisTick } from '@/lib/analytics-utils';
import {
  useAddToCartRate,
  useConversionRate,
  useCountries,
  usePurchaseRate,
  useRevenue,
  type DateRange,
} from '@/hooks/useAnalytics';
import {
  AreaTrendCard,
  EmptyState,
  ErrorState,
  formatAnalyticsErrorMessage,
} from '@/components/analytics/AnalyticsShared';
import { KpiCard } from '@/components/analytics/KpiCard';
import { COUNTRY_NAMES } from '@/components/analytics/geography-utils';

type ConversionTabProps = TabProps & {
  range: DateRange;
  prevRange?: DateRange;
};

function ConversionCountryFilter({
  countries,
  countryFilter,
  onCountryChange,
}: {
  countries: any[];
  countryFilter: string;
  onCountryChange: (value: string) => void;
}) {
  return (
    <div className="flex items-center justify-end">
      <div className="flex items-center gap-2">
        <label htmlFor="conversion-country-filter" className="text-sm text-muted-foreground">
          Country
        </label>
        <select
          id="conversion-country-filter"
          value={countryFilter}
          onChange={(event) => onCountryChange(event.target.value)}
          className="h-8 rounded-md border border-input bg-background px-2 text-sm min-w-44"
          data-testid="conversion-country-filter"
        >
          <option value="">All Countries</option>
          {countries.map((country) => (
            <option key={country.country} value={country.country}>
              {COUNTRY_NAMES[country.country as string] || country.country} ({country.count?.toLocaleString()})
            </option>
          ))}
        </select>
      </div>
    </div>
  );
}

function ConversionKpiGrid({
  addToCart,
  atcLoading,
  convLoading,
  convRate,
  prevAddToCart,
  prevConvRate,
  prevPurchase,
  primaryRevenue,
  purchase,
  purchaseLoading,
  revenueLoading,
}: {
  addToCart: any;
  atcLoading: boolean;
  convLoading: boolean;
  convRate: any;
  prevAddToCart: any;
  prevConvRate: any;
  prevPurchase: any;
  primaryRevenue?: { currency: string; revenue: number } | null;
  purchase: any;
  purchaseLoading: boolean;
  revenueLoading: boolean;
}) {
  return (
    <div className="grid gap-4 grid-cols-2 lg:grid-cols-4" data-testid="conversion-kpi-cards">
      <KpiCard
        title="Conversion Rate"
        value={convRate?.rate}
        prevValue={prevConvRate?.rate}
        loading={convLoading}
        icon={TrendingUp}
        sparkData={convRate?.dates}
        sparkKey="rate"
        format="percent"
        tooltip="Percentage of tracked searches that led to a conversion event"
        testId="kpi-conversion-rate"
      />
      <KpiCard
        title="Add-to-Cart Rate"
        value={addToCart?.rate}
        prevValue={prevAddToCart?.rate}
        loading={atcLoading}
        icon={ShoppingCart}
        sparkData={addToCart?.dates}
        sparkKey="rate"
        format="percent"
        tooltip="Percentage of tracked searches that led to an add-to-cart event"
        testId="kpi-add-to-cart-rate"
      />
      <KpiCard
        title="Purchase Rate"
        value={purchase?.rate}
        prevValue={prevPurchase?.rate}
        loading={purchaseLoading}
        icon={CheckCircle2}
        sparkData={purchase?.dates}
        sparkKey="rate"
        format="percent"
        tooltip="Percentage of tracked searches that led to a purchase event"
        testId="kpi-purchase-rate"
      />
      <RevenueKpiCard revenue={primaryRevenue} loading={revenueLoading} testId="kpi-revenue" />
    </div>
  );
}

function RevenueKpiCard({
  revenue,
  loading,
  testId,
}: {
  revenue?: { currency: string; revenue: number } | null;
  loading: boolean;
  testId: string;
}) {
  return (
    <KpiCard
      title="Revenue"
      value={revenue ? formatCurrency(revenue.revenue, revenue.currency) : null}
      loading={loading}
      icon={DollarSign}
      testId={testId}
    />
  );
}

function ConversionMetricChart({
  chartHeight,
  data,
  emptyState,
  gradientColor,
  gradientId,
  loading,
  seriesLabel,
  testId,
  title,
}: {
  chartHeight: number;
  data?: any[];
  emptyState: ReactNode;
  gradientColor: string;
  gradientId: string;
  loading: boolean;
  seriesLabel: string;
  testId: string;
  title: string;
}) {
  return (
    <AreaTrendCard
      testId={testId}
      title={title}
      loading={loading}
      data={data}
      chartHeight={chartHeight}
      gradientId={gradientId}
      gradientColor={gradientColor}
      dataKey="rate"
      strokeColor={gradientColor}
      yAxisFormatter={formatPercentAxisTick}
      tooltipValueFormatter={(value) => `${(value * 100).toFixed(1)}%`}
      seriesLabel={seriesLabel}
      emptyState={emptyState}
    />
  );
}

function RevenueBreakdownCard({ revenueEntries }: { revenueEntries: Array<{ currency: string; revenue: number }> | null }) {
  if (!revenueEntries || revenueEntries.length <= 1) return null;

  return (
    <Card data-testid="revenue-breakdown">
      <CardHeader className="pb-2">
        <CardTitle className="text-base font-medium">Revenue by Currency</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-2">
          {revenueEntries.map((entry) => (
            <div key={entry.currency} className="flex items-center justify-between py-1 border-b border-border/50 last:border-0">
              <span className="text-sm font-medium">{entry.currency}</span>
              <span className="text-sm tabular-nums font-mono">
                {formatCurrency(entry.revenue, entry.currency)}
              </span>
            </div>
          ))}
        </div>
      </CardContent>
    </Card>
  );
}

export function ConversionsTab({ index, range, prevRange }: ConversionTabProps) {
  const [countryFilter, setCountryFilter] = useState('');
  const { data: countriesData, error: countriesError } = useCountries(index, range);

  // conversionRate endpoint is currently global; country filtering applies to add-to-cart,
  // purchase, and revenue hooks where the country query param is supported.
  const { data: convRate, isLoading: convLoading, error: convError } = useConversionRate(index, range);
  const { data: prevConvRate } = useConversionRate(index, prevRange!);
  const { data: addToCart, isLoading: atcLoading, error: addToCartError } = useAddToCartRate(index, range, countryFilter || undefined);
  const { data: prevAddToCart } = useAddToCartRate(index, prevRange!, countryFilter || undefined);
  const { data: purchase, isLoading: purchaseLoading, error: purchaseError } = usePurchaseRate(index, range, countryFilter || undefined);
  const { data: prevPurchase } = usePurchaseRate(index, prevRange!, countryFilter || undefined);
  const { data: revenue, isLoading: revenueLoading, error: revenueError } = useRevenue(index, range, countryFilter || undefined);
  const countries: any[] = countriesData?.countries || [];
  const errorMessage = formatAnalyticsErrorMessage(
    countriesError,
    convError,
    addToCartError,
    purchaseError,
    revenueError,
  );

  const totalRevenue = useMemo(() => {
    if (!revenue?.currencies) return null;
    const entries = Object.values(revenue.currencies) as Array<{ currency: string; revenue: number }>;
    if (entries.length === 0) return null;
    return entries;
  }, [revenue]);

  const primaryRevenue = totalRevenue?.[0];

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
      <ConversionCountryFilter countries={countries} countryFilter={countryFilter} onCountryChange={setCountryFilter} />
      <ConversionKpiGrid
        addToCart={addToCart}
        atcLoading={atcLoading}
        convLoading={convLoading}
        convRate={convRate}
        prevAddToCart={prevAddToCart}
        prevConvRate={prevConvRate}
        prevPurchase={prevPurchase}
        primaryRevenue={primaryRevenue}
        purchase={purchase}
        purchaseLoading={purchaseLoading}
        revenueLoading={revenueLoading}
      />
      <ConversionMetricChart
        chartHeight={240}
        data={convRate?.dates}
        emptyState={(
          <EmptyState
            icon={TrendingUp}
            title="No conversion data yet"
            description="Conversion events will appear here once you start sending click and conversion events via the Insights API."
          />
        )}
        gradientColor="#10b981"
        gradientId="convGradient"
        loading={convLoading}
        seriesLabel="Conversion Rate"
        testId="conversion-rate-chart"
        title="Conversion Rate Over Time"
      />
      <div className="grid gap-4 grid-cols-1 lg:grid-cols-2">
        <ConversionMetricChart
          chartHeight={180}
          data={addToCart?.dates}
          emptyState={<div className="h-44 flex items-center justify-center text-sm text-muted-foreground">No data available</div>}
          gradientColor="#6366f1"
          gradientId="atcGradient"
          loading={atcLoading}
          seriesLabel="Add-to-Cart Rate"
          testId="atc-rate-chart"
          title="Add-to-Cart Rate Over Time"
        />
        <ConversionMetricChart
          chartHeight={180}
          data={purchase?.dates}
          emptyState={<div className="h-44 flex items-center justify-center text-sm text-muted-foreground">No data available</div>}
          gradientColor="#f59e0b"
          gradientId="purchaseGradient"
          loading={purchaseLoading}
          seriesLabel="Purchase Rate"
          testId="purchase-rate-chart"
          title="Purchase Rate Over Time"
        />
      </div>
      <RevenueBreakdownCard revenueEntries={totalRevenue} />
    </div>
  );
}
