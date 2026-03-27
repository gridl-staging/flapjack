import { useState } from 'react';
import {
  ChevronLeft as ChevronLeftIcon,
  Globe,
  MapPin,
  Search,
} from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import type { TabProps } from '@/lib/analytics-types';
import {
  useGeoBreakdown,
  useGeoRegions,
  useGeoTopSearches,
  type DateRange,
} from '@/hooks/useAnalytics';
import {
  EmptyState,
  ErrorState,
  formatAnalyticsErrorMessage,
  TableSkeleton,
} from '@/components/analytics/AnalyticsShared';
import { COUNTRY_NAMES, countryFlag } from '@/components/analytics/geography-utils';

function GeographySummaryCards({
  countryCount,
  totalSearches,
}: {
  countryCount: number;
  totalSearches: number;
}) {
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
      <Card data-testid="geo-countries-count">
        <CardHeader className="pb-1">
          <CardTitle className="text-xs font-medium text-muted-foreground flex items-center gap-1.5 uppercase tracking-wide">
            <Globe className="h-3.5 w-3.5" />
            Countries
          </CardTitle>
        </CardHeader>
        <CardContent className="pb-3">
          <span className="text-2xl font-bold tabular-nums">{countryCount}</span>
        </CardContent>
      </Card>
      <Card>
        <CardHeader className="pb-1">
          <CardTitle className="text-xs font-medium text-muted-foreground flex items-center gap-1.5 uppercase tracking-wide">
            <MapPin className="h-3.5 w-3.5" />
            Total Searches
          </CardTitle>
        </CardHeader>
        <CardContent className="pb-3">
          <span className="text-2xl font-bold tabular-nums">{totalSearches.toLocaleString()}</span>
        </CardContent>
      </Card>
    </div>
  );
}

function GeographyCountryTable({
  countries,
  onSelectCountry,
  totalSearches,
}: {
  countries: any[];
  onSelectCountry: (countryCode: string) => void;
  totalSearches: number;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm font-medium">Searches by Country</CardTitle>
      </CardHeader>
      <CardContent>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b text-left text-muted-foreground">
              <th className="pb-2 font-medium w-8">#</th>
              <th className="pb-2 font-medium">Country</th>
              <th className="pb-2 font-medium text-right">Searches</th>
              <th className="pb-2 font-medium text-right w-20">Share</th>
              <th className="pb-2 w-32"></th>
            </tr>
          </thead>
          <tbody>
            {countries.map((countryRow: any, index: number) => {
              const countryCode = countryRow.country as string;
              const searchCount = (countryRow.count as number) || 0;
              const sharePercentage = totalSearches > 0 ? (searchCount / totalSearches) * 100 : 0;
              const countryName = COUNTRY_NAMES[countryCode] || countryCode;

              return (
                <tr
                  key={countryCode}
                  className="border-b border-border/50 hover:bg-muted/50 cursor-pointer transition-colors"
                  onClick={() => onSelectCountry(countryCode)}
                >
                  <td className="py-2.5 text-muted-foreground tabular-nums">{index + 1}</td>
                  <td className="py-2.5">
                    <span className="mr-2">{countryFlag(countryCode)}</span>
                    <span className="font-medium">{countryName}</span>
                    <span className="text-muted-foreground ml-1.5 text-xs">({countryCode})</span>
                  </td>
                  <td className="py-2.5 text-right tabular-nums font-medium" data-testid="country-count">
                    {searchCount.toLocaleString()}
                  </td>
                  <td className="py-2.5 text-right tabular-nums text-muted-foreground" data-testid="country-share">
                    {sharePercentage.toFixed(1)}%
                  </td>
                  <td className="py-2.5 pl-3">
                    <div className="h-1.5 bg-muted rounded-full overflow-hidden">
                      <div
                        className="h-full bg-primary rounded-full"
                        style={{ width: `${Math.min(sharePercentage * 2, 100)}%` }}
                      />
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </CardContent>
    </Card>
  );
}

function CountrySearchesCard({
  error,
  countryName,
  isLoading,
  searches,
}: {
  error: unknown;
  countryName: string;
  isLoading: boolean;
  searches: any[];
}) {
  const errorMessage = formatAnalyticsErrorMessage(error);

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm font-medium">Top Searches from {countryName}</CardTitle>
      </CardHeader>
      <CardContent>
        {isLoading ? (
          <TableSkeleton rows={5} />
        ) : errorMessage ? (
          <ErrorState message={errorMessage} />
        ) : !searches.length ? (
          <EmptyState icon={Search} title="No searches" description={`No search data from ${countryName} in this period.`} />
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-muted-foreground">
                <th className="pb-2 font-medium w-8">#</th>
                <th className="pb-2 font-medium">Query</th>
                <th className="pb-2 font-medium text-right">Count</th>
              </tr>
            </thead>
            <tbody>
              {searches.map((searchRow: any, index: number) => (
                <tr key={index} className="border-b border-border/50">
                  <td className="py-2.5 text-muted-foreground tabular-nums">{index + 1}</td>
                  <td className="py-2.5 font-mono">{searchRow.search || '(empty)'}</td>
                  <td className="py-2.5 text-right tabular-nums font-medium">{(searchRow.count as number)?.toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </CardContent>
    </Card>
  );
}

function CountryRegionsCard({
  country,
  error,
  regions,
  regionsLoading,
  regionsTotal,
}: {
  country: string;
  error: unknown;
  regions: any[];
  regionsLoading: boolean;
  regionsTotal: number;
}) {
  const errorMessage = formatAnalyticsErrorMessage(error);

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-sm font-medium flex items-center gap-2">
          <MapPin className="h-4 w-4" />
          {country === 'US' ? 'States' : 'Regions'}
        </CardTitle>
      </CardHeader>
      <CardContent>
        {regionsLoading ? (
          <TableSkeleton rows={5} />
        ) : errorMessage ? (
          <ErrorState message={errorMessage} />
        ) : !regions.length ? (
          <div className="py-8 text-center text-sm text-muted-foreground">No region data available</div>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b text-left text-muted-foreground">
                <th className="pb-2 font-medium w-8">#</th>
                <th className="pb-2 font-medium">{country === 'US' ? 'State' : 'Region'}</th>
                <th className="pb-2 font-medium text-right">Searches</th>
                <th className="pb-2 font-medium text-right w-16">Share</th>
              </tr>
            </thead>
            <tbody>
              {regions.map((regionRow: any, index: number) => {
                const sharePercentage = regionsTotal > 0 ? ((regionRow.count || 0) / regionsTotal) * 100 : 0;
                return (
                  <tr key={index} className="border-b border-border/50">
                    <td className="py-2 text-muted-foreground tabular-nums text-xs">{index + 1}</td>
                    <td className="py-2 font-medium">{regionRow.region}</td>
                    <td className="py-2 text-right tabular-nums">{(regionRow.count as number)?.toLocaleString()}</td>
                    <td className="py-2 text-right tabular-nums text-muted-foreground text-xs">{sharePercentage.toFixed(1)}%</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </CardContent>
    </Card>
  );
}

function CountryDrillDown({
  index,
  range,
  country,
  onBack,
}: {
  index: string;
  range: DateRange;
  country: string;
  onBack: () => void;
}) {
  const { data, isLoading, error: topSearchesError } = useGeoTopSearches(index, country, range);
  const { data: regionsData, isLoading: regionsLoading, error: regionsError } = useGeoRegions(index, country, range);
  const searches: any[] = data?.searches || [];
  const regions: any[] = regionsData?.regions || [];
  const name = COUNTRY_NAMES[country] || country;

  const regionsTotal = regions.reduce((sum: number, row: any) => sum + (row.count || 0), 0);

  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <Button variant="ghost" size="sm" onClick={onBack}>
          <ChevronLeftIcon className="h-4 w-4 mr-1" />
          All Countries
        </Button>
        <span className="text-lg font-semibold">
          {countryFlag(country)} {name}
        </span>
      </div>

      <div className="grid gap-4 grid-cols-1 lg:grid-cols-2">
        <CountrySearchesCard error={topSearchesError} countryName={name} isLoading={isLoading} searches={searches} />
        <CountryRegionsCard country={country} error={regionsError} regions={regions} regionsLoading={regionsLoading} regionsTotal={regionsTotal} />
      </div>
    </div>
  );
}

export function GeographyTab({ index, range }: TabProps) {
  const { data, isLoading, error } = useGeoBreakdown(index, range);
  const [selectedCountry, setSelectedCountry] = useState<string | null>(null);

  const countries: any[] = data?.countries || [];
  const total: number = data?.total || 0;
  const errorMessage = formatAnalyticsErrorMessage(error);

  if (isLoading) {
    return (
      <Card><CardContent className="py-6"><TableSkeleton rows={8} /></CardContent></Card>
    );
  }

  if (errorMessage) {
    return (
      <Card>
        <CardContent className="py-12">
          <ErrorState message={errorMessage} />
        </CardContent>
      </Card>
    );
  }

  if (!countries.length) {
    return (
      <Card>
        <CardContent className="py-12">
          <EmptyState
            icon={Globe}
            title="No geographic data"
            description="Geographic breakdown requires country data in search events. Create a demo index to get sample data with geographic distribution."
          />
        </CardContent>
      </Card>
    );
  }

  if (selectedCountry) {
    return (
      <CountryDrillDown
        index={index}
        range={range}
        country={selectedCountry}
        onBack={() => setSelectedCountry(null)}
      />
    );
  }

  return (
    <div className="space-y-4">
      <GeographySummaryCards countryCount={countries.length} totalSearches={total} />
      <GeographyCountryTable countries={countries} onSelectCountry={setSelectedCountry} totalSearches={total} />
    </div>
  );
}
