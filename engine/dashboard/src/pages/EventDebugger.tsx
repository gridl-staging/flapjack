import { useState, useEffect, useMemo } from 'react';
import { useDebugEvents } from '@/hooks/useDebugEvents';
import {
  buildDebugEventRows,
  buildEventVolumeSeries,
  resolveTimeRange,
  type TimeRangePreset,
} from './eventDebuggerUtils';
import {
  EventContentPanel,
  EventDebuggerHeader,
  EventFiltersPanel,
  EventVolumePanel,
} from './EventDebuggerSections';

const EVENT_REFRESH_INTERVAL_MS = 5000;

export function EventDebugger() {
  const [statusFilter, setStatusFilter] = useState('');
  const [eventTypeFilter, setEventTypeFilter] = useState('');
  const [indexFilter, setIndexFilter] = useState('');
  const [timeRange, setTimeRange] = useState<TimeRangePreset>('24h');
  const [selectedEventRowKey, setSelectedEventRowKey] = useState<string | null>(null);
  const [rangeAnchorMs, setRangeAnchorMs] = useState(() => Date.now());
  const { from: fromTimestampMs, until: untilTimestampMs } = resolveTimeRange(timeRange, rangeAnchorMs);

  useEffect(() => {
    setRangeAnchorMs(Date.now());

    if (timeRange === 'all') {
      return;
    }

    const intervalId = window.setInterval(() => {
      setRangeAnchorMs(Date.now());
    }, EVENT_REFRESH_INTERVAL_MS);

    return () => window.clearInterval(intervalId);
  }, [timeRange]);

  const { data, isLoading, isError } = useDebugEvents(
    {
      status: statusFilter || undefined,
      eventType: eventTypeFilter || undefined,
      index: indexFilter || undefined,
      from: fromTimestampMs,
      until: untilTimestampMs,
    },
    timeRange === 'all' ? EVENT_REFRESH_INTERVAL_MS : false,
  );

  const events = data?.events ?? [];
  const eventRows = useMemo(() => buildDebugEventRows(events), [events]);
  const okCount = events.filter((event) => event.httpCode === 200).length;
  const errorCount = events.length - okCount;
  const selectedEvent = useMemo(
    () => eventRows.find((row) => row.rowKey === selectedEventRowKey)?.event ?? null,
    [eventRows, selectedEventRowKey],
  );
  const volumeData = useMemo(
    () => buildEventVolumeSeries(events, fromTimestampMs, untilTimestampMs),
    [events, fromTimestampMs, untilTimestampMs],
  );

  useEffect(() => {
    if (!selectedEventRowKey) {
      return;
    }

    const stillSelected = eventRows.some((row) => row.rowKey === selectedEventRowKey);
    if (!stillSelected) {
      setSelectedEventRowKey(null);
    }
  }, [eventRows, selectedEventRowKey]);

  return (
    <div className="space-y-6">
      <EventDebuggerHeader eventCount={data?.count} />
      <EventFiltersPanel
        eventTypeFilter={eventTypeFilter}
        indexFilter={indexFilter}
        onEventTypeFilterChange={setEventTypeFilter}
        onIndexFilterChange={setIndexFilter}
        onStatusFilterChange={setStatusFilter}
        onTimeRangeChange={setTimeRange}
        statusFilter={statusFilter}
        timeRange={timeRange}
      />
      <EventVolumePanel
        events={events}
        errorCount={errorCount}
        isError={isError}
        isLoading={isLoading}
        okCount={okCount}
        volumeData={volumeData}
      />
      <EventContentPanel
        eventRows={eventRows}
        isError={isError}
        isLoading={isLoading}
        onSelectEventRow={setSelectedEventRowKey}
        selectedEvent={selectedEvent}
        selectedEventRowKey={selectedEventRowKey}
      />
    </div>
  );
}
