import { describe, expect, it } from 'vitest';
import {
  buildDebugEventRows,
  buildEventVolumeSeries,
  resolveTimeRange,
} from './eventDebuggerUtils';

const BASE_EVENT = {
  timestampMs: 1_709_251_200_000,
  index: 'products',
  eventType: 'view',
  eventSubtype: null,
  eventName: 'Viewed Product',
  userToken: 'user_abc',
  objectIds: ['obj1'],
  httpCode: 200,
  validationErrors: [],
};

describe('eventDebuggerUtils', () => {
  it('builds unique row keys for duplicate events', () => {
    const rows = buildDebugEventRows([BASE_EVENT, BASE_EVENT]);

    expect(rows).toHaveLength(2);
    expect(rows[0].rowKey).not.toBe(rows[1].rowKey);
    expect(rows[0].rowKey.endsWith('::0')).toBe(true);
    expect(rows[1].rowKey.endsWith('::1')).toBe(true);
  });

  it('resolves finite and unbounded date ranges', () => {
    expect(resolveTimeRange('24h', 1_700_000_000_000)).toEqual({
      from: 1_700_000_000_000 - 24 * 60 * 60 * 1000,
      until: 1_700_000_000_000,
    });
    expect(resolveTimeRange('all', 1_700_000_000_000)).toEqual({});
  });

  it('aggregates volume buckets with separate ok and error counts', () => {
    const series = buildEventVolumeSeries(
      [
        BASE_EVENT,
        { ...BASE_EVENT, timestampMs: BASE_EVENT.timestampMs + 60_000, httpCode: 422 },
      ],
      BASE_EVENT.timestampMs,
      BASE_EVENT.timestampMs + 5 * 60_000,
    );

    expect(series).toHaveLength(2);
    expect(series[0]).toMatchObject({ total: 2, ok: 1, error: 1 });
    expect(series[1]).toMatchObject({ total: 0, ok: 0, error: 0 });
  });
});
