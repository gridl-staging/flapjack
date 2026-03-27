import { describe, it, expect } from 'vitest';
import type { SearchParams } from '@/lib/types';
import { mergeSearchParams } from './SearchBrowse';

const baseSearchParams: SearchParams = {
  query: '',
  hitsPerPage: 20,
  page: 0,
  attributesToHighlight: ['*'],
};

describe('mergeSearchParams', () => {
  it('applies explicit page updates from pagination controls', () => {
    const updated = mergeSearchParams(baseSearchParams, { page: 1 });
    expect(updated.page).toBe(1);
  });

  it('resets page to 0 when query changes', () => {
    const updated = mergeSearchParams({ ...baseSearchParams, page: 3 }, { query: 'laptop' });
    expect(updated.page).toBe(0);
  });

  it('resets page to 0 when filters change', () => {
    const updated = mergeSearchParams({ ...baseSearchParams, page: 2 }, { filters: 'brand:Apple' });
    expect(updated.page).toBe(0);
  });

  it('keeps explicit page when query and page are updated together', () => {
    const updated = mergeSearchParams(baseSearchParams, { query: 'laptop', page: 2 });
    expect(updated.page).toBe(2);
  });
});
