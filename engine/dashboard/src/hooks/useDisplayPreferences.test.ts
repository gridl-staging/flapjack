import { beforeEach, describe, expect, it, vi } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import type { DisplayPreferences, FieldInfo } from '@/lib/types';

const DISPLAY_PREFERENCES_PERSIST_KEY = 'flapjack-display-prefs';

type UseDisplayPreferencesHook = typeof import('./useDisplayPreferences').useDisplayPreferences;
type AutoDetectPreferencesFn = typeof import('./useDisplayPreferences').autoDetectPreferences;
type GetPreferencesFn = typeof import('./useDisplayPreferences').getPreferences;

let useDisplayPreferences: UseDisplayPreferencesHook;
let autoDetectPreferences: AutoDetectPreferencesFn;
let getPreferences: GetPreferencesFn;

const productPreferences: DisplayPreferences = {
  titleAttribute: 'name',
  subtitleAttribute: null,
  imageAttribute: 'image_url',
  tagAttributes: ['tags'],
};

function createLocalStorageMock(initialValues: Record<string, string> = {}): Storage {
  const values = new Map<string, string>(Object.entries(initialValues));

  return {
    getItem: (key: string) => values.get(key) ?? null,
    setItem: (key: string, value: string) => {
      values.set(key, value);
    },
    removeItem: (key: string) => {
      values.delete(key);
    },
    clear: () => {
      values.clear();
    },
    key: (index: number) => Array.from(values.keys())[index] ?? null,
    get length() {
      return values.size;
    },
  };
}

async function resetStoreAndLoadModule(initialStorage: Record<string, string> = {}) {
  Object.defineProperty(window, 'localStorage', {
    value: createLocalStorageMock(initialStorage),
    configurable: true,
  });
  vi.resetModules();
  const module = await import('./useDisplayPreferences');
  useDisplayPreferences = module.useDisplayPreferences;
  autoDetectPreferences = module.autoDetectPreferences;
  getPreferences = module.getPreferences;
}

describe('useDisplayPreferences', () => {
  beforeEach(async () => {
    await resetStoreAndLoadModule();
  });

  it('returns null preferences for an index with no saved preferences', () => {
    const { result } = renderHook(() => useDisplayPreferences('unknown-index'));

    expect(result.current.preferences).toBeNull();
    expect(result.current.getPreferences('unknown-index')).toBeNull();
  });

  it('saves and retrieves preferences per index', () => {
    const { result } = renderHook(() => useDisplayPreferences('products'));

    act(() => {
      result.current.setPreferences('products', productPreferences);
      result.current.setPreferences('orders', {
        titleAttribute: 'order_id',
        subtitleAttribute: null,
        imageAttribute: null,
        tagAttributes: ['status'],
      });
    });

    expect(result.current.preferences).toEqual(productPreferences);
    expect(result.current.getPreferences('products')).toEqual(productPreferences);
    expect(result.current.getPreferences('orders')).toEqual({
      titleAttribute: 'order_id',
      subtitleAttribute: null,
      imageAttribute: null,
      tagAttributes: ['status'],
    });
  });

  it('clears preferences for a specific index only', () => {
    const { result } = renderHook(() => useDisplayPreferences('products'));

    act(() => {
      result.current.setPreferences('products', productPreferences);
      result.current.setPreferences('orders', {
        titleAttribute: 'order_id',
        subtitleAttribute: null,
        imageAttribute: null,
        tagAttributes: ['status'],
      });
    });

    act(() => {
      result.current.clearPreferences('products');
    });

    expect(result.current.getPreferences('products')).toBeNull();
    expect(result.current.getPreferences('orders')).toEqual({
      titleAttribute: 'order_id',
      subtitleAttribute: null,
      imageAttribute: null,
      tagAttributes: ['status'],
    });
  });

  it('persists all index preferences under one flapjack-display-prefs key', () => {
    const { result } = renderHook(() => useDisplayPreferences('products'));

    act(() => {
      result.current.setPreferences('products', productPreferences);
      result.current.setPreferences('orders', {
        titleAttribute: 'order_id',
        subtitleAttribute: null,
        imageAttribute: null,
        tagAttributes: ['status'],
      });
    });

    const persistedRaw = window.localStorage.getItem(DISPLAY_PREFERENCES_PERSIST_KEY);

    expect(window.localStorage.length).toBe(1);
    expect(persistedRaw).not.toBeNull();

    const persisted = JSON.parse(persistedRaw ?? '{}');
    expect(Object.keys(persisted)).toEqual(expect.arrayContaining(['state', 'version']));
    expect(persisted.state.preferencesByIndex).toEqual({
      products: productPreferences,
      orders: {
        titleAttribute: 'order_id',
        subtitleAttribute: null,
        imageAttribute: null,
        tagAttributes: ['status'],
      },
    });
  });

  it('rehydrates saved preferences from persisted storage on module load', async () => {
    await resetStoreAndLoadModule({
      [DISPLAY_PREFERENCES_PERSIST_KEY]: JSON.stringify({
        state: {
          preferencesByIndex: {
            products: productPreferences,
          },
        },
        version: 0,
      }),
    });

    const { result } = renderHook(() => useDisplayPreferences('products'));

    expect(result.current.preferences).toEqual(productPreferences);
    expect(getPreferences('products')).toEqual(productPreferences);
  });

  it('exposes non-hook helpers for reading saved preferences', () => {
    const { result } = renderHook(() => useDisplayPreferences('products'));
    act(() => {
      result.current.setPreferences('products', productPreferences);
    });

    expect(getPreferences('products')).toEqual(productPreferences);
    expect(getPreferences('missing')).toBeNull();
  });
});

describe('autoDetectPreferences', () => {
  beforeEach(async () => {
    await resetStoreAndLoadModule();
  });

  it('returns sensible defaults for common field names', () => {
    const fields: FieldInfo[] = [
      { name: 'name', type: 'text' },
      { name: 'description', type: 'text' },
      { name: 'image_url', type: 'text' },
      { name: 'tags', type: 'text' },
      { name: 'price', type: 'number' },
    ];

    expect(autoDetectPreferences(fields)).toEqual({
      titleAttribute: 'name',
      subtitleAttribute: null,
      imageAttribute: 'image_url',
      tagAttributes: ['tags'],
    });
  });

  it('returns null when no common names match', () => {
    const fields: FieldInfo[] = [
      { name: 'x1', type: 'text' },
      { name: 'x2', type: 'number' },
      { name: 'x3', type: 'boolean' },
    ];

    expect(autoDetectPreferences(fields)).toBeNull();
  });

  it('returns null for empty field lists', () => {
    expect(autoDetectPreferences([])).toBeNull();
  });
});
