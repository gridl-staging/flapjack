import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  buildDialogEntry,
  buildEntryDescription,
  buildObjectId,
  createCompoundFormState,
  createPluralFormState,
  createStopwordFormState,
} from './shared';

describe('dictionary shared helpers', () => {
  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it('buildObjectId includes a uuid suffix when randomUUID is available', () => {
    vi.spyOn(Date, 'now').mockReturnValue(1_710_000_000_000);
    vi.spyOn(globalThis.crypto, 'randomUUID')
      .mockReturnValueOnce('uuid-a')
      .mockReturnValueOnce('uuid-b');

    const first = buildObjectId('stopwords');
    const second = buildObjectId('stopwords');

    expect(first).not.toBe(second);
    expect(first).toBe('stopwords-1710000000000-uuid-a');
    expect(second).toBe('stopwords-1710000000000-uuid-b');
  });

  it('buildObjectId uses getRandomValues when randomUUID is unavailable', () => {
    vi.spyOn(Date, 'now').mockReturnValue(1_710_000_000_000);
    const getRandomValues = vi.fn((buffer: Uint32Array) => {
      buffer[0] = 123_456_789;
      return buffer;
    });
    vi.stubGlobal('crypto', { getRandomValues } as Crypto);

    const first = buildObjectId('stopwords');
    const second = buildObjectId('stopwords');

    expect(getRandomValues).toHaveBeenCalledTimes(2);
    expect(first).not.toBe(second);
    expect(first).toMatch(/^stopwords-1710000000000-[a-z0-9]+-[a-z0-9]+$/);
    expect(second).toMatch(/^stopwords-1710000000000-[a-z0-9]+-[a-z0-9]+$/);
  });

  it('buildObjectId stays unique when crypto is unavailable', () => {
    vi.spyOn(Date, 'now').mockReturnValue(1_710_000_000_000);
    vi.spyOn(Math, 'random').mockReturnValue(0.123456789);
    vi.stubGlobal('crypto', undefined);

    const first = buildObjectId('stopwords');
    const second = buildObjectId('stopwords');

    expect(first).not.toBe(second);
    expect(first).toMatch(/^stopwords-1710000000000-[a-z0-9]+-[a-z0-9]+$/);
    expect(second).toMatch(/^stopwords-1710000000000-[a-z0-9]+-[a-z0-9]+$/);
  });

  it('buildDialogEntry trims and splits plural values for API submission', () => {
    const entry = buildDialogEntry(
      'plurals',
      createStopwordFormState(),
      { ...createPluralFormState(), words: ' shoe , shoes ', language: 'en' },
      createCompoundFormState(),
    );

    expect(entry).toEqual(
      expect.objectContaining({
        words: ['shoe', 'shoes'],
        language: 'en',
      }),
    );
    expect(entry?.objectID).toMatch(/^plurals-/);
  });

  it('buildDialogEntry preserves compound decomposition order in the rendered description', () => {
    const entry = buildDialogEntry(
      'compounds',
      createStopwordFormState(),
      createPluralFormState(),
      { ...createCompoundFormState(), word: 'notebook', decomposition: 'note, book', language: 'en' },
    );

    expect(entry).toEqual(
      expect.objectContaining({
        word: 'notebook',
        decomposition: ['note', 'book'],
        language: 'en',
      }),
    );
    expect(buildEntryDescription(entry!)).toBe('notebook -> note + book');
  });

  it('buildDialogEntry returns null for blank stopword input', () => {
    const entry = buildDialogEntry(
      'stopwords',
      { ...createStopwordFormState(), word: '   ', language: 'en', state: 'enabled' },
      createPluralFormState(),
      createCompoundFormState(),
    );

    expect(entry).toBeNull();
  });

  it('buildDialogEntry returns null when plural words resolve to an empty list', () => {
    const entry = buildDialogEntry(
      'plurals',
      createStopwordFormState(),
      { ...createPluralFormState(), words: ' , , ', language: 'en' },
      createCompoundFormState(),
    );

    expect(entry).toBeNull();
  });

  it('buildDialogEntry returns null when compound form is incomplete', () => {
    const missingWord = buildDialogEntry(
      'compounds',
      createStopwordFormState(),
      createPluralFormState(),
      { ...createCompoundFormState(), word: '   ', decomposition: 'note,book', language: 'en' },
    );
    const missingDecomposition = buildDialogEntry(
      'compounds',
      createStopwordFormState(),
      createPluralFormState(),
      { ...createCompoundFormState(), word: 'notebook', decomposition: '  ,  ', language: 'en' },
    );

    expect(missingWord).toBeNull();
    expect(missingDecomposition).toBeNull();
  });
});
