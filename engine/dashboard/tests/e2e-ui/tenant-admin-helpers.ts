/**
 * Shared tenant-admin helpers for dictionary and security-sources e2e specs.
 *
 * Single owner of:
 * - resetAllDictionaries: clears all three dictionary types
 * - SECURITY_SOURCES_BASELINE / resetSecuritySources: manages the loopback baseline
 *
 * Built on top of api-helpers.ts primitives so specs don't duplicate
 * setup/teardown logic or carry separate inline sources of truth.
 */
import type { APIRequestContext } from '@playwright/test';
import {
  clearDictionary,
  replaceSecuritySources,
  type SecuritySourceEntryPayload,
} from '../fixtures/api-helpers';
import type { DictionaryName } from '../../src/lib/types';
import { DICTIONARY_NAMES } from '../../src/pages/dictionaries/shared';

// ---- Dictionaries ----

/** Clear all three dictionary types (stopwords, plurals, compounds). */
export async function resetAllDictionaries(request: APIRequestContext): Promise<void> {
  await Promise.all(
    DICTIONARY_NAMES.map((dictName: DictionaryName) => clearDictionary(request, dictName)),
  );
}

// ---- Security Sources ----

/** The canonical loopback entry used as baseline in security-sources tests. */
export const SECURITY_SOURCES_BASELINE: SecuritySourceEntryPayload[] = [
  { source: '127.0.0.1/32', description: 'local test client' },
];

/** Reset security sources to the loopback-only baseline. */
export async function resetSecuritySources(request: APIRequestContext): Promise<void> {
  await replaceSecuritySources(request, SECURITY_SOURCES_BASELINE);
}

/** Clear all security sources (empty allowlist). */
export async function clearSecuritySources(request: APIRequestContext): Promise<void> {
  await replaceSecuritySources(request, []);
}
