import { test, expect } from '@playwright/test';
import type { APIRequestContext } from '@playwright/test';
import { API_BASE } from '../fixtures/local-instance';
import {
  addDocuments,
  createIndex,
  createExperiment,
  deletePersonalizationStrategy,
  deleteExperiment,
  flushAnalytics,
  getExperimentResults,
  getRules,
  getRecommendations,
  getPersonalizationProfile,
  getSettings,
  getExperimentByName,
  listExperiments,
  searchIndex,
  updateExperiment,
  updateSettings,
  waitForExperimentResults,
  waitForPersonalizationProfile,
} from '../fixtures/api-helpers';
import {
  addDocumentsAndWaitForSearchable,
  postChat,
  setChatSearchMode,
  setChatStubProvider,
} from '../fixtures/chat-api-helpers';
import {
  DEFAULT_RECOMMENDATION_MAX_RECOMMENDATIONS,
  DEFAULT_RECOMMENDATION_THRESHOLD,
} from '../../src/lib/recommendation-contract';
import type { ChatResponse } from '../../src/lib/types';

type FakeResponse = {
  ok: () => boolean;
  status: () => number;
  text: () => Promise<string>;
  json: () => Promise<unknown>;
};

function response(status: number, body = '', jsonBody: unknown = {}): FakeResponse {
  return {
    ok: () => status >= 200 && status < 300,
    status: () => status,
    text: async () => body,
    json: async () => jsonBody,
  };
}

test.describe('api-helpers deleteExperiment', () => {
  test('stops a running experiment and retries delete with an encoded experiment id', async () => {
    const calls: string[] = [];
    const experimentId = 'exp/running?draft#frag';
    const encodedExperimentId = encodeURIComponent(experimentId);
    const request = {
      delete: async (url: string) => {
        calls.push(`DELETE ${url}`);
        if (url.endsWith(`/2/abtests/${encodedExperimentId}`) && calls.length === 1) {
          return response(409, 'running');
        }
        return response(204);
      },
      post: async (url: string) => {
        calls.push(`POST ${url}`);
        return response(200);
      },
    } as unknown as APIRequestContext;

    await deleteExperiment(request, experimentId);

    expect(calls).toEqual([
      `DELETE ${API_BASE}/2/abtests/${encodedExperimentId}`,
      `POST ${API_BASE}/2/abtests/${encodedExperimentId}/stop`,
      `DELETE ${API_BASE}/2/abtests/${encodedExperimentId}`,
    ]);
  });

  test('ignores missing experiment', async () => {
    const request = {
      delete: async () => response(404),
      post: async () => response(200),
    } as unknown as APIRequestContext;

    await expect(deleteExperiment(request, 'does-not-exist')).resolves.toBeUndefined();
  });

  test('throws if stop fails before retrying delete', async () => {
    const request = {
      delete: async () => response(409, 'running'),
      post: async () => response(500, 'stop failed'),
    } as unknown as APIRequestContext;

    await expect(deleteExperiment(request, 'exp-running')).rejects.toThrow(
      /stopExperiment before delete failed/i,
    );
  });

  test('throws when multiple experiments share a name', async () => {
    const request = {
      get: async () => ({
        ok: () => true,
        status: () => 200,
        text: async () => '',
        json: async () => ({
          abtests: [
            { id: 'exp-1', name: 'dup-name', status: 'running' },
            { id: 'exp-2', name: 'dup-name', status: 'stopped' },
          ],
        }),
      }),
    } as unknown as APIRequestContext;

    await expect(getExperimentByName(request, 'dup-name')).rejects.toThrow(/multiple experiments found/i);
  });
});

test.describe('api-helpers experiments create/list contract', () => {
  test('createIndex posts uid payload to /1/indexes', async () => {
    let postedData: unknown;
    const request = {
      post: async (url: string, options: { data: unknown }) => {
        expect(url).toBe(`${API_BASE}/1/indexes`);
        postedData = options.data;
        return response(201, '', { uid: 'new-index' });
      },
    } as unknown as APIRequestContext;

    await createIndex(request, 'new-index');

    expect(postedData).toEqual({ uid: 'new-index' });
  });

  test('createIndex throws when backend returns a failure response', async () => {
    const request = {
      post: async () => response(400, 'bad request'),
    } as unknown as APIRequestContext;

    await expect(createIndex(request, 'bad-index')).rejects.toThrow(/createIndex failed/i);
  });

  test('createExperiment translates legacy dashboard payload into Algolia variants contract', async () => {
    let postedData: unknown;
    const request = {
      post: async (url: string, options: { data: unknown }) => {
        expect(url).toBe(`${API_BASE}/2/abtests`);
        postedData = options.data;
        return response(201, '', { abTestID: 123 });
      },
    } as unknown as APIRequestContext;

    const created = await createExperiment(request, {
      name: 'exp-payload-check',
      indexName: 'products',
      trafficSplit: 0.25,
      control: { name: 'control' },
      variant: { name: 'variant', queryOverrides: { filters: 'brand:Apple' } },
      primaryMetric: 'ctr',
      minimumDays: 14,
    });
    expect(created.id).toBe('123');
    expect(created.name).toBe('exp-payload-check');
    expect(created.status).toBe('draft');

    expect(postedData).toEqual(
      expect.objectContaining({
        name: 'exp-payload-check',
        variants: [
          expect.objectContaining({
            index: 'products',
            trafficPercentage: 75,
            description: 'control',
          }),
          expect.objectContaining({
            index: 'products',
            trafficPercentage: 25,
            description: 'variant',
            customSearchParameters: { filters: 'brand:Apple' },
          }),
        ],
        metrics: [{ name: 'clickThroughRate' }],
      }),
    );
    expect((postedData as { endAt: string }).endAt).toMatch(/^\d{4}-\d{2}-\d{2}T/);
  });

  test('listExperiments normalizes null abtests to an empty array', async () => {
    const request = {
      get: async () => response(200, '', { abtests: null, count: 0, total: 0 }),
    } as unknown as APIRequestContext;

    await expect(listExperiments(request)).resolves.toEqual([]);
  });

  test('listExperiments maps Algolia abTestID to id', async () => {
    const request = {
      get: async () => response(200, '', {
        abtests: [{ abTestID: 45, name: 'algolia-style', status: 'active' }],
        count: 1,
        total: 1,
      }),
    } as unknown as APIRequestContext;

    await expect(listExperiments(request)).resolves.toEqual([
      expect.objectContaining({ id: '45', name: 'algolia-style', status: 'active' }),
    ]);
  });

  test('createExperiment throws when response does not include any id-like field', async () => {
    const request = {
      post: async () => response(201, '', { name: 'id-missing' }),
    } as unknown as APIRequestContext;

    await expect(createExperiment(request, {
      name: 'id-missing',
      indexName: 'products',
      trafficSplit: 0.5,
      control: { name: 'control' },
      variant: { name: 'variant' },
      primaryMetric: 'ctr',
      minimumDays: 7,
    })).rejects.toThrow(/response missing id-like field/i);
  });

  test('updateExperiment encodes path and falls back to provided id/name when response omits them', async () => {
    let putUrl = '';
    let putData: unknown;
    const request = {
      put: async (url: string, options: { data: unknown }) => {
        putUrl = url;
        putData = options.data;
        return response(200, '', { status: 'running' });
      },
    } as unknown as APIRequestContext;

    const experimentId = 'exp/running?draft#frag';
    const payload = {
      name: 'updated-name',
      indexName: 'products',
      trafficSplit: 0.4,
      control: { name: 'control' },
      variant: { name: 'variant', queryOverrides: { filters: 'brand:Apple' } },
      primaryMetric: 'ctr',
      minimumDays: 10,
    };

    const updated = await updateExperiment(request, experimentId, payload);

    expect(putUrl).toBe(`${API_BASE}/2/abtests/${encodeURIComponent(experimentId)}`);
    expect(putData).toEqual(payload);
    expect(updated).toEqual(expect.objectContaining({
      id: experimentId,
      name: 'updated-name',
      status: 'running',
    }));
  });

  test('getExperimentResults normalizes mixed result payload shapes consumed by experiments ui', async () => {
    let getUrl = '';
    const request = {
      get: async (url: string) => {
        getUrl = url;
        return response(200, '', {
          status: 'running',
          gate: { minimumNReached: true, minimumDaysReached: 'false' },
          control: { searches: '7' },
          variant: { searches: 11 },
          bayesian: [],
          sampleRatioMismatch: 'yes',
          guardRailAlerts: [{ type: 'srm' }, null, 'warn'],
          interleaving: { deltaAb: 0.42, totalQueries: 8 },
        });
      },
    } as unknown as APIRequestContext;

    const experimentId = 'exp/results?beta';
    const results = await getExperimentResults(request, experimentId);

    expect(getUrl).toBe(`${API_BASE}/2/abtests/${encodeURIComponent(experimentId)}/results`);
    expect(results.status).toBe('running');
    expect(results.gate).toEqual(expect.objectContaining({
      minimumNReached: true,
      minimumDaysReached: false,
    }));
    expect(results.control.searches).toBe(0);
    expect(results.variant.searches).toBe(11);
    expect(results.bayesian).toBeNull();
    expect(results.sampleRatioMismatch).toBe(false);
    expect(results.guardRailAlerts).toEqual([{ type: 'srm' }]);
    expect(results.interleaving).toEqual(expect.objectContaining({
      deltaAB: 0.42,
      totalQueries: 8,
    }));
  });

  test('waitForExperimentResults polls until predicate matches and returns the matching payload', async () => {
    const statuses = ['draft', 'running'];
    let getCalls = 0;
    const request = {
      get: async () => {
        const status = statuses[Math.min(getCalls, statuses.length - 1)];
        getCalls += 1;
        return response(200, '', {
          status,
          gate: { minimumNReached: true, minimumDaysReached: false },
          control: { searches: 3 },
          variant: { searches: 4 },
          bayesian: { probabilityVariantBest: 0.9 },
          sampleRatioMismatch: false,
          guardRailAlerts: [],
          interleaving: null,
        });
      },
    } as unknown as APIRequestContext;

    const resolved = await waitForExperimentResults(
      request,
      'exp-poll',
      (results) => results.status === 'running',
      100,
      0,
    );

    expect(getCalls).toBe(2);
    expect(resolved.status).toBe('running');
  });

  test('waitForExperimentResults timeout includes last observed status for triage', async () => {
    const request = {
      get: async () => response(200, '', {
        status: 'running',
        gate: { minimumNReached: false, minimumDaysReached: false },
        control: { searches: 1 },
        variant: { searches: 0 },
        bayesian: null,
        sampleRatioMismatch: false,
        guardRailAlerts: [],
        interleaving: null,
      }),
    } as unknown as APIRequestContext;

    await expect(
      waitForExperimentResults(
        request,
        'exp-timeout',
        () => false,
        5,
        0,
      ),
    ).rejects.toThrow(/last status: running/i);
  });
});

test.describe('api-helpers rules listing', () => {
  test('getRules paginates /rules/search so cleanup sees every rule', async () => {
    const calls: Array<{ url: string; data: unknown }> = [];
    const firstPageRule = { objectID: 'rule-page-0' };
    const secondPageRule = { objectID: 'rule-page-1' };
    const request = {
      post: async (url: string, options: { data: unknown }) => {
        calls.push({ url, data: options.data });
        const page = (options.data as { page: number }).page;

        return response(200, '', page === 0
          ? { hits: [firstPageRule], nbPages: 2 }
          : { hits: [secondPageRule], nbPages: 2 });
      },
      get: async () => {
        throw new Error('unexpected GET /rules fallback');
      },
    } as unknown as APIRequestContext;

    await expect(getRules(request, 'products')).resolves.toEqual({
      ok: true,
      items: [firstPageRule, secondPageRule],
    });

    expect(calls).toEqual([
      {
        url: `${API_BASE}/1/indexes/products/rules/search`,
        data: { query: '', page: 0, hitsPerPage: 1000 },
      },
      {
        url: `${API_BASE}/1/indexes/products/rules/search`,
        data: { query: '', page: 1, hitsPerPage: 1000 },
      },
    ]);
  });
});

test.describe('api-helpers settings/search error boundaries', () => {
  test('searchIndex encodes index names before building request paths', async () => {
    let calledUrl = '';
    const request = {
      post: async (url: string) => {
        calledUrl = url;
        return response(200, '', { hits: [] });
      },
    } as unknown as APIRequestContext;

    const indexName = 'products/2026?beta';
    await searchIndex(request, indexName, 'phone');

    expect(calledUrl).toBe(`${API_BASE}/1/indexes/${encodeURIComponent(indexName)}/query`);
  });

  test('addDocuments throws when batch request fails', async () => {
    const request = {
      post: async () => response(503, 'backend unavailable'),
    } as unknown as APIRequestContext;

    await expect(
      addDocuments(request, 'products', [{ objectID: 'doc-1' }]),
    ).rejects.toThrow(/addDocuments failed/i);
  });

  test('searchIndex throws when query request fails', async () => {
    const request = {
      post: async () => response(500, 'query failed'),
    } as unknown as APIRequestContext;

    await expect(searchIndex(request, 'products', 'phone')).rejects.toThrow(/searchIndex failed/i);
  });

  test('getSettings throws when settings request fails', async () => {
    const request = {
      get: async () => response(404, 'index not found'),
    } as unknown as APIRequestContext;

    await expect(getSettings(request, 'missing-index')).rejects.toThrow(/getSettings failed/i);
  });

  test('updateSettings throws when settings update fails', async () => {
    const request = {
      put: async () => response(400, 'bad payload'),
    } as unknown as APIRequestContext;

    await expect(
      updateSettings(request, 'products', { searchableAttributes: ['name'] }),
    ).rejects.toThrow(/updateSettings failed/i);
  });
});

test.describe('api-helpers chat helpers', () => {
  test('setChatSearchMode updates encoded settings path and waits for mode readback from the same path', async () => {
    let putUrl = '';
    let putData: unknown;
    const getUrls: string[] = [];
    let getCalls = 0;
    const request = {
      put: async (url: string, options: { data: unknown }) => {
        putUrl = url;
        putData = options.data;
        return response(200);
      },
      get: async (url: string) => {
        getUrls.push(url);
        getCalls += 1;
        return response(200, '', { mode: getCalls === 1 ? 'neuralSearch' : 'keywordSearch' });
      },
    } as unknown as APIRequestContext;

    const indexName = 'products/2026?beta';
    await setChatSearchMode(request, indexName, 'keywordSearch');

    const expectedSettingsPath = `${API_BASE}/1/indexes/${encodeURIComponent(indexName)}/settings`;
    expect(putUrl).toBe(expectedSettingsPath);
    expect(putData).toEqual({ mode: 'keywordSearch' });
    expect(getCalls).toBe(2);
    expect(getUrls).toEqual([expectedSettingsPath, expectedSettingsPath]);
  });

  test('setChatStubProvider enables NeuralSearch mode with stub AI provider settings', async () => {
    let putUrl = '';
    let postedData: unknown;
    const getUrls: string[] = [];
    let getCalls = 0;
    const request = {
      put: async (url: string, options: { data: unknown }) => {
        putUrl = url;
        postedData = options.data;
        return response(200);
      },
      get: async (url: string) => {
        getUrls.push(url);
        getCalls += 1;
        return response(200, '', getCalls === 1
          ? { mode: 'keywordSearch' }
          : {
            mode: 'neuralSearch',
            userData: {
              aiProvider: { baseUrl: 'stub', apiKey: 'stub-key' },
            },
          });
      },
    } as unknown as APIRequestContext;

    const indexName = 'products/2026?beta';
    await setChatStubProvider(request, indexName);

    const expectedSettingsPath = `${API_BASE}/1/indexes/${encodeURIComponent(indexName)}/settings`;
    expect(putUrl).toBe(expectedSettingsPath);
    expect(postedData).toEqual({
      mode: 'neuralSearch',
      userData: {
        aiProvider: { baseUrl: 'stub', apiKey: 'stub-key' },
      },
    });
    expect(getCalls).toBe(2);
    expect(getUrls).toEqual([expectedSettingsPath, expectedSettingsPath]);
  });

  test('setChatStubProvider waits until settings readback includes the stub aiProvider payload', async () => {
    let getCalls = 0;
    const request = {
      put: async () => response(200),
      get: async () => {
        getCalls += 1;
        if (getCalls === 1) {
          return response(200, '', { mode: 'neuralSearch', userData: {} });
        }
        return response(200, '', {
          mode: 'neuralSearch',
          userData: {
            aiProvider: { baseUrl: 'stub', apiKey: 'stub-key' },
          },
        });
      },
    } as unknown as APIRequestContext;

    await setChatStubProvider(request, 'products');

    expect(getCalls).toBe(2);
  });

  test('setChatStubProvider accepts redacted aiProvider.apiKey values during persisted readback polling', async () => {
    let getCalls = 0;
    const request = {
      put: async () => response(200),
      get: async () => {
        getCalls += 1;
        return response(200, '', {
          mode: 'neuralSearch',
          userData: {
            aiProvider: { baseUrl: 'stub', apiKey: '<redacted>' },
          },
        });
      },
    } as unknown as APIRequestContext;

    await setChatStubProvider(request, 'products');

    expect(getCalls).toBe(1);
  });

  test('addDocumentsAndWaitForSearchable polls search until added documents are queryable', async () => {
    const calls: string[] = [];
    let searchAttempts = 0;
    const readiness = {
      query: 'chatfixturechatrag',
      expectedMinimumHits: 2,
    };
    const documents = [
      { objectID: 'chat-doc-1', content: `Wireless headphones with noise cancellation ${readiness.query}` },
      { objectID: 'chat-doc-2', content: `Bluetooth speaker with 20-hour battery life ${readiness.query}` },
    ];
    const request = {
      post: async (url: string, options: { data: unknown }) => {
        calls.push(url);
        if (url.endsWith('/batch')) {
          expect(options.data).toEqual({
            requests: documents.map((document) => ({ action: 'addObject', body: document })),
          });
          return response(200);
        }

        if (url.endsWith('/query')) {
          searchAttempts += 1;
          expect(options.data).toEqual({ query: readiness.query });
          return response(200, '', { nbHits: searchAttempts === 1 ? 1 : 2, hits: documents });
        }

        throw new Error(`Unexpected URL: ${url}`);
      },
    } as unknown as APIRequestContext;

    await addDocumentsAndWaitForSearchable(request, 'products', documents, readiness);

    expect(searchAttempts).toBe(2);
    expect(calls).toEqual([
      `${API_BASE}/1/indexes/products/batch`,
      `${API_BASE}/1/indexes/products/query`,
      `${API_BASE}/1/indexes/products/query`,
    ]);
  });

  test('postChat posts the encoded chat path, preserves camelCase fields, and includes conversationId when provided', async () => {
    let calledUrl = '';
    let postedData: unknown;
    const chatResponse: ChatResponse = {
      answer: 'Based on your search for "headphones": content here',
      sources: [{ objectID: 'doc-1', content: 'Wireless headphones' }],
      conversationId: 'conv_abc123',
      queryID: 'q_xyz789',
    };
    const request = {
      post: async (url: string, options: { data: unknown }) => {
        calledUrl = url;
        postedData = options.data;
        return response(200, '', chatResponse);
      },
    } as unknown as APIRequestContext;

    await expect(
      postChat(request, 'products/2026?beta', 'headphones', 'conv_abc123'),
    ).resolves.toEqual(chatResponse);

    expect(calledUrl).toBe(`${API_BASE}/1/indexes/${encodeURIComponent('products/2026?beta')}/chat`);
    expect(postedData).toEqual({ query: 'headphones', conversationId: 'conv_abc123' });
  });

  test('postChat throws when the chat request fails', async () => {
    const request = {
      post: async () => response(500, 'provider unavailable'),
    } as unknown as APIRequestContext;

    await expect(postChat(request, 'products', 'headphones')).rejects.toThrow(
      'postChat failed (500): provider unavailable',
    );
  });

  test('postChat omits conversationId when one is not provided', async () => {
    let postedData: unknown;
    const request = {
      post: async (_url: string, options: { data: unknown }) => {
        postedData = options.data;
        return response(200, '', { answer: 'ok', sources: [] });
      },
    } as unknown as APIRequestContext;

    await postChat(request, 'products', 'headphones');

    expect(postedData).toEqual({ query: 'headphones' });
  });
});

test.describe('api-helpers analytics flush', () => {
  test('posts to /2/analytics/flush with optional index parameter', async () => {
    let calledUrl = '';
    let queryParams: unknown;
    const request = {
      post: async (url: string, options: { params?: unknown }) => {
        calledUrl = url;
        queryParams = options.params;
        return response(200, '', { status: 'ok' });
      },
    } as unknown as APIRequestContext;

    await flushAnalytics(request, 'products');

    expect(calledUrl).toBe(`${API_BASE}/2/analytics/flush`);
    expect(queryParams).toEqual({ index: 'products' });
  });

  test('throws when analytics flush request fails', async () => {
    const request = {
      post: async () => response(500, 'flush failed'),
    } as unknown as APIRequestContext;

    await expect(flushAnalytics(request)).rejects.toThrow(/flushAnalytics failed/i);
  });
});

test.describe('api-helpers recommendations preview', () => {
  test('posts batched recommendations to wildcard endpoint and preserves raw indexName in request body', async () => {
    let calledUrl = '';
    let postedData: unknown;
    const request = {
      post: async (url: string, options: { data: unknown }) => {
        calledUrl = url;
        postedData = options.data;
        return response(200, '', { results: [] });
      },
    } as unknown as APIRequestContext;

    await getRecommendations(request, {
      requests: [
        {
          indexName: 'products/2026?beta',
          model: 'related-products',
          objectID: 'sku-1',
        },
      ],
    });

    expect(calledUrl).toBe(`${API_BASE}/1/indexes/*/recommendations`);
    expect(postedData).toEqual({
      requests: [
        {
          indexName: 'products/2026?beta',
          model: 'related-products',
          objectID: 'sku-1',
          threshold: DEFAULT_RECOMMENDATION_THRESHOLD,
          maxRecommendations: DEFAULT_RECOMMENDATION_MAX_RECOMMENDATIONS,
        },
      ],
    });
  });

  test('supports optional objectID, facetName, and facetValue per request with defaulted and explicit values', async () => {
    let postedData: unknown;
    const request = {
      post: async (_url: string, options: { data: unknown }) => {
        postedData = options.data;
        return response(200, '', { results: [] });
      },
    } as unknown as APIRequestContext;

    await getRecommendations(request, {
      requests: [
        {
          indexName: 'products',
          model: 'trending-items',
        },
        {
          indexName: 'products',
          model: 'trending-facets',
          facetName: 'brand',
          facetValue: 'Apple',
          threshold: 45,
          maxRecommendations: 5,
        },
      ],
    });

    expect(postedData).toEqual({
      requests: [
        {
          indexName: 'products',
          model: 'trending-items',
          threshold: DEFAULT_RECOMMENDATION_THRESHOLD,
          maxRecommendations: DEFAULT_RECOMMENDATION_MAX_RECOMMENDATIONS,
        },
        {
          indexName: 'products',
          model: 'trending-facets',
          facetName: 'brand',
          facetValue: 'Apple',
          threshold: 45,
          maxRecommendations: 5,
        },
      ],
    });
  });

  test('throws when recommendations request fails', async () => {
    const request = {
      post: async () => response(503, 'backend unavailable'),
    } as unknown as APIRequestContext;

    await expect(
      getRecommendations(request, {
        requests: [{ indexName: 'products', model: 'trending-items' }],
      }),
    ).rejects.toThrow(/getRecommendations failed/i);
  });
});

test.describe('api-helpers personalization strategy', () => {
  test('deletePersonalizationStrategy deletes the shared strategy endpoint', async () => {
    let deletedUrl = '';
    const request = {
      delete: async (url: string) => {
        deletedUrl = url;
        return response(200, '', { deletedAt: '2026-03-16T00:00:00Z' });
      },
    } as unknown as APIRequestContext;

    await expect(deletePersonalizationStrategy(request)).resolves.toBeUndefined();
    expect(deletedUrl).toBe(`${API_BASE}/1/strategies/personalization`);
  });
});

test.describe('api-helpers personalization profile polling', () => {
  test('getPersonalizationProfile returns null for 404 responses', async () => {
    const request = {
      get: async (url: string) => {
        expect(url).toBe(
          `${API_BASE}/1/profiles/personalization/${encodeURIComponent('missing/user')}`,
        );
        return response(404, 'not found');
      },
    } as unknown as APIRequestContext;

    await expect(getPersonalizationProfile(request, 'missing/user')).resolves.toBeNull();
  });

  test('waitForPersonalizationProfile retries 404 responses until a profile is available', async () => {
    const returnedProfile = {
      userToken: 'known-user',
      lastEventAt: '2026-03-16T00:00:00Z',
      scores: {
        brand: { Apple: 20 },
      },
    };

    let attempts = 0;
    const request = {
      get: async () => {
        attempts += 1;
        if (attempts < 3) {
          return response(404, 'not found');
        }
        return response(200, '', returnedProfile);
      },
      post: async () => response(200, '', { status: 'ok' }),
    } as unknown as APIRequestContext;

    await expect(
      waitForPersonalizationProfile(request, 'known-user', 100, 0),
    ).resolves.toEqual(returnedProfile);
    expect(attempts).toBe(3);
  });

  test('waitForPersonalizationProfile throws after the timeout when no profile becomes available', async () => {
    const request = {
      get: async () => response(404, 'not found'),
      post: async () => response(200, '', { status: 'ok' }),
    } as unknown as APIRequestContext;

    await expect(
      waitForPersonalizationProfile(request, 'never-ready', 0, 0),
    ).rejects.toThrow(/timed out/i);
  });

  test('waitForPersonalizationProfile uses the shared 90s default timeout when none is passed', async () => {
    const originalDateNow = Date.now;
    const originalSetTimeout = globalThis.setTimeout;
    let now = 0;

    Date.now = () => now;
    globalThis.setTimeout = ((...args: Parameters<typeof setTimeout>) => {
      const [handler] = args;
      now = 90_001;
      if (typeof handler === 'function') {
        handler();
      }
      return 0 as ReturnType<typeof setTimeout>;
    }) as typeof setTimeout;

    const request = {
      get: async () => response(404, 'not found'),
      post: async () => response(200, '', { status: 'ok' }),
    } as unknown as APIRequestContext;

    try {
      await expect(waitForPersonalizationProfile(request, 'never-ready')).rejects.toThrow(
        /90000ms/i,
      );
    } finally {
      Date.now = originalDateNow;
      globalThis.setTimeout = originalSetTimeout;
    }
  });
});
