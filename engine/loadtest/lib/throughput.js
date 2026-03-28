// Shared throughput helpers for search-throughput.js, write-throughput.js, and mixed-workload.js.
// All query terms and field values are deterministic against the seed data produced by
// seed-loadtest-data.sh (derived from engine/dashboard/tour/product-seed-data.mjs).
import { Rate } from "k6/metrics";

// -- Search query mix modeled on engine/_dev/s/bench_latency.sh patterns --

// Searchable attributes from seed settings: name, description, brand, category, tags.
// Facetable: category, brand, subcategory, color, tags.
// Filterable: price, inStock, releaseYear.

// Text queries — brand/product names guaranteed present in seed data.
const TEXT_QUERIES = [
  "MacBook",
  "Samsung",
  "Dell",
  "ThinkPad",
  "Sony",
  "Logitech",
  "Bose",
  "Razer",
];

// Short queries — single characters that match many documents.
const SHORT_QUERIES = ["s", "a", "d", "l", "m", "b"];

// Long queries — multi-word strings spanning multiple searchable attributes.
const LONG_QUERIES = [
  "MacBook Pro laptop professional space",
  "Samsung Galaxy tablet wireless charging",
  "wireless noise cancelling headphones audio",
  "mechanical keyboard gaming accessories RGB",
  "Dell XPS ultrabook high performance display",
];

// Facet field arrays aligned to seed attributesForFaceting.
const FACET_SETS = [
  ["category", "brand", "subcategory"],
  ["brand", "color", "tags"],
  ["category", "color"],
];

// Numeric/boolean filter expressions using seed filterOnly fields.
const FILTER_EXPRESSIONS = [
  "brand:Apple",
  "brand:Samsung",
  "brand:Dell",
  "category:Laptops",
  "category:Audio",
  "inStock:true",
  "releaseYear:2024",
  "price < 500",
  "price > 1000",
  "brand:HP AND category:Laptops",
];

// Query type definitions matching bench_latency.sh patterns:
//   text, text+facets, filter, short, long, empty+facets
const QUERY_TYPES = [
  "text",
  "text_facets",
  "filter",
  "short",
  "long",
  "empty_facets",
];

const SEARCH_HITS_PER_PAGE = 20;

function pickForIteration(values, iterationIndex) {
  return values[iterationIndex % values.length];
}

function buildSearchPayload(query, extraFields = {}) {
  return {
    query,
    hitsPerPage: SEARCH_HITS_PER_PAGE,
    ...extraFields,
  };
}

function readJsonField(response, fieldPath) {
  try {
    return response.json(fieldPath);
  } catch (_) {
    return undefined;
  }
}

// Deterministic query selection driven by iteration index — no randomness.
export function buildSearchRequest(iterationIndex) {
  const typeIndex = iterationIndex % QUERY_TYPES.length;
  const queryType = QUERY_TYPES[typeIndex];

  switch (queryType) {
    case "text":
      return buildSearchPayload(pickForIteration(TEXT_QUERIES, iterationIndex));
    case "text_facets":
      return buildSearchPayload(pickForIteration(TEXT_QUERIES, iterationIndex), {
        facets: pickForIteration(FACET_SETS, iterationIndex),
      });
    case "filter":
      return buildSearchPayload(pickForIteration(TEXT_QUERIES, iterationIndex), {
        filters: pickForIteration(FILTER_EXPRESSIONS, iterationIndex),
      });
    case "short":
      return buildSearchPayload(pickForIteration(SHORT_QUERIES, iterationIndex));
    case "long":
      return buildSearchPayload(pickForIteration(LONG_QUERIES, iterationIndex));
    case "empty_facets":
      return buildSearchPayload("", {
        facets: pickForIteration(FACET_SETS, iterationIndex),
      });
    default:
      return buildSearchPayload(TEXT_QUERIES[0]);
  }
}

export const SEARCH_RESPONSE_CHECKS = {
  "search returns 200": (response) => response.status === 200,
  "search returns hits array": (response) => Array.isArray(readJsonField(response, "hits")),
};

export const WRITE_RESPONSE_CHECKS = {
  "write returns 200": (response) => response.status === 200,
  "write returns numeric taskID": (response) => Number.isInteger(readJsonField(response, "taskID")),
  "write returns objectIDs array": (response) => {
    const ids = readJsonField(response, "objectIDs");
    return Array.isArray(ids) && ids.length > 0;
  },
};

// -- Write payload builder --

// Brands and categories from seed data for realistic write documents.
const WRITE_BRANDS = ["Apple", "Dell", "Samsung", "Lenovo", "HP", "Sony", "Bose", "Logitech"];
const WRITE_CATEGORIES = ["Laptops", "Tablets", "Audio", "Accessories", "Monitors"];
const WRITE_SUBCATEGORIES = ["Professional", "Business", "Gaming", "Budget", "Headphones"];
const WRITE_COLORS = ["Black", "Silver", "White", "Gray", "Blue"];

// Build a batch payload with unique objectIDs derived from VU + iteration context.
// Each call produces a single-document batch to keep per-request overhead low.
export function buildWriteBatchPayload(vuId, iterationIndex) {
  const objectID = `loadtest-write-vu${vuId}-iter${iterationIndex}`;
  return {
    requests: [
      {
        action: "addObject",
        body: {
          objectID,
          name: `Throughput Product VU${vuId} #${iterationIndex}`,
          description: `Synthetic write document for load testing. VU ${vuId}, iteration ${iterationIndex}.`,
          brand: WRITE_BRANDS[iterationIndex % WRITE_BRANDS.length],
          category: WRITE_CATEGORIES[iterationIndex % WRITE_CATEGORIES.length],
          subcategory: WRITE_SUBCATEGORIES[iterationIndex % WRITE_SUBCATEGORIES.length],
          price: 10 + (iterationIndex % 200) * 5,
          rating: 1 + (iterationIndex % 5),
          reviewCount: iterationIndex % 500,
          inStock: iterationIndex % 3 !== 0,
          tags: [`series-${iterationIndex % 20}`, `vu-${vuId}`],
          color: WRITE_COLORS[iterationIndex % WRITE_COLORS.length],
          releaseYear: 2022 + (iterationIndex % 5),
          _geo: {
            lat: 37.7749 + (iterationIndex % 10) * 0.01,
            lng: -122.4194 + (iterationIndex % 10) * 0.01,
          },
        },
      },
    ],
  };
}

// -- Threshold helpers --
// Shared threshold definitions that scenarios can spread into their options.thresholds.

// Search latency thresholds — p95 and p99 on http_req_duration for search requests.
export const SEARCH_THRESHOLDS = {
  "http_req_duration{type:search}": ["p(95)<500", "p(99)<1000"],
  "http_req_failed{type:search}": ["rate<0.01"],
  "http_reqs{type:search}": ["rate>5"],
  "checks{check:search returns 200,type:search}": ["rate==1"],
  "checks{check:search returns hits array,type:search}": ["rate==1"],
};

export const writeHttp4xxRate = new Rate("write_http_4xx_rate");
export const writeHttpUnexpected4xxRate = new Rate("write_http_unexpected_4xx_rate");
export const writeHttp5xxRate = new Rate("write_http_5xx_rate");

export function recordWriteHttpStatusCode(statusCode) {
  writeHttp4xxRate.add(statusCode >= 400 && statusCode < 500);
  writeHttpUnexpected4xxRate.add(
    statusCode >= 400 && statusCode < 500 && statusCode !== 429
  );
  writeHttp5xxRate.add(statusCode >= 500 && statusCode < 600);
}

// Single-node write baselines intentionally allow substantial 429 backpressure under
// sustained overload. The contract still requires low latency, no unexpected
// client/server failures, and at least a small stream of successful task
// publications so the scenario proves forward progress instead of total
// saturation.
export const WRITE_THRESHOLDS = {
  "http_req_duration{type:write}": ["p(95)<1000", "p(99)<2000"],
  "http_req_failed{type:write}": ["rate<0.99"],
  write_http_4xx_rate: ["rate<0.99"],
  write_http_unexpected_4xx_rate: ["rate<0.005"],
  write_http_5xx_rate: ["rate<0.005"],
  "checks{check:write returns 200,type:write}": ["rate>0.01"],
  "checks{check:write returns numeric taskID,type:write}": ["rate>0.01"],
  "checks{check:write returns objectIDs array,type:write}": ["rate>0.01"],
};

// Seeded field names exported for acceptance check alignment.
export const SEEDED_FIELDS = {
  brand: WRITE_BRANDS,
  category: WRITE_CATEGORIES,
  subcategory: WRITE_SUBCATEGORIES,
  color: WRITE_COLORS,
  tags: true,
  price: true,
  inStock: true,
  releaseYear: true,
};
