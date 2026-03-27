/**
 * Shared test data for dashboard E2E tests.
 * Canonical source of truth for seeded index names, test corpus,
 * and derived counts used across setup, teardown, and spec files.
 */

// Canonical seeded-index name — every setup, teardown, and helper file
// should import this instead of defining its own literal.
export const TEST_INDEX = 'e2e-products';

export const PRODUCTS = [
  { objectID: 'p01', name: 'MacBook Pro 16"', description: 'Apple M3 Max chip laptop', image_url: 'https://cdn.example.test/products/p01.jpg', brand: 'Apple', category: 'Laptops', price: 3499, rating: 4.8, inStock: true, tags: ['laptop', 'professional'] },
  { objectID: 'p02', name: 'ThinkPad X1 Carbon', description: 'Lightweight business laptop', image_url: 'https://cdn.example.test/products/p02.jpg', brand: 'Lenovo', category: 'Laptops', price: 1849, rating: 4.6, inStock: true, tags: ['laptop', 'business'] },
  { objectID: 'p03', name: 'Dell XPS 15', description: 'Creative laptop with OLED display', image_url: 'https://cdn.example.test/products/p03.jpg', brand: 'Dell', category: 'Laptops', price: 2499, rating: 4.5, inStock: true, tags: ['laptop', 'creative'] },
  { objectID: 'p04', name: 'iPad Pro 12.9"', description: 'M2 chip tablet by Apple', image_url: 'https://cdn.example.test/products/p04.jpg', brand: 'Apple', category: 'Tablets', price: 1099, rating: 4.7, inStock: true, tags: ['tablet', 'professional'] },
  { objectID: 'p05', name: 'Galaxy Tab S9', description: 'Samsung premium Android tablet', image_url: 'https://cdn.example.test/products/p05.jpg', brand: 'Samsung', category: 'Tablets', price: 1199, rating: 4.4, inStock: false, tags: ['tablet', 'android'] },
  { objectID: 'p06', name: 'Sony WH-1000XM5', description: 'Wireless noise canceling headphones', image_url: 'https://cdn.example.test/products/p06.jpg', brand: 'Sony', category: 'Audio', price: 349, rating: 4.7, inStock: true, tags: ['headphones', 'wireless'] },
  { objectID: 'p07', name: 'AirPods Pro 2', description: 'Apple wireless earbuds with ANC', image_url: 'https://cdn.example.test/products/p07.jpg', brand: 'Apple', category: 'Audio', price: 249, rating: 4.6, inStock: true, tags: ['earbuds', 'wireless'] },
  { objectID: 'p08', name: 'Samsung 990 Pro 2TB', description: 'NVMe M.2 SSD storage', image_url: 'https://cdn.example.test/products/p08.jpg', brand: 'Samsung', category: 'Storage', price: 179, rating: 4.8, inStock: true, tags: ['ssd', 'storage'] },
  { objectID: 'p09', name: 'LG UltraGear 27" 4K', description: '144Hz gaming monitor', image_url: 'https://cdn.example.test/products/p09.jpg', brand: 'LG', category: 'Monitors', price: 699, rating: 4.5, inStock: true, tags: ['monitor', 'gaming'] },
  { objectID: 'p10', name: 'Logitech MX Master 3S', description: 'Wireless ergonomic mouse', image_url: 'https://cdn.example.test/products/p10.jpg', brand: 'Logitech', category: 'Accessories', price: 99, rating: 4.7, inStock: true, tags: ['mouse', 'wireless'] },
  { objectID: 'p11', name: 'Keychron Q1 Pro', description: 'Wireless mechanical keyboard', image_url: 'https://cdn.example.test/products/p11.jpg', brand: 'Keychron', category: 'Accessories', price: 199, rating: 4.6, inStock: true, tags: ['keyboard', 'wireless'] },
  { objectID: 'p12', name: 'CalDigit TS4', description: 'Thunderbolt 4 dock with 18 ports', image_url: 'https://cdn.example.test/products/p12.jpg', brand: 'CalDigit', category: 'Accessories', price: 399, rating: 4.8, inStock: false, tags: ['dock', 'thunderbolt'] },
];

const firstProduct = PRODUCTS[0];

export const displayPreferencesFixtures = {
  indexName: TEST_INDEX,
  preferences: {
    withTitle: {
      titleAttribute: 'name',
      subtitleAttribute: null,
      imageAttribute: null,
      tagAttributes: [],
    },
    full: {
      titleAttribute: 'name',
      subtitleAttribute: 'description',
      imageAttribute: 'image_url',
      tagAttributes: ['category', 'brand'],
    },
    titleAndTags: {
      titleAttribute: 'name',
      subtitleAttribute: null,
      imageAttribute: null,
      tagAttributes: ['tags'],
    },
  },
  autoDetect: {
    titleCandidates: ['name'],
    imageCandidates: ['image_url'],
    tagCandidates: ['tags'],
  },
  expectedFirstProduct: {
    objectID: firstProduct.objectID,
    name: firstProduct.name,
    description: firstProduct.description,
    image_url: firstProduct.image_url,
    brand: firstProduct.brand,
    category: firstProduct.category,
  },
} as const;

export const SYNONYMS = [
  { objectID: 'syn-laptop-notebook', type: 'synonym' as const, synonyms: ['laptop', 'notebook', 'computer'] },
  { objectID: 'syn-phone-mobile', type: 'synonym' as const, synonyms: ['headphones', 'earphones', 'earbuds'] },
  { objectID: 'syn-screen-display', type: 'synonym' as const, synonyms: ['monitor', 'screen', 'display'] },
];

export const RULES = [
  {
    objectID: 'rule-pin-macbook',
    conditions: [{ pattern: 'laptop', anchoring: 'contains' }],
    consequence: { promote: [{ objectID: 'p01', position: 0 }] },
    description: 'Pin MacBook Pro to top when searching laptop',
  },
  {
    objectID: 'rule-hide-galaxy-tab',
    conditions: [{ pattern: 'tablet', anchoring: 'contains' }],
    consequence: { hide: [{ objectID: 'p05' }] },
    description: 'Hide Galaxy Tab S9 when searching tablet',
  },
];

export const SETTINGS = {
  searchableAttributes: ['name', 'description', 'brand', 'category', 'tags'],
  attributesForFaceting: ['category', 'brand', 'filterOnly(price)', 'filterOnly(inStock)'],
  customRanking: ['desc(rating)', 'asc(price)'],
};

/** Expected counts for verifying the migration success card */
export const EXPECTED_COUNTS = {
  documents: PRODUCTS.length,
  synonyms: SYNONYMS.length,
  rules: RULES.length,
};
