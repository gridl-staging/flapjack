# Browser Test Coverage Checklist

**Last updated:** 2026-03-25
**Coverage footprint:** 41 browser spec files in `tests/e2e-ui/` (4 smoke + 37 full). API-contract coverage lives separately in 4 `tests/e2e-api/` specs.
**Test type:** Browser tests (Playwright + Chromium, headless); sections marked `(Mocked)` use routed responses, all others use a real server
**Runner:** `npm test` or `npx playwright test --project=e2e-ui`

---

## Test Categories — IMPORTANT DISTINCTION

| Category | Directory | What it tests | Browser? |
|----------|-----------|---------------|----------|
| **Browser tests** | `tests/e2e-ui/` | Real browser coverage for dashboard flows; most specs hit a real server, explicitly marked `(Mocked)` specs route responses for hard-to-reproduce states | YES (Chromium) |
| **API-level tests** | `tests/e2e-api/` | REST API calls against real server. **No browser. No `page.goto()`.** | NO (HTTP only) |

This checklist covers browser spec files in `tests/e2e-ui/` only. Most sections are browser-unmocked real-server coverage; sections labeled `(Mocked)` are browser-mocked exceptions for targeted error-state coverage.

---

## Per-Page Coverage

### Smoke Tests — [critical-paths.spec.ts](e2e-ui/smoke/critical-paths.spec.ts) (7 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Overview loads with real data | Done |
| 2 | Search returns real results | Done |
| 3 | Sidebar navigation works | Done |
| 4 | Settings page loads with searchable attributes | Done |
| 5 | API Keys page loads | Done |
| 6 | System health displays | Done |
| 7 | Create and delete index | Done |

### Smoke Tests — [index-tab-bar.spec.ts](e2e-ui/smoke/index-tab-bar.spec.ts) (1 test)

| # | Test | Status |
|---|------|--------|
| 1 | Enters an index from Overview and traverses all index tabs with page-body assertions | Done |

### Smoke Tests — [settings-tabs.spec.ts](e2e-ui/smoke/settings-tabs.spec.ts) (1 test)

| # | Test | Status |
|---|------|--------|
| 1 | Loads tabbed settings and navigates all six tabs | Done |

### Smoke Tests — [sidebar-sections.spec.ts](e2e-ui/smoke/sidebar-sections.spec.ts) (1 test)

| # | Test | Status |
|---|------|--------|
| 1 | Renders grouped sections and navigates through representative links | Done |

### Overview Page — [overview.spec.ts](e2e-ui/full/overview.spec.ts) (16 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Index list shows e2e-products with document count (12) | Done |
| 2 | Stat cards: total indexes, documents, storage | Done |
| 3 | Health indicator shows Healthy | Done |
| 4 | Server health badge shows connected status | Done |
| 5 | Create new index, verify appears, then delete | Done |
| 6 | Create Index dialog shows template options | Done |
| 7 | Selecting Movies template auto-fills index name | Done |
| 8 | Export All and Upload buttons visible | Done |
| 9 | Per-index export and import buttons visible | Done |
| 10 | Index row shows storage size and update info | Done |
| 11 | Analytics summary section displays data | Done |
| 12 | Analytics chart renders in overview analytics section | Done |
| 13 | View Details link navigates to analytics page | Done |
| 14 | Settings link navigates to settings page | Done |
| 15 | Clicking index navigates to search page | Done |
| 16 | Export All button triggers download | Done |

### Search & Browse — [search.spec.ts](e2e-ui/full/search.spec.ts) (20 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Searching for "laptop" returns laptop products | Done |
| 2 | Filtering by Audio category shows only audio products | Done |
| 3 | Filtering by Apple brand shows only Apple products | Done |
| 4 | Clearing facet filters restores all results | Done |
| 5 | Searching for nonsense query shows no results | Done |
| 6 | Searching for "notebook" returns laptop results via synonyms | Done |
| 7 | Result count is displayed in the results header | Done |
| 8 | Pagination controls appear when results exceed one page | Done |
| 9 | Combining category and brand facets narrows results | Done |
| 10 | Analytics tracking toggle is visible and can be switched | Done |
| 11 | Add Documents button opens dialog with tab options | Done |
| 12 | Index stats shown in breadcrumb area | Done |
| 13 | Pressing Enter in search box triggers search | Done |
| 14 | Typo tolerance returns results for misspelled queries | Done |
| 15 | Different searches return distinct result sets | Done |
| 16 | Synonym "screen" returns monitor results | Done |
| 17 | Synonym "earbuds" returns headphone results | Done |
| 18 | Facets panel shows category values | Done |
| 19 | Facets panel shows brand facet values | Done |
| 20 | Facet values show document counts | Done |

### Analytics — [analytics.spec.ts](e2e-ui/full/analytics.spec.ts) (34 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Overview tab loads with KPI cards showing data | Done |
| 2 | Search volume chart renders SVG on Overview tab (not empty state) | Done |
| 3 | Top searches table shows data on Overview tab | Done |
| 4 | No-Result Rate Over Time chart renders SVG (not empty state) | Done |
| 5 | No Results tab loads with rate banner and table | Done |
| 6 | Devices tab shows platform breakdown | Done |
| 7 | Geography tab shows country list | Done |
| 8 | Geography drill-down: click country row, see details, click back | Done |
| 9 | date range toggle switches between 7d, 30d, 90d and refreshes data | Done |
| 10 | Searches tab shows top searches table with data | Done |
| 11 | Searches tab filter input narrows results | Done |
| 12 | Filters tab shows filter section (data or empty state) | Done |
| 13 | Filters tab: clicking a filter row expands to show filter values | Done |
| 14 | Searches tab shows country filter dropdown with seeded geo data | Done |
| 15 | Searches tab shows device filter dropdown with seeded device data | Done |
| 16 | Searches tab column headers are clickable for sorting | Done |
| 17 | Flush button triggers analytics refresh | Done |
| 18 | Analytics page shows BETA badge | Done |
| 19 | Clear Analytics button opens confirmation dialog | Done |
| 20 | Filters tab loads and shows filter data table | Done |
| 21 | breadcrumb shows Overview > index > Analytics and links work | Done |
| 22 | breadcrumb index link navigates to search page | Done |
| 23 | date range label shows formatted date range | Done |
| 24 | KPI cards show formatted numeric values, not just visibility | Done |
| 25 | KPI cards with time-series data render sparkline SVGs | Done |
| 26 | Top searches table shows ranked queries with counts | Done |
| 27 | Clear Analytics dialog shows correct warning text and index name | Done |
| 28 | Flush button can be clicked and returns to ready state | Done |
| 29 | Conversions tab loads with KPI cards | Done |
| 30 | Conversions tab shows conversion rate chart | Done |
| 31 | Conversions tab shows add-to-cart and purchase charts with empty revenue breakdown state | Done |
| 32 | Conversions tab shows country filter | Done |
| 33 | Conversions KPI cards show titles | Done |
| 34 | Clear Analytics confirm path clears seeded analytics and shows reset state for isolated index | Done |

### Analytics Deep Data — [analytics-deep.spec.ts](e2e-ui/full/analytics-deep.spec.ts) (24 tests)

| # | Test | Status |
|---|------|--------|
| 1 | KPI cards show non-zero numeric values from seeded data | Done |
| 2 | Search volume chart renders SVG with data path | Done |
| 3 | Top 10 searches table shows ranked queries descending | Done |
| 4 | KPI cards show delta comparison badges | Done |
| 5 | Searches tab displays sortable table in descending order | Done |
| 6 | Searches tab text filter narrows results client-side | Done |
| 7 | No Results tab shows rate banner (0-100%) | Done |
| 8 | No Results tab shows zero-result queries table | Done |
| 9 | Devices tab shows platform cards (desktop > mobile) | Done |
| 10 | Devices tab shows device chart with SVG rendering | Done |
| 11 | Geography tab shows country table with US as top | Done |
| 12 | Geography country percentages sum to ~100% | Done |
| 13 | Geography click country shows drill-down | Done |
| 14 | Geography back button returns to country list | Done |
| 15 | Switching to 30d updates KPI values | Done |
| 16 | Total Searches KPI sparkline renders SVG path | Done |
| 17 | No-Result Rate KPI sparkline renders SVG path | Done |
| 18 | Search query cells contain non-empty text strings | Done |
| 19 | Search count cells contain comma-formatted numbers | Done |
| 20 | Volume bars have non-zero width for rows with counts | Done |
| 21 | Country rows: flag, name, code, count, share % | Done |
| 22 | Drill-down shows country-specific search queries | Done |
| 23 | US drill-down: States table shows state names | Done |
| 24 | Device counts add up across platform cards | Done |

### Analytics Conversions (Mocked) — [analytics-conversions-mocked.spec.ts](e2e-ui/full/analytics-conversions-mocked.spec.ts) (3 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Conversions tab handles missing conversion data gracefully | Done |
| 2 | Conversions tab handles 404 from conversion endpoints | Done |
| 3 | Conversions tab handles slow responses without crashing | Done |

### Rules — [rules.spec.ts](e2e-ui/full/rules.spec.ts) (17 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Rules page loads with seeded rules | Done |
| 2 | Rule cards show pattern and actions | Done |
| 3 | Rule badges (pin/hide) display | Done |
| 4 | Rules count badge shows correct number | Done |
| 5 | Search input filters rules | Done |
| 6 | Add Rule button opens dialog | Done |
| 7 | Merchandising Studio link navigates | Done |
| 8 | Rule card structure (ID, pattern, description) | Done |
| 9 | Delete rule via API + UI verification | Done |
| 10 | Create rule via Add Rule dialog (JSON editor) | Done |
| 11 | Delete rule via UI confirm dialog | Done |
| 12 | Clear All rules button + cancel | Done |
| 13 | Add Rule dialog opens in form mode by default | Done |
| 14 | Create rule via form mode with condition and promote | Done |
| 15 | Edit existing rule loads in form mode with fields populated | Done |
| 16 | Form mode JSON preview updates as fields change | Done |
| 17 | Form/JSON tab toggle works | Done |

### Rules Form (Mocked) — [rules-form-mocked.spec.ts](e2e-ui/full/rules-form-mocked.spec.ts) (2 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Loads complex rule into form mode with all fields populated | Done |
| 2 | Rule with empty conditions loads as conditionless | Done |

### Synonyms — [synonyms.spec.ts](e2e-ui/full/synonyms.spec.ts) (10 tests)

| # | Test | Status |
|---|------|--------|
| 1 | List shows seeded synonyms | Done |
| 2 | Synonym type badges (Multi-way) | Done |
| 3 | Synonym count badge | Done |
| 4 | Create and delete multi-way synonym | Done |
| 5 | Create one-way synonym via dialog | Done |
| 6 | Search/filter synonyms | Done |
| 7 | Add Synonym button opens dialog | Done |
| 8 | Synonym card structure (equals-joined words) | Done |
| 9 | Delete synonym via API + UI verification | Done |
| 10 | Clear All button shows confirmation (cancel) | Done |

### Settings — [settings.spec.ts](e2e-ui/full/settings.spec.ts) (15 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Searchable attributes from seeded settings | Done |
| 2 | Faceting attributes display | Done |
| 3 | JSON editor toggle | Done |
| 4 | Ranking/custom ranking configuration | Done |
| 5 | Compact index button visible and enabled | Done |
| 6 | Compact index button triggers compaction | Done |
| 7 | FilterOnly faceting attributes | Done |
| 8 | Breadcrumb back to index | Done |
| 9 | All major sections present | Done |
| 10 | Reset button appears after modification and reverts | Done |
| 11 | Save settings + verify persistence | Done |
| 12 | Search tab query type persists after save and reload | Done |
| 13 | Language and text tab query languages persist after save and reload | Done |
| 14 | Ranking tab distinct settings persist after save and reload | Done |
| 15 | Shows tabs in DOM and active content at narrow width | Done |

### Merchandising — [merchandising.spec.ts](e2e-ui/full/merchandising.spec.ts) (14 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Search for products shows results | Done |
| 2 | Pin button visible on result cards | Done |
| 3 | Hide button visible on result cards | Done |
| 4 | Pinning shows badge + moves to position 1 | Done |
| 5 | Hiding moves result to hidden section | Done |
| 6 | Pin + hide multiple results (combined counts) | Done |
| 7 | Save as rule → cross-page verify on Rules page | Done |
| 8 | Different queries return different results | Done |
| 9 | Results summary shows hit count | Done |
| 10 | How It Works help card visible | Done |
| 11 | Drag handle visible on all result cards | Done |
| 12 | Result cards are draggable (have draggable attribute) | Done |
| 13 | Drag and drop pins item at target position | Done |
| 14 | Up/down arrow buttons work for pinned items | Done |

### API Keys — [api-keys.spec.ts](e2e-ui/full/api-keys.spec.ts) (12 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Loads seeded key and verifies description, value, and restrict-sources badge | Done |
| 2 | API keys page loads and shows heading and create button | Done |
| 3 | Create key dialog shows all form sections | Done |
| 4 | Toggling permissions updates selection badges | Done |
| 5 | Create a new API key and verify it appears in the list | Done |
| 6 | Create then delete an API key | Done |
| 7 | Key cards display permissions badges | Done |
| 8 | Copy button visible on key cards | Done |
| 9 | Clicking copy button shows Copied feedback | Done |
| 10 | Key with no index scope shows All Indexes badge | Done |
| 11 | Create key with restricted index scope shows index badge | Done |
| 12 | Create key with restrict sources via UI and verify badges | Done |

### Search Logs — [search-logs.spec.ts](e2e-ui/full/search-logs.spec.ts) (11 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Log entries appear after visiting pages | Done |
| 2 | Log entry shows HTTP method and URL | Done |
| 3 | Expand log entry shows curl command and status | Done |
| 4 | Collapse expanded log entry | Done |
| 5 | Clear Logs removes entries and shows empty state | Done |
| 6 | Filter input narrows log entries by URL | Done |
| 7 | View mode toggle (Endpoint ↔ Curl) | Done |
| 8 | Curl view shows actual curl commands with correct format | Done |
| 9 | Expanded log entry shows request body and response | Done |
| 10 | Export button visible | Done |
| 11 | Request count badge shows accurate count | Done |

### System — [system.spec.ts](e2e-ui/full/system.spec.ts) (21 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Health tab: server status "ok" | Done |
| 2 | Health tab: active writers count | Done |
| 3 | Health tab: facet cache with numeric values | Done |
| 4 | Health tab: index health summary + green dots | Done |
| 5 | Health tab: auto-refresh notice | Done |
| 6 | Health tab: version badge with build profile | Done |
| 7 | Health tab: uptime card with time value | Done |
| 8 | Health tab: tenants loaded card | Done |
| 9 | Health tab: memory card with heap usage and progress bar | Done |
| 10 | Health tab: pressure level indicator | Done |
| 11 | Indexes tab: e2e-products with doc count | Done |
| 12 | Indexes tab: total indexes/docs/storage cards | Done |
| 13 | Indexes tab: health status column (Healthy) | Done |
| 14 | Indexes tab: click index → search page | Done |
| 15 | Replication tab: Node ID card | Done |
| 16 | Replication tab: Enabled/Disabled status | Done |
| 17 | Replication tab: auto-refresh notice | Done |
| 18 | Snapshots tab: Local Export/Import section | Done |
| 19 | Snapshots tab: per-index export/import buttons | Done |
| 20 | Snapshots tab: S3 Backups section | Done |
| 21 | All four tabs visible + clickable | Done |

### Migrate — [migrate.spec.ts](e2e-ui/full/migrate.spec.ts) (13 tests)

| # | Test | Status |
|---|------|--------|
| 1 | All form sections visible on load | Done |
| 2 | Migrate button disabled when empty | Done |
| 3 | Filling credentials enables button | Done |
| 4 | API key visibility toggle (eye button) | Done |
| 5 | Overwrite toggle on/off | Done |
| 6 | Target index placeholder mirrors source | Done |
| 7 | Custom target overrides source in button | Done |
| 8 | Clearing source re-disables button | Done |
| 9 | Clearing app ID re-disables button | Done |
| 10 | Invalid credentials shows error | Done |
| 11 | Info section content (3 items) | Done |
| 12 | Target field helper text | Done |
| 13 | API key security note | Done |

### Migrate (Algolia) — [migrate-algolia.spec.ts](e2e-ui/full/migrate-algolia.spec.ts) (2 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Full Algolia migration: fill form → migrate → verify success → browse | Done |
| 2 | Invalid Algolia credentials show error state | Done |

*Note: Skipped when Algolia credentials not available.*

### Navigation & Layout — [navigation.spec.ts](e2e-ui/full/navigation.spec.ts) (15 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Sidebar shows all main nav items | Done |
| 2 | Sidebar shows seeded index | Done |
| 3 | Click Overview → overview page | Done |
| 4 | Click API Logs → logs page | Done |
| 5 | Click Migrate → migrate page | Done |
| 6 | Click API Keys → keys page | Done |
| 7 | Click Metrics → metrics page | Done |
| 8 | Click System → system page | Done |
| 9 | Click index → search page | Done |
| 10 | Header shows logo + connection status | Done |
| 11 | Theme toggle light/dark | Done |
| 12 | Indexing queue button opens panel | Done |
| 13 | Search sub-page nav buttons | Done |
| 14 | Breadcrumb navigates to overview | Done |
| 15 | Unknown route shows 404 | Done |

### Cross-Page Flows — [cross-page-flows.spec.ts](e2e-ui/full/cross-page-flows.spec.ts) (8 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Overview → click index → Search page | Done |
| 2 | Full lifecycle: create → docs → search → delete | Done |
| 3 | Merchandising → pin → save rule → Rules page | Done |
| 4 | System Indexes tab → click → search page | Done |
| 5 | Settings persistence after save + reload | Done |
| 6 | Search with analytics → Analytics page | Done |
| 7 | Overview analytics → Analytics page link | Done |
| 8 | Full navigation cycle (5 pages) | Done |

### Auth Flow — [auth-flow.spec.ts](e2e-ui/full/auth-flow.spec.ts) (5 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Auth gate appears for unauthenticated users | Done |
| 2 | Valid API key authenticates successfully | Done |
| 3 | Invalid API key shows error | Done |
| 4 | Authenticated user can access dashboard | Done |
| 5 | Logout returns to auth gate | Done |

### Connection Health — [connection-health.spec.ts](e2e-ui/full/connection-health.spec.ts) (4 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Connected badge when server is healthy | Done |
| 2 | BETA badge visible in header | Done |
| 3 | Disconnected banner when server unreachable | Done |
| 4 | Recovery from disconnected state | Done |

### Query Suggestions — [query-suggestions.spec.ts](e2e-ui/full/query-suggestions.spec.ts) (11 tests)

| # | Test | Status |
|---|------|--------|
| 1 | seeded config renders in the list after navigation | Done |
| 2 | page loads with heading and Create Config button | Done |
| 3 | empty state shows Create Your First Config when no configs exist | Done |
| 4 | create config dialog shows all required form fields | Done |
| 5 | exclude word chips can be added and removed before submit | Done |
| 6 | cancel closes dialog without creating a config | Done |
| 7 | created config card shows source index, status, and action buttons | Done |
| 8 | rebuild button triggers a build and shows toast | Done |
| 9 | build logs can be expanded and collapsed after rebuild | Done |
| 10 | delete config removes it from the list | Done |
| 11 | sidebar Query Suggestions link navigates to the page | Done |

### Dictionaries — [dictionaries.spec.ts](e2e-ui/full/dictionaries.spec.ts) (8 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Loads seeded stopword entry in default Stopwords view | Done |
| 2 | Switches between Stopwords, Plurals, and Compounds tabs | Done |
| 3 | Adds a stopword entry through the dialog | Done |
| 4 | Adds a plural entry and renders combined words | Done |
| 5 | Adds a compound entry and renders decomposition chain | Done |
| 6 | Deletes a seeded stopword entry through the UI | Done |
| 7 | Deletes a seeded plural entry through the UI | Done |
| 8 | Shows empty state when the selected dictionary has no entries | Done |

### Security Sources — [security-sources.spec.ts](e2e-ui/full/security-sources.spec.ts) (6 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Loads seeded security source in the page list | Done |
| 2 | Adds a security source through the add dialog | Done |
| 3 | Blocks blank source submission locally | Done |
| 4 | Surfaces malformed source error returned by the server | Done |
| 5 | Deletes a seeded source through the UI | Done |
| 6 | Shows empty state when there are no security sources | Done |

### Event Debugger — [event-debugger.spec.ts](e2e-ui/full/event-debugger.spec.ts) (7 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Page loads with heading and event count badge | Done |
| 2 | Seeded events appear in the event table | Done |
| 3 | Event rows show correct index and user token | Done |
| 4 | Clicking an event row opens the detail panel | Done |
| 5 | Status filter narrows displayed events | Done |
| 6 | Event type filter works | Done |
| 7 | Sidebar navigation link to Event Debugger exists | Done |

### Event Debugger (Mocked) — [event-debugger-mocked.spec.ts](e2e-ui/full/event-debugger-mocked.spec.ts) (3 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Shows error state when debug endpoint returns 500 | Done |
| 2 | Shows empty state when no events exist | Done |
| 3 | Handles events with validation errors | Done |

### Experiments — [experiments.spec.ts](e2e-ui/full/experiments.spec.ts) (14 tests)

| # | Test | Status |
|---|------|--------|
| 1 | load-and-verify: seeded experiment renders in experiments table | Done |
| 2 | running experiment can be stopped from the list UI | Done |
| 3 | create dialog submit creates an experiment through the UI flow | Done |
| 4 | stopped experiment can be deleted from the list UI | Done |
| 5 | clicking experiment name in list navigates to detail page | Done |
| 6 | detail page shows experiment name, status, index, and metric | Done |
| 7 | detail page shows progress bar for running experiment collecting data | Done |
| 8 | detail page shows control and variant metric cards | Done |
| 9 | back link navigates from detail to experiments list | Done |
| 10 | running experiment detail page renders from arrange-time creation | Done |
| 11 | running experiment detail shows SRM and guard rail banners for skewed traffic | Done |
| 12 | running experiment detail shows interleaving card when interleaving metrics exist | Done |
| 13 | stopped experiment detail shows stopped status and no declare winner | Done |
| 14 | declare winner flow concludes a soft-gated experiment through browser dialog | Done |

### Personalization — [personalization.spec.ts](e2e-ui/full/personalization.spec.ts) (3 tests)

| # | Test | Status |
|---|------|--------|
| 1 | shows setup state when strategy is not configured | Done |
| 2 | uses starter strategy defaults, persists event and facet edits, and unlocks profile lookup after save | Done |
| 3 | profile lookup shows known user profile and unknown-user empty state | Done |

### Recommendations — [recommendations.spec.ts](e2e-ui/full/recommendations.spec.ts) (3 tests)

| # | Test | Status |
|---|------|--------|
| 1 | load-and-verify: recommendations page renders real preview controls | Done |
| 2 | all five recommendation models: model switching enforces inputs and renders result-or-empty states | Done |
| 3 | keeps placeholder, success, empty, and submit-error recommendation states distinct | Done |

### Chat / RAG — [chat.spec.ts](e2e-ui/full/chat.spec.ts) (4 tests)

| # | Test | Status |
|---|------|--------|
| 1 | shows setup prompt when not in NeuralSearch mode | Done |
| 2 | shows chat interface when NeuralSearch is enabled with documents | Done |
| 3 | sends query, displays answer with sources, and supports multi-turn | Done |
| 4 | shows no-results message on an empty index | Done |

### Display Preferences — [display-preferences.spec.ts](e2e-ui/full/display-preferences.spec.ts) (6 tests)

| # | Test | Status |
|---|------|--------|
| 1 | loads seeded Browse content before modal interactions | Done |
| 2 | opens the modal with title/subtitle/image/tag controls and index field options | Done |
| 3 | saves full preferences and renders configured title/subtitle/image/tags | Done |
| 4 | clears preferences and reverts cards to default field-value rendering | Done |
| 5 | persists saved preferences across page navigation and refresh | Done |
| 6 | keeps preferences isolated per index | Done |

### Metrics — [metrics.spec.ts](e2e-ui/full/metrics.spec.ts) (10 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Overview tab shows version badge and uptime | Done |
| 2 | Overview tab shows aggregate request cards with numeric values | Done |
| 3 | Overview tab shows aggregate doc/storage cards | Done |
| 4 | Overview tab shows auto-refresh notice | Done |
| 5 | Per-Index tab shows table with seeded index row | Done |
| 6 | Per-Index tab shows doc count and storage for seeded index | Done |
| 7 | Per-Index tab shows search count for seeded index | Done |
| 8 | Per-Index tab shows auto-refresh notice | Done |
| 9 | Per-Index tab column sort updates aria-sort state | Done |
| 10 | Both tabs are visible and clickable | Done |

### Hybrid Search — [hybrid-search.spec.ts](e2e-ui/full/hybrid-search.spec.ts) (4 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Hybrid controls hidden when no embedders configured | Done |
| 2 | Hybrid controls visible when embedders configured | Done |
| 3 | Semantic ratio slider updates label | Done |
| 4 | Search results appear with hybrid search active | Done |

### Vector Settings — [vector-settings.spec.ts](e2e-ui/full/vector-settings.spec.ts) (5 tests)

| # | Test | Status |
|---|------|--------|
| 1 | Displays search mode and embedders sections with seeded data | Done |
| 2 | Set search mode to Neural Search and verify persistence | Done |
| 3 | Add userProvided embedder via dialog | Done |
| 4 | Delete an embedder via confirm dialog | Done |
| 5 | Embedder settings persist after save and navigation | Done |

---

## Summary

| Page/Feature | File | Tests | Coverage |
|-------------|------|-------|----------|
| Smoke (critical paths) | critical-paths.spec.ts | 7 | Full |
| Smoke (index tabs) | index-tab-bar.spec.ts | 1 | Full |
| Smoke (settings tabs) | settings-tabs.spec.ts | 1 | Full |
| Smoke (sidebar sections) | sidebar-sections.spec.ts | 1 | Full |
| Overview | overview.spec.ts | 16 | Full |
| Search & Browse | search.spec.ts | 20 | Full |
| Analytics | analytics.spec.ts | 34 | Full |
| Analytics Deep Data | analytics-deep.spec.ts | 24 | Full |
| Analytics Conversions (Mocked) | analytics-conversions-mocked.spec.ts | 3 | Full |
| Rules | rules.spec.ts | 17 | Full |
| Rules Form (Mocked) | rules-form-mocked.spec.ts | 2 | Full |
| Synonyms | synonyms.spec.ts | 10 | Full |
| Settings | settings.spec.ts | 15 | Full |
| Merchandising | merchandising.spec.ts | 14 | Full |
| API Keys | api-keys.spec.ts | 12 | Full |
| Search Logs | search-logs.spec.ts | 11 | Full |
| System | system.spec.ts | 21 | Full |
| Migrate | migrate.spec.ts | 13 | Full |
| Migrate (Algolia) | migrate-algolia.spec.ts | 2 | Full |
| Navigation/Layout | navigation.spec.ts | 15 | Full |
| Cross-Page Flows | cross-page-flows.spec.ts | 8 | Full |
| Auth Flow | auth-flow.spec.ts | 5 | Full |
| Connection Health | connection-health.spec.ts | 4 | Full |
| Query Suggestions | query-suggestions.spec.ts | 11 | Full |
| Dictionaries | dictionaries.spec.ts | 8 | Full |
| Security Sources | security-sources.spec.ts | 6 | Full |
| Event Debugger | event-debugger.spec.ts | 7 | Full |
| Event Debugger (Mocked) | event-debugger-mocked.spec.ts | 3 | Full |
| Experiments | experiments.spec.ts | 14 | Full |
| Personalization | personalization.spec.ts | 3 | Full |
| Recommendations | recommendations.spec.ts | 3 | Full |
| Chat / RAG | chat.spec.ts | 4 | Full |
| Display Preferences | display-preferences.spec.ts | 6 | Full |
| Metrics | metrics.spec.ts | 10 | Full |
| Hybrid Search | hybrid-search.spec.ts | 4 | Full |
| Vector Settings | vector-settings.spec.ts | 5 | Full |
| **TOTAL** | **36 files** | **340 active** | **Full** |

---

## Quality Standards

- **Zero ESLint violations** — `npx eslint --config tests/e2e-ui/eslint.config.mjs 'tests/e2e-ui/**/*.spec.ts'` passes clean
- **Zero CSS class selectors** — all locators use `data-testid`, `getByRole`, `getByText`, or `getByPlaceholder`
- **Zero attribute selectors** — no `.locator('[data-testid="..."]')`, uses `.getByTestId('...')` instead
- **Zero API calls in spec files** — all `request.*` calls moved to `fixtures/api-helpers.ts`
- **Zero conditional assertions** — no `if (await isVisible())` guards that silently pass
- **Zero sleeps** — all waits use Playwright auto-retry (`expect().toBeVisible()`, `expect().toPass()`)
- **Content verification** — tests assert actual data values (numbers, percentages, text), not just visibility
- **Real server** — every browser-unmocked test runs against a live Flapjack backend with seeded data
- **Real browser** — Chromium via Playwright (headless mode for CI/local)
- **Simulated human** — all interactions use getByRole/getByText/getByTestId locators
- **Deterministic data** — 12 products, 3 synonyms, 2 rules, settings seeded via seed.setup.ts
- **Cleanup** — tests that create data clean up via fixture helpers (not raw API calls)

---

## Running Tests

```bash
cd engine/dashboard

# Run all browser specs (headless, default)
npm test

# Run smoke tests only (~5s)
npm run test:e2e-ui:smoke

# Run a specific test file
npx playwright test tests/e2e-ui/full/overview.spec.ts

# Run API-level tests (no browser)
npm run test:e2e-api

# Show HTML report after run
npx playwright show-report
```

---

## Seed Data Reference

From `tests/fixtures/test-data.ts`:

- **12 products** (p01-p12): Laptops, Tablets, Audio, Storage, Monitors, Accessories
- **9 brands**: Apple, Lenovo, Dell, Samsung, Sony, LG, Logitech, Keychron, CalDigit
- **3 synonyms**: laptop/notebook/computer, headphones/earphones/earbuds, monitor/screen/display
- **2 rules**: Pin MacBook Pro for "laptop", Hide Galaxy Tab for "tablet"
- **Settings**: 5 searchable attributes, 4 faceting attributes, 2 custom ranking rules
- **Analytics**: 7 days of search/click/geo/device data seeded via `/2/analytics/seed`
