# Test Specification: Display Preferences (Tier 2)

**Feature:** Per-index display preferences for Browse cards — title, subtitle, image, and tag rendering
**BDD Spec:** B-SRH-003
**Priority:** P2
**Last Updated:** 2026-03-17

---

## Test Fixtures

**Location:** `tests/fixtures/test-data.ts`

```typescript
// Display Preferences fixture contract for this feature:
// extend PRODUCTS rows with image_url so E2E coverage can assert image selection/rendering.
// Example: image_url: 'https://cdn.example.test/products/p01.jpg'

export const displayPreferencesFixtures = {
  indexName: 'e2e-products',
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
    // Fields in test-data.ts that should match auto-detect heuristics:
    titleCandidates: ['name'],       // matches common name: name, title, label, headline, product_name
    imageCandidates: ['image_url'],  // matches common name: image, image_url, imageUrl, thumbnail, photo, picture, img
    tagCandidates: ['tags'],         // matches common name: tags
  },
  expectedFirstProduct: {
    objectID: 'p01',
    name: 'MacBook Pro 16"',
    description: 'Apple M3 Max chip laptop',
    image_url: 'https://cdn.example.test/products/p01.jpg',
    brand: 'Apple',
    category: 'Laptops',
  },
}
```

---

## TEST: Open Display Preferences modal from Browse page

**User Story:** B-SRH-003
**Type:** E2E Full
**File:** `tests/e2e-ui/full/display-preferences.spec.ts`

### Setup
- Index `e2e-products` exists with seeded documents
- Navigate to `/search/e2e-products`

### Execute
1. Wait for results to load (`[data-testid="results-panel"]` visible)
2. Locate and click the Display Preferences trigger button in the action bar

### Verify UI
- Modal dialog is visible with title containing "Display Preferences"
- Modal shows field selection controls for title, subtitle, image, and tags
- Available fields include names from the index (e.g. `name`, `description`, `brand`)
- Cancel/close dismisses the modal without saving

### Expected Values
- Modal opens in the context of index `e2e-products`
- Field list reflects the index's actual fields (derived from `useIndexFields`)

### Cleanup
- None (read-only)

---

## TEST: Auto-detect prefills likely field candidates

**User Story:** B-SRH-003
**Type:** Unit
**File:** `src/hooks/useDisplayPreferences.test.ts`

### Setup
- Provide a field list containing: `name`, `description`, `image_url`, `brand`, `category`, `price`, `rating`, `inStock`, `tags`

### Execute
1. Call auto-detect logic with the field list
2. Inspect the returned defaults

### Verify
- `titleAttribute` is `'name'` (matches common title pattern)
- `subtitleAttribute` is `null` (no `subtitle` or `description`-matching heuristic strong enough to auto-select)
- `imageAttribute` is `'image_url'` (matches common image pattern)
- `tagAttributes` is `['tags']` (matches common tag pattern)

### Expected Values
- Auto-detect only picks fields whose names match the predefined common-name lists
- Fields that don't match any heuristic are left unselected

### Cleanup
- None

---

## TEST: Save display preferences via modal

**User Story:** B-SRH-003
**Type:** E2E Full
**File:** `tests/e2e-ui/full/display-preferences.spec.ts`

### Setup
- Navigate to `/search/e2e-products`
- Open Display Preferences modal

### Execute
1. Select `name` as title attribute
2. Select `description` as subtitle attribute
3. Select `image_url` as image attribute
4. Select `tags` in the tag multi-select
5. Click Save/Apply button

### Verify UI
- Modal closes
- Document cards now render `name` as a prominent title
- Document cards now render `description` as a subtitle
- Document cards now render an image element sourced from `image_url`
- Document cards now render tag pills for `tags` values
- The first card shows title "MacBook Pro 16\"" and subtitle "Apple M3 Max chip laptop"
- The first card image uses src `https://cdn.example.test/products/p01.jpg`
- Tag pills show "laptop" and "professional" for the first card
- Remaining field-value rows do NOT include `name`, `description`, `image_url`, or `tags` (consumed-field exclusion)

### Verify API
- No API calls are made to save preferences (localStorage only)

### Expected Values
- First card title: `'MacBook Pro 16"'` (from `displayPreferencesFixtures.expectedFirstProduct.name`)
- First card subtitle: `'Apple M3 Max chip laptop'`
- First card image src: `'https://cdn.example.test/products/p01.jpg'`
- Consumed fields excluded from remaining rows: `name`, `description`, `image_url`, `tags`

### Cleanup
- Clear localStorage key for display preferences after test

---

## TEST: Clear display preferences resets to default rendering

**User Story:** B-SRH-003
**Type:** E2E Full
**File:** `tests/e2e-ui/full/display-preferences.spec.ts`

### Setup
- Index `e2e-products` with saved display preferences (title=`name`, tags=`tags`)
- Navigate to `/search/e2e-products`
- Verify cards are rendering with display preferences applied

### Execute
1. Open Display Preferences modal
2. Click Clear/Reset button
3. Confirm the clear action if prompted

### Verify UI
- Modal closes (or updates to show empty selections)
- Document cards revert to the default field-value row rendering
- No title/subtitle/image/tag header area is shown on cards
- Cards show up to 6 field-value rows in the standard layout
- The first field-value row shows the first field from the canonical field order

### Expected Values
- Card rendering matches the pre-preferences default exactly
- localStorage entry for this index's preferences is removed or nulled

### Cleanup
- None (preferences already cleared by the test)

---

## TEST: DocumentCard with preferences renders configured fields first

**User Story:** B-SRH-003
**Type:** Unit
**File:** `src/components/search/DocumentCard.test.tsx`

### Setup
```typescript
const mockDocument = {
  objectID: 'p01',
  name: 'MacBook Pro 16"',
  description: 'Apple M3 Max chip laptop',
  image_url: 'https://cdn.example.test/products/p01.jpg',
  brand: 'Apple',
  category: 'Laptops',
  price: 3499,
  rating: 4.8,
  inStock: true,
  tags: ['laptop', 'professional'],
}

const mockPreferences = {
  titleAttribute: 'name',
  subtitleAttribute: 'description',
  imageAttribute: 'image_url',
  tagAttributes: ['tags'],
}
```

### Execute
1. Render DocumentCard with the mock document and preferences

### Verify
- Title text "MacBook Pro 16\"" is visible
- Subtitle text "Apple M3 Max chip laptop" is visible
- Card image is visible with src `https://cdn.example.test/products/p01.jpg`
- Tag pills "laptop" and "professional" are visible
- Field-value rows do NOT include `name`, `description`, `image_url`, or `tags`
- Remaining rows include `brand`, `category`, `price`, `rating`, `inStock`

### Expected Values
- Consumed fields (`name`, `description`, `image_url`, `tags`) appear only in the header area, not in field-value rows
- The six-field preview limit (PREVIEW_FIELD_COUNT) applies to the remaining non-consumed fields

### Cleanup
- None

---

## TEST: DocumentCard with preferences preserves existing card chrome

**User Story:** B-SRH-003
**Type:** Unit
**File:** `src/components/search/DocumentCard.test.tsx`

### Setup
```typescript
const onDelete = vi.fn()

render(
  <DocumentCard
    document={mockDocument}
    displayPreferences={mockPreferences}
    onDelete={onDelete}
  />
)
```

### Execute
1. Render DocumentCard with configured header preferences
2. Click Copy
3. Toggle JSON view
4. Click Delete

### Verify
- objectID badge is visible
- JSON toggle remains visible and opens the JSON viewer
- Copy action remains visible and writes the document JSON to the clipboard
- Delete action remains visible and calls `onDelete(objectID)`
- Configured title/subtitle rendering remains visible after using the controls

### Expected Values
- Display preferences change only the content layout; existing card chrome and actions remain available in the configured-preferences path

### Cleanup
- Restore any clipboard mocks

---

## TEST: DocumentCard fails soft for missing/null configured fields and unusable image values

**User Story:** B-SRH-003
**Type:** Unit
**File:** `src/components/search/DocumentCard.test.tsx`

### Setup
```typescript
const degradedDocument = {
  objectID: 'p02',
  description: null,
  image_url: 'javascript:alert(1)',
  brand: 'Fallback Brand',
  category: 'Fallback Category',
  price: 99,
  tags: null,
}

const degradedPreferences = {
  titleAttribute: 'name',
  subtitleAttribute: 'description',
  imageAttribute: 'image_url',
  tagAttributes: ['tags'],
}
```

### Execute
1. Render DocumentCard with the degraded document and configured preferences

### Verify
- Missing configured fields do not render broken header nodes or throw
- `null` configured subtitle/tag values do not break card rendering
- No `<img>` element is rendered for an unusable image value such as `javascript:alert(1)`; the value falls back to safe text instead
- Remaining field-value rows still render unaffected fields such as `brand`, `category`, and `price`
- Consumed-field filtering only removes configured fields that actually exist on the document

### Expected Values
- Configured header rendering fails soft for degraded data without hiding or corrupting the remaining field rows
- Unusable image values never render as executable or broken image elements

### Cleanup
- None

---

## TEST: DocumentCard without preferences renders default field-value rows

**User Story:** B-SRH-003
**Type:** Unit
**File:** `src/components/search/DocumentCard.test.tsx`

### Setup
```typescript
const mockDocument = {
  objectID: 'p01',
  name: 'MacBook Pro 16"',
  description: 'Apple M3 Max chip laptop',
  brand: 'Apple',
  category: 'Laptops',
  price: 3499,
  rating: 4.8,
  inStock: true,
  tags: ['laptop', 'professional'],
}
// No preferences provided (null or undefined)
```

### Execute
1. Render DocumentCard with the mock document and no preferences

### Verify
- No title/subtitle/tag header area is rendered
- Field-value rows show the first 6 fields in canonical order: `name`, `description`, `brand`, `category`, `price`, `rating`
- Expand button shows "+2 more fields" for `inStock` and `tags`
- objectID badge is visible

### Expected Values
- Rendering matches the current DocumentCard behavior exactly
- PREVIEW_FIELD_COUNT = 6 applies

### Cleanup
- None

---

## TEST: Preferences persist across page navigation and browser refresh

**User Story:** B-SRH-003
**Type:** E2E Full
**File:** `tests/e2e-ui/full/display-preferences.spec.ts`

### Setup
- Navigate to `/search/e2e-products`
- Save display preferences (title=`name`, tags=`tags`)
- Verify cards render with preferences

### Execute
1. Navigate away to `/overview`
2. Navigate back to `/search/e2e-products`
3. Verify cards still render with preferences (title and tags visible)
4. Reload the page (`page.reload()`)
5. Wait for results to load again

### Verify UI
- After navigation: cards still show `name` as title and `tags` as tag pills
- After reload: cards still show `name` as title and `tags` as tag pills
- No Display Preferences modal auto-opens on load

### Expected Values
- localStorage key `flapjack-display-preferences` (or similar) contains a JSON object keyed by index name
- The stored preferences for `e2e-products` match what was saved

### Cleanup
- Clear localStorage display preferences after test

---

## TEST: Per-index isolation — switching indexes loads correct preferences

**User Story:** B-SRH-003
**Type:** E2E Full
**File:** `tests/e2e-ui/full/display-preferences.spec.ts`

### Setup
- Index `e2e-products` exists with seeded documents
- A second test index exists (or use a different index name for localStorage seeding)
- Save preferences for `e2e-products`: title=`name`, tags=`tags`
- Save different preferences for the second index (or no preferences)

### Execute
1. Navigate to `/search/e2e-products`
2. Verify cards render with `e2e-products` preferences (title=`name`)
3. Navigate to the second index's browse page
4. Verify cards do NOT show `e2e-products` preferences

### Verify UI
- On `e2e-products`: title "MacBook Pro 16\"" visible in first card's header area
- On second index: default field-value row rendering (no title header area)

### Expected Values
- Preferences are stored as a map keyed by index name
- Loading preferences for one index never leaks into another

### Cleanup
- Clear all test localStorage entries

---

## Unit Test Specifications

### Hook: useDisplayPreferences

**File:** `src/hooks/useDisplayPreferences.test.ts`

#### Test: Returns null preferences for an index with no saved preferences
```typescript
const { result } = renderHook(() => useDisplayPreferences('unknown-index'))

expect(result.current.preferences).toBeNull()
```

#### Test: Saves and retrieves preferences per index
```typescript
const { result } = renderHook(() => useDisplayPreferences('my-index'))

act(() => {
  result.current.setPreferences('my-index', {
    titleAttribute: 'name',
    subtitleAttribute: null,
    imageAttribute: null,
    tagAttributes: ['tags'],
  })
})

expect(result.current.getPreferences('my-index')).toEqual({
  titleAttribute: 'name',
  subtitleAttribute: null,
  imageAttribute: null,
  tagAttributes: ['tags'],
})
```

#### Test: Clears preferences for a specific index
```typescript
const { result } = renderHook(() => useDisplayPreferences('my-index'))

act(() => {
  result.current.setPreferences('my-index', {
    titleAttribute: 'name',
    subtitleAttribute: null,
    imageAttribute: null,
    tagAttributes: [],
  })
})

act(() => {
  result.current.clearPreferences('my-index')
})

expect(result.current.getPreferences('my-index')).toBeNull()
```

#### Test: Auto-detect returns sensible defaults for common field names
```typescript
const fields: FieldInfo[] = [
  { name: 'name', type: 'text' },
  { name: 'description', type: 'text' },
  { name: 'image_url', type: 'text' },
  { name: 'tags', type: 'text' },
  { name: 'price', type: 'number' },
]

const detected = autoDetectPreferences(fields)

expect(detected.titleAttribute).toBe('name')
expect(detected.imageAttribute).toBe('image_url')
expect(detected.tagAttributes).toEqual(['tags'])
expect(detected.subtitleAttribute).toBeNull()
```

#### Test: Auto-detect returns all-null when no common names match
```typescript
const fields: FieldInfo[] = [
  { name: 'x1', type: 'text' },
  { name: 'x2', type: 'number' },
]

const detected = autoDetectPreferences(fields)

expect(detected.titleAttribute).toBeNull()
expect(detected.subtitleAttribute).toBeNull()
expect(detected.imageAttribute).toBeNull()
expect(detected.tagAttributes).toEqual([])
```

### Component: DisplayPreferencesModal

**File:** `src/components/search/DisplayPreferencesModal.test.tsx`

#### Test: Renders title, subtitle, image, and tag controls
```typescript
render(<DisplayPreferencesModal indexName="my-index" open={true} onOpenChange={vi.fn()} />)

expect(screen.getByRole('dialog')).toBeInTheDocument()
// Title single-select
expect(screen.getByLabelText(/title/i)).toBeInTheDocument()
// Subtitle single-select
expect(screen.getByLabelText(/subtitle/i)).toBeInTheDocument()
// Image single-select
expect(screen.getByLabelText(/image/i)).toBeInTheDocument()
// Tags multi-select
expect(screen.getByText(/tags/i)).toBeInTheDocument()
```

#### Test: Calls save with selected preferences on confirm
```typescript
// Mock useIndexFields to return test fields
// Render modal, select title='name', click Save
// Assert store was called with { titleAttribute: 'name', ... }
```

#### Test: Calls clear and resets when reset button clicked
```typescript
// Render modal with existing preferences
// Click Clear/Reset
// Assert store.clearPreferences was called with the index name
```

### Component: DocumentCard (display preferences behavior)

**File:** `src/components/search/DocumentCard.test.tsx`

#### Test: Renders title and subtitle in header when preferences provided
```typescript
// See full TEST block above: "DocumentCard with preferences renders configured fields first"
```

#### Test: Preserves objectID badge, JSON toggle, copy action, and delete action when preferences are provided
```typescript
// See full TEST block above: "DocumentCard with preferences preserves existing card chrome"
```

#### Test: Excludes consumed fields from remaining field-value rows
```typescript
// Given preferences with titleAttribute='name' and tagAttributes=['tags']
// Render DocumentCard
// Assert 'name' and 'tags' do NOT appear as field-value row labels
```

#### Test: Fails soft for missing/null configured values and unusable image values
```typescript
// See full TEST block above: "DocumentCard fails soft for missing/null configured fields and unusable image values"
```

#### Test: Falls back to default rendering when no preferences
```typescript
// See full TEST block above: "DocumentCard without preferences renders default field-value rows"
```

---

## Coverage Mapping

| Tier 3 Test File | Behaviors Owned |
|---|---|
| `src/hooks/useDisplayPreferences.test.ts` | get/set/clear per-index preferences, auto-detect heuristic, all-null default for unknown index |
| `src/components/search/DisplayPreferencesModal.test.tsx` | modal renders controls, save on confirm, clear on reset, field list from useIndexFields |
| `src/components/search/DocumentCard.test.tsx` | title/subtitle/image/tag rendering with preferences, consumed-field exclusion, preserved objectID/JSON/copy/delete controls, fail-soft handling for missing/null configured values and unusable image values, default fallback without preferences |
| `tests/e2e-ui/full/display-preferences.spec.ts` | open modal from Browse, save title/subtitle/image/tag preferences and observe card update, clear and revert, persistence across navigation/reload, per-index isolation |

---

## Contract Boundaries (Out of Scope)

- Display Preferences persistence is client-side only (`localStorage` via Zustand persist); no backend API writes are allowed.
- No metrics overlay, click/impression/CTR rendering, or analytics-driven card annotations are included in this contract.
- The modal configures field names only; it does not render sample image previews.
- Browse load does not auto-open the modal and does not show a detected-preferences banner.
- This contract applies to Browse (`/search/:indexName`) only; merchandising/rules surfaces are excluded.

---

## Notes

- Image rendering is in-scope for this Stage 1 acceptance contract; `tests/fixtures/test-data.ts` must include `image_url` for Display Preferences E2E coverage.
- Auto-detect common-name lists: title candidates (`name`, `title`, `label`, `headline`, `product_name`), image candidates (`image`, `image_url`, `imageUrl`, `thumbnail`, `photo`, `picture`, `img`), tag candidates (`tags`, `labels`, `categories`).
- The six-field preview limit (`PREVIEW_FIELD_COUNT = 6`) applies to remaining fields after consumed fields are excluded.
- Configured rendering preserves the existing objectID badge, JSON toggle, copy action, and delete action.
- Missing configured fields are skipped, null configured values fail soft, and unusable image values fall back without rendering an unsafe image element.
- No API calls are made by this feature — all persistence is client-side localStorage via Zustand `persist`.
