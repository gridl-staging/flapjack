# API Keys Page

## api-keys-1: List API keys (SMOKE)
1. Go to /keys
2. See the "API Keys" heading
3. See the "Create Key" button
4. See at least one API key card (the master key)
5. See each key card showing: description, key value, permissions, and index scope
6. See the "Copy" button next to each key value

## api-keys-2: Create a new key
1. Go to /keys
2. Click "Create Key" button
3. See the Create Key dialog open
4. Enter a description "e2e-test-key"
5. Select permissions (e.g., search only)
6. Click "Create"
7. See the dialog close
8. See the new key "e2e-test-key" appear in the keys list
9. See the key value displayed
10. Cleanup: delete the created key

## api-keys-3: Delete a key
1. Go to /keys
2. Create a temporary key via API
3. Refresh the page
4. See the temporary key in the list
5. Click the trash icon on the temporary key card
6. Confirm the deletion in the browser dialog
7. See the key removed from the list

## api-keys-4: Copy key to clipboard
1. Go to /keys
2. See a key card with its value displayed
3. Click the "Copy" button next to the key value
4. See the button text change to "Copied" with a checkmark icon
5. See the button revert back to "Copy" after a moment

## api-keys-5: Filter keys by index
1. Go to /keys
2. See the "Filter by Index" bar (if multiple indexes exist)
3. Click on an index name in the filter bar
4. See only keys with access to that index displayed
5. Click "All" to reset the filter
6. See all keys displayed again

## api-keys-6: Create key with restrict sources and verify key card badges
1. Go to /keys
2. Click "Create Key"
3. Enter a description prefixed with "E2E"
4. Enter restrict sources with CIDR values using commas and newlines (example: `10.0.0.0/8, 192.168.1.0/24` and `172.16.0.0/12`)
5. Submit the dialog
6. See the created key card show a "Restrict Sources" section
7. See each submitted CIDR rendered as a badge in that section
8. Cleanup: delete created keys with the `E2E` description prefix

## api-keys-7: Blank restrict sources is omitted from create payload
1. Open the Create Key dialog
2. Enter a description and leave restrict sources blank (or whitespace-only)
3. Submit the dialog
4. Verify request payload does not include `restrictSources`
