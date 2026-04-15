# Flapjack CI/CD Workflows

This directory contains GitHub Actions workflows that are synced to the public `gridl-staging/flapjack` repository.

## How It Works

1. **Development (private dev repo)**: Tests are run manually via the canonical runner
   ```bash
   ./engine/s/test --ci
   ```

2. **Public Repo (gridl-staging/flapjack)**: Tests run automatically
   - On every push to `main` after `debbie sync staging` / `debbie sync prod`
   - Nightly at 2 AM UTC (comprehensive test suite)

## Workflows

### ci.yml - Continuous Integration

Runs on every push to `main` in the public repo only.

**Tests included:**
- Rust engine (rustfmt, clippy, fast tests)
- Rust engine (all tests) - main branch only
- Installer tests (Ubuntu + macOS)
- Dashboard (unit tests, build, page tests)
- Dashboard integration tests (main branch only, requires Algolia secrets)
- All SDKs (PHP 8.1-8.3, Python 3.9-3.12, JS, Go 1.21-1.23, Ruby 3.1-3.3, Java, C#)
- Integrations (Laravel Scout, WordPress)

**Repository Check:**
All jobs check `github.repository == gridl-staging/flapjack` to ensure they only run in the public repo.

### nightly.yml - Comprehensive Nightly Tests

Runs every night at 2 AM UTC on the public repo only.

**Additional coverage:**
- Extended version matrices (PHP 8.4, Python 3.13, Node 18/20/22, Java 17/21, .NET 7/8)
- All Rust tests (not just fast subset)
- Dashboard integration tests
- Cross-platform installer tests
- Full SDK compatibility matrix

## Sync Process

Use Debbie from the canonical dev repo to publish workflow updates:

```bash
uv run --project <path-to-debbie-project> debbie sync staging
uv run --project <path-to-debbie-project> debbie sync prod
```

## Required GitHub Secrets

Set these in the public repo settings (`gridl-staging/flapjack`):

- `ALGOLIA_APP_ID` - For integration tests
- `ALGOLIA_ADMIN_KEY` - For integration tests

## Local Development

To run the full test suite locally in the private dev repo:

```bash
# Run the CI-aligned suite (unit + integ + server + dashboard)
./engine/s/test --ci

# Run the broad local suite (everything except Algolia-gated lane)
./engine/s/test --all

# With Algolia credentials for integration tests
export ALGOLIA_APP_ID="your-app-id"
export ALGOLIA_ADMIN_KEY="your-admin-key"
./engine/s/test --all --sdk-algolia
```

## Workflow Design

The workflows use a tiered approach:

- **Fast tests on every push**: Essential checks that run quickly
- **Comprehensive tests on main**: Full test suite after merge
- **Nightly tests**: Extended compatibility matrix, all versions

This balances speed (fast PR feedback) with coverage (catch edge cases).
