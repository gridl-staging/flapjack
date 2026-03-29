# Contributing

Thanks for your interest in improving Flapjack.

## Bug Reports

Use GitHub Issues and select the bug report template when available.

Include:

- What you expected to happen.
- What actually happened.
- Reproduction steps.
- Version/commit, environment, and logs.

## Feature Requests

Open a GitHub Issue describing:

- The problem you are trying to solve.
- Why existing behavior is insufficient.
- Proposed API or UX shape when relevant.

## Pull Request Workflow

1. Fork the repository.
2. Create a focused branch for one logical change.
3. Add or update tests for behavior changes.
4. Run the required checks locally.
5. Open a PR with a clear description, rationale, and validation notes.

## Code Style

- Follow existing Rust conventions and module patterns in the codebase.
- Keep changes focused and avoid unrelated refactors.
- Run and pass formatting and linting before opening a PR:
  - `cargo fmt --check`
  - `cargo clippy -p <crate>`

## Testing Expectations

Run the project test entry point before opening a PR:

- `engine/s/test`

When possible, run focused crate or file-level tests first for fast feedback, then run broader checks as needed.

## Architecture and Testing References

- [Architecture](engine/docs2/3_IMPLEMENTATION/ARCHITECTURE.md)
- [Testing](engine/docs2/1_STRATEGY/TESTING.md)
