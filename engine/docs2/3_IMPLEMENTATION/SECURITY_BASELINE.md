# Security Baseline

This document is the canonical public hardening baseline for the open-source
Flapjack repo. It is intentionally scoped: it describes what is verified today,
what operators should strongly prefer in production, and what still remains
outside the current proof bar.

- For deployment and operator runbooks, see [OPERATIONS.md](./OPERATIONS.md).
- For env vars and defaults, see [OPS_CONFIGURATION.md](./OPS_CONFIGURATION.md).
- For shipped/readiness status, see [../FEATURES.md](../FEATURES.md).

## What is verified today

### Production auth floor

- `FLAPJACK_NO_AUTH=1` is blocked in production mode.
- `FLAPJACK_ADMIN_KEY` is required in production mode.
- short production admin keys are rejected at startup.

Proof surfaces:

- `cargo test -p flapjack-server --test env_mode_test`
- `engine/flapjack-http/src/startup.rs`

### Admin-only operational surfaces

- `/metrics` requires admin-key auth.
- admin-key rotation is available at `POST /internal/rotate-admin-key`.
- rotated keys invalidate the previous admin key immediately for metrics access.

Proof surfaces:

- `cargo test -p flapjack-server --test admin_key_test`
- `cargo test -p flapjack-http router::tests::metrics_returns_200_with_admin_key_only -- --exact`

### Canonical auth failures

- malformed or invalid credentials return the same canonical
  `{"message":"Invalid Application-ID or API key","status":403}` shape
- malformed secured keys do not leak decode/parse internals in the response

Proof surfaces:

- `cargo test -p flapjack --test test_security_audit`
- `engine/flapjack-http/src/auth/mod.rs`

### `restrictSources` coverage

- `restrictSources` persists through key storage and serialization
- allow-list enforcement is fail-closed on protected routes
- forwarded-header handling only trusts known proxy CIDRs

Proof surfaces:

- `cargo test -p flapjack --test test_security_sources_parity`
- `engine/flapjack-http/src/auth_tests/key_store_tests.rs`
- `engine/flapjack-http/src/middleware.rs`

### Request correlation and error consistency

- every response carries `x-request-id`
- JSON log mode preserves the request ID in span context
- non-JSON errors are normalized into the canonical JSON envelope

Proof surfaces:

- `engine/flapjack-http/src/middleware.rs`
- `engine/flapjack-http/src/router_tests.rs`
- `engine/flapjack-http/src/startup.rs`

## Public deployment safety review

The public docs should normalize the following production posture:

- run with `FLAPJACK_ENV=production`
- provide a strong admin key
- do not use `--no-auth` or `FLAPJACK_NO_AUTH=1` outside local loopback-only dev
- keep `/internal/*` and `/metrics` behind both auth and network controls
- keep secrets in an env file or secret store, not in tracked config

Current public docs are aligned with that baseline:

- `README.md` keeps no-auth examples explicitly local
- `engine/examples/systemd/README.md` requires a production env file and admin key
- `engine/docs2/3_IMPLEMENTATION/DEPLOYMENT.md` points production operators to the systemd path and env-file pattern

## Strong operator recommendations

These are not launch blockers for the OSS repo, but they are the recommended
production baseline:

- terminate TLS at a trusted proxy or load balancer
- restrict `/internal/*` and `/metrics` with firewall/VPC rules even though they are auth-gated
- use dedicated service accounts and least-privilege filesystem ownership
- back up the data dir before upgrades and before risky maintenance
- keep trusted proxy CIDRs explicit instead of broadly trusting forwarded headers
- use `FLAPJACK_LOG_FORMAT=json` when central log collection is in place

## Not yet proven deeply enough

These areas remain outside the current OSS hardening proof bar:

- full OWASP-style deep pass
- formal pentest or hostile-network review
- SaaS/multi-tenant isolation review beyond the current targeted tests
- SSO, SCIM, audit-log, and enterprise IAM controls
- distributed tracing / full cross-node forensic tooling

## Scope boundary

Use this document for bounded security claims. Do not turn it into a wish list.
If a new hardening measure is only recommended or only planned, label it that
way instead of presenting it as shipped protection.
