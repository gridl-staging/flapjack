# Security Policy

## Supported Versions

Only the current release line is supported for security fixes.

| Release Line | Supported |
| --- | --- |
| Current release line | Yes |
| Older release lines | No |

## Reporting a Vulnerability

Report security issues by email to `security@flapjack.foo`.

Please include:

- A clear description of the issue and impact.
- Steps to reproduce, proof-of-concept details, or logs.
- Affected version/build and deployment context.
- Any suggested remediation if available.

## Response Targets

- Acknowledge new reports within 48 hours.
- Provide regular status updates during triage and remediation.
- Target a fix or mitigation for critical vulnerabilities within 90 days.

## In Scope

- Core search engine (`engine/src/`).
- HTTP/API layer and request handling.
- Authentication and authorization behavior.
- Replication and data synchronization logic.

## Out of Scope

- Dashboard cosmetic-only issues with no security impact.

## Technical Hardening Details

For implementation-level hardening controls, see:

- [Security baseline](engine/docs2/3_IMPLEMENTATION/SECURITY_BASELINE.md)
