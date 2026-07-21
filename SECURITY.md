# Security policy

## Supported versions

RunMesh has not published a stable release. Security fixes are currently made
on the `main` branch. A version support table will be added when the first
release is published.

## Reporting a vulnerability

Please do not disclose a suspected vulnerability in a public issue, discussion,
or pull request.

GitHub private vulnerability reporting is not currently enabled for this
repository. To request a private reporting channel, open a minimal issue that
contains only a request to contact the maintainer and no vulnerability details.
The maintainer will arrange a private channel and acknowledge the report. This
file will be updated with a direct private-reporting link when that repository
feature is enabled.

Include the affected component and version or commit, reproduction conditions,
security impact, and any suggested mitigation in the private report. Please
allow reasonable time for investigation and remediation before disclosure.

## Local development credentials

The credentials committed in `compose.yaml`, `deploy/docker/local.env`, the
Keycloak realm fixture, examples, and test fixtures are intentionally public,
development-only values. They secure nothing outside the disposable local
Compose environment and must never be reused in a shared or production system.

Production mode rejects development authentication. Deployments must provide
unique secrets outside the repository for the database, Redis, S3, worker API
keys, and the API-key pepper, and must configure a trusted OIDC issuer and
audience. See [`docs/security.md`](docs/security.md) and
[`docs/deployment.md`](docs/deployment.md).

## Scope

Useful reports include authentication or authorization bypasses, cross-tenant
data access, worker lease-fencing failures, secret exposure, unsafe artifact
access, injection, denial-of-service paths, and vulnerable deployment defaults.
Reports about the documented public local-development credentials are not
vulnerabilities unless those credentials are accepted when development
authentication is disabled.
