---
name: user-otel-demo
description: Build and run the complete local JDBC OpenTelemetry demo with SQL Server and the live diagnostics dashboard
agent: agent
---

# Run the local user-endpoint OpenTelemetry demo

Read [the executable demo guide](../../docs/otel-poc/user-endpoint-demo/README.md)
and the help in [the runner](../../docs/otel-poc/user-endpoint-demo/run.ps1).
Use the checked-in runner and assets; do not reconstruct files from the older
manual runbook, scaffold another project, or modify driver sources.

1. Check native x64 hardware, local Docker Linux containers and Compose v2,
   PowerShell 7+, JDK 21, Maven, available memory/disk, and the guide's host ports.
   Do not silently install prerequisites, change global configuration, stop an
   existing service, or use a remote Docker context. Resolve blockers with the user.
2. Before starting SQL Server, obtain explicit acceptance of the SQL Server
   Developer container license and permission to pull/build the historical local
   demo images. Explain that this is an unauthenticated, loopback-only test stack.
3. Run the runner with `-Action Start -AcceptSqlServerLicense` from the checkout.
   Start builds the packaged core/adapter, compiles the workload, builds the UI,
   starts the dedicated local SQL and monitoring stack, generates real telemetry,
   and verifies it downstream. Allow several minutes. Do not treat process startup,
   a successful flush, or healthy containers alone as success.
4. Open the dashboard URL reported by the runner. Report verified connection and
   statement successes/failures, prepared statements, error-origin events, slow SQL,
   metrics, and any missing evidence. SQL text, exact maxima, and unreported regions
   must remain unavailable, never replaced with mock values.
5. To generate more data, use `-Action Workload`; to recheck, use `-Action Verify`.
   Leave the demo running for inspection unless cleanup was requested. Stop only
   its manifest-owned project using `-Action Stop -RemoveVolumes` when requested.

Never read existing test/cloud credentials or repository database configuration.
Never print passwords, container environments, raw JDBC exceptions, or resolved
Compose configuration. Do not commit generated state, caches, secrets, or logs.
If execution fails, distinguish completed steps from unverified ones; preserve the
owned project for diagnosis and provide its scoped Stop command. Do not commit or
push repository changes as part of running this prompt.