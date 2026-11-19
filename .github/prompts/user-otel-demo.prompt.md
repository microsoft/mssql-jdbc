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

Treat the [connection error/span contract](../../docs/mssql-jdbc-connection-error-category-mapping.md),
[metrics proposal](../../docs/mssql-jdbc-performance-activity-metrics-proposal.md),
and [statement spans proposal](../../docs/mssql-jdbc-statement-spans-proposal.md)
as authoritative designs. Read the [current feature contract](../../docs/user-otel-endpoint.md)
for source-grounded behavior and remaining differences; do not edit the designs
or equate a demo pass with exhaustive proposal acceptance. Automatic `otelEndpoint`
selection owns a private SDK/transport rather than the older application-SDK design.

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
   metrics, and any missing evidence. Display SQL only from emitted statement-root
   masked `db.query.text` after the viewer's fail-closed check, never raw SQL or
   child/resource enrichment. Missing/rejected SQL stays unavailable; masking can
   retain identifiers, so review screenshots. Exact maximum remains unavailable
   through classic Prometheus histograms; region is shown only when emitted.
5. To generate more data, use `-Action Workload`; to recheck, use `-Action Verify`.
   Leave the demo running for inspection unless cleanup was requested. Stop only
   its manifest-owned project using `-Action Stop -RemoveVolumes` when requested.

Verify current metric labels, not the old two-dimension schema: four instruments
(count, duration, error count, root-only retry count), dotted
`mssql_performance_activity`, `mssql_operation_kind`, and schema `1.0`;
`mssql_operation_outcome` applies to count/duration, not error/retry. Use checked-in
queries for bounded type/protocol/authentication/retry breakdowns, outcome-based
success ratios, and independent activity means/percentiles. Old
`mssql_activity`/`mssql_activity_type` dashboards require migration. Do not treat
absent error/retry series as zero or demand retries from the baseline. Scope is
`com.microsoft.sqlserver.jdbc` with driver version, and duration has 17 explicit
boundary advice values from `0.0001` to `30` seconds. A valid unsampled parent
suppresses eligible trace trees, not metrics; root diagnostic replay remains
name/value allowlisted. Report missing evidence rather than inventing it.

Never read existing test/cloud credentials or repository database configuration.
Never print passwords, container environments, raw JDBC exceptions, or resolved
Compose configuration. Do not commit generated state, caches, secrets, or logs.
If execution fails, distinguish completed steps from unverified ones; preserve the
owned project for diagnosis and provide its scoped Stop command. Do not commit or
push repository changes as part of running this prompt.