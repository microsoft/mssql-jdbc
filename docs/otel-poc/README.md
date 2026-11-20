# User-supplied OpenTelemetry endpoint

The feature exports JDBC telemetry to an endpoint supplied by the application:

**JDBC driver + optional OTel JAR → application-supplied OTLP/HTTP endpoint**

- [Feature configuration and contract](../user-otel-endpoint.md): `otelEndpoint`,
  optional application-provided `otelAccessTokenCallback`, export levels, and
  connection/statement duration thresholds.
- [Complete local demo](user-endpoint-demo/README.md): run `/user-otel-demo` in
  Copilot Agent mode, or use the checked-in PowerShell runner.
- [Exploratory verification](user-endpoint-demo/README.md#exploratory-levels-and-thresholds):
  level/threshold expectations and evidence boundaries alongside the same runner.

The feature does not discover a monitoring endpoint or acquire credentials for the
application. If the endpoint needs a bearer token, the application provides the
callback. No cloud account, subscription, or Microsoft ingestion service is needed.

The local demo supplies a test SQL Server, standard Collector, Prometheus, Jaeger,
Grafana, and a live dashboard. These are **demo infrastructure**, not dependencies
applications must deploy to use the feature. The only shared build helper here is
the public Maven [settings](settings.xml); the live dashboard source is under
[internal/fdh-ui](internal/fdh-ui/package.json).