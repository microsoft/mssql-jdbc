# OpenTelemetry UX proposal for the JDBC driver

## Overview

This document describes the intended user experience for enabling OpenTelemetry export from the Microsoft JDBC Driver for SQL Server through an optional companion artifact.

The goal is to make the feature simple to adopt while preserving a lightweight driver core and allowing different deployment environments to choose the most appropriate telemetry path.

## Proposed experience

Applications that want to emit driver telemetry would:

1. Upgrade to a driver version that includes the telemetry bridge support.
2. Add a new optional dependency on the companion artifact:
   - `mssql-jdbc-opentelemetry`
3. Configure the behavior through new connection string parameters.

## New connection string parameters

### Summary of connection string options

| Property | Applies to | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `otelProfile` | All profiles | No (opt-in) | *(unset — telemetry disabled)* | Selects the telemetry delivery profile. One of `ARC`, `SQLDB`, or `CUSTOM`. |
| `otelAuth` | `SQLDB` | No | `DEFAULT` (`DefaultAzureCredential`) | Selects the Azure authentication mechanism used to acquire a token for the telemetry endpoint. |
| `otelEndpoint` | `CUSTOM` | Yes (for `CUSTOM`) | *(none)* | The OpenTelemetry endpoint to which telemetry is sent. For `ARC` and `SQLDB` the endpoint is discovered automatically. |
| `otelAccessTokenCallbackClass` | `CUSTOM` | No | *(none)* | Fully-qualified class name of a callback that generates an authentication token for the custom endpoint. Omit it for a no-authentication endpoint. |

Notes:

- Telemetry is fully opt-in: if `otelProfile` is not set, the driver emits no telemetry and the companion artifact is not required.
- `otelAuth` is only meaningful with `otelProfile=SQLDB`; it is ignored by the `ARC` and `CUSTOM` profiles.
- `otelEndpoint` and `otelAccessTokenCallbackClass` are only used by `otelProfile=CUSTOM`; they are ignored (and discovered/derived internally) by the `ARC` and `SQLDB` profiles.
- `otelAccessTokenCallbackClass` is optional even for `CUSTOM`: if omitted, the driver sends telemetry to `otelEndpoint` without an authorization token, which supports no-authentication collectors.

### `otelProfile`

Controls which telemetry delivery profile is used.

Supported values:

- `ARC`
- `SQLDB`
- `CUSTOM`

### `otelProfile=ARC`

Use this profile when the application is connecting to SQL Server and Azure Arc is enabled for that environment, with the Arc agent already configured to receive telemetry from the application.

Behavior:

- The driver sends telemetry to the OpenTelemetry collector endpoint exposed by the Arc agent on the predefined local port.
- No additional authentication is required from the application.
- The Arc agent is responsible for forwarding the telemetry to the downstream destination such as Azure / OneLake.

This is the simplest deployment path for environments that already rely on Arc-based telemetry collection.

### `otelProfile=SQLDB`

Use this profile when the application is connecting to Azure SQL Database and telemetry should be exported toward that SQLDB-oriented destination. In this profile, the driver deduces the telemetry endpoint and additional authorization-related details such as the ARM ID during its server-side feature negotiation flow, so the application does not need to provide `otelEndpoint` explicitly.

Additional option:

- `otelAuth`

`otelAuth` selects the authentication mechanism used to acquire a token for the telemetry endpoint. The value is case-insensitive and should map to a specific Azure authentication flow.

Supported values (exact proposal):

- `DEFAULT` (or omitted) — use `DefaultAzureCredential` and let the environment discover the best available credential.
- `SERVICEPRINCIPAL` — use a service principal credential.
- `MANAGEDIDENTITY` — use a managed identity credential.
- `ENVIRONMENT` — use an environment-based credential.
- `AZURECLI` — use the Azure CLI credential.
- `AZUREPOWERSHELL` — use the Azure PowerShell credential.
- `AZUREDEVELOPERCLI` — use the Azure Developer CLI credential.
- `WORKLOADIDENTITY` — use a workload identity credential.
- `SHAREDTOKENCACHE` — use a shared token cache credential.
- `VISUALSTUDIOCODE` — use the Visual Studio Code credential.
- `INTERACTIVEBROWSER` — use an interactive browser credential.

If `otelAuth` is not supplied, the driver should default to `DefaultAzureCredential` and allow the platform to discover the best available credential.

Behavior:

- The driver acquires a token using the selected authentication mechanism.
- The token is attached as an authorization header on the telemetry message.
- This approach is intended for applications deployed in the same subscription as the Azure SQL Database target.

### Additional SQLDB considerations

For applications running in a different subscription, or for applications running outside Azure (for example on a developer laptop), additional RBAC configuration may be required.

In those cases, the identity used by the application must be granted the appropriate Azure role for the target telemetry destination. For example, a custom role such as `SQLDBOpenTelemetryRole` may be required to allow the identity to publish telemetry data.

Without that permission, telemetry export may fail even if the application can acquire a token successfully.

### `otelProfile=CUSTOM`

Use this profile when the application wants to provide its own telemetry destination and authentication strategy.

Options:

- `otelEndpoint` (required)
- `otelAccessTokenCallbackClass` (optional)

Behavior:

- `otelEndpoint` specifies the OpenTelemetry endpoint to which telemetry should be sent.
- `otelAccessTokenCallbackClass` provides a callback that generates an authentication token for the endpoint. It is optional: if omitted, telemetry is sent to `otelEndpoint` without an authorization token, which supports collectors or gateways that do not require authentication.

This profile is intended for bring-your-own-telemetry scenarios where the application already has a target collector or gateway and a custom authorization mechanism.

## Proposed telemetry signals

The POC already uses the following OTel-style signal names for the driver. These are the names we should preserve in the UX language and in any future implementation mapping. For each event, the driver should publish both a span and a duration metric using the same base name.

| OTel-style name | Type | Description |
| --- | --- | --- |
| `mssql.jdbc.connection.prelogin` | Span + metric | Measures the TDS prelogin negotiation time during connection setup. |
| `mssql.jdbc.connection.login` | Span + metric | Measures the authentication and login handshake time during connection setup. |
| `mssql.jdbc.connection.token.acquisition` | Span + metric | Measures the time spent acquiring an Azure AD access token for federated authentication flows. |
| `mssql.jdbc.statement.request.build` | Span + metric | Measures the client-side time spent building the TDS request before it is sent to the server. |
| `mssql.jdbc.statement.first.server.response` | Span + metric | Measures the time from sending the request to receiving the first server response. |
| `mssql.jdbc.statement.prepare` | Span + metric | Measures the time spent preparing a statement using `sp_prepare`. |
| `mssql.jdbc.statement.prepexec` | Span + metric | Measures the combined prepare-and-execute time for `sp_prepexec`. |
| `mssql.jdbc.statement.execute` | Span + metric | Measures the full statement execution time for a SQL command. |
| `mssql.jdbc.useragent` | Attribute / metadata | Captures the JDBC driver user-agent string generated when the driver is loaded. |

## Reliability and failure handling

Telemetry emission is strictly **fire-and-forget**. Observability is a side channel and must never affect the correctness or availability of database operations.

Guarantees:

- **Telemetry failures never propagate to the application.** When the driver publishes a telemetry event, the call to the telemetry bridge is wrapped so that any exception thrown while building or publishing the event is caught internally. The failure is logged at `FINE` on the driver's telemetry logger and then swallowed — it is never rethrown into the connection, statement, or result set code path.
- **No impact on query results or timing semantics.** Emission happens at the close of an already-measured performance scope, after the driver has finished the work being measured. A slow or failing bridge cannot corrupt or delay the actual JDBC operation result.
- **Missing companion artifact is non-fatal.** If the `mssql-jdbc-opentelemetry` companion is not on the classpath, the driver logs a single informational message indicating the optional jar is absent and then continues normally with telemetry publishing skipped. The driver does not fail to load, connect, or execute.
- **No thread-local or context leakage on failure.** Any per-call context the driver stages for the bridge is always cleared in a `finally` block, even when publishing throws, so a telemetry failure cannot leak state into subsequent operations.

Net effect: enabling telemetry can add observability, but it cannot introduce new failure modes into the application's data path. If telemetry cannot be emitted for any reason, the driver behaves exactly as it would with telemetry disabled.

## Adoption model

To adopt this feature, an application would:

1. Upgrade the JDBC driver to the new version that supports the bridge.
2. Add the companion dependency:

```xml
<dependency>
  <groupId>com.microsoft.sqlserver</groupId>
  <artifactId>mssql-jdbc-opentelemetry</artifactId>
  <version>1.0.0</version>
</dependency>
```

3. Configure the relevant connection string parameters for the chosen profile.

## UX goals

This design aims to provide:

- a simple opt-in model
- a lightweight core driver
- a clear separation between the driver and the observability implementation
- support for common Azure deployment patterns
- a flexible custom path for advanced scenarios

## Open questions

The following items may need further refinement:

- exact naming of the new artifact and package namespace
- the final set of supported `otelAuth` values
- whether role names and RBAC guidance should be documented in the driver docs or in Azure-specific guidance
- the OTel signal names are currently scoped as JDBC-specific (`mssql.jdbc.*`). We should decide whether they should be made generic across all Microsoft SQL drivers instead of per-driver names, so that telemetry from different language drivers can be correlated under a shared naming convention.
