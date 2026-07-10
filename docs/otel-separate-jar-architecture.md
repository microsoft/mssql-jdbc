# Transforming OTel POC to Separate JAR Architecture

**Date:** July 4, 2026  
**Status:** Design Guide  
**References:** `proposal-server-side-statistics-and-otel-export.md` (Solution 1: Optional OTel SDK Bridge)

---

## 1. Overview: From POC to Production JAR

The transformation moves OpenTelemetry support from the core driver into a **separate, optional companion JAR** (`mssql-jdbc-otel`). This design:

- **Keeps the core driver free of OTel dependencies**
- **Enables zero-friction opt-in via pom.xml** (like `azure-identity`)
- **Uses ServiceLoader SPI for automatic discovery**
- **Works with any OTel SDK already running in the application**

### Deployment Pattern

```
Developer's pom.xml
│
├─ <dependency>com.microsoft.sqlserver:mssql-jdbc:13.5.0</dependency>
│  └─ [No OTel deps; callback SPI exists]
│
└─ <dependency>com.microsoft.sqlserver:mssql-jdbc-otel:13.5.0</dependency>
   ├─ [Declares opentelemetry-api, -sdk, -exporter-otlp]
   └─ [Implements callback + SPI registration]
```

When `mssql-jdbc-otel` is on the classpath, the driver discovers and uses it automatically.

---

## 2. Multi-Module Maven Structure

### Current Single-Module Layout
```
mssql-jdbc/
├─ pom.xml                     (main POM)
├─ src/main/java/com/microsoft/sqlserver/jdbc/
│  ├─ PerformanceLogCallback.java        (callback interface)
│  ├─ PerformanceLog.java                (internal infrastructure)
│  └─ ...
├─ src/test/java/...
```

### Post-Transformation Multi-Module Layout
```
mssql-jdbc/
├─ pom.xml                     (parent/aggregator)
│
├─ mssql-jdbc-core/            (core driver, renamed from current)
│  ├─ pom.xml
│  ├─ src/main/java/com/microsoft/sqlserver/jdbc/
│  │  ├─ PerformanceLogCallback.java     (unchanged)
│  │  ├─ PerformanceLog.java             (+ SPI discovery logic)
│  │  ├─ spi/
│  │  │  └─ TelemetryBridgeFactory.java  (NEW: SPI interface)
│  │  └─ ...
│  ├─ src/main/resources/
│  │  └─ META-INF/services/              (already exists, still valid)
│  └─ src/test/java/...
│
├─ mssql-jdbc-otel/            (NEW: OTel bridge JAR)
│  ├─ pom.xml
│  ├─ src/main/java/com/microsoft/sqlserver/jdbc/otel/
│  │  ├─ OpenTelemetryPerformanceCallback.java
│  │  ├─ OTelTelemetryBridgeFactory.java (SPI implementation)
│  │  ├─ MetricInstruments.java
│  │  ├─ EventBatcher.java
│  │  └─ ...
│  ├─ src/main/resources/
│  │  └─ META-INF/services/
│  │     └─ com.microsoft.sqlserver.jdbc.spi.TelemetryBridgeFactory
│  │        (contains: com.microsoft.sqlserver.jdbc.otel.OTelTelemetryBridgeFactory)
│  └─ src/test/java/...
│
└─ mssql-jdbc-samples/         (sample projects showing usage)
```

---

## 3. Core Concepts: ServiceLoader SPI

### 3.1 SPI Interface (in Core Driver)

**File:** `src/main/java/com/microsoft/sqlserver/jdbc/spi/TelemetryBridgeFactory.java`

```java
package com.microsoft.sqlserver.jdbc.spi;

/**
 * Service Provider Interface for telemetry/observability bridges.
 * Implementations are discovered via ServiceLoader.
 */
public interface TelemetryBridgeFactory {
    /**
     * Create or retrieve a PerformanceLogCallback that bridges to a telemetry system.
     * Called once at driver initialization if otelEndpoint is set and an implementation is found.
     *
     * @param connectionString The full connection string (for reading OTel-specific properties)
     * @return A PerformanceLogCallback, or null if this factory cannot provide one
     */
    PerformanceLogCallback createCallback(String connectionString) throws Exception;
}
```

### 3.2 ServiceLoader Registration (in OTel JAR)

**File:** `mssql-jdbc-otel/src/main/resources/META-INF/services/com.microsoft.sqlserver.jdbc.spi.TelemetryBridgeFactory`

```
com.microsoft.sqlserver.jdbc.otel.OTelTelemetryBridgeFactory
```

### 3.3 Discovery in Core Driver

**File:** `src/main/java/com/microsoft/sqlserver/jdbc/PerformanceLog.java` (or new `TelemetryBridgeLoader.java`)

```java
package com.microsoft.sqlserver.jdbc;

import com.microsoft.sqlserver.jdbc.spi.TelemetryBridgeFactory;
import java.util.ServiceLoader;

class TelemetryBridgeLoader {
    static PerformanceLogCallback tryLoadOTelBridge(String connectionString) {
        try {
            ServiceLoader<TelemetryBridgeFactory> loader = 
                ServiceLoader.load(TelemetryBridgeFactory.class);
            
            for (TelemetryBridgeFactory factory : loader) {
                PerformanceLogCallback callback = factory.createCallback(connectionString);
                if (callback != null) {
                    SQLServerDriver.log("OTel bridge loaded: " + factory.getClass().getName());
                    return callback;
                }
            }
        } catch (Exception e) {
            SQLServerDriver.log("OTel bridge discovery failed: " + e);
            // Silently ignore; driver continues without OTel
        }
        return null;
    }
}
```

### 3.4 Core-to-OTel Bridge Contract

This is the contract we want to standardize for the split architecture:

- The core driver keeps ownership of the internal performance callback and the lifecycle of the event.
- The core driver creates a narrow `TelemetryEvent` payload that contains:
  - metric/span identifiers,
  - duration and exception context,
  - SQL text and statement type when available,
  - auth context such as the auth scheme and any relevant headers needed for correlation or propagation.
- The core driver calls a bridge method on the optional `mssql-jdbc-otel` implementation via a simple interface such as `TelemetryBridge.publish(TelemetryEvent)`.
- `mssql-jdbc-otel` owns the mapping of that payload into OpenTelemetry metrics/spans and chooses how to handle headers safely.

**Core contract (shape):**

```java
public interface TelemetryBridge {
    void publish(TelemetryEvent event) throws Exception;
}

public final class TelemetryEvent {
    private final PerformanceActivity activity;
    private final int connectionId;
    private final int statementId;
    private final long duration;
    private final Exception exception;
    private final String userSql;
    private final StatementType statementType;
    private final Map<String, String> authHeaders;
    private final String authScheme;
    private final String spanName;
    private final String traceParent;
    private final String correlationId;
}
```

This keeps the dependency direction explicit: `mssql-jdbc` → `mssql-jdbc-otel` for export, but without introducing any OTel types into the core driver.

---

## 4. Core Driver Changes (Minimal)

### 4.1 SPI Discovery Integration

**File:** `src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java` (during PRELOGIN/LOGIN)

```java
// In connection initialization, after parsing connection string:

if (connectionProps.containsKey("otelEndpoint")) {
    // Try to load OTel bridge via SPI
    PerformanceLogCallback otelCallback = 
        TelemetryBridgeLoader.tryLoadOTelBridge(connectionString);
    
    if (otelCallback != null) {
        SQLServerDriver.registerPerformanceLogCallback(otelCallback);
    } else {
        // Log warning: otelEndpoint set but no OTel bridge found
        log("Warning: otelEndpoint set but mssql-jdbc-otel JAR not on classpath");
    }
}
```

### 4.2 New Properties Remain in Core

Connection string properties stay in `SQLServerConnection.java` / `SQLServerDataSource.java`:

```
serverExecutionStatistics = {false, io, time, all}
otelEndpoint = <URL>
otelExportInterval = 60
otelBatchSize = 1000
otelSamplingRate = 0.5
otelServiceName = my-app
otelHeaders = Authorization=Bearer ...
otelResourceAttributes = region=us-east
```

The **core driver reads these but doesn't act on them** (except `serverExecutionStatistics`, which is TDS-based). The **OTel JAR reads them via connection object** after callback is instantiated.

---

## 5. OTel Bridge JAR Implementation

### 5.1 SPI Implementation

**File:** `mssql-jdbc-otel/src/main/java/com/microsoft/sqlserver/jdbc/otel/OTelTelemetryBridgeFactory.java`

```java
package com.microsoft.sqlserver.jdbc.otel;

import com.microsoft.sqlserver.jdbc.spi.TelemetryBridgeFactory;
import com.microsoft.sqlserver.jdbc.PerformanceLogCallback;

public class OTelTelemetryBridgeFactory implements TelemetryBridgeFactory {
    
    @Override
    public PerformanceLogCallback createCallback(String connectionString) throws Exception {
        // Parse connectionString for OTel properties
        // Initialize OpenTelemetry SDK (or retrieve GlobalOpenTelemetry if app already set it up)
        // Return OpenTelemetryPerformanceCallback instance
        return new OpenTelemetryPerformanceCallback(connectionString);
    }
}
```

### 5.2 Callback Implementation

**File:** `mssql-jdbc-otel/src/main/java/com/microsoft/sqlserver/jdbc/otel/OpenTelemetryPerformanceCallback.java`

```java
package com.microsoft.sqlserver.jdbc.otel;

import com.microsoft.sqlserver.jdbc.PerformanceLogCallback;
import com.microsoft.sqlserver.jdbc.PerformanceActivity;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;

public class OpenTelemetryPerformanceCallback implements PerformanceLogCallback {
    
    private final Meter meter;
    private final LongHistogram connectionLatency;
    private final LongHistogram statementLatency;
    // ... more instruments
    
    public OpenTelemetryPerformanceCallback(String connectionString) {
        // Try to use GlobalOpenTelemetry if app already configured it
        var otel = GlobalOpenTelemetry.get();
        
        if (otel == GlobalOpenTelemetry.noop()) {
            // App hasn't configured OTel; initialize our own exporter
            String endpoint = parseConnectionProperty(connectionString, "otelEndpoint");
            var exporter = OtlpHttpMetricExporter.builder()
                .setEndpoint(endpoint)
                .build();
            
            OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setMeterProvider(/* ... */)
                .build();
            otel = sdk;
        }
        
        this.meter = otel.getMeterProvider().get("com.microsoft.sqlserver.jdbc");
        this.connectionLatency = meter.histogramBuilder("db.connection.time")
            .setDescription("Connection latency (ms)")
            .ofLongs()
            .build();
        
        // ... initialize other instruments
    }
    
    @Override
    public void publish(PerformanceActivity activity, int connectionId, 
                       long duration, Exception exception) {
        // Record to appropriate OTel instrument
        // Add attributes: connectionId, activity type, exception (if any)
        connectionLatency.record(duration, /* attributes */);
    }
    
    @Override
    public void publish(PerformanceActivity activity, int connectionId, 
                       int statementId, long duration, Exception exception) {
        // Record statement-level metrics
        statementLatency.record(duration, /* attributes */);
    }
    
    @Override
    public boolean useNanoseconds() {
        return true; // Export high-precision metrics to OTel
    }
}
```

### 5.3 Dependencies (pom.xml)

**File:** `mssql-jdbc-otel/pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project>
    <modelVersion>4.0.0</modelVersion>
    
    <groupId>com.microsoft.sqlserver</groupId>
    <artifactId>mssql-jdbc-otel</artifactId>
    <version>13.5.0</version>
    <packaging>jar</packaging>
    <name>Microsoft JDBC Driver for SQL Server — OpenTelemetry Bridge</name>
    
    <dependencies>
        <!-- Core driver (compile-time dependency for SPI interfaces) -->
        <dependency>
            <groupId>com.microsoft.sqlserver</groupId>
            <artifactId>mssql-jdbc-core</artifactId>
            <version>13.5.0</version>
            <scope>provided</scope>  <!-- provided: caller must bring it -->
        </dependency>
        
        <!-- OpenTelemetry API (required) -->
        <dependency>
            <groupId>io.opentelemetry</groupId>
            <artifactId>opentelemetry-api</artifactId>
            <version>1.36.0</version>
        </dependency>
        
        <!-- OpenTelemetry SDK (required for standalone use) -->
        <dependency>
            <groupId>io.opentelemetry</groupId>
            <artifactId>opentelemetry-sdk</artifactId>
            <version>1.36.0</version>
        </dependency>
        
        <!-- OTLP/HTTP Exporter (required) -->
        <dependency>
            <groupId>io.opentelemetry</groupId>
            <artifactId>opentelemetry-exporter-otlp-http-metrics</artifactId>
            <version>1.36.0</version>
        </dependency>
        
        <!-- Protobuf for OTLP (transitive) -->
        <!-- (pulls com.google.protobuf:protobuf-java) -->
    </dependencies>
</project>
```

---

## 6. Developer Usage

### 6.1 pom.xml (with OTel Support)

```xml
<dependencies>
    <!-- JDBC Driver (core, no OTel) -->
    <dependency>
        <groupId>com.microsoft.sqlserver</groupId>
        <artifactId>mssql-jdbc</artifactId>
        <version>13.5.0</version>
    </dependency>
    
    <!-- Optional: OTel bridge (includes OTel SDK + exporter) -->
    <dependency>
        <groupId>com.microsoft.sqlserver</groupId>
        <artifactId>mssql-jdbc-otel</artifactId>
        <version>13.5.0</version>
    </dependency>
</dependencies>
```

### 6.2 Connection String

```
jdbc:sqlserver://server.database.windows.net:1433;
  database=mydb;
  authentication=ActiveDirectoryDefault;
  serverExecutionStatistics=all;
  otelEndpoint=http://localhost:4317/v1/metrics;
  otelExportInterval=60;
  otelBatchSize=1000;
  otelServiceName=my-app
```

### 6.3 Programmatic Connection (No Code Changes)

```java
// If app already configured OTel globally:
OpenTelemetrySdk.builder()
    .setMeterProvider(/* ... */)
    .build();

// Then when JDBC connection opens:
var url = "jdbc:sqlserver://...;otelEndpoint=http://...";
var conn = DriverManager.getConnection(url, "user", "pass");
// ^ OTel bridge auto-discovers and registers callback
```

---

## 7. Build & Release

### 7.1 Parent pom.xml

```xml
<project>
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.microsoft.sqlserver</groupId>
    <artifactId>mssql-jdbc-parent</artifactId>
    <packaging>pom</packaging>
    <version>13.5.0</version>
    
    <modules>
        <module>mssql-jdbc-core</module>
        <module>mssql-jdbc-otel</module>
    </modules>
</project>
```

### 7.2 Release Artifacts

```
Maven Central:
├─ com.microsoft.sqlserver:mssql-jdbc:13.5.0.jar         (core only, 4 MB)
├─ com.microsoft.sqlserver:mssql-jdbc-otel:13.5.0.jar    (bridge + OTel SDK, 12 MB)
├─ com.microsoft.sqlserver:mssql-jdbc:13.5.0-javadoc.jar
├─ com.microsoft.sqlserver:mssql-jdbc-otel:13.5.0-javadoc.jar
└─ ... sources, etc.
```

---

## 8. Deployment Scenarios

### Scenario A: Developer with OTel Already Configured

**Minimal changes:**
1. Add `mssql-jdbc-otel` to pom.xml
2. Set connection string properties (`otelEndpoint`, etc.)
3. JDBC driver discovers bridge via ServiceLoader
4. Bridge reuses app's GlobalOpenTelemetry instance

### Scenario B: Developer with No OTel Setup

**Steps:**
1. Add `mssql-jdbc-otel` to pom.xml (brings SDK + exporter)
2. Set connection string properties
3. Bridge initializes its own OpenTelemetry SDK on first connection
4. Metrics flow to OTLP endpoint

### Scenario C: Developer Without OTel (Stays Unchanged)

**No action:**
1. Only `mssql-jdbc` in pom.xml (core driver)
2. PerformanceLogCallback remains available for programmatic use
3. No OTel SDK overhead

---

## 9. Transition from POC

### POC State (Assumptions)

The POC likely has:
- OTel classes mixed into core driver package
- Connection string property parsing
- Callback implementation registering itself
- Possibly bundled OTel dependencies or shading

### Transformation Steps

1. **Extract SPI interface:** Create `TelemetryBridgeFactory` in core driver
   
2. **Create new module:** `mssql-jdbc-otel/`
   
3. **Move callback:** `OpenTelemetryPerformanceCallback` → new module
   
4. **Add ServiceLoader registration:** `META-INF/services/TelemetryBridgeFactory`
   
5. **Implement factory:** `OTelTelemetryBridgeFactory` wraps callback creation
   
6. **Update core driver:** Add `TelemetryBridgeLoader` discovery logic; remove OTel classes
   
7. **Update pom.xml:** Declare optional dependencies, add new module build
   
8. **Update README:** Show new usage pattern (`mssql-jdbc-otel` in pom.xml)
   
9. **Update tests:** Separate core driver tests from OTel bridge tests

---

## 10. Comparison: Before vs. After

| Aspect | POC (Embedded) | Post-Transform (Separate JAR) |
|--------|---|---|
| **Core JAR size** | +12 MB (OTel SDK) | -12 MB (lightweight) |
| **OTel dependency** | Mandatory in core | Optional bridge JAR only |
| **Developer pom.xml** | Single `mssql-jdbc` coordinate | `mssql-jdbc` + optional `mssql-jdbc-otel` |
| **Discovery** | Hard-coded or config-based | ServiceLoader SPI |
| **Multi-OTel support** | Single callback instance | Works with app's GlobalOpenTelemetry |
| **Backward compat** | Depends on POC | ✅ Full (core driver unchanged) |
| **Shading** | May be needed | Not needed (separate JAR) |

---

## 11. Testing Strategy

### 11.1 Core Driver Tests (No OTel)

**Location:** `mssql-jdbc-core/src/test/java/com/microsoft/sqlserver/jdbc/`

- Callback interface tests (remain unchanged)
- SPI discovery tests (with mock factory)
- Connection tests (verify no OTel when JAR absent)

### 11.2 OTel Bridge Tests

**Location:** `mssql-jdbc-otel/src/test/java/com/microsoft/sqlserver/jdbc/otel/`

- OTel metric export tests
- Callback implementation tests
- Factory instantiation tests
- End-to-end: connection → metrics → OTLP endpoint

### 11.3 Integration Tests

**Location:** `mssql-jdbc-otel/src/test/java/.../integration/`

- Core driver + OTel JAR together
- Verify discovery and callback registration
- Verify metrics flow to mock OTLP collector

---

## 12. Documentation Updates

### 12.1 README.md

Add section:

```markdown
#### Optional: OpenTelemetry Support

To export performance metrics to an OpenTelemetry-compatible backend:

1. Add the OTel bridge to your pom.xml:
   ```xml
   <dependency>
       <groupId>com.microsoft.sqlserver</groupId>
       <artifactId>mssql-jdbc-otel</artifactId>
       <version>13.5.0</version>
   </dependency>
   ```

2. Add connection properties:
   ```
   jdbc:sqlserver://server:1433;database=mydb;otelEndpoint=http://localhost:4317/v1/metrics
   ```

The bridge automatically discovers and registers itself when on the classpath.
```

### 12.2 New Document: `docs/otel-integration-guide.md`

Full guide to OTel setup, metrics, attributes, advanced configuration.

---

## 13. Advantages of This Approach

✅ **Zero dependencies in core driver** — stays lightweight  
✅ **Optional add-on** — developers opt-in via pom.xml  
✅ **Familiar pattern** — mirrors `azure-identity`, `azure-keyvault` model  
✅ **ServiceLoader SPI** — loose coupling, discoverable at runtime  
✅ **Works with app's OTel config** — respects GlobalOpenTelemetry  
✅ **No shading needed** — separate JAR avoids version conflicts  
✅ **Backward compatible** — existing code unaffected  
✅ **Testable** — clear boundary for unit/integration tests  

---

## 14. Future Extensibility

The SPI design allows for multiple implementations:

- **Alternative exporters:** E.g., `mssql-jdbc-prometheus`, `mssql-jdbc-datadog`
- **Custom bridges:** Teams can implement `TelemetryBridgeFactory` for proprietary systems
- **Metrics aggregation:** Future JAR that batches metrics before export

```
// Hypothetical future extension:
<dependency>
    <groupId>com.microsoft.sqlserver</groupId>
    <artifactId>mssql-jdbc-prometheus</artifactId>
    <version>13.5.0</version>
</dependency>
```

---

## 15. Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| **OTel JAR not on classpath** | Driver logs warning; continues without OTel |
| **Multiple SPI implementations** | ServiceLoader returns first match; first impl takes precedence |
| **OTel version conflicts** | Developers manage OTel versions in their pom.xml; bridge declares ranges |
| **Silent failures** | Bridge constructor throws exception if initialization fails; caught and logged |
| **Permission issues** | OTLP export may fail in sandboxed environment; exception logged, driver continues |

---

## Conclusion

This transformation achieves **Solution 1** (Optional OTel SDK Bridge) from the proposal while maintaining **core driver simplicity** and **backward compatibility**. The SPI-based discovery pattern is proven in the Java ecosystem and aligns with existing Azure SDK patterns already used in the JDBC driver.
